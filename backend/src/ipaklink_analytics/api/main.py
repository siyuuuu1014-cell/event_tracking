from asyncio import timeout
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import monotonic
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.write_concern import WriteConcern
from psycopg import Error
from psycopg_pool import AsyncConnectionPool
from psycopg_pool import PoolTimeout
from redis.asyncio import Redis
from redis.exceptions import RedisError

from ipaklink_analytics.api.collection import RequestProblem, reply, router as collection_router
from ipaklink_analytics.api.admin_grants import router as admin_grants_router
from ipaklink_analytics.api.admin_login import router as admin_login_router
from ipaklink_analytics.api.config_admin import router as config_admin_router
from ipaklink_analytics.api.reporting import router as reporting_router
from ipaklink_analytics.api.event_reports import router as event_reports_router
from ipaklink_analytics.api.jobs import router as jobs_router
from ipaklink_analytics.api.quality import router as quality_router
from ipaklink_analytics.api.corpus import router as corpus_router
from ipaklink_analytics.application.collection import get_collection_config
from ipaklink_analytics.application.ingest import IngestService
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.identity import AuthFailure, IdentityService, IdentityUnavailable
from ipaklink_analytics.infrastructure.admin_access import AccessDenied, AdminAccess
from ipaklink_analytics.infrastructure.admin_sessions import AdminSessions
from ipaklink_analytics.infrastructure.kafka import KafkaPublisher
from ipaklink_analytics.infrastructure.logging import get_logger
from ipaklink_analytics.infrastructure.postgres.ingest import IngestRepository
from ipaklink_analytics.infrastructure.postgres.policy import PostgresPolicyRepository
from ipaklink_analytics.infrastructure.quality_telemetry import record_collection
from ipaklink_analytics.infrastructure.rate_limit import RateLimiter
from ipaklink_analytics.infrastructure.readiness import Readiness
from ipaklink_analytics.infrastructure.report_store import ReportStore, SnapshotUnavailable, WarehouseQueryError
from ipaklink_analytics.ports.policy import PolicyRepository, PolicyUnavailable


def envelope(code: int, message: str, data: object = None) -> dict[str, object]:
    return {"code": code, "message": message, "data": data}


def create_app(
    settings: Settings | None = None, policy_repository: PolicyRepository | None = None
) -> FastAPI:
    config = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.logger = get_logger(config.log_dir)
        pool = AsyncConnectionPool(
            config.postgres_dsn.get_secret_value(),
            open=False,
            min_size=0,
            max_size=8,
            timeout=config.health_timeout_seconds,
        )
        redis = Redis.from_url(
            config.redis_url.get_secret_value(),
            socket_timeout=config.health_timeout_seconds,
            socket_connect_timeout=config.health_timeout_seconds,
        )
        await pool.open()
        app.state.readiness = Readiness(config, pool, redis)
        app.state.policy_repository = policy_repository or PostgresPolicyRepository(pool)
        app.state.identity = IdentityService(config)
        app.state.rate_limiter = RateLimiter(redis, config.environment)
        publisher = KafkaPublisher(config.kafka_bootstrap_servers)
        app.state.ingest = IngestService(IngestRepository(pool), publisher, app.state.identity)
        app.state.admin_settings = config
        app.state.admin_sessions = AdminSessions(pool, redis, config.environment)
        app.state.admin_access = AdminAccess(pool)
        app.state.reports = ReportStore(config, pool, redis)
        app.state.pg = pool
        mongo = MongoClient(config.mongo_uri.get_secret_value(),uuidRepresentation="standard",
                            tz_aware=True,serverSelectionTimeoutMS=3000,timeoutMS=7000)
        app.state.mongo = mongo.get_database(config.mongo_database,write_concern=WriteConcern("majority"))
        try:
            yield
        finally:
            await publisher.close()
            await redis.aclose()
            await pool.close()
            mongo.close()

    app = FastAPI(title="ipaklink Event Tracking", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def record_request(request: Request, call_next):
        started = monotonic()
        request_id = str(uuid4())
        request.state.request_id = request_id
        try:
            response = await call_next(request)
        except Exception as error:
            request.app.state.logger.error(
                "request.failed", extra={"requestId": request_id, "errorType": type(error).__name__}
            )
            response = JSONResponse(envelope(500, "analytics.internalError"), status_code=500)
        response.headers["X-Request-Id"] = request_id
        route = request.scope.get("route")
        if getattr(route, "path", None) == "/ipaklink/v1/analytics/events/report":
            platform, version = getattr(request.state,"quality_dimensions",(None,None))
            counts = getattr(request.state,"quality_counts",{})
            try:
                async with timeout(0.5):
                    await record_collection(request.app.state.pg,request_id,platform,version,counts,response.status_code==429)
            except (Error, PoolTimeout, TimeoutError) as error:
                request.app.state.logger.warning("quality.collectionUnavailable",extra={"errorType":type(error).__name__})
        request.app.state.logger.info(
            "request.completed",
            extra={
                "requestId": request_id,
                "route": getattr(route, "path", "unmatched"),
                "method": request.method,
                "resultCode": response.status_code,
                "durationMs": round((monotonic() - started) * 1000, 2),
            },
        )
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_: Request, __: RequestValidationError) -> JSONResponse:
        return JSONResponse(envelope(400, "analytics.invalidRequest"), status_code=400)

    @app.exception_handler(RequestProblem)
    async def request_problem(_: Request, error: RequestProblem) -> JSONResponse:
        return reply(error.code, error.message, headers=error.headers)

    @app.exception_handler(AuthFailure)
    async def invalid_credentials(_: Request, __: AuthFailure) -> JSONResponse:
        return reply(401, "analytics.unauthenticated", headers={"WWW-Authenticate": "Bearer"})

    async def dependency_unavailable(request: Request, error: Exception) -> JSONResponse:
        request.app.state.logger.warning(
            "dependency.unavailable",
            extra={"requestId": request.state.request_id, "errorType": type(error).__name__},
        )
        return reply(503, "analytics.temporarilyUnavailable")

    @app.exception_handler(AccessDenied)
    async def forbidden(_: Request, __: AccessDenied) -> JSONResponse:
        return reply(403, "analytics.permissionDenied")

    @app.exception_handler(SnapshotUnavailable)
    async def snapshot_unavailable(_: Request, __: SnapshotUnavailable) -> JSONResponse:
        return reply(409, "analytics.snapshotExpired")

    for error_class in (IdentityUnavailable, PolicyUnavailable, RedisError, PoolTimeout, Error, WarehouseQueryError, PyMongoError):
        app.add_exception_handler(error_class, dependency_unavailable)
    app.include_router(collection_router)
    app.include_router(admin_login_router)
    app.include_router(admin_grants_router)
    app.include_router(reporting_router)
    app.include_router(event_reports_router)
    app.include_router(jobs_router)
    app.include_router(quality_router)
    app.include_router(corpus_router)
    app.include_router(config_admin_router)

    @app.get("/health/live", tags=["health"])
    async def live() -> dict[str, object]:
        return envelope(200, "ok", {"status": "alive"})

    @app.get("/health/ready", tags=["health"])
    async def ready(request: Request) -> JSONResponse:
        dependencies = await request.app.state.readiness.check()
        healthy = all(value == "ready" for value in dependencies.values())
        code = 200 if healthy else 503
        return JSONResponse(
            envelope(
                code,
                "ok" if healthy else "analytics.temporarilyUnavailable",
                {"status": "ready" if healthy else "unavailable", "dependencies": dependencies},
            ),
            status_code=code,
        )

    @app.get(
        "/ipaklink/v1/analytics/config",
        operation_id="IPK_ANL_01",
        tags=["collection"],
        responses={400: {"description": "Invalid parameters"}, 503: {"description": "Policy unavailable"}},
    )
    async def collection_config(
        request: Request,
        response: Response,
        platform: Literal["ios", "android"],
        appVersion: Annotated[str, Query(min_length=1, max_length=40)],
        installId: UUID,
        authorization: Annotated[str | None, Header()] = None,
    ):
        response.headers["Cache-Control"] = "private, no-store"
        if authorization is not None:
            request.app.state.identity.validate_app(authorization)
        try:
            data = await get_collection_config(
                request.app.state.policy_repository, platform, appVersion, installId
            )
        except PolicyUnavailable:
            return JSONResponse(
                envelope(503, "analytics.temporarilyUnavailable"),
                status_code=503,
                headers={"Cache-Control": "private, no-store"},
            )
        return envelope(200, "ok", data)

    return app


app = create_app()

