from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import monotonic
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from ipaklink_analytics.application.collection import get_collection_config
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.logging import get_logger
from ipaklink_analytics.infrastructure.postgres.policy import PostgresPolicyRepository
from ipaklink_analytics.infrastructure.readiness import Readiness
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
        try:
            yield
        finally:
            await redis.aclose()
            await pool.close()

    app = FastAPI(title="ipaklink Event Tracking", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def record_request(request: Request, call_next):
        started = monotonic()
        request_id = str(uuid4())
        try:
            response = await call_next(request)
        except Exception as error:
            request.app.state.logger.error(
                "request.failed", extra={"requestId": request_id, "errorType": type(error).__name__}
            )
            response = JSONResponse(envelope(500, "analytics.internalError"), status_code=500)
        response.headers["X-Request-Id"] = request_id
        route = request.scope.get("route")
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
            # Access-token verifier is wired when the identity integration is implemented.
            return JSONResponse(
                envelope(503, "analytics.temporarilyUnavailable"),
                status_code=503,
                headers={"Cache-Control": "private, no-store"},
            )
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

