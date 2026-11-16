import json
import math
import time
import zlib
from collections import Counter
from typing import Annotated

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from ipaklink_analytics.domain.ingest import Batch, Device

MAX_BYTES = 262144
router = APIRouter(prefix="/ipaklink/v1/analytics", tags=["collection"])


class RequestProblem(Exception):
    def __init__(self, code: int, message: str, headers: dict[str, str] | None = None) -> None:
        self.code = code
        self.message = message
        self.headers = headers


def reply(code: int, message: str, data=None, headers=None) -> JSONResponse:
    return JSONResponse({"code": code, "message": message, "data": data}, code, headers=headers)


async def take(request: Request, key: str, burst: int, per_minute: int, cost: int = 1) -> None:
    if cost > burst:
        raise RequestProblem(413, "analytics.batchTooLarge")
    wait = await request.app.state.rate_limiter.take(key, burst, per_minute, cost)
    if wait:
        raise RequestProblem(429, "analytics.rateLimited", {"Retry-After": str(wait)})


def client_key(request: Request) -> str:
    host = request.client.host if request.client else "unknown"
    return request.app.state.identity.opaque("ip", host)


async def read_json(request: Request) -> tuple[object, int]:
    encoding = request.headers.get("content-encoding", "identity").lower().strip()
    if encoding not in ("identity", "gzip"):
        raise RequestProblem(400, "analytics.invalidRequest")
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS) if encoding == "gzip" else None
    body = bytearray()
    wire_bytes = 0
    try:
        async for chunk in request.stream():
            wire_bytes += len(chunk)
            if wire_bytes > MAX_BYTES:
                raise RequestProblem(413, "analytics.batchTooLarge")
            body.extend(decoder.decompress(chunk, MAX_BYTES + 1 - len(body)) if decoder else chunk)
            if len(body) > MAX_BYTES or (decoder and decoder.unconsumed_tail):
                raise RequestProblem(413, "analytics.batchTooLarge")
        if decoder and (not decoder.eof or decoder.unused_data):
            raise RequestProblem(400, "analytics.invalidRequest")
        def finite_number(value: str) -> float:
            number = float(value)
            if not math.isfinite(number):
                raise ValueError("Non-finite JSON number")
            return number

        return json.loads(body, parse_float=finite_number, parse_constant=finite_number), len(body)
    except (zlib.error, ValueError, RecursionError) as error:
        raise RequestProblem(400, "analytics.invalidRequest") from error


@router.post(
    "/anonymous-tokens", operation_id="IPK_ANL_02", status_code=201,
    responses={403: {"description": "Collection disabled"}, 429: {"description": "Rate limited"}},
)
async def anonymous_token(device: Device, request: Request) -> JSONResponse:
    await take(request, f"token:ip:{client_key(request)}", 30, 60)
    await take(request, f"token:install:{device.installId}", 8, 15)
    policy = await request.app.state.policy_repository.get_active()
    if not (
        policy.rules.anonymous_enabled and policy.schema_versions
        and policy.rules.enabled_for(device.platform, device.appVersion, device.installId)
    ):
        raise RequestProblem(403, "analytics.collectionDisabled")
    token = request.app.state.identity.issue(device, int(time.time()))
    return reply(
        201, "ok", {"analyticsToken": token, "expiresIn": 900, "policyVersion": policy.version},
        {"Cache-Control": "no-store"},
    )


@router.post(
    "/events/report", operation_id="IPK_ANL_03",
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": Batch.model_json_schema()}},
        }
    },
)
async def report(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
    x_analytics_token: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    await take(request, f"batch:ip:{client_key(request)}", 60, 120)
    value, decoded_size = await read_json(request)
    if isinstance(value, dict) and isinstance(value.get("events"), list) and len(value["events"]) > 100:
        raise RequestProblem(413, "analytics.batchTooLarge")
    try:
        batch = Batch.model_validate(value)
    except ValidationError as error:
        raise RequestProblem(400, "analytics.invalidRequest") from error
    request.state.quality_dimensions = (batch.platform, batch.appVersion)
    identity = request.app.state.identity.authenticate(batch, authorization, x_analytics_token)
    policy = await request.app.state.policy_repository.get_active()
    rules = policy.rules
    if not (
        rules.enabled_for(batch.platform, batch.appVersion, batch.installId)
        and policy.schema_versions
        and (identity.identity_type != "anonymous" or rules.anonymous_enabled)
    ):
        raise RequestProblem(403, "analytics.collectionDisabled")
    if len(batch.events) > rules.max_batch_size or decoded_size > rules.max_batch_bytes:
        raise RequestProblem(413, "analytics.batchTooLarge")
    await take(request, f"batch:install:{batch.installId}", 10, 30)
    burst, rate = (100, 600) if identity.identity_type == "anonymous" else (200, 1200)
    quota_key = f"events:install:{batch.installId}" if identity.identity_type == "anonymous" else f"events:user:{identity.actor_key}"
    await take(request, quota_key, burst, rate, len(batch.events))
    results = await request.app.state.ingest.report(
        batch, identity, policy.version, rules.max_age_seconds, request.state.request_id,
        rules.enabled_event_names,
    )
    request.state.quality_counts = Counter(item["status"] for item in results)
    return reply(200, "ok", {"batchId": str(batch.batchId), "results": results})
