from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, SecretStr, Field

from ipaklink_analytics.api.collection import client_key, reply, take
from ipaklink_analytics.infrastructure.admin_access import AccessDenied
from ipaklink_analytics.infrastructure.admin_sessions import COOKIE

PREFIX = "/ipaklink/v1/internal/analytics"
router = APIRouter(prefix=PREFIX, tags=["admin-session"])


class Credentials(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{2,63}$")
    password: SecretStr = Field(min_length=1, max_length=256)


def enforce_origin(request: Request) -> None:
    origin = request.headers.get("origin")
    if request.headers.get("sec-fetch-site") in {"cross-site", "same-site"}:
        raise AccessDenied
    settings = request.app.state.admin_settings
    allowed_origins = {settings.admin_origin}
    if settings.environment == "local":
        allowed_origins.add("http://127.0.0.1:5173")
    if origin is not None and origin not in allowed_origins:
        raise AccessDenied


@router.post("/sessions", operation_id="IPK_ANL_46", status_code=201)
async def login(request: Request, body: Credentials):
    enforce_origin(request)
    username = body.username.lower()
    await take(request, f"admin-login:ip:{client_key(request)}", 15, 10)
    username_key = request.app.state.identity.opaque("admin-login", username)
    await take(request, f"admin-login:user:{username_key}", 5, 3)
    sessions = request.app.state.admin_sessions
    token, data = await sessions.login(username, body.password.get_secret_value(), request.cookies.get(COOKIE))
    response = reply(201, "ok", data, {"Cache-Control": "no-store"})
    response.set_cookie(COOKIE, token, max_age=sessions.ttl, path="/",
                        secure=request.app.state.admin_settings.environment != "local",
                        httponly=True, samesite="strict")
    return response


@router.delete("/sessions/current", operation_id="IPK_ANL_47")
async def logout(request: Request):
    enforce_origin(request)
    await request.app.state.admin_sessions.logout(request.cookies.get(COOKIE))
    response = reply(200, "ok", headers={"Cache-Control": "no-store"})
    response.delete_cookie(COOKIE, path="/", samesite="strict", httponly=True,
                           secure=request.app.state.admin_settings.environment != "local")
    return response
