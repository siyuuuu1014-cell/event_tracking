import hashlib
import hmac
import secrets
from dataclasses import dataclass

import jwt

from ipaklink_analytics.config import Settings
from ipaklink_analytics.domain.ingest import Batch, Device


class AuthFailure(Exception):
    pass


class IdentityUnavailable(Exception):
    pass


@dataclass(frozen=True)
class Identity:
    identity_type: str
    actor_key: str


class IdentityService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        secret = settings.signing_secret.get_secret_value()
        if len(secret.encode()) < 32:
            if settings.environment != "local" or secret:
                raise ValueError("ANALYTICS_SIGNING_SECRET must contain at least 32 bytes")
            secret = secrets.token_urlsafe(48)
        self.secret = secret.encode()
        self.issuer = f"ipaklink-analytics:{settings.environment}"

    def opaque(self, purpose: str, value: str) -> str:
        material = f"{self.issuer}:{purpose}:{value}".encode()
        return hmac.new(self.secret, material, hashlib.sha256).hexdigest()

    def issue(self, device: Device, now: int) -> str:
        return jwt.encode(
            {
                "iss": self.issuer,
                "aud": "analytics-ingest",
                "sub": str(device.installId),
                "epoch": str(device.identityEpoch),
                "platform": device.platform,
                "version": device.appVersion,
                "iat": now,
                "exp": now + 900,
            },
            self.secret,
            algorithm="HS256",
        )

    def validate_app(self, authorization: str) -> str:
        scheme, separator, token = authorization.partition(" ")
        if not separator or scheme.lower() != "bearer" or not token:
            raise AuthFailure
        cfg = self.settings
        secret = cfg.app_jwt_secret.get_secret_value()
        if not secret or not cfg.app_jwt_issuer or not cfg.app_jwt_audience:
            raise IdentityUnavailable
        try:
            claims = jwt.decode(
                token, secret, algorithms=["HS256"],
                issuer=cfg.app_jwt_issuer, audience=cfg.app_jwt_audience,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.InvalidTokenError as error:
            raise AuthFailure from error
        subject = claims.get("sub") or claims.get("uid")
        if not isinstance(subject, str) or not subject or len(subject) > 128:
            raise AuthFailure
        return subject

    def authenticate(
        self, batch: Batch, authorization: str | None, anonymous_token: str | None
    ) -> Identity:
        if batch.identityType == "authenticated":
            if not authorization or anonymous_token is not None:
                raise AuthFailure
            return Identity("authenticated", self.opaque("user", self.validate_app(authorization)))
        if not anonymous_token or authorization is not None:
            raise AuthFailure
        try:
            claims = jwt.decode(
                anonymous_token, self.secret, algorithms=["HS256"],
                issuer=self.issuer, audience="analytics-ingest",
                options={"require": ["exp", "iat", "sub", "epoch"]},
            )
        except jwt.InvalidTokenError as error:
            raise AuthFailure from error
        expected = (
            str(batch.installId), str(batch.identityEpoch), batch.platform, batch.appVersion
        )
        if tuple(claims.get(k) for k in ("sub", "epoch", "platform", "version")) != expected:
            raise AuthFailure
        return Identity(
            "anonymous", self.opaque("anonymous", f"{batch.installId}:{batch.identityEpoch}")
        )
