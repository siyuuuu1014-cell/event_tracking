import asyncio
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from secrets import token_urlsafe

from argon2 import PasswordHasher
from argon2.exceptions import VerificationError
from redis.asyncio import Redis
from starlette.concurrency import run_in_threadpool

from ipaklink_analytics.infrastructure.identity import AuthFailure

PASSWORDS = PasswordHasher(time_cost=2, memory_cost=19456, parallelism=1)
COOKIE = "analytics_admin_session"


class AdminSessions:
    def __init__(self, pool, redis: Redis, environment: str) -> None:
        self.pool = pool
        self.redis = redis
        self.prefix = f"analytics:admin:session:{environment}:"
        self.ttl = 8 * 60 * 60
        self.hash_workers = asyncio.Semaphore(4)
        self.dummy_hash = PASSWORDS.hash(token_urlsafe(32))

    def key(self, token: str) -> str:
        return self.prefix + sha256(token.encode()).hexdigest()

    async def login(self, username, password, previous):
        async with self.pool.connection() as pg:
            row = await (await pg.execute(
                """SELECT account_id,display_name,password_hash,status
                   FROM analytics_admin_account WHERE username=%s""", (username,),
            )).fetchone()
        async with self.hash_workers:
            try:
                await run_in_threadpool(PASSWORDS.verify, row[2] if row else self.dummy_hash, password)
            except VerificationError as error:
                raise AuthFailure from error
        if row is None or row[3] != "active":
            raise AuthFailure
        await self.logout(previous)
        token = token_urlsafe(32)
        await self.redis.set(self.key(token), row[0], ex=self.ttl)
        expires_at = datetime.now(UTC) + timedelta(seconds=self.ttl)
        return token, {"accountId": row[0], "displayName": row[1],
                       "expiresAt": expires_at.isoformat().replace("+00:00", "Z")}

    async def principal(self, token: str | None) -> str:
        if token is None or len(token) != 43 or not token.isascii():
            raise AuthFailure
        value = await self.redis.get(self.key(token))
        if value is None:
            raise AuthFailure
        return value.decode()

    async def logout(self, token: str | None) -> None:
        if token and len(token) == 43 and token.isascii():
            await self.redis.delete(self.key(token))
