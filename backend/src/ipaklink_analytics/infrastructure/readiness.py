import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import clickhouse_connect
from confluent_kafka.admin import AdminClient
from psycopg_pool import AsyncConnectionPool
from pymongo import MongoClient
from redis.asyncio import Redis

from ipaklink_analytics.config import Settings


class Readiness:
    def __init__(self, settings: Settings, pool: AsyncConnectionPool, redis: Redis) -> None:
        self.settings = settings
        self.pool = pool
        self.redis = redis
        self.lock = asyncio.Lock()

    async def check(self) -> dict[str, str]:
        async with self.lock:
            results = await asyncio.gather(
                self._probe(self._postgres),
                self._probe(self._redis),
                self._probe(lambda: asyncio.to_thread(self._kafka)),
                self._probe(lambda: asyncio.to_thread(self._clickhouse)),
                self._probe(lambda: asyncio.to_thread(self._mongo)),
            )
        return dict(zip(("postgresql", "redis", "kafka", "clickhouse", "mongodb"), results))

    @staticmethod
    async def _probe(probe: Callable[[], Coroutine[Any, Any, Any]]) -> str:
        try:
            await probe()
        except Exception:
            return "unavailable"
        return "ready"

    async def _postgres(self) -> None:
        async with self.pool.connection() as conn:
            async with asyncio.timeout(self.settings.health_timeout_seconds):
                await conn.execute("SELECT 1 FROM analytics_schema_migration LIMIT 1")

    async def _redis(self) -> None:
        await self.redis.ping()

    def _kafka(self) -> None:
        admin = AdminClient({"bootstrap.servers": self.settings.kafka_bootstrap_servers})
        metadata = admin.list_topics(timeout=self.settings.health_timeout_seconds)
        for name in ("analytics.event", "analytics.event.dlq.v1"):
            if name not in metadata.topics or metadata.topics[name].error:
                raise RuntimeError("Required Kafka topic unavailable")

    def _clickhouse(self) -> None:
        cfg = self.settings
        client = clickhouse_connect.get_client(
            host=cfg.clickhouse_host,
            port=cfg.clickhouse_port,
            database=cfg.clickhouse_database,
            username=cfg.clickhouse_user,
            password=cfg.clickhouse_password.get_secret_value(),
            connect_timeout=cfg.health_timeout_seconds,
            send_receive_timeout=cfg.health_timeout_seconds,
            query_retries=0,
        )
        try:
            client.command("SELECT 1 FROM events_raw LIMIT 1")
        finally:
            client.close()

    def _mongo(self) -> None:
        timeout = self.settings.health_timeout_seconds * 1000
        with MongoClient(
            self.settings.mongo_uri.get_secret_value(),
            serverSelectionTimeoutMS=timeout,
            connectTimeoutMS=timeout,
            socketTimeoutMS=timeout,
        ) as client:
            hello = client.admin.command("hello")
            if not hello.get("isWritablePrimary") or not hello.get("setName"):
                raise RuntimeError("MongoDB replica set primary unavailable")

