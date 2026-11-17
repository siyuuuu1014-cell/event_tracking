import asyncio
import json
from hashlib import sha256
from random import randint
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

import clickhouse_connect
from fastapi.encoders import jsonable_encoder
from redis.asyncio import Redis
from redis.exceptions import RedisError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.subject_erasure import UNFINISHED

DATASET = "pageVisits"


class SnapshotUnavailable(Exception):
    pass


class WarehouseQueryError(Exception):
    pass


def clickhouse_client(settings: Settings, *, read_timeout: int = 10):
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port,
        database=settings.clickhouse_database, username=settings.clickhouse_user,
        password=settings.clickhouse_password.get_secret_value(),
        connect_timeout=settings.health_timeout_seconds, send_receive_timeout=read_timeout,
        query_retries=0,
    )


class ReportStore:
    def __init__(self, settings: Settings, pool: AsyncConnectionPool, redis: Redis | None = None) -> None:
        self.settings = settings
        self.pool = pool
        self.query_slots = asyncio.Semaphore(4)
        self.cache = redis
        self.inflight: dict[str, asyncio.Task] = {}

    async def snapshot(
        self, snapshot_id: str | None, dates: tuple[date, date] | None = None, *, dataset: str = DATASET
    ) -> tuple[dict | None, list[tuple[date, int]]]:
        target = "status='published'" if snapshot_id is None else "snapshot_id=%s"
        values = () if snapshot_id is None else (snapshot_id,)
        async with self.pool.connection() as connection:
            async with connection.transaction():
                await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                if (await (await connection.execute(UNFINISHED)).fetchone())[0]:
                    raise WarehouseQueryError
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(
                        f"""SELECT snapshot_id,metric_version,generated_at,source_watermark,
                                   status,(expires_at IS NOT NULL AND expires_at<=now()) AS expired
                            FROM analytics_report_snapshot WHERE {target}""",
                        values,
                    )
                    snapshot = await cursor.fetchone()
                    if snapshot is None and snapshot_id is None:
                        return None, []
                    if snapshot is None or snapshot["expired"] or snapshot["status"] not in {"published", "archived"}:
                        raise SnapshotUnavailable
                    if dates is None:
                        return snapshot, []
                    await cursor.execute(
                        """SELECT date_key,generation FROM analytics_report_snapshot_build
                            WHERE snapshot_id=%s AND dataset=%s AND date_key >= %s AND date_key < %s""",
                        (snapshot["snapshot_id"], dataset, *dates),
                    )
                    return snapshot, [(row["date_key"], row["generation"]) for row in await cursor.fetchall()]

    async def snapshot_has_dataset(self, snapshot_id: str, dataset: str) -> bool:
        async with self.pool.connection() as connection:
            result = await (await connection.execute(
                "SELECT EXISTS (SELECT 1 FROM analytics_report_snapshot_build WHERE snapshot_id=%s AND dataset=%s)",
                (snapshot_id, dataset),
            )).fetchone()
        return result[0]

    async def cached_query(self, snapshot: dict, callback: Callable[..., Any], *args) -> Any:
        if self.cache is None:
            return await self.query(callback,*args)
        material = json.dumps(
            jsonable_encoder((snapshot["snapshot_id"],snapshot["metric_version"],callback.__name__,args)),
            sort_keys=True,separators=(",",":"),
        )
        digest = sha256(material.encode()).hexdigest()
        key = f"analytics:report:{self.settings.environment}:v1:{digest}"
        inflight = self.inflight.get(key)
        if inflight is None:
            inflight = asyncio.create_task(self._cached_result(key,callback,args))
            self.inflight[key] = inflight

            def discard(done):
                self.inflight.pop(key,None)
                if not done.cancelled():
                    done.exception()

            inflight.add_done_callback(discard)
        return await asyncio.shield(inflight)

    async def _cached_result(self, key, callback, args):
        try:
            async with asyncio.timeout(0.3):
                value = await self.cache.get(key)
            if value is not None:
                return json.loads(value)
        except (TimeoutError,RedisError,ValueError):
            pass
        result = await self.query(callback,*args)
        encoded = json.dumps(jsonable_encoder(result),separators=(",",":"),allow_nan=False)
        if len(encoded) <= 512 * 1024:
            try:
                async with asyncio.timeout(0.3):
                    await self.cache.set(key,encoded,ex=60+randint(0,10))
            except (TimeoutError,RedisError):
                pass
        return result

    async def query(self, callback: Callable[..., Any], *args) -> Any:
        # Keep the permit until the synchronous driver actually exits, even if HTTP is cancelled.
        try:
            await asyncio.wait_for(self.query_slots.acquire(), timeout=2)
        except TimeoutError as error:
            raise WarehouseQueryError from error
        task = asyncio.create_task(asyncio.to_thread(self._execute, callback, args))
        def release(done):
            self.query_slots.release()
            if not done.cancelled():
                done.exception()
        task.add_done_callback(release)
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=8)
        except (TimeoutError, WarehouseQueryError) as error:
            raise WarehouseQueryError from error

    def _execute(self, callback, args):
        try:
            with clickhouse_client(self.settings) as client:
                return callback(client, *args)
        except Exception as error:
            raise WarehouseQueryError from error


def is_stale(snapshot: dict, now: datetime) -> bool:
    return (now - snapshot["source_watermark"]).total_seconds() > 900
