from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from ipaklink_analytics.infrastructure.subject_erasure import SUBJECT_DATA_LOCK, SUPPRESSED


@dataclass(frozen=True)
class Claim:
    status: str
    payload: dict[str, Any] | None = None
    lease: UUID | None = None


class IngestRepository:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    async def active_schemas(self) -> dict[tuple[str, int], dict[str, Any]]:
        async with self.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT event_name, schema_version, property_schema FROM analytics_event_schema "
                    "WHERE source = 'app' AND status = 'active'"
                )
            ).fetchall()
        return {(name, version): schema for name, version, schema in rows}

    async def claim(
        self,
        payload: dict[str, Any],
        digest: str,
        actor_key: str,
        epoch: UUID,
        now: datetime,
        first_report_allowed: bool,
    ) -> Claim:
        event_id = payload["eventId"]
        lease = uuid4()
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock_shared(%s)",(SUBJECT_DATA_LOCK,))
                subjects = [actor_key]
                properties = payload["properties"]
                if payload["eventName"] == "search_result_open" and properties.get("targetType") == "user":
                    subjects.append(properties["targetId"])
                if await (await conn.execute(SUPPRESSED,(subjects,))).fetchone():
                    return Claim("suppressed")
                async with conn.cursor(row_factory=dict_row) as cur:
                    if first_report_allowed:
                        await cur.execute(
                            """
                            INSERT INTO analytics_ingest_receipt (
                                event_id,event_name,schema_version,policy_version,producer_scope,
                                subject_epoch,payload_hash,canonical_payload,canonical_time,
                                first_received_at,lease_token,lease_until,expires_at
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now()+interval '20 seconds',%s)
                            ON CONFLICT (event_id) DO NOTHING
                            RETURNING event_id
                            """,
                            (
                                event_id, payload["eventName"], payload["schemaVersion"],
                                payload["policyVersion"], actor_key, epoch, digest, Jsonb(payload),
                                payload["occurredAt"], now, lease, now + timedelta(days=14),
                            ),
                        )
                        if await cur.fetchone():
                            return Claim("claimed", payload, lease)
                    await cur.execute(
                        """
                        SELECT state, producer_scope, subject_epoch, payload_hash, canonical_payload,
                               lease_until > now() AS leased
                        FROM analytics_ingest_receipt WHERE event_id=%s FOR UPDATE
                        """,
                        (event_id,),
                    )
                    row = await cur.fetchone()
                    if row is None:
                        return Claim("outOfRange")
                    if (
                        row["payload_hash"] != digest
                        or row["producer_scope"] != actor_key
                        or row["subject_epoch"] != epoch
                    ):
                        return Claim("conflict")
                    if row["state"] == "published":
                        return Claim("duplicate")
                    if row["leased"]:
                        return Claim("busy")
                    await cur.execute(
                        "UPDATE analytics_ingest_receipt SET lease_token=%s, "
                        "lease_until=now()+interval '20 seconds' WHERE event_id=%s",
                        (lease, event_id),
                    )
                    return Claim("claimed", row["canonical_payload"], lease)

    async def published(self, event_id: UUID, lease: UUID) -> bool:
        async with self.pool.connection() as conn:
            cursor = await conn.execute(
                "UPDATE analytics_ingest_receipt SET state='published',published_at=now(), "
                "lease_token=NULL,lease_until=NULL WHERE event_id=%s AND lease_token=%s "
                "AND lease_until > now() AND state='pending'",
                (event_id, lease),
            )
            return cursor.rowcount == 1
