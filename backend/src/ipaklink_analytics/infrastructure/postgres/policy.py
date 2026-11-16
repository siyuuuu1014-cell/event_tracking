from psycopg import Error
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import ValidationError

from ipaklink_analytics.domain.collection import PolicySnapshot
from ipaklink_analytics.ports.policy import PolicyUnavailable


class PostgresPolicyRepository:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    async def get_active(self) -> PolicySnapshot:
        try:
            async with self.pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(
                        """
                        SELECT p.policy_version AS version, p.rules,
                               ARRAY(SELECT DISTINCT e.schema_version
                                     FROM analytics_event_schema e
                                     WHERE e.status = 'active' AND e.source = 'app'
                                       AND (
                                            p.rules->'enabled_event_names' IS NULL
                                            OR p.rules->'enabled_event_names' = 'null'::jsonb
                                            OR p.rules->'enabled_event_names' @> jsonb_build_array(e.event_name)
                                       )
                                     ORDER BY e.schema_version) AS schema_versions
                        FROM analytics_collection_policy p
                        WHERE p.status = 'active' AND p.effective_at <= now()
                        """
                    )
                    row = await cursor.fetchone()
            if row is None:
                raise PolicyUnavailable
            return PolicySnapshot.model_validate(row)
        except (Error, PoolTimeout, ValidationError) as error:
            raise PolicyUnavailable from error

