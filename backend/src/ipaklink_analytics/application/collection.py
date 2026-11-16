from typing import Any
from uuid import UUID

from ipaklink_analytics.ports.policy import PolicyRepository


async def get_collection_config(
    repository: PolicyRepository, platform: str, app_version: str, install_id: UUID
) -> dict[str, Any]:
    snapshot = await repository.get_active()
    policy = snapshot.rules
    enabled = bool(snapshot.schema_versions) and policy.enabled_for(
        platform, app_version, install_id
    )
    return {
        "enabled": enabled,
        "anonymousEnabled": enabled and policy.anonymous_enabled,
        "policyVersion": snapshot.version,
        "schemaVersions": snapshot.schema_versions,
        "maxBatchSize": policy.max_batch_size,
        "maxBatchBytes": policy.max_batch_bytes,
        "maxAgeSeconds": policy.max_age_seconds,
        "flushIntervalMs": policy.flush_interval_ms,
        "configTtlSeconds": policy.config_ttl_seconds,
    }

