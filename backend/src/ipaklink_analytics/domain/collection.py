from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class CollectionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool
    anonymous_enabled: bool
    rollout_percent: int = Field(default=100, ge=0, le=100)
    platforms: list[Literal["android", "ios"]] = Field(default_factory=list)
    app_versions: list[str] = Field(default_factory=list)
    enabled_event_names: list[str] | None = None
    max_batch_size: int = Field(default=100, ge=1, le=100)
    max_batch_bytes: int = Field(default=262144, ge=1, le=262144)
    max_age_seconds: int = Field(default=604800, ge=1, le=604800)
    flush_interval_ms: int = Field(default=10000, ge=1000)
    config_ttl_seconds: int = Field(default=300, ge=1)

    def enabled_for(self, platform: str, app_version: str, install_id: UUID) -> bool:
        if not self.enabled or platform not in self.platforms:
            return False
        if self.app_versions and app_version not in self.app_versions:
            return False
        return install_id.int % 100 < self.rollout_percent


class PolicySnapshot(BaseModel):
    version: int = Field(gt=0)
    schema_versions: list[int]
    rules: CollectionPolicy

