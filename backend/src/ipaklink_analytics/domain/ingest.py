from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator


Version = Annotated[StrictInt, Field(ge=1, le=65535)]
AppVersion = Annotated[str, Field(min_length=1, max_length=40, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.+-]*$")]


class WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Device(WireModel):
    installId: UUID
    identityEpoch: UUID
    platform: Literal["ios", "android"]
    appVersion: AppVersion


class Batch(Device):
    batchId: UUID
    identityType: Literal["anonymous", "authenticated"]
    appLanguage: Literal["zh", "en", "ru", "uz"] | None = None
    regionId: Annotated[StrictInt, Field(ge=0, le=4294967295)] | None = None
    policyVersion: Annotated[StrictInt, Field(ge=1, le=2147483647)]
    events: list[Any] = Field(min_length=1, max_length=100)


class Event(WireModel):
    eventId: UUID
    eventName: str = Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_]*$")
    schemaVersion: Version
    occurredAt: datetime
    sessionId: UUID
    sequence: Annotated[StrictInt, Field(ge=1, le=2147483647)]
    screenName: str | None = Field(default=None, max_length=80, pattern=r"^[A-Za-z][A-Za-z0-9_]*$")
    pageViewId: UUID | None = None
    properties: dict[str, Any]

    @field_validator("occurredAt", mode="before")
    @classmethod
    def require_iso_text(cls, value: object) -> object:
        if not isinstance(value, str) or "T" not in value:
            raise ValueError("Expected ISO date time")
        return value

    @field_validator("occurredAt")
    @classmethod
    def normalize_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("Time zone required")
        value = value.astimezone(UTC)
        return value.replace(microsecond=(value.microsecond // 1000) * 1000)
