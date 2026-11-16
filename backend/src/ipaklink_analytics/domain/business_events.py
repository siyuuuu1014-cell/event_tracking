from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, Field

from ipaklink_analytics.domain.ingest import AppVersion, WireModel

Ref = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")]
OpaqueRef = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class Target(WireModel):
    targetType: Literal["company", "product"]
    targetId: Ref


class View(Target):
    viewRef: Ref


class Favorite(Target):
    action: Literal["add", "remove"]


class Inquiry(WireModel):
    inquiryRef: Ref
    companyId: Ref
    productId: Ref | None = None
    contactFlowId: UUID | None = None
    clientRequestId: Ref | None = None


class Message(WireModel):
    dialogRef: OpaqueRef
    messageRef: Ref
    messageType: Literal["text", "image", "file", "audio", "card", "other"]
    senderKind: Literal["human", "system", "inquiryCard"]


class TranslationSync(WireModel):
    recordRef: Ref
    mode: Literal["text", "dialog", "image", "file"]
    outcome: Literal["success", "failed"]


class Registration(WireModel):
    channel: Literal["phone", "email"]


BUSINESS_PROPERTIES = {
    "business_view_recorded": View,
    "favorite_changed": Favorite,
    "inquiry_created": Inquiry,
    "message_send_succeeded": Message,
    "translate_sync_completed": TranslationSync,
    "account_registered": Registration,
}


class BusinessEvent(WireModel):
    eventId: UUID
    eventName: str
    schemaVersion: Literal[1]
    occurredAt: AwareDatetime
    receivedAt: AwareDatetime
    businessDate: str
    source: Literal["server"]
    producer: Ref
    identityType: Literal["authenticated"]
    actorKey: OpaqueRef
    aggregateRef: Ref
    platform: Literal["android", "ios"] | None = None
    appVersion: AppVersion | None = None
    regionId: Annotated[int, Field(strict=True, ge=0, le=4294967295)] | None = None
    payloadHash: OpaqueRef
    properties: dict
