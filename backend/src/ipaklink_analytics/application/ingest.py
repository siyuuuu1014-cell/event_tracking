import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator, FormatChecker
from psycopg import Error
from psycopg_pool import PoolTimeout
from pydantic import ValidationError

from ipaklink_analytics.domain.ingest import Batch, Event
from ipaklink_analytics.infrastructure.identity import Identity, IdentityService
from ipaklink_analytics.infrastructure.kafka import BrokerUnavailable, DeliveryUnknown, KafkaPublisher
from ipaklink_analytics.infrastructure.postgres.ingest import IngestRepository

BUSINESS_TIMEZONE = ZoneInfo("Asia/Tashkent")
PAGE_EVENTS = {"screen_view", "screen_exit", "entity_detail_view", "contact_supplier_click",
               "card_impression", "card_impression_click"}
SCREENS = set("""
AccountSecurity AddFriend AllReviews AvatarCrop AvatarPicker BindChannel BizPhrase
CameraHistory CameraTranslate ChatDetail ChatFileImport ChatFiles ChatInfo ClaimApply ClaimCompany
ClaimDetail Comments CompanyEdit CompanyInquiry CustomPhrase DialogHistory DialogTranslate
DocPreview DocTranslate EditProfile FactoryDetail FactoryList FollowList FoodList ForgotPassword
GlobalSearch HomeMain InquiryEdit InteractionMain LanguageSettings LikeFavList Login MeMain
MessageMain MessageSearch MoreCategories MyCompany MyFavorites MyInquiry MyLikes MyReviews
NearbyCompanies NewProduct NotificationSettings ProductDetail ProductList ProductManage
PublishCompany RecentViews Register Search SearchFilter ShopDetail ShopList StrangerHome
Subscription SupplierDetail SupplierList SwitchAccount TextTranslate TranslateMain WriteReview
""".split())


def encode(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False
    ).encode("utf-8")


class IngestService:
    def __init__(
        self, repository: IngestRepository, publisher: KafkaPublisher, identity_service: IdentityService
    ) -> None:
        self.repository = repository
        self.publisher = publisher
        self.identity_service = identity_service
        self.capacity = asyncio.Semaphore(24)

    async def report(
        self, batch: Batch, identity: Identity, policy_version: int, max_age: int,
        request_id: str, enabled_names: list[str] | None = None,
    ):
        schemas = await self.repository.active_schemas()
        if enabled_names is not None:
            enabled = set(enabled_names)
            schemas = {key: schema for key, schema in schemas.items() if key[0] in enabled}
        validators = {
            key: Draft202012Validator(schema, format_checker=FormatChecker())
            for key, schema in schemas.items()
        }
        names = {name for name, _ in schemas}
        now = datetime.now(UTC)
        now = now.replace(microsecond=now.microsecond // 1000 * 1000)
        return await asyncio.gather(
            *(
                self._report_one(
                    index, value, batch, identity, policy_version, max_age,
                    request_id, validators, names, now,
                )
                for index, value in enumerate(batch.events)
            )
        )

    async def _report_one(
        self, index, raw, batch, identity, policy_version, max_age,
        request_id, validators, names, now,
    ):
        event_id = None
        if isinstance(raw, dict):
            try:
                event_id = str(UUID(str(raw.get("eventId"))))
            except ValueError:
                pass

        def result(status, error=None):
            data = {"index": index, "eventId": event_id, "status": status}
            if error:
                data["errorCode"] = error
            return data

        try:
            if len(encode(raw)) > 8192:
                return result("rejected", "invalidEvent")
            event = Event.model_validate(raw)
        except (ValidationError, ValueError, TypeError):
            return result("rejected", "invalidEvent")

        if event.eventName not in names:
            return result("rejected", "unknownEvent")
        validator = validators.get((event.eventName, event.schemaVersion))
        if validator is None:
            return result("rejected", "unsupportedSchema")
        if event.screenName and event.screenName not in SCREENS:
            return result("rejected", "invalidEvent")
        if event.eventName in PAGE_EVENTS and (not event.screenName or not event.pageViewId):
            return result("rejected", "invalidEvent")
        if not validator.is_valid(event.properties):
            return result("rejected", "invalidProperties")
        properties = dict(event.properties)
        if event.eventName == "entity_detail_view":
            expected = properties.get("productId") if properties["targetType"] == "product" else properties["companyId"]
            if properties["targetId"] != expected:
                return result("rejected", "invalidProperties")
        if event.eventName == "chat_session_open":
            properties["dialogRef"] = self.identity_service.opaque("dialog", properties["dialogRef"])
        if event.eventName == "search_result_open" and properties["targetType"] == "user":
            properties["targetId"] = self.identity_service.opaque("user", properties["targetId"])

        event_data = event.model_dump(mode="json", exclude_none=True)
        event_data["properties"] = properties
        stable = {
            **event_data,
            "source": "app",
            "producer": "analytics-collector",
            "actorKey": identity.actor_key,
            "identityType": identity.identity_type,
            "platform": batch.platform,
            "appVersion": batch.appVersion,
            "businessDate": event.occurredAt.astimezone(BUSINESS_TIMEZONE).date().isoformat(),
        }
        if batch.appLanguage is not None:
            stable["appLanguage"] = batch.appLanguage
        if batch.regionId is not None:
            stable["regionId"] = batch.regionId
        if identity.identity_type == "anonymous":
            stable["anonKey"] = identity.actor_key
        digest = hashlib.sha256(encode(stable)).hexdigest()
        payload = {
            **stable,
            "receivedAt": now.isoformat(),
            "policyVersion": policy_version,
            "requestId": request_id,
            "ingestBatchId": str(batch.batchId),
            "payloadHash": digest,
        }
        current = now - timedelta(seconds=max_age) <= event.occurredAt <= now + timedelta(minutes=5)
        try:
            async with asyncio.timeout(8):
                async with self.capacity:
                    claim = await self.repository.claim(
                        payload, digest, identity.actor_key, batch.identityEpoch, now, current
                    )
                    if claim.status == "duplicate":
                        return result("duplicate")
                    if claim.status == "conflict":
                        return result("rejected", "eventIdConflict")
                    if claim.status == "suppressed":
                        return result("rejected", "invalidEvent")
                    if claim.status == "outOfRange":
                        return result("rejected", "timeOutOfRange")
                    if claim.status == "busy":
                        return result("retry", "ingestBusy")
                    assert claim.lease is not None and claim.payload is not None
                    wire = encode(claim.payload)
                    signature = self.identity_service.opaque("kafka", wire.decode("utf-8"))
                    await self.publisher.publish("analytics.event", claim.payload["sessionId"], wire, signature)
                    if await self.repository.published(event.eventId, claim.lease):
                        return result("accepted")
                    return result("retry", "ackUnknown")
        except BrokerUnavailable:
            return result("retry", "brokerUnavailable")
        except (DeliveryUnknown, Error, PoolTimeout, TimeoutError):
            return result("retry", "ackUnknown")
