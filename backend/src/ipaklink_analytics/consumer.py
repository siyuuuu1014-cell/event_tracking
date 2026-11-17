import hashlib
import hmac
import json
import re
import signal
import threading
from datetime import UTC, date, datetime
from uuid import UUID

import clickhouse_connect
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from psycopg import Error
from psycopg_pool import ConnectionPool, PoolTimeout

from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE, SCREENS, encode
from ipaklink_analytics.config import Settings
from ipaklink_analytics.domain.business_events import BUSINESS_PROPERTIES, BusinessEvent
from ipaklink_analytics.domain.ingest import Event
from ipaklink_analytics.infrastructure.identity import IdentityService
from ipaklink_analytics.infrastructure.logging import get_logger
from ipaklink_analytics.infrastructure.quality_telemetry import record_dlq
from ipaklink_analytics.infrastructure.subject_erasure import persist_events

INPUT = "analytics.event"
DLQ = "analytics.event.dlq.v1"
COLUMNS = (
    "event_id", "event_name", "schema_version", "occurred_at", "received_at", "business_date",
    "source", "producer", "actor_key", "anon_key", "identity_type", "session_id", "page_view_id",
    "screen_name", "sequence", "platform", "app_version", "app_language", "region_id",
    "aggregate_ref", "policy_version", "request_id", "ingest_batch_id", "payload_hash",
    "properties_json", "topic", "kafka_partition", "kafka_offset", "broker_at", "ingested_at",
    "replay_run_id",
)
EVENT_KEYS = set(Event.model_fields)
DIGEST_EXCLUDED = {"receivedAt", "policyVersion", "requestId", "ingestBatchId", "payloadHash"}
SAFE_ENVELOPE = EVENT_KEYS | DIGEST_EXCLUDED | {
    "source", "producer", "actorKey", "anonKey", "identityType", "platform",
    "appVersion", "businessDate", "appLanguage", "regionId",
}


def timestamp(raw: str) -> datetime:
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        raise ValueError("Invalid timestamp")
    return dt.astimezone(UTC)


def event_row(message, identity: IdentityService) -> tuple:
    wire = message.value().decode("utf-8")
    supplied = dict(message.headers() or []).get("analytics-signature")
    if not isinstance(supplied, bytes) or not hmac.compare_digest(
        supplied, identity.opaque("kafka", wire).encode("ascii")
    ):
        raise ValueError("Untrusted producer signature")
    body = json.loads(wire)
    if isinstance(body, dict) and body.get("source") == "server":
        return business_event_row(message, body)
    if not isinstance(body, dict) or set(body) - SAFE_ENVELOPE:
        raise ValueError("Invalid event envelope")
    event = Event.model_validate({name: value for name, value in body.items() if name in EVENT_KEYS})
    if event.screenName and event.screenName not in SCREENS:
        raise ValueError("Unknown screen")
    scope = body["identityType"]
    actor = body["actorKey"]
    if (
        body["source"] != "app" or body["producer"] != "analytics-collector"
        or scope not in {"anonymous", "authenticated"}
        or not re.fullmatch(r"[a-f0-9]{64}", actor)
        or body.get("anonKey") != (actor if scope == "anonymous" else None)
    ):
        raise ValueError("Unsupported producer envelope")
    stable = {key: value for key, value in body.items() if key not in DIGEST_EXCLUDED}
    if hashlib.sha256(encode(stable)).hexdigest() != body["payloadHash"]:
        raise ValueError("Payload digest mismatch")
    transport_ts = message.timestamp()[1]
    broker_at = None if transport_ts is None or transport_ts < 0 else datetime.fromtimestamp(transport_ts / 1000, UTC)
    return (
        event.eventId, event.eventName, event.schemaVersion, event.occurredAt,
        timestamp(body["receivedAt"]), date.fromisoformat(body["businessDate"]), body["source"],
        body["producer"], actor, body.get("anonKey"), scope, event.sessionId, event.pageViewId,
        event.screenName, event.sequence, body["platform"], body["appVersion"],
        body.get("appLanguage"), body.get("regionId"), None, body["policyVersion"], body["requestId"],
        UUID(body["ingestBatchId"]), body["payloadHash"], encode(event.properties).decode("utf-8"),
        message.topic(), message.partition(), message.offset(), broker_at, datetime.now(UTC), None,
    )


def business_event_row(message, body: dict) -> tuple:
    event = BusinessEvent.model_validate(body)
    schema = BUSINESS_PROPERTIES.get(event.eventName)
    if schema is None:
        raise ValueError("Unsupported business event")
    properties = schema.model_validate(event.properties)
    timestamp_utc = event.occurredAt.astimezone(UTC)
    day = timestamp_utc.astimezone(BUSINESS_TIMEZONE).date()
    if day.isoformat() != event.businessDate:
        raise ValueError("Invalid business date")
    stable = {key: value for key,value in body.items() if key not in DIGEST_EXCLUDED}
    if hashlib.sha256(encode(stable)).hexdigest() != event.payloadHash:
        raise ValueError("Payload digest mismatch")
    transport_ts = message.timestamp()[1]
    broker_at = None if transport_ts is None or transport_ts < 0 else datetime.fromtimestamp(transport_ts / 1000, UTC)
    return (
        event.eventId,event.eventName,event.schemaVersion,timestamp_utc,event.receivedAt.astimezone(UTC),
        day,event.source,event.producer,event.actorKey,None,event.identityType,None,None,None,None,
        event.platform,event.appVersion,None,event.regionId,event.aggregateRef,None,None,None,event.payloadHash,
        encode(properties.model_dump(mode="json",exclude_none=True)).decode("utf-8"),
        message.topic(),message.partition(),message.offset(),broker_at,datetime.now(UTC),None,
    )


def write_dlq(producer: Producer, message) -> None:
    errors = []
    payload = encode({
        "sourceTopic": message.topic(),
        "partition": message.partition(),
        "offset": str(message.offset()),
        "errorCode": "invalidEvent",
    })
    key = f"{message.topic()}:{message.partition()}:{message.offset()}"
    producer.produce(DLQ, key=key, value=payload, on_delivery=lambda error, _: errors.append(error))
    if producer.flush(5) != 0 or errors != [None]:
        raise RuntimeError("DLQ delivery could not be confirmed")


def main() -> None:
    cfg = Settings()
    if not cfg.signing_secret.get_secret_value():
        raise ValueError("ANALYTICS_SIGNING_SECRET is required by the Kafka consumer")
    identity = IdentityService(cfg)
    logger = get_logger(cfg.log_dir, "analytics-consumer")
    stop = threading.Event()
    for signal_type in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_type, lambda *_: stop.set())
    warehouse = clickhouse_connect.get_client(
        host=cfg.clickhouse_host,
        port=cfg.clickhouse_port,
        database=cfg.clickhouse_database,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password.get_secret_value(),
        connect_timeout=5,
        send_receive_timeout=15,
    )
    consumer = Consumer({
        "bootstrap.servers": cfg.kafka_bootstrap_servers,
        "group.id": cfg.kafka_consumer_group,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
    })
    dlq = Producer({
        "bootstrap.servers": cfg.kafka_bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "delivery.timeout.ms": 6000,
    })
    quality_pool = ConnectionPool(cfg.postgres_dsn.get_secret_value(),min_size=0,max_size=2,
                                  timeout=cfg.health_timeout_seconds,open=True)
    consumer.subscribe([INPUT])
    try:
        while not stop.is_set():
            messages = consumer.consume(num_messages=100, timeout=0.75)
            if not messages:
                continue
            rows = []
            offsets = {}
            for message in messages:
                if message.error():
                    raise KafkaException(message.error())
                try:
                    rows.append(event_row(message, identity))
                except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
                    write_dlq(dlq, message)
                    try:
                        with quality_pool.connection() as pg:
                            record_dlq(pg,message)
                    except (Error,PoolTimeout) as error:
                        logger.warning("quality.dlqUnavailable",extra={"errorType":type(error).__name__})
                offsets[(message.topic(), message.partition())] = message.offset() + 1
            persisted = 0
            if rows:
                with quality_pool.connection() as pg:
                    persisted = persist_events(pg,warehouse,rows,COLUMNS)
            committed = consumer.commit(
                offsets=[TopicPartition(topic, part, offset) for (topic, part), offset in offsets.items()],
                asynchronous=False,
            )
            if any(partition.error for partition in committed):
                raise RuntimeError("Offset commit failed")
            logger.info("warehouse.batchPersisted", extra={"count": persisted})
    except Exception as error:
        logger.error("warehouse.failed", extra={"errorType": type(error).__name__})
        raise
    finally:
        consumer.close()
        warehouse.close()
        quality_pool.close()


if __name__ == "__main__":
    main()
