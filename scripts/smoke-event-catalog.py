"""Exercise each frozen event on its actual local ingestion channel."""
import hashlib
import json
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep
from urllib.request import Request, urlopen
from uuid import uuid4

import psycopg
from bson import ObjectId
from confluent_kafka import Consumer, Producer, TopicPartition
from pymongo import MongoClient

from ipaklink_analytics.application.corpus_review import change_review
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE, encode
from ipaklink_analytics.config import Settings
from ipaklink_analytics.domain.corpus_review import ReviewDecision
from ipaklink_analytics.infrastructure.identity import IdentityService
from ipaklink_analytics.infrastructure.report_store import clickhouse_client

settings = Settings()
assert settings.environment == "local"
now = datetime.now(UTC)
tag = uuid4().hex
actor = IdentityService(settings)
day = now.astimezone(BUSINESS_TIMEZONE).date()
uid = lambda: str(uuid4())


def wait(check):
    until = monotonic() + 35
    while monotonic() < until:
        if result := check():
            return result
        sleep(0.25)
    raise AssertionError("Timed out waiting for event delivery")


def post(path, data, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Analytics-Token"] = token
    req = Request(f"http://127.0.0.1:8000/ipaklink/v1/analytics/{path}",
                  data=json.dumps(data).encode(), headers=headers)
    with urlopen(req, timeout=10) as response:
        return json.load(response)["data"]


search, flow, task, impression, listing, page, session = (uid() for _ in range(7))
target = {"targetType": "company", "targetId": f"company_{tag}"}
card = {**target, "impressionId": impression, "listRequestId": listing, "placementCode": "searchResults", "position": 1}
translation = {"taskId": task, "mode": "text", "sourceLang": "zh", "targetLang": "uz"}
fixtures = {
    "app_session_start": {"launchSource": "coldStart"},
    "app_background": {},
    "screen_view": {"entrySource": "search"},
    "screen_exit": {"visibleMs": 30000, "exitReason": "navigate"},
    "search_submit": {"searchId": search, "scope": "all", "queryLengthBucket": "4-10", "filterCount": 0},
    "search_result": {"searchId": search, "scope": "all", "outcome": "success", "latencyMs": 10, "resultCount": 1},
    "search_result_open": {**target, "searchId": search, "position": 1},
    "entity_detail_view": {**target, "companyId": target["targetId"], "entrySource": "search"},
    "contact_supplier_click": {"contactFlowId": flow, "companyId": target["targetId"]},
    "inquiry_submit": {"clientRequestId": tag, "companyId": target["targetId"], "contactFlowId": flow},
    "inquiry_submit_failed": {"clientRequestId": tag, "attemptNo": 1, "errorCode": "networkTimeout"},
    "chat_session_open": {"dialogRef": f"dlg_{tag}", "entrySource": "message"},
    "notification_open": {"category": "system"},
    "message_translation_toggle": {"displayMode": "translated"},
    "translate_started": translation,
    "translate_completed": {**translation, "outcome": "success", "latencyMs": 5},
    "translate_copy": {"mode": "text"},
    "card_impression": {**card, "visibleMs": 1000},
    "card_impression_click": card,
}
device = {"installId": uid(), "identityEpoch": uid(), "platform": "ios", "appVersion": "8.6."+tag[:6]}
issued = post("anonymous-tokens", device)
batch = {**device, "identityType": "anonymous", "policyVersion": issued["policyVersion"]}
token = issued["analyticsToken"]
valid = []
invalid = []
for seq, (name, props) in enumerate(fixtures.items(), 1):
    event = {"eventName": name, "eventId": uid(), "schemaVersion": 1, "occurredAt": now.isoformat(),
             "sessionId": session, "sequence": seq, "screenName": "FactoryDetail", "pageViewId": page,
             "properties": props}
    valid.append(event)
    bad = {**event, "eventId": uid()}
    if props:
        bad["properties"] = {key: value for key, value in props.items() if key != next(iter(props))}
    else:
        del bad["occurredAt"]
    invalid.append(bad)

def submit(events):
    return post("events/report", {**batch, "batchId": uid(), "events": events}, token)["results"]

for events, expected in ((valid, "accepted"), (valid, "duplicate"), (invalid, "rejected")):
    outcomes = submit(events)
    assert len(outcomes) == len(events) and all(row["status"] == expected for row in outcomes), list(zip(fixtures,outcomes))

broker = {"bootstrap.servers": settings.kafka_bootstrap_servers}
producer = Producer({**broker, "acks": "all", "delivery.timeout.ms": 5000})
reader = Consumer({**broker, "group.id": "event-catalog-"+tag, "enable.auto.commit": False})
dlq, corpus = "analytics.event.dlq.v1", "analytics.corpus.status"
topics = [TopicPartition(name, 0) for name in (dlq, corpus)]
reader.assign([TopicPartition(p.topic,p.partition,reader.get_watermark_offsets(p,timeout=5)[1]) for p in topics])
business = {
    "business_view_recorded": {**target, "viewRef": "view_"+tag},
    "favorite_changed": {**target, "action": "add"},
    "inquiry_created": {"inquiryRef": "inq_"+tag, "companyId": target["targetId"]},
    "message_send_succeeded": {"dialogRef": actor.opaque("dialog", tag), "messageRef": "msg_"+tag,
                               "messageType": "text", "senderKind": "human"},
    "translate_sync_completed": {"recordRef": "sync_"+tag, "mode": "text", "outcome": "success"},
    "account_registered": {"channel": "phone"},
}
good_ids, bad_offsets, deliveries = {}, set(), []
for name, properties in business.items():
    event_id = uid()
    good_ids[event_id] = name
    for mode in ("valid", "duplicate", "invalid"):
        base = {"eventId": uid() if mode == "invalid" else event_id, "eventName": name, "schemaVersion": 1,
                "occurredAt": now.isoformat(), "businessDate": day.isoformat(), "source": "server",
                "producer": "catalog-smoke", "identityType": "authenticated", "actorKey": actor.opaque("user",tag),
                "aggregateRef": name+"_"+tag, "properties": properties if mode != "invalid" else {}}
        wire = encode({**base, "receivedAt": now.isoformat(), "payloadHash": hashlib.sha256(encode(base)).hexdigest()})
        def delivered(error, message, mode=mode):
            deliveries.append(error)
            if error is None and mode == "invalid":
                bad_offsets.add((message.partition(), message.offset()))
        producer.produce("analytics.event", key=base["aggregateRef"], value=wire,
                         headers={"analytics-signature": actor.opaque("kafka",wire.decode())}, on_delivery=delivered)
assert producer.flush(10) == 0 and len(deliveries) == 18 and all(e is None for e in deliveries)

dsn = settings.postgres_dsn.get_secret_value()
ids = {event["eventId"]: event["eventName"] for event in valid}
with psycopg.connect(dsn) as pg:
    receipts = pg.execute("SELECT event_id,state FROM analytics_ingest_receipt WHERE event_id=ANY(%s::uuid[])",
                          (list(ids),)).fetchall()
    assert len(receipts) == 19 and all(state == "published" for _, state in receipts)
all_ids = {**ids, **good_ids}
with clickhouse_client(settings) as ch:
    def landed():
        data = ch.query("SELECT event_id,any(event_name),count() FROM events_raw WHERE event_id IN {ids:Array(UUID)} GROUP BY event_id",
                        parameters={"ids": list(all_ids)}).result_rows
        return data if len(data) == len(all_ids) and all(n >= 2 for i, _, n in data if str(i) in good_ids) else None
    assert {str(i): n for i, n, _ in wait(landed)} == all_ids

with MongoClient(settings.mongo_uri.get_secret_value(),uuidRepresentation="standard") as mongo:
    db = mongo[settings.mongo_database]
    dataset = {"_id": ObjectId(), "datasetCode": "catalog_"+tag, "revision": 1, "status": "active",
               "purposeCode": "test", "allowedLanguages": ["zh", "uz"], "policyRef": "synthetic",
               "retentionDays": 1, "createdAt": now}
    item = {"_id": ObjectId(), "datasetId": dataset["_id"], "revision": 1, "reviewStatus": "pending",
            "sourceLang": "zh", "targetLang": "uz", "sourceRef": tag, "text": "synthetic",
            "policyRef": "synthetic", "createdAt": now, "expiresAt": now+timedelta(days=1)}
    db.corpus_datasets.insert_one(dataset)
    db.corpus_items.insert_one(item)
    params = {"itemId": str(item["_id"]), "expectedItemRevision": 1, "status": "approved",
              "reasonCode": "reviewPassed", "ticketId": "CATALOG-"+tag}
    try:
        ReviewDecision.model_validate({k: v for k, v in params.items() if k != "reasonCode"})
    except ValueError:
        pass
    else:
        raise AssertionError("Missing review reason was accepted")
    body = ReviewDecision.model_validate(params)
    with mongo.start_session() as handle:
        for _ in range(2):
            with handle.start_transaction():
                row = db.corpus_items.find_one({"_id": item["_id"]}, session=handle)
                change_review(db,handle,row,dataset,body,"catalog-smoke",tag,now)
    query = {"aggregateRef": str(item["_id"]), "topic": corpus}
    out = db.analytics_mongo_outbox.find_one(query)
    assert out and db.analytics_mongo_outbox.count_documents(query) == 1

    seen_dlq = set()
    seen_corpus = False
    def delivered_events():
        global seen_corpus
        msg = reader.poll(0.5)
        if msg and not msg.error():
            payload = json.loads(msg.value())
            if msg.topic() == dlq:
                seen_dlq.add((payload["partition"], int(payload["offset"])))
            elif payload.get("eventId") == str(out["_id"]):
                assert payload["eventName"] == "corpus_asset_status_changed" and payload["status"] == "approved"
                seen_corpus = True
        return seen_corpus and len(bad_offsets & seen_dlq) == 6
    try:
        wait(delivered_events)
        wait(lambda: db.analytics_mongo_outbox.find_one({**query,"status":"published"}))
    finally:
        reader.close()

print(json.dumps({"passed": 26, "app": list(fixtures), "business": list(business),
                  "corpus": "corpus_asset_status_changed", "businessDLQ": len(bad_offsets & seen_dlq)}))
