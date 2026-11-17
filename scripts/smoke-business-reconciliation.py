"""Compare synthetic upstream commits and real warehouse writes at a shared cutoff."""
import hashlib
import json
from datetime import UTC, datetime, timedelta
from time import monotonic, sleep
from uuid import uuid4

import psycopg
from confluent_kafka import Producer

from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE, encode
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.identity import IdentityService
from ipaklink_analytics.infrastructure.report_store import clickhouse_client

settings = Settings()
assert settings.environment == "local"
identity = IdentityService(settings)
tag = uuid4().hex
cutoff = datetime.now(UTC) + timedelta(minutes=2)
day = cutoff.astimezone(BUSINESS_TIMEZONE).date() - timedelta(days=1)
previous = day - timedelta(days=1)
before, after = cutoff - timedelta(minutes=4), cutoff + timedelta(seconds=1)
producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers, "acks": "all",
                     "delivery.timeout.ms": 5000})
events = {
    "inquiry_created": ("inquiryRef", {"companyId": "company_"+tag}),
    "message_send_succeeded": ("messageRef", {"dialogRef": identity.opaque("dialog",tag),
                                             "messageType": "text", "senderKind": "human"}),
    "business_view_recorded": ("viewRef", {"targetType": "company", "targetId": "company_"+tag}),
}
inquiry, message, view = events
upstream = [(name, name+"_common_"+tag, day, before) for name in events]
upstream += [(inquiry, inquiry+"_missing_"+tag, day, before), (view, view+"_late_"+tag, day, after),
             (view, view+"_previous_"+tag, previous, before)]
deliveries, ids = [], []

def send(name, scenario, business_day, available_at):
    key, fixed_properties = events[name]
    fact_id = f"{name}_{scenario}_{tag}"
    event_id = str(uuid4())
    timestamp = datetime.combine(business_day, datetime.min.time(), BUSINESS_TIMEZONE)
    base = {"eventId": event_id, "eventName": name, "schemaVersion": 1, "occurredAt": timestamp.isoformat(),
            "businessDate": business_day.isoformat(), "source": "server", "producer": "reconciliation-smoke",
            "identityType": "authenticated", "actorKey": identity.opaque("user",tag), "aggregateRef": f"{name}_{tag}",
            "properties": {**fixed_properties, key: fact_id}}
    wire = encode({**base, "receivedAt": available_at.isoformat(),
                   "payloadHash": hashlib.sha256(encode(base)).hexdigest()})
    producer.produce("analytics.event", key=fact_id, value=wire,
                     headers={"analytics-signature": identity.opaque("kafka",wire.decode())},
                     on_delivery=lambda error, _: deliveries.append(error))
    ids.append(event_id)

for name in events:
    send(name,"common",day,before)
send(inquiry,"common",day,before)
send(message,"extra",day,before)
send(view,"late",day,after)
send(view,"previous",previous,before)
assert producer.flush(12)==0 and len(deliveries)==len(ids) and not any(deliveries)

with psycopg.connect(settings.postgres_dsn.get_secret_value()) as pg:
    pg.execute("CREATE TEMP TABLE source_facts(event_name text, fact_key text, business_date date, committed_at timestamptz)")
    with pg.cursor() as cursor:
        cursor.executemany("INSERT INTO source_facts VALUES (%s,%s,%s,%s)",upstream)
    source = set(pg.execute(
        "SELECT DISTINCT event_name,fact_key FROM source_facts WHERE business_date=%s AND committed_at<=%s",
        (day,cutoff),
    ).fetchall())
with clickhouse_client(settings) as ch:
    deadline = monotonic()+25
    while monotonic()<deadline:
        landed = ch.query("SELECT uniqExact(event_id) FROM events_raw WHERE event_id IN {ids:Array(UUID)}",
                          parameters={"ids": ids}).first_row[0]
        if landed == len(ids):
            break
        sleep(0.25)
    assert landed == len(ids),(landed,len(ids))
    warehouse = set(ch.query(
        """SELECT DISTINCT event_name, multiIf(
               event_name='inquiry_created',JSONExtractString(properties_json,'inquiryRef'),
               event_name='message_send_succeeded',JSONExtractString(properties_json,'messageRef'),
               JSONExtractString(properties_json,'viewRef')) AS fact_key
           FROM events_raw
           WHERE event_id IN {ids:Array(UUID)} AND business_date={day:Date} AND source='server'
           AND received_at<={cutoff:DateTime64(3,'UTC')} AND ingested_at<={cutoff:DateTime64(3,'UTC')}""",
        parameters={"ids": ids,"day": day,"cutoff":cutoff},
    ).result_rows)

expected = {inquiry:(2,1,-1,1,0), message:(1,2,1,0,1), view:(1,1,0,0,0)}
for name in events:
    a = {key for event,key in source if event==name}
    b = {key for event,key in warehouse if event==name}
    actual = (len(a),len(b),len(b)-len(a),len(a-b),len(b-a))
    assert actual==expected[name],(name,actual,expected[name])
    print(json.dumps(dict(factCode=name, date=str(day), watermark=cutoff.isoformat(),
                          sourceCount=actual[0],warehouseCount=actual[1],delta=actual[2],
                          missing=actual[3],excess=actual[4])))
