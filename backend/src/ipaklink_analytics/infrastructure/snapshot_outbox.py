import json
from datetime import UTC
from uuid import uuid4

from confluent_kafka import Producer
from psycopg.types.json import Jsonb

TOPIC = "analytics.dashboard.updated.v1"
SCOPE = "analytics"
REPORTS = ("overview","pages","features","funnels","retention","quality","cardImpressions")


def publication(snapshot, generated, watermark, metric_version=1):
    event_id = uuid4()
    payload = {"eventId":str(event_id),"snapshotId":snapshot,"reportScope":SCOPE,
               "reports":list(REPORTS),"metricVersion":metric_version,
               "generatedAt":generated.astimezone(UTC).isoformat().replace("+00:00","Z"),
               "sourceWatermark":watermark.astimezone(UTC).isoformat().replace("+00:00","Z")}
    return event_id,snapshot,TOPIC,Jsonb(payload)


INSERT = """
INSERT INTO analytics_outbox(event_id,aggregate_id,topic,payload_safe)
VALUES (%s,%s,%s,%s)
"""


class SnapshotRelay:
    def __init__(self, bootstrap):
        self.producer = Producer({"bootstrap.servers":bootstrap,"acks":"all","enable.idempotence":True,
                                  "delivery.timeout.ms":5000})

    def send_next(self, pg, logger):
        lease = uuid4()
        row = pg.execute(
            """WITH candidate AS (
                   SELECT event_id FROM analytics_outbox
                   WHERE topic=%s AND status='pending' AND available_at<=now()
                     AND (lease_until IS NULL OR lease_until<=now())
                   ORDER BY available_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED
               )
               UPDATE analytics_outbox o SET lease_token=%s,lease_until=now()+interval '2 minutes',
                   attempts=attempts+1 FROM candidate WHERE o.event_id=candidate.event_id
               RETURNING o.event_id,o.payload_safe,o.attempts""",
            (TOPIC,lease),
        ).fetchone()
        if row is None:
            return False
        try:
            errors = []
            self.producer.produce(
                TOPIC,key=row["payload_safe"]["reportScope"],value=json.dumps(row["payload_safe"]),
                headers={"schemaVersion":"1","contentType":"application/json",
                         "traceId":str(row["event_id"]),"producer":"analytics-jobs"},
                on_delivery=lambda error,_:errors.append(error),
            )
            if self.producer.flush(6) != 0 or errors != [None]:
                raise RuntimeError("Snapshot delivery unconfirmed")
        except Exception as error:
            logger.warning("snapshot.notificationFailed",extra={"errorType":type(error).__name__})
            pg.execute(
                """UPDATE analytics_outbox SET status=%s,available_at=now()+%s*interval '1 second',
                   lease_token=NULL,lease_until=NULL WHERE event_id=%s AND lease_token=%s""",
                ("failed" if row["attempts"]>=5 else "pending",min(300,2**min(row["attempts"],8)),row["event_id"],lease),
            )
        else:
            pg.execute(
                """UPDATE analytics_outbox SET status='published',published_at=now(),lease_token=NULL,lease_until=NULL
                   WHERE event_id=%s AND lease_token=%s""",(row["event_id"],lease),
            )
        return True
