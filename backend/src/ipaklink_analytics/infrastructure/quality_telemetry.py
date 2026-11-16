from datetime import UTC, datetime
from hashlib import sha256
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition

OBSERVATION_SQL = """
INSERT INTO analytics_quality_observation
    (observation_key,platform,app_version,rejected,duplicates,rate_limited,retry,dlq)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (observation_key) DO NOTHING
"""

FIELDS = ("rejected","duplicates","rateLimited","retry","dlq")


async def record_collection(pool,key,platform,version,counts,rate_limited):
    if not (rate_limited or any(counts.get(status,0) for status in ("rejected","duplicate","retry"))):
        return
    async with pool.connection() as pg:
        await pg.execute(OBSERVATION_SQL,
                         (key,platform,version,counts.get("rejected",0),counts.get("duplicate",0),
                          int(rate_limited),counts.get("retry",0),0))


def record_dlq(pg,message):
    key = f"dlq:{message.topic()}:{message.partition()}:{message.offset()}"
    pg.execute(OBSERVATION_SQL,(key,None,None,0,0,0,0,1))


def sample_kafka(settings,pg):
    topic = "analytics.event"
    consumer = Consumer({"bootstrap.servers":settings.kafka_bootstrap_servers,
                         "group.id":settings.kafka_consumer_group,
                         "enable.auto.commit":False,"enable.auto.offset.store":False})
    try:
        meta = consumer.list_topics(topic=topic,timeout=5).topics.get(topic)
        if meta is None or meta.error or not meta.partitions:
            raise RuntimeError("Kafka topic metadata is unavailable")
        offsets = consumer.committed([TopicPartition(topic,p) for p in sorted(meta.partitions)],timeout=5)
        positions = []
        for offset in offsets:
            if offset.error:
                raise RuntimeError("Partition commit metadata is unavailable")
            low,high = consumer.get_watermark_offsets(TopicPartition(topic,offset.partition),timeout=5)
            next_offset = min(high,max(low,offset.offset))
            positions.append((offset.partition,next_offset,high))
        now = datetime.now(UTC)
        with pg.transaction(), pg.cursor() as cursor:
            for partition,next_offset,high in positions:
                window = pg.execute(
                    """SELECT count(*) AS samples, min(next_offset) AS first_offset,max(next_offset) AS last_offset,
                              min(sampled_at)<%s-interval '4 minutes' AS enough_history
                       FROM analytics_kafka_lag_sample
                       WHERE topic=%s AND partition=%s AND sampled_at>%s-interval '6 minutes'""",
                    (now,topic,partition,now),
                ).fetchone()
                stalled = False if high==next_offset else None
                if high>next_offset and window["enough_history"] and window["samples"]>=2:
                    stalled = window["first_offset"]==window["last_offset"]==next_offset
                cursor.execute(
                    """INSERT INTO analytics_kafka_lag_sample(sampled_at,topic,partition,next_offset,high_offset,stalled)
                       VALUES (%s,%s,%s,%s,%s,%s)""",(now,topic,partition,next_offset,high,stalled),
                )
    finally:
        consumer.close()

def enqueue_quality_alerts(settings, pg):
    rows = pg.execute(
        """SELECT sampled_at,sum(high_offset-next_offset) AS lag,count(*) FILTER(WHERE stalled) AS stalled
           FROM analytics_kafka_lag_sample
           WHERE sampled_at>now()-interval '3 minutes'
           GROUP BY sampled_at ORDER BY sampled_at DESC LIMIT 2""",
    ).fetchall()
    if len(rows)!=2:
        return
    stamp = rows[0]["sampled_at"]
    window = int(stamp.timestamp())//300
    rules = (
        ("kafkaLag",all(row["lag"]>=settings.quality_lag_threshold for row in rows)),
        ("kafkaStalled",all(row["stalled"]>0 for row in rows)),
    )
    for name,match in rules:
        if match:
            digest = sha256(f"{name}:{window}".encode()).hexdigest()
            pg.execute(
                """INSERT INTO analytics_alert_delivery(delivery_id,alert_key,fact_code,channel_code,destination_ref)
                   VALUES (%s,%s,%s,'localLog','analytics-jobs')
                   ON CONFLICT (alert_key,channel_code,destination_ref) DO NOTHING""",
                (uuid4(),digest,name),
            )
