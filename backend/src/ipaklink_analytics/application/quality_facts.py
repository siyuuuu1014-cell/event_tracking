from collections import defaultdict
from datetime import datetime, time

from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE

QUALITY_DATASET = "quality"
RECONCILIATION_DATASET = "reconciliation"

EVENTS = """
SELECT * FROM events_raw
WHERE business_date>={fromDate:Date} AND business_date<{toDate:Date}
  AND ingested_at<={watermark:DateTime64(3,'UTC')}
  AND arrayExists(c -> c.1=topic AND c.2=kafka_partition AND kafka_offset<=c.3,
       {watermarks:Array(Tuple(String,UInt32,UInt64))})
ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
LIMIT 1 BY event_id
"""


def build_quality(pg,ch,params):
    start,end,cutoff,generation = (params[key] for key in ("fromDate","toDate","watermark","generation"))
    left,right = [datetime.combine(day,time.min,BUSINESS_TIMEZONE) for day in (start,end)]
    with pg.transaction():
        pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        receipt_counts = pg.execute(
            """SELECT canonical_payload->>'businessDate' AS day,count(*)
               FROM analytics_ingest_receipt WHERE state='published' AND published_at<=%s
               AND canonical_payload->>'businessDate'>=%s AND canonical_payload->>'businessDate'<%s
               GROUP BY 1""",(cutoff,str(start),str(end)),
        ).fetchall()
        accepted = pg.execute(
            """SELECT date_trunc('minute',first_received_at,'UTC'),canonical_payload->>'appVersion',
                      canonical_payload->>'platform',count(*)
               FROM analytics_ingest_receipt WHERE state='published' AND published_at<=%s
                 AND first_received_at>=%s AND first_received_at<%s GROUP BY 1,2,3""",(cutoff,left,right),
        ).fetchall()
        counters = pg.execute(
            """SELECT date_trunc('minute',recorded_at,'UTC'),app_version,platform,
                      sum(rejected),sum(rate_limited),sum(duplicates),sum(dlq)
               FROM analytics_quality_observation WHERE recorded_at>=%s AND recorded_at<%s AND recorded_at<=%s
               GROUP BY 1,2,3""",(left,right,cutoff),
        ).fetchall()
        lags = pg.execute(
            """SELECT DISTINCT ON(bucket) bucket,lag FROM (
                   SELECT date_trunc('minute',sampled_at,'UTC') AS bucket,sampled_at,sum(high_offset-next_offset) AS lag
                   FROM analytics_kafka_lag_sample WHERE sampled_at>=%s AND sampled_at<%s AND sampled_at<=%s
                   GROUP BY 1,2
               ) s ORDER BY bucket,sampled_at DESC""",(left,right,cutoff),
        ).fetchall()
    values = defaultdict(lambda:[0,0,0,0,0,0,None,None])
    for bucket,version,platform,count in accepted:
        values[(bucket,"collector","app",version or "unknown",platform or "unknown")][0] = count
    for bucket,version,platform,rejected,limited,duplicate,dlq in counters:
        row = values[(bucket,"collector","app",version or "unknown",platform or "unknown")]
        row[1],row[2],row[3],row[5] = rejected,limited,duplicate,dlq
    for bucket,lag in lags:
        values[(bucket,"kafka","app","unknown","unknown")][6] = lag
    warehouse = {}
    if params["watermarks"]:
        warehouse = dict(ch.query(
            "SELECT business_date,count() FROM ("+EVENTS+") WHERE source='app' GROUP BY business_date",
            parameters=params,settings={"max_execution_time":30},
        ).result_rows)
        for bucket,source,version,platform,late,p95 in ch.query(
            """SELECT toStartOfMinute(received_at),source,ifNull(app_version,'unknown'),ifNull(platform,'unknown'),
                      countIf(dateDiff('second',occurred_at,received_at)>900),
                      quantileExact(0.95)(greatest(0,dateDiff('millisecond',received_at,ingested_at)))
               FROM ("""+EVENTS+""")
               WHERE toDate(received_at,'Asia/Tashkent')>={fromDate:Date}
                 AND toDate(received_at,'Asia/Tashkent')<{toDate:Date}
               GROUP BY 1,2,3,4""",parameters=params,settings={"max_execution_time":30},
        ).result_rows:
            row=values[(bucket,"warehouse",source,version,platform)]
            row[4],row[7]=late,p95
    minute_rows = [(generation,*key,*row) for key,row in values.items()]
    if minute_rows:
        ch.insert("agg_quality_minute",minute_rows,column_names=[
            "generation","bucket_at","service","source","app_version","platform","accepted","rejected",
            "rate_limited","duplicate","late","dlq","lag_records","p95_ingest_delay_ms",
        ])
    source = {datetime.fromisoformat(day).date():count for day,count in receipt_counts}
    if source or warehouse:
        ch.insert("fact_reconciliation_day",[
            (generation,day,"appEvents","app",source.get(day,0),warehouse.get(day,0),
             warehouse.get(day,0)-source.get(day,0),None,cutoff)
            for day in sorted(source.keys()|warehouse.keys())
        ],column_names=["generation","business_date","fact_code","source","source_count","warehouse_count",
                        "delta","unattributed_count","source_watermark"])


def read_reconciliation(ch,pairs):
    return list(ch.query(
        """SELECT fact_code AS factCode,business_date AS date,source_count AS sourceCount,
                  warehouse_count AS warehouseCount,delta,unattributed_count AS unattributedCount
           FROM fact_reconciliation_day WHERE (business_date,generation) IN {pairs:Array(Tuple(Date,UInt64))}
           ORDER BY business_date,fact_code""",
        parameters={"pairs":pairs},settings={"max_execution_time":5},
    ).named_results())
