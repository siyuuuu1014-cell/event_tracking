from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Request, Response

from ipaklink_analytics.api.admin_login import PREFIX
from ipaklink_analytics.api.event_reports import ReportRange, load_events, report_meta
from ipaklink_analytics.api.jobs import valid_dates
from ipaklink_analytics.api.reporting import Page, Size, require
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.infrastructure.admin_access import AccessDenied, QUALITY_READ
from ipaklink_analytics.infrastructure.quality_telemetry import FIELDS
from ipaklink_analytics.infrastructure.report_store import WarehouseQueryError

router = APIRouter(prefix=PREFIX,tags=["analytics-quality"])


def percentile(values):
    if not values:
        return None
    values.sort()
    return values[(len(values)*95-1)//100]


def summarize(receipts, events, counters, lag, recorded):
    delays = [max(0,int((row["ingested"]-row["received"]).total_seconds()*1000)) for row in events]
    result = {"accepted":len(receipts),"late":sum((row["received"]-row["time"]).total_seconds()>900 for row in events),
              "p95IngestDelayMs":percentile(delays),
              "lagRecords":lag[0] if lag else None,"stalePartitions":lag[1] if lag else None}
    result.update({key:value if recorded else None for key,value in zip(FIELDS,counters,strict=True)})
    return result


async def observations(request,query,snapshot,source,granularity):
    if not snapshot or source=="server":
        return None,[],[]
    left,right = (datetime.combine(value,datetime.min.time(),BUSINESS_TIMEZONE)
                  for value in (query.start,query.end))
    cutoff=snapshot["source_watermark"]
    group = ("date_trunc('day',recorded_at,'Asia/Tashkent')" if granularity=="day"
             else "date_trunc('minute',recorded_at,'UTC')")
    lag_group = ("date_trunc('day',sampled_at,'Asia/Tashkent')" if granularity=="day"
                 else "date_trunc('minute',sampled_at,'UTC')")
    async with request.app.state.pg.connection() as pg:
        since = (await (await pg.execute(
            "SELECT applied_at FROM analytics_schema_migration WHERE migration_id='009_quality_telemetry.sql'",
        )).fetchone())[0]
        counters = await (await pg.execute(
            f"""SELECT {group} AS bucket,sum(rejected),sum(duplicates),sum(rate_limited),sum(retry),sum(dlq)
                FROM analytics_quality_observation WHERE recorded_at >= %s AND recorded_at < %s AND recorded_at<=%s
                AND (%s::text IS NULL OR platform=%s) AND (%s::text IS NULL OR app_version=%s)
                GROUP BY 1 ORDER BY 1""",(left,right,cutoff,query.platform,query.platform,query.version,query.version),
        )).fetchall()
        lag=[]
        if query.platform is None and query.version is None:
            lag = await (await pg.execute(
                f"""WITH samples AS (
                        SELECT {lag_group} AS bucket,sampled_at,sum(high_offset-next_offset) AS lag,
                               CASE WHEN count(*)=count(stalled) THEN count(*) FILTER (WHERE stalled) END AS stalled
                        FROM analytics_kafka_lag_sample WHERE sampled_at>=%s AND sampled_at<%s AND sampled_at<=%s
                        GROUP BY 1,2
                    )
                    SELECT DISTINCT ON(bucket) bucket,lag,stalled FROM samples ORDER BY bucket,sampled_at DESC""",
                (left,right,cutoff),
            )).fetchall()
    return since,counters,lag


@router.get("/quality",operation_id="IPK_ANL_17")
async def quality(request: Request,response: Response,query: Annotated[ReportRange,Depends()],
                  source: Literal["app","server"]|None=None,granularity: Literal["day","minute"]="day",
                  page: Page=1,size: Size=20):
    await require(request,QUALITY_READ)
    if query.region is not None:
        raise AccessDenied
    valid_dates(query.start,query.end,1 if granularity=="minute" else 31)
    response.headers["Cache-Control"]="private, no-store"
    snapshot,coverage,events = await load_events(request,query.snapshot,query.start,query.end,None)
    events = [r for r in events if (source is None or r["source"]==source) and
              (query.platform is None or r["platform"]==query.platform) and
              (query.version is None or r["version"]==query.version)]
    receipts = []
    if snapshot and source!="server":
        async with request.app.state.pg.connection() as pg:
            receipts = await (await pg.execute(
                """SELECT event_id,first_received_at,(canonical_payload->>'businessDate')::date
                   FROM analytics_ingest_receipt WHERE state='published' AND published_at<=%s
                     AND canonical_payload->>'businessDate'>=%s AND canonical_payload->>'businessDate'<%s
                     AND (%s::text IS NULL OR canonical_payload->>'platform'=%s)
                     AND (%s::text IS NULL OR canonical_payload->>'appVersion'=%s) LIMIT 100001""",
                (snapshot["source_watermark"],str(query.start),str(query.end),query.platform,query.platform,
                 query.version,query.version),
            )).fetchall()
        if len(receipts)>100000:
            raise WarehouseQueryError
        receipts = [r for r in receipts if r[2] in coverage]
    warehouse_daily,source_daily = defaultdict(int),defaultdict(int)
    event_buckets,receipt_buckets = defaultdict(list),defaultdict(list)
    def bucket(day, instant):
        if granularity=="day":
            return datetime.combine(day,datetime.min.time(),BUSINESS_TIMEZONE).astimezone(UTC)
        return instant.astimezone(UTC).replace(second=0,microsecond=0)
    for event in events:
        event_buckets[bucket(event["date"],event["received"])].append(event)
        if event["source"]=="app":
            warehouse_daily[event["date"]]+=1
    for row in receipts:
        receipt_buckets[bucket(row[2],row[1])].append(row)
        source_daily[row[2]]+=1
    observed,counter_samples,lag_samples = await observations(request,query,snapshot,source,granularity)
    counters = {row[0]:row[1:] for row in counter_samples}
    lags = {row[0]:row[1:] for row in lag_samples}
    zeros = (0,)*len(FIELDS)
    step = timedelta(days=1) if granularity=="day" else timedelta(minutes=1)
    cutoff = snapshot["source_watermark"] if snapshot else None
    series = [{"bucket":key,**summarize(receipt_buckets[key],event_buckets[key],counters.get(key,zeros),
                                        lags.get(key),observed is not None and cutoff>=observed and key+step>observed)}
              for key in sorted(receipt_buckets.keys()|event_buckets.keys()|counters.keys()|lags.keys())]
    reconciled = [{"factCode":"appEvents","date":day,"sourceCount":source_daily[day],
                   "warehouseCount":warehouse_daily[day],"delta":warehouse_daily[day]-source_daily[day],
                   "unattributedCount":None}
                  for day in sorted(source_daily.keys()|warehouse_daily.keys())] if source!="server" else []
    totals = tuple(sum(row[i] for row in counters.values()) for i in range(len(FIELDS)))
    end = datetime.combine(query.end,datetime.min.time(),BUSINESS_TIMEZONE)
    result = {**summarize(receipts,events,totals,lags[max(lags)] if lags else None,
                          observed is not None and cutoff>=observed and end>observed),"series":series,"list":reconciled[(page-1)*size:page*size],
              "page":page,"size":size,"total":len(reconciled),"meta":report_meta(snapshot,coverage,query)}
    return {"code":200,"message":"ok","data":result}
