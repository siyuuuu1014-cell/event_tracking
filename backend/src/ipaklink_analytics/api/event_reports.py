import json
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response

from ipaklink_analytics.api.admin_login import PREFIX
from ipaklink_analytics.api.collection import RequestProblem
from ipaklink_analytics.api.config_admin import audit, require_ticket
from ipaklink_analytics.api.reporting import Page, Size, SnapshotId, require
from ipaklink_analytics.application.activity_facts import RETENTION_DATASET, TRANSLATE_DATASET, query_retention
from ipaklink_analytics.application.business_facts import CHAT_DATASET, chat_reply_metrics
from ipaklink_analytics.application.event_reports import (
    ACTIVE, FEATURE_EVENTS, GROUPS, feature_metrics, funnel, group_key, matches,
    metric, overview_metrics, ratio, retention, translation_sync_metric,
)
from ipaklink_analytics.application.page_reports import business_today, date_window, meta
from ipaklink_analytics.application.search_facts import SEARCH_DATASET
from ipaklink_analytics.application.warehouse_metrics import (
    METRIC_DATASET, daily_overview, query_feature, query_search_funnel,
)
from ipaklink_analytics.infrastructure.admin_access import AccessDenied, REPORT_READ, SESSION_READ
from ipaklink_analytics.infrastructure.report_store import DATASET, WarehouseQueryError

router = APIRouter(prefix=PREFIX, tags=["analytics-reporting"])
MAX_EVENTS = 100000


class ReportRange:
    def __init__(
        self, fromDate: date | None = None, toDate: date | None = None,
        snapshotId: SnapshotId = None, platform: Literal["ios","android"] | None = None,
        regionId: Annotated[int | None, Query(ge=1,le=4294967295)] = None,
        appVersion: Annotated[str | None, Query(min_length=1,max_length=40)] = None,
    ) -> None:
        self.now = datetime.now(UTC)
        try:
            self.start, self.end = date_window(fromDate, toDate, self.now)
        except ValueError as error:
            raise RequestProblem(400, "analytics.invalidRequest") from error
        self.snapshot = snapshotId
        self.platform, self.region, self.version = platform, regionId, appVersion


def select_events(client, start, end, caps, names, regions, session):
    clauses = [
        "business_date>={start:Date}", "business_date<{end:Date}",
        "arrayExists(c -> c.1=business_date AND c.2=topic AND c.3=kafka_partition "
        "AND kafka_offset<=c.4 AND ingested_at<=c.5, "
        "{caps:Array(Tuple(Date,String,UInt32,UInt64,DateTime64(3,'UTC')))})",
    ]
    args = {"start":start,"end":end,"caps":caps,"limit":MAX_EVENTS+1}
    if names:
        clauses.append("event_name IN {names:Array(String)}")
        args["names"] = sorted(names)
    if regions:
        clauses.append("region_id IN {regions:Array(UInt32)}")
        args["regions"] = sorted(regions)
    if session:
        clauses.append("session_id={session:UUID}")
        args["session"] = session
    where = " AND ".join(clauses)
    rows = list(client.query(
        f"""SELECT event_id AS id,event_name AS name,business_date AS date,occurred_at AS time,
                   actor_key AS actor,identity_type AS identity,source,platform,app_version AS version,
                   region_id AS region,received_at AS received,ingested_at AS ingested,sequence,screen_name AS screen,page_view_id AS page,properties_json AS payload
            FROM (SELECT * FROM events_raw WHERE {where}
                  ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
                  LIMIT 1 BY event_id)
            ORDER BY occurred_at,sequence,event_id LIMIT {{limit:UInt32}}""",
        parameters=args, settings={"max_execution_time":6},
    ).named_results())
    if len(rows) > MAX_EVENTS:
        raise WarehouseQueryError
    for row in rows:
        for field in ("time", "received", "ingested"):
            if row[field].tzinfo is None:
                row[field] = row[field].replace(tzinfo=UTC)
        row["props"] = json.loads(row.pop("payload"))
    return rows


async def load_events(request, snapshot_id, start, end, names, regions=None, session=None):
    store = request.app.state.reports
    snapshot, _ = await store.snapshot(snapshot_id)
    if not snapshot:
        return None, set(), []
    async with request.app.state.pg.connection() as conn:
        values = await (await conn.execute(
            """SELECT b.date_key,b.source_offsets FROM analytics_report_snapshot_build s
               JOIN analytics_report_build b USING(dataset,date_key,generation)
               WHERE s.snapshot_id=%s AND s.dataset=%s AND s.date_key >= %s AND s.date_key < %s""",
            (snapshot["snapshot_id"],DATASET,start,end),
        )).fetchall()
    caps = [(day, row["topic"],row["partition"],int(row["offset"]),datetime.fromisoformat(data["ingestedThrough"]))
            for day,data in values for row in data.get("topics",[])]
    results = await store.query(select_events,start,end,caps,names,regions,session) if caps else []
    return snapshot, {day for day,_ in values}, results


async def authorize(request, response, query):
    actor = await require(request, REPORT_READ)
    if query.region is not None and not actor.all_regions and query.region not in actor.region_ids:
        raise AccessDenied
    response.headers["Cache-Control"] = "private, no-store"
    return None if actor.all_regions else actor.region_ids


def report_meta(snapshot, coverage, query):
    days = sum(query.start <= day < query.end for day in coverage)
    return meta(snapshot,query.start,query.end,days/(query.end-query.start).days if snapshot else None,query.now)


@router.get("/overview", operation_id="IPK_ANL_11")
async def overview(request: Request,response: Response,query: Annotated[ReportRange,Depends()],
                   includePrevious: bool=True):
    regions = await authorize(request,response,query)
    length = query.end-query.start
    previous_start = query.start-length if includePrevious else query.start
    names = ACTIVE | {"business_view_recorded","inquiry_created","translate_completed"}
    snapshot, coverage, events = await load_events(
        request,query.snapshot,previous_start-timedelta(days=29),query.end,names,regions,
    )
    events = [e for e in events if matches(e,query.platform,query.region,query.version,regions)]
    cards = overview_metrics(events,query.start,query.end,coverage)
    if includePrevious:
        for card, previous in zip(cards,overview_metrics(events,previous_start,query.start,coverage),strict=True):
            before, current = previous["value"],card["value"]
            card["previousValue"] = before
            if card["unit"]=="ratio":
                card["differencePercentagePoints"] = (current-before)*100 if current is not None and before is not None else None
            else:
                card["relativeChange"] = (current-before)/before if before and current is not None else None
    series = []
    for day in sorted(day for day in coverage if query.start <= day < query.end):
        series.extend({"date":day, **point} for point in sorted(
            overview_metrics(events,day,day+timedelta(days=1),coverage),key=lambda item:item["metricCode"],
        ))
    if snapshot and regions is None and not any((query.platform,query.region,query.version)):
        store = request.app.state.reports
        _, pairs = await store.snapshot(snapshot["snapshot_id"],(query.start,query.end),dataset=METRIC_DATASET)
        built = await store.query(daily_overview,sorted(pairs)) if pairs else {}
        for row in series:
            values = built.get((row["date"],row["metricCode"]))
            if values is None or row["status"]=="suppressed":
                continue
            value,top,bottom = values
            adjusted = metric(row["metricCode"],ratio(top,bottom),"ratio",numerator=top,denominator=bottom) if top is not None else metric(
                row["metricCode"],value,small=value is not None and 0<value<20,
            )
            row.update(adjusted)
    return {"code":200,"message":"ok","data":{"cards":cards,"series":series,"meta":report_meta(snapshot,coverage,query)}}


@router.get("/features", operation_id="IPK_ANL_13")
async def features(
    request: Request,response: Response,query: Annotated[ReportRange,Depends()],
    feature: Literal["search","chat","translate","favorite"],groupBy: str | None=None,
    scope: Literal["all","company","product","user"] | None=None,
    mode: Literal["text","dialog","image","file"] | None=None,
    sourceLang: Annotated[str | None,Query(max_length=20)]=None,targetLang: Annotated[str | None,Query(max_length=20)]=None,
    errorCode: Annotated[str | None,Query(max_length=80)]=None,page: Page=1,size: Size=20,
):
    regions = await authorize(request,response,query)
    if ((groupBy is not None and groupBy not in GROUPS[feature])
        or (scope is not None and feature!="search")
        or (any(x is not None for x in (mode,sourceLang,targetLang)) and feature!="translate")
        or (errorCode is not None and feature not in {"search","translate"})):
        raise RequestProblem(400,"analytics.invalidRequest")
    warehouse_dataset = {"search": SEARCH_DATASET,"translate": TRANSLATE_DATASET}.get(feature)
    store = request.app.state.reports
    snapshot,pairs = await store.snapshot(query.snapshot,(query.start,query.end),dataset=warehouse_dataset) if warehouse_dataset else (None,[])
    filters = {"scope":scope,"mode":mode,"sourceLang":sourceLang,"targetLang":targetLang,"errorCode":errorCode}
    if len(pairs)==(query.end-query.start).days:
        coverage = {day for day,_ in pairs}
        dimensions = {"platform":query.platform,"regionId":query.region,"appVersion":query.version,
                      "allowedRegions":sorted(regions) if regions is not None else None,**filters}
        data = await store.cached_query(snapshot,query_feature,sorted(pairs),feature,dimensions,groupBy,page,size)
    else:
        snapshot,coverage,rows = await load_events(
            request,snapshot["snapshot_id"] if snapshot else query.snapshot,
            query.start,query.end,FEATURE_EVENTS[feature],regions,
        )
        rows = [row for row in rows if matches(row,query.platform,query.region,query.version,regions)
                and all(value is None or row["props"].get(key)==value for key,value in filters.items())]
        daily, groups = {}, {}
        for row in rows:
            daily.setdefault(row["date"],[]).append(row)
            if groupBy and (key := group_key(row,groupBy)):
                groups.setdefault(key,[]).append(row)
        items = [{"groupKey":key,**m} for key,values in sorted(groups.items())
                 for m in sorted(feature_metrics(values,feature),key=lambda record:record["metricCode"])]
        data = {"summary":feature_metrics(rows,feature) if coverage else [],
                "series":[{"date":day,**m} for day,values in sorted(daily.items()) for m in feature_metrics(values,feature)],
                "list":items[(page-1)*size:page*size],"total":len(items)}
    if feature=="translate" and snapshot and not any((sourceLang,targetLang,errorCode)):
        _,_,sync_rows = await load_events(request,snapshot["snapshot_id"],query.start,query.end,
                                          {"translate_sync_completed"},regions)
        sync_rows = [row for row in sync_rows if row["source"]=="server"
                     and matches(row,query.platform,query.region,query.version,regions)
                     and (mode is None or row["props"].get("mode")==mode)]
        if sync_rows:
            by_day = {}
            for row in sync_rows:
                by_day.setdefault(row["date"],[]).append(row)
            data = {**data,"summary":[*data["summary"],translation_sync_metric(sync_rows)],
                    "series":sorted([*data["series"],*({"date":day.isoformat(),**translation_sync_metric(rows)}
                                                      for day,rows in by_day.items())],
                                    key=lambda row:(str(row["date"]),row["metricCode"]))}
    if feature=="chat" and snapshot:
        _, reply_pairs = await store.snapshot(snapshot["snapshot_id"],(query.start,query.end),dataset=CHAT_DATASET)
        refs = sorted({(row["props"]["dialogRef"],row["props"]["messageRef"]) for row in rows
                       if row["name"]=="message_send_succeeded" and row["source"]=="server"})
        if reply_pairs and refs:
            data["summary"].extend(await store.cached_query(snapshot,chat_reply_metrics,sorted(reply_pairs),refs))
    async with request.app.state.pg.connection() as pg:
        since = (await (await pg.execute(
            "SELECT min(activated_at) FROM analytics_event_schema WHERE event_name=ANY(%s) AND activated_at IS NOT NULL",
            (list(FEATURE_EVENTS[feature]),),
        )).fetchone())[0]
    return {"code":200,"message":"ok","data":{**data,"feature":feature,"page":page,"size":size,
            "coverageStartDate":business_today(since) if since else None,"meta":report_meta(snapshot,coverage,query)}}


@router.get("/funnels/{funnelCode}", operation_id="IPK_ANL_14")
async def funnels(
    request: Request,response: Response,query: Annotated[ReportRange,Depends()],
    funnelCode: Literal["F01","F02"],entrySource: Annotated[str|None,Query(max_length=40)]=None,
    targetType: Literal["company","product"]|None=None, searchScope: Literal["all","company","product"]|None=None,
):
    regions = await authorize(request,response,query)
    if (query.end-query.start).days>31 or (funnelCode=="F01" and (targetType or searchScope)):
        raise RequestProblem(400,"analytics.invalidRequest")
    names = FEATURE_EVENTS["search"] | {"entity_detail_view"} if funnelCode=="F02" else {
        "entity_detail_view","contact_supplier_click","inquiry_submit","inquiry_created",
    }
    store = request.app.state.reports
    snapshot,pairs = (await store.snapshot(query.snapshot,(query.start,query.end),dataset=SEARCH_DATASET)
                      if funnelCode=="F02" and searchScope in {"company","product"} and not (entrySource or targetType)
                      else (None,[]))
    if len(pairs)==(query.end-query.start).days:
        coverage = {day for day,_ in pairs}
        filters = {"scope":searchScope,"platform":query.platform,"regionId":query.region,"appVersion":query.version,
                   "allowedRegions":sorted(regions) if regions is not None else None}
        data = await store.cached_query(snapshot,query_search_funnel,sorted(pairs),filters)
    else:
        snapshot,coverage,rows = await load_events(
            request,snapshot["snapshot_id"] if snapshot else query.snapshot,
            query.start,query.end+timedelta(days=1),names,regions,
        )
        data = funnel(rows,funnelCode,query.start,query.end,entrySource,targetType,searchScope,
                      lambda row:matches(row,query.platform,query.region,query.version,regions))
    if 0 < data["steps"][0]["uniqueActors"] < 20:
        steps = [dict(step,count=None,uniqueActors=None,stepRate=None,totalRate=None,p50FromPreviousMs=None)
                 for step in data["steps"]]
        data = dict(data,steps=steps,businessCount=None,attributedCount=None,attributionCoverage=None,
                    attributionReasons=[],zeroResultCount=None,failedCount=None)
    return {"code":200,"message":"ok","data":{**data,"meta":report_meta(snapshot,coverage,query)}}


@router.get("/retention", operation_id="IPK_ANL_15")
async def retained(request: Request,response: Response,query: Annotated[ReportRange,Depends()],
                   offsetDays: Annotated[str,Query(pattern=r"^(1|7|30)(,(1|7|30)){0,2}$")]="1,7,30",
                   page: Page=1,size: Size=20):
    regions = await authorize(request,response,query)
    offsets = {int(day) for day in offsetDays.split(",")}
    if len(offsets)!=len(offsetDays.split(",")):
        raise RequestProblem(400,"analytics.invalidRequest")
    store = request.app.state.reports
    snapshot,pairs = await store.snapshot(query.snapshot,(query.start,query.end),dataset=RETENTION_DATASET)
    if len(pairs) == (query.end-query.start).days:
        coverage = {day for day,_ in pairs}
        cohorts,unknown = await store.cached_query(snapshot,query_retention,sorted(pairs),query.platform,query.region,query.version,
                                                   sorted(regions) if regions is not None else None,sorted(offsets))
    else:
        snapshot,coverage,rows = await load_events(
            request,snapshot["snapshot_id"] if snapshot else query.snapshot,query.start-timedelta(days=90),
            query.end+timedelta(days=max(offsets)),ACTIVE,
        )
        cohorts,unknown = retention(rows,query.start,query.end,coverage,snapshot["source_watermark"] if snapshot else None,
                                    offsets,lambda row:matches(row,query.platform,query.region,query.version,regions))
    return {"code":200,"message":"ok","data":{"list":cohorts[(page-1)*size:page*size],"total":len(cohorts),
            "page":page,"size":size,"unknownCohortCount":unknown,"meta":report_meta(snapshot,coverage,query)}}


@router.get("/sessions/{sessionId}", operation_id="IPK_ANL_16")
async def session_timeline(request: Request,response: Response,sessionId: UUID,
                           ticketId: Annotated[str,Query(min_length=1,max_length=80)],
                           snapshotId: SnapshotId=None,page: Page=1,size: Size=20):
    actor = await require(request,SESSION_READ)
    response.headers["Cache-Control"] = "private, no-store"
    async with request.app.state.pg.connection() as pg:
        await require_ticket(pg,actor,ticketId)
        await audit(pg,request,actor,"sessionRead",{"sessionRef":str(sessionId)},ticketId)
    now = datetime.now(UTC)
    today = business_today(now)
    start,end = today-timedelta(days=29),today+timedelta(days=1)
    snapshot,coverage,rows = await load_events(request,snapshotId,start,end,None,
                                              None if actor.all_regions else actor.region_ids,sessionId)
    rows = [r for r in rows if r["time"] >= now-timedelta(days=30)]
    if not rows:
        raise RequestProblem(404,"analytics.notFound")
    exits = {(r["actor"],r["page"]):r for r in rows if r["name"]=="screen_exit" and r["page"]}
    data = []
    for row in rows[(page-1)*size:page*size]:
        closing = exits.get((row["actor"],row["page"])) if row["page"] else None
        outcome = row["props"].get("outcome")
        target = row["props"].get("targetType")
        data.append({"eventName":row["name"],"occurredAt":row["time"],"sequence":row["sequence"],
                     "screenName":row["screen"],"visibleMs":closing["props"].get("visibleMs") if closing else None,
                     "closureStatus":("closed" if closing else "open") if row["page"] else None,
                     "outcome":outcome if outcome in {"success","failed","cancelled"} else None,
                     "targetType":target if target in {"company","product","user"} else None,"targetRef":None})
    return {"code":200,"message":"ok","data":{"sessionId":sessionId,"list":data,"total":len(rows),
            "page":page,"size":size,"meta":meta(snapshot,start,end,len(coverage)/30,now)}}
