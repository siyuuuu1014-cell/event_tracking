from datetime import UTC, date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Query, Request, Response

from ipaklink_analytics.api.admin_login import enforce_origin
from ipaklink_analytics.api.collection import RequestProblem, client_key, take
from ipaklink_analytics.application.card_reports import CARD_DATASET, query_cards
from ipaklink_analytics.application.page_reports import date_window, empty_buckets, meta, query_pages
from ipaklink_analytics.infrastructure.admin_access import AccessDenied, REPORT_READ
from ipaklink_analytics.infrastructure.admin_sessions import COOKIE

router = APIRouter(prefix="/ipaklink/v1/internal/analytics", tags=["analytics-reporting"])
SnapshotId = Annotated[str | None, Query(min_length=1, max_length=80)]
Page = Annotated[int, Query(ge=1, le=100000)]
Size = Annotated[int, Query(ge=1, le=100)]


async def require(request: Request, permission: str):
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        enforce_origin(request)
    await take(request, f"admin:ip:{client_key(request)}", 100, 180)
    principal = await request.app.state.admin_sessions.principal(request.cookies.get(COOKIE))
    actor = await request.app.state.admin_access.require(principal, permission)
    key = request.app.state.identity.opaque("admin-limit", actor.principal_id)
    await take(request, f"admin:actor:{key}", 40, 90)
    return actor


@router.get("/metadata", operation_id="IPK_ANL_10")
async def metadata(
    request: Request, response: Response, snapshotId: SnapshotId = None,
):
    actor = await require(request, REPORT_READ)
    response.headers["Cache-Control"] = "private, no-store"
    snapshot, _ = await request.app.state.reports.snapshot(snapshotId)
    available_reports = ["overview", "pages", "features", "funnels", "retention"]
    if "analytics.quality.read" in actor.permissions:
        available_reports.append("quality")
    if snapshot and await request.app.state.reports.snapshot_has_dataset(snapshot["snapshot_id"], CARD_DATASET):
        available_reports.append("cardImpressions")
    now = datetime.now(UTC)
    state = meta(snapshot, now.date(), now.date(), None, now)
    return {
        "code": 200, "message": "ok",
        "data": {
            **{k: state[k] for k in ("snapshotId", "metricVersion", "timezone", "generatedAt", "sourceWatermark", "status")},
            "availableReports": available_reports,
            "platforms": ["ios", "android"],
            "features": ["search","chat","translate","favorite"],
            "regions": [{"regionId": value, "label": str(value)} for value in sorted(actor.region_ids)],
            "permissions": sorted(actor.permissions),
        },
    }


@router.get("/pages", operation_id="IPK_ANL_12")
async def pages(
    request: Request, response: Response,
    fromDate: date | None = None, toDate: date | None = None, snapshotId: SnapshotId = None,
    platform: Literal["ios", "android"] | None = None,
    regionId: Annotated[int | None, Query(ge=1, le=4294967295)] = None,
    appVersion: Annotated[str | None, Query(max_length=40)] = None,
    screenName: Annotated[str | None, Query(max_length=80)] = None,
    sortBy: Literal["pv", "uv", "p95VisibleMs", "exitRate"] = "pv", page: Page = 1, size: Size = 20,
):
    actor = await require(request, REPORT_READ)
    response.headers["Cache-Control"] = "private, no-store"
    if regionId is not None and not actor.all_regions and regionId not in actor.region_ids:
        raise AccessDenied
    now = datetime.now(UTC)
    try:
        start, end = date_window(fromDate, toDate, now)
    except ValueError as error:
        raise RequestProblem(400, "analytics.invalidRequest") from error
    store = request.app.state.reports
    snapshot, pairs = await store.snapshot(snapshotId, (start, end))
    filters = [
        ("platform", "String", platform), ("app_version", "String", appVersion),
        ("screen_name", "String", screenName),
        ("region_id", "UInt32", regionId) if regionId is not None or actor.all_regions
        else ("region_id", "Array(UInt32)", sorted(actor.region_ids)),
    ]
    rows, total, buckets = [], 0, empty_buckets(screenName)
    if pairs:
        rows, total, buckets = await store.cached_query(snapshot, query_pages, sorted(pairs), filters, sortBy, page, size, screenName)
    days = (end - start).days
    return {
        "code": 200, "message": "ok",
        "data": {"list": rows, "total": total, "page": page, "size": size,
                 "durationBuckets": buckets,
                 "meta": meta(snapshot, start, end, len(pairs) / days if snapshot else None, now)},
    }

@router.get("/card-impressions", operation_id="IPK_ANL_23")
async def card_impressions(
    request: Request, response: Response,
    fromDate: date | None = None, toDate: date | None = None, snapshotId: SnapshotId = None,
    platform: Literal["ios", "android"] | None = None,
    regionId: Annotated[int | None, Query(ge=1, le=4294967295)] = None,
    appVersion: Annotated[str | None, Query(max_length=40)] = None,
    placementCode: Literal["homeRecommendation", "searchResults"] | None = None,
    targetType: Literal["company", "product"] | None = None,
    groupBy: Literal["placement", "targetType"] = "placement", page: Page = 1, size: Size = 20,
):
    actor = await require(request, REPORT_READ)
    response.headers["Cache-Control"] = "private, no-store"
    if regionId is not None and not actor.all_regions and regionId not in actor.region_ids:
        raise AccessDenied
    now = datetime.now(UTC)
    try:
        start, end = date_window(fromDate, toDate, now)
    except ValueError as error:
        raise RequestProblem(400, "analytics.invalidRequest") from error
    store = request.app.state.reports
    snapshot, pairs = await store.snapshot(snapshotId, (start, end), dataset=CARD_DATASET)
    filters = [
        ("platform", "String", platform), ("app_version", "String", appVersion),
        ("placement_code", "String", placementCode), ("target_type", "String", targetType),
        ("region_id", "UInt32", regionId) if regionId is not None or actor.all_regions
        else ("region_id", "Array(UInt32)", sorted(actor.region_ids)),
    ]
    rows, total = await store.cached_query(snapshot, query_cards, sorted(pairs), filters, groupBy, page, size) if pairs else ([], 0)
    return {
        "code": 200, "message": "ok",
        "data": {"list": rows, "total": total, "page": page, "size": size,
                 "meta": meta(snapshot, start, end, len(pairs) / (end - start).days if snapshot else None, now)},
    }
