from datetime import UTC, date, datetime, timedelta

from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.infrastructure.report_store import is_stale

SORT = {"pv": "pv", "uv": "uv", "p95VisibleMs": "p95VisibleMs", "exitRate": "exitRate"}
BUCKETS = ((0, 5000), (5000, 15000), (15000, 30000), (30000, 60000), (60000, None))


def business_today(now: datetime) -> date:
    return now.astimezone(BUSINESS_TIMEZONE).date()


def date_window(start: date | None, end: date | None, now: datetime) -> tuple[date, date]:
    if start is None and end is None:
        end = business_today(now)
        start = end - timedelta(days=7)
    if not start or not end or not 0 < (end - start).days <= 90:
        raise ValueError("Invalid report date window")
    return start, end


def meta(snapshot: dict | None, start: date, end: date, coverage: float | None, now: datetime) -> dict:
    status = "noData"
    if snapshot:
        status = "stale" if is_stale(snapshot, now) else ("partial" if coverage is not None and coverage < 1 else "ready")
    return {
        "snapshotId": snapshot["snapshot_id"] if snapshot else None,
        "metricVersion": snapshot["metric_version"] if snapshot else None,
        "timezone": "Asia/Tashkent",
        "generatedAt": snapshot["generated_at"].astimezone(UTC).isoformat() if snapshot else None,
        "sourceWatermark": snapshot["source_watermark"].astimezone(UTC).isoformat() if snapshot else None,
        "fromDate": start.isoformat(),
        "toDate": end.isoformat(),
        "status": status,
        "provisional": end > business_today(now),
        "coverage": coverage,
    }


def empty_buckets(screen: str | None) -> list[dict]:
    return [{"lowerMs": lo, "upperMs": hi, "count": 0} for lo, hi in BUCKETS] if screen else []


def query_pages(client, pairs, filters, sort_by: str, page: int, size: int, screen: str | None):
    clauses = ["(business_date,generation) IN {pairs:Array(Tuple(Date,UInt64))}"]
    parameters = {"pairs": pairs, "size": size, "offset": (page - 1) * size}
    for column, typ, value in filters:
        if value is not None:
            operator = "IN" if typ.startswith("Array") else "="
            clauses.append(column + " " + operator + " {" + column + ":" + typ + "}")
            parameters[column] = value
    where = " AND ".join(clauses)
    # Visibility data uses closed visits and valid duration. Open visits still count as PV.
    eligible = "closure_status='closed' AND visible_ms IS NOT NULL"
    query = f"""
    SELECT screen_name AS screenName, count() AS pv, uniqExact(actor_key) AS uv,
           countIf(closure_status='closed') AS closedCount,
           countIf({eligible}) AS eligibleDurationCount,
           closedCount / pv AS closureRate,
           if(eligibleDurationCount=0,NULL,avgIf(assumeNotNull(visible_ms),{eligible})) AS avgVisibleMs,
           if(eligibleDurationCount=0,NULL,quantileExactIf(0.5)(assumeNotNull(visible_ms),{eligible})) AS p50VisibleMs,
           if(eligibleDurationCount=0,NULL,quantileExactIf(0.95)(assumeNotNull(visible_ms),{eligible})) AS p95VisibleMs,
           countIf(is_session_exit=1) / nullIf(countIf(is_session_exit IS NOT NULL),0) AS exitRate
    FROM fact_page_visit WHERE {where}
    GROUP BY screen_name ORDER BY {SORT[sort_by]} DESC NULLS LAST, screen_name ASC
    LIMIT {{size:UInt32}} OFFSET {{offset:UInt64}}
    """
    settings = {"max_execution_time": 5}
    rows = list(client.query(query, parameters=parameters, settings=settings).named_results())
    total = client.query(
        f"SELECT uniqExact(screen_name) FROM fact_page_visit WHERE {where}", parameters=parameters, settings=settings
    ).first_row[0]
    buckets = empty_buckets(screen)
    if screen:
        bands = []
        for lo, hi in BUCKETS:
            high = f" AND visible_ms < {hi}" if hi is not None else ""
            bands.append(f"countIf({eligible} AND visible_ms >= {lo}{high})")
        hits = client.query(
            f"SELECT {', '.join(bands)} FROM fact_page_visit WHERE {where}", parameters=parameters, settings=settings
        ).first_row
        for bucket, count in zip(buckets, hits, strict=True):
            bucket["count"] = count
    for row in rows:
        if row["uv"] < 20:
            row.update({field: None for field in row if field != "screenName"}, status="suppressed")
        else:
            row["status"] = "ready"
    if screen and rows and rows[0]["status"] == "suppressed":
        for bucket in buckets:
            bucket["count"] = None
    # Suppression applies across every representation, including an out-of-range page.
    if screen and not rows and any(bucket["count"] for bucket in buckets):
        users = client.query(
            f"SELECT uniqExact(actor_key) FROM fact_page_visit WHERE {where}", parameters=parameters, settings=settings
        ).first_row[0]
        if users < 20:
            for bucket in buckets:
                bucket["count"] = None
    return rows, total, buckets
