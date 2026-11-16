from collections import Counter, defaultdict
from datetime import timedelta
from statistics import median

from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE

ACTIVE = {
    "app_session_start", "screen_view", "search_submit", "entity_detail_view",
    "chat_session_open", "translate_started", "inquiry_submit", "card_impression",
}
FEATURE_EVENTS = {
    "search": {"search_submit", "search_result", "search_result_open"},
    "chat": {"chat_session_open", "message_send_succeeded"},
    "translate": {"translate_started", "translate_completed"},
    "favorite": {"favorite_changed"},
}
GROUPS = {
    "search": {"scope", "errorCode", "appVersion"},
    "chat": {"entrySource", "appVersion"},
    "translate": {"mode", "languagePair", "errorCode", "appVersion"},
    "favorite": {"appVersion"},
}


def ratio(top, bottom):
    return top / bottom if bottom else None


def metric(code, value, unit="count", *, small=False, numerator=None, denominator=None):
    data = {"metricCode": code, "value": None if small else value, "unit": unit,
            "status": "suppressed" if small else ("ready" if value is not None else "noData")}
    if unit == "ratio":
        data.update(numerator=None if small else numerator, denominator=None if small else denominator)
    return data


def window_rows(rows, start, end):
    return [row for row in rows if start <= row["date"] < end]


def matches(row, platform, region, version, regions):
    return ((platform is None or row["platform"] == platform)
            and (version is None or row["version"] == version)
            and (region is None or row["region"] == region)
            and (regions is None or row["region"] in regions))


def overview_metrics(all_rows, start, end, available):
    day = end - timedelta(days=1)
    result = []
    for name, window in (("dau", 1), ("wau", 7), ("mau", 30)):
        first = day - timedelta(days=window - 1)
        covered = all(first + timedelta(days=i) in available for i in range(window))
        actors = {r["actor"] for r in window_rows(all_rows, first, end) if r["name"] in ACTIVE and r["actor"]}
        result.append(metric(name, len(actors) if covered else None, small=covered and 0 < len(actors) < 20))
    rows = window_rows(all_rows, start, end)
    covered = any(start <= day < end for day in available)
    anonymous = {r["actor"] for r in rows if r["identity"] == "anonymous" and r["name"] in ACTIVE}
    result.append(metric("anonymousUv", len(anonymous) if covered else None, small=0 < len(anonymous) < 20))
    for code, event in (("businessViewPv", "business_view_recorded"), ("inquiryCreated", "inquiry_created")):
        subset = [r for r in rows if r["name"] == event and r["source"] == "server"]
        result.append(metric(code, len(subset) if subset else None, small=0 < len({r["actor"] for r in subset}) < 20))
    tasks = [r for r in rows if r["name"] == "translate_completed" and r["props"].get("outcome") in {"success","failed"}]
    unique_tasks = {(r["actor"], r["props"].get("taskId")): r for r in tasks}
    top = sum(r["props"]["outcome"] == "success" for r in unique_tasks.values())
    total = len(unique_tasks)
    result.append(metric("translateSuccessRate", ratio(top,total), "ratio",
                         small=0 < len({r["actor"] for r in unique_tasks.values()}) < 20,
                         numerator=top, denominator=total))
    return result


def feature_metrics(rows, feature):
    counts = Counter(row["name"] for row in rows)
    small = 0 < len({row["actor"] for row in rows}) < 20
    if feature == "search":
        distinct = {(r["actor"],r["props"].get("searchId")):r for r in rows if r["name"] == "search_result"}
        succeeded = [r for r in distinct.values() if r["props"].get("outcome") == "success"]
        zero = sum(r["props"].get("resultCount") == 0 for r in succeeded)
        with_results = {(r["actor"],r["props"].get("searchId")) for r in succeeded if r["props"].get("resultCount",0)>0}
        opened = {(r["actor"],r["props"].get("searchId")) for r in rows if r["name"]=="search_result_open"}
        matched = len(with_results & opened)
        return [metric("searchSubmitted",counts["search_submit"],small=small),
                metric("searchOpened",counts["search_result_open"],small=small),
                metric("searchFailed",len(distinct)-len(succeeded),small=small),
                metric("searchZeroResultRate",ratio(zero,len(succeeded)),"ratio",small=small,
                       numerator=zero,denominator=len(succeeded)),
                metric("searchOpenRate",ratio(matched,len(with_results)),"ratio",small=small,
                       numerator=matched,denominator=len(with_results))]
    if feature == "chat":
        return [metric("chatOpened",counts["chat_session_open"],small=small),
                metric("messageSent",len({(r["props"].get("dialogRef"),r["props"].get("messageRef")) for r in rows
                                          if r["name"]=="message_send_succeeded" and r["source"]=="server"}) or None,small=small)]
    if feature == "favorite":
        rows = [r for r in rows if r["source"]=="server"]
        return [metric("favoriteAdded",sum(r["props"].get("action")=="add" for r in rows) if rows else None,small=small),
                metric("favoriteRemoved",sum(r["props"].get("action")=="remove" for r in rows) if rows else None,small=small),
                metric("favoriteActors",len({r["actor"] for r in rows}) if rows else None,small=small)]
    tasks = {(r["actor"],r["props"].get("taskId")):r for r in rows if r["name"] == "translate_completed"}
    completed = [r["props"] for r in tasks.values() if r["props"].get("outcome") in {"success","failed"}]
    success = sum(p["outcome"]=="success" for p in completed)
    latencies = sorted(p["latencyMs"] for p in completed if p["outcome"]=="success" and isinstance(p.get("latencyMs"),int))
    p95 = latencies[(95 * len(latencies) - 1) // 100] if latencies else None
    return [metric("translateStarted",counts["translate_started"],small=small),
            metric("translateSuccessRate",ratio(success,len(completed)),"ratio",small=small,
                   numerator=success,denominator=len(completed)),
            metric("translateP95Ms",p95,"ms",small=small)]


def group_key(row, group):
    if group == "appVersion":
        return row["version"]
    if group == "languagePair":
        origin, target = (row["props"].get(key) for key in ("sourceLang","targetLang"))
        return f"{origin}:{target}" if origin and target else None
    return row["props"].get(group)


def funnel(rows, code, start, end, entry, target, scope, anchor_filter):
    search = code == "F02"
    names = ("search_submit","search_result","search_result_open","entity_detail_view") if search else (
        "entity_detail_view","contact_supplier_click","inquiry_submit","inquiry_created")
    steps = ("submit","hasResults","open","detail") if search else ("detail","contact","submit","created")
    seconds = 900 if search else 86400
    buckets = defaultdict(list)
    for row in rows:
        if row["name"] not in names or not row["actor"]:
            continue
        key = (row["actor"],row["props"].get("searchId")) if search else row["actor"]
        if not search or key[1]:
            buckets[key].append(row)
    completed = [dict() for _ in steps]
    zero, failed = set(), set()
    attributed = set()
    for key, events in buckets.items():
        for first in events:
            props = first["props"]
            if first["name"] != names[0] or not start <= first["date"] < end or not anchor_filter(first):
                continue
            if entry is not None and props.get("entrySource") != entry:
                continue
            if search and props.get("scope")=="user":
                continue
            if scope is not None and props.get("scope") != scope:
                continue
            completed[0][key] = (first["actor"], None)
            previous = first
            for index, name in enumerate(names[1:],1):
                candidates = [r for r in events if r["name"] == name and r["time"] >= previous["time"]
                              and r["time"]-first["time"] <= timedelta(seconds=seconds)]
                if search and index == 1:
                    if any(r["props"].get("outcome")=="failed" for r in candidates):
                        failed.add(key)
                    if any(r["props"].get("outcome")=="success" and r["props"].get("resultCount")==0 for r in candidates):
                        zero.add(key)
                    candidates = [r for r in candidates if r["props"].get("outcome")=="success"
                                  and r["props"].get("resultCount",0)>0]
                if search and index in {2,3}:
                    allowed = {target} if target else {"company","product"}
                    candidates = [r for r in candidates if r["props"].get("targetType") in allowed]
                if search and index == 3:
                    candidates = [r for r in candidates if r["props"].get("targetId")==previous["props"].get("targetId")
                                  and r["props"].get("targetType")==previous["props"].get("targetType")]
                if not search:
                    company = first["props"].get("companyId")
                    candidates = [r for r in candidates if r["props"].get("companyId")==company]
                    if index >= 2:
                        flow = previous["props"].get("contactFlowId")
                        candidates = [r for r in candidates if flow and r["props"].get("contactFlowId")==flow]
                    if index == 3:
                        request = previous["props"].get("clientRequestId")
                        candidates = [r for r in candidates if r["source"]=="server" and request
                                      and r["props"].get("clientRequestId")==request]
                if not candidates:
                    break
                current = candidates[0]
                completed[index].setdefault(key,(first["actor"],(current["time"]-previous["time"]).total_seconds()*1000))
                if not search and index == 3 and start <= current["date"] < end:
                    attributed.add(current["props"].get("inquiryRef"))
                previous = current
    summary = []
    for i, name in enumerate(steps):
        values = completed[i]
        count = len(values)
        elapsed = [m for _, m in values.values() if m is not None]
        summary.append({"stepCode":name,"count":count,"uniqueActors":len({a for a,_ in values.values()}),
                        "stepRate":ratio(count,len(completed[max(0,i-1)])),
                        "totalRate":ratio(count,len(completed[0])),
                        "p50FromPreviousMs":median(elapsed) if elapsed else None})
    business = {r["props"].get("inquiryRef") for r in rows if r["name"]=="inquiry_created" and r["source"]=="server"
                and start <= r["date"] < end and anchor_filter(r)}
    attributed_count = len(business & attributed)
    return {"funnelCode":code,"windowSeconds":seconds,"entityUnit":"search" if search else "actor","steps":summary,
            "businessCount":None if search or not business else len(business),
            "attributedCount":None if search or not business else attributed_count,
            "attributionCoverage":None if search else ratio(attributed_count,len(business)),
            "attributionReasons":[{"reasonCode":"missingFlow","count":len(business)-attributed_count}]
                if not search and len(business)>attributed_count else [],
            "zeroResultCount":len(zero) if search else None,"failedCount":len(failed) if search else None}


def retention(rows, start, end, coverage, watermark, offsets, matches_cohort):
    first = {}
    visits = defaultdict(set)
    for row in rows:
        if row["name"] not in ACTIVE or not row["actor"]:
            continue
        first.setdefault(row["actor"], row)
        visits[row["actor"]].add(row["date"])
    grouped = defaultdict(list)
    unknown = 0
    for actor, row in first.items():
        if not start <= row["date"] < end or not matches_cohort(row):
            continue
        prior = row["date"]-timedelta(days=1)
        if prior not in coverage:
            unknown += 1
            continue
        grouped[row["date"]].append(actor)
    result = []
    today = watermark.astimezone(BUSINESS_TIMEZONE).date() if watermark else None
    for day, actors in sorted(grouped.items()):
        visible = len(actors) >= 20
        item = {"cohortDate":day,"cohortSize":len(actors) if visible else None}
        for offset in (1,7,30):
            observation = day+timedelta(days=offset)
            expected = observation+timedelta(days=1)
            relevant = offset in offsets
            mature = bool(today and expected <= today and observation in coverage)
            count = sum(observation in visits[actor] for actor in actors) if relevant and mature and visible else None
            key = f"d{offset}"
            item.update({key+"RetainedCount":count,key+"Rate":ratio(count,len(actors)) if count is not None else None,
                         key+"Mature":mature if relevant else None,
                         key+"ExpectedMatureDate":expected if relevant else None})
        result.append(item)
    return result, unknown if unknown >= 20 or unknown == 0 else None

def translation_sync_metric(rows):
    latest = {(row["actor"],row["props"]["recordRef"]):row for row in rows}
    failed = sum(row["props"]["outcome"]=="failed" for row in latest.values())
    actors = {row["actor"] for row in latest.values()}
    return metric("translateSyncFailureRate",ratio(failed,len(latest)),"ratio",
                  small=0<len(actors)<20,numerator=failed,denominator=len(latest))
