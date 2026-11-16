"""Run via Get-Content -Raw scripts/smoke-cards.py | docker compose exec -T api python -."""

import json
from datetime import UTC, datetime, timedelta
from http.cookiejar import CookieJar
from secrets import token_urlsafe
from time import monotonic, sleep
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from ipaklink_analytics.application.build_pages import build
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.admin_sessions import PASSWORDS
from ipaklink_analytics.infrastructure.report_store import clickhouse_client

settings = Settings()
assert settings.environment == "local"
browser = build_opener(HTTPCookieProcessor(CookieJar()))
dsn = settings.postgres_dsn.get_secret_value()
BASE = "http://127.0.0.1:8000/ipaklink/v1"
version = "9." + uuid4().hex[:8]


def send(path, method="GET", data=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Analytics-Token"] = token
    request = Request(BASE + path, headers=headers, method=method,
                      data=json.dumps(data).encode() if data is not None else None)
    try:
        result = browser.open(request, timeout=15)
    except HTTPError as error:
        result = error
    with result:
        return result.status, json.load(result)


def seed(index, now):
    device = {"installId":str(uuid4()),"identityEpoch":str(uuid4()),"platform":"ios","appVersion":version}
    status, issued = send("/analytics/anonymous-tokens","POST",device)
    assert status == 201, issued
    properties = {"impressionId":str(uuid4()), "listRequestId":str(uuid4()),
                  "placementCode":"searchResults" if index < 20 else "homeRecommendation",
                  "targetType":"company","targetId":"company_smoke","position":1}
    common = {"schemaVersion":1,"sessionId":str(uuid4()),"screenName":"GlobalSearch","pageViewId":str(uuid4())}

    def event(name, sequence, props, offset=0):
        return {**common,"eventId":str(uuid4()),"eventName":name,"sequence":sequence,
                "occurredAt":(now+timedelta(milliseconds=offset)).isoformat(),"properties":props}

    exposure = event("card_impression",1,{**properties,"visibleMs":1000})
    events = [exposure]
    if index == 0:
        events += [
            event("card_impression_click",2,properties,20),
            event("card_impression_click",3,properties,30),
            event("card_impression",4,{**properties,"impressionId":str(uuid4()),"visibleMs":1000},40),
            event("card_impression",5,{**properties,"visibleMs":999},50),
        ]
    if index == 1:
        events += [event("card_impression_click",2,{**properties,"listRequestId":str(uuid4())},20)]
    if index == 2:
        unbound = event("card_impression",2,{**properties,"visibleMs":1000},20)
        unbound.pop("pageViewId")
        events += [unbound]
    batch = {**device,"batchId":str(uuid4()),"identityType":"anonymous","regionId":1,
             "policyVersion":issued["data"]["policyVersion"],"events":events}
    status, result = send("/analytics/events/report","POST",batch,issued["data"]["analyticsToken"])
    assert status == 200, result
    outcomes = [row["status"] for row in result["data"]["results"]]
    expected = ["accepted"] * len(events)
    if index in {0,2}:
        expected[-1] = "rejected"
    assert outcomes == expected, result
    return [event["eventId"] for event, outcome in zip(events, outcomes, strict=True) if outcome == "accepted"]


now = datetime.now(UTC) - timedelta(seconds=1)
accepted = [event_id for index in range(21) for event_id in seed(index, now)]
with clickhouse_client(settings) as ch:
    deadline = monotonic() + 20
    while monotonic() < deadline:
        rows = ch.query("SELECT uniqExact(event_id) FROM events_raw WHERE event_id IN {ids:Array(UUID)}",
                        parameters={"ids":accepted}).first_row[0]
        if rows == len(accepted):
            break
        sleep(0.2)
    assert rows == len(accepted), rows

today = now.astimezone(BUSINESS_TIMEZONE).date()
end = today + timedelta(days=1)
snapshot = build(settings, today, end)
assert snapshot

username = "cards." + uuid4().hex
account = "acct_" + uuid4().hex
password = token_urlsafe(30)
with psycopg.connect(dsn) as pg:
    pg.execute("INSERT INTO analytics_admin_account(account_id,username,display_name,password_hash) VALUES (%s,%s,%s,%s)",
               (account,username,"Card smoke",PASSWORDS.hash(password)))
    pg.execute("INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,expires_at) VALUES (%s,%s,'analyst',%s,now()+interval '5 minutes')",
               (uuid4(),account,Jsonb({"regionIds":[1]})))

internal = "/internal/analytics"
try:
    assert send(internal+"/sessions","POST",{"username":username,"password":password})[0] == 201
    status, metadata = send(internal+"/metadata")
    assert status == 200 and "cardImpressions" in metadata["data"]["availableReports"], metadata
    path = internal+"/card-impressions?" + urlencode({
        "fromDate":today,"toDate":end,"snapshotId":snapshot,"appVersion":version,"platform":"ios",
    })
    status, report = send(path)
    assert status == 200 and report["data"]["total"] == 2, report
    suppressed, results = report["data"]["list"]
    assert suppressed == {"groupKey":"homeRecommendation","impressions":None,"clickedImpressions":None,
                          "ctr":None,"status":"suppressed"}, suppressed
    assert results == {"groupKey":"searchResults","impressions":20,"clickedImpressions":1,
                       "ctr":0.05,"status":"ready"}, results
    assert send(path+"&regionId=2")[0] == 403
    assert send(path+"&groupBy=targetType&size=1&page=2")[1]["data"]["list"] == []
    print(json.dumps({"snapshotId":snapshot,"impressions":20,"clickedImpressions":1,"ctr":0.05,
                      "checked":["event validation","deduplication","attribution","suppression","permissions","pagination"]}))
finally:
    send(internal+"/sessions/current","DELETE")
    with psycopg.connect(dsn) as pg:
        pg.execute("UPDATE analytics_admin_account SET status='disabled' WHERE account_id=%s", (account,))
