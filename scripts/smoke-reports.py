"""Run via: Get-Content -Raw scripts/smoke-reports.py | docker compose exec -T api python -"""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import psycopg
from psycopg.types.json import Jsonb

from ipaklink_analytics.application.build_pages import build
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.report_store import clickhouse_client

BASE = "http://127.0.0.1:8000/ipaklink/v1"
settings = Settings()
assert settings.environment == "local"
from http.cookies import SimpleCookie
from secrets import token_urlsafe
from urllib.parse import urlencode

from ipaklink_analytics.infrastructure.admin_sessions import COOKIE, PASSWORDS


def request(path, *, method="GET", data=None, token=None, anonymous=None, extra=None, new_token=None):
    headers = {"Content-Type": "application/json", **(extra or {})}
    if token:
        headers["Cookie"] = f"{COOKIE}={token}"
    if anonymous:
        headers["X-Analytics-Token"] = anonymous
    req = Request(BASE + path, data=json.dumps(data).encode() if data is not None else None,
                  method=method, headers=headers)
    try:
        result = urlopen(req, timeout=15)
    except HTTPError as error:
        result = error
    with result:
        if new_token is not None and result.status == 201:
            cookie = SimpleCookie(result.headers["Set-Cookie"])
            assert cookie[COOKIE]["httponly"] and cookie[COOKIE]["samesite"] == "strict"
            new_token.append(cookie[COOKIE].value)
        return result.status, json.load(result)


def account(platform_admin=False):
    username = "smoke." + uuid4().hex
    actor = f"acct_{uuid4().hex}"
    password = token_urlsafe(32)
    with psycopg.connect(settings.postgres_dsn.get_secret_value()) as pg:
        pg.execute(
            "INSERT INTO analytics_admin_account(account_id,username,display_name,password_hash) "
            "VALUES(%s,%s,'Smoke',%s)", (actor, username, PASSWORDS.hash(password)),
        )
        if platform_admin:
            pg.execute(
                "INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,expires_at) "
                "VALUES(%s,%s,'platformAdmin',%s,now()+interval '5 minutes')",
                (uuid4(), actor, Jsonb({"allRegions":True})),
            )
    creds = {"username": username, "password": password}
    assert request(INTERNAL + "/sessions", method="POST", data=creds, extra={"Origin":"https://other.invalid"})[0] == 403
    assert request(INTERNAL + "/sessions", method="POST", data={**creds,"password":"bad"})[0] == 401
    token = []
    status, body = request(INTERNAL + "/sessions", method="POST", data=creds, new_token=token)
    assert status == 201 and body["data"]["accountId"] == actor, (status, body)
    return actor, token[0]


INTERNAL = "/internal/analytics"
actor, admin = account(True)
analyst, limited = account()
ticket = "SMOKE-" + uuid4().hex
with psycopg.connect(settings.postgres_dsn.get_secret_value()) as pg:
    pg.execute(
        "INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,ticket_id,expires_at) "
        "VALUES(%s,%s,'platformAdmin',%s,%s,now()+interval '5 minutes')",
        (uuid4(), actor, Jsonb({"allRegions":True}), ticket),
    )

assert request(INTERNAL + "/pages", token=limited)[0] == 403
grant_body = {"principalId":analyst, "roleCode":"analyst", "regionIds":[2],
              "datasetCodes":[], "ticketId":ticket, "expiresAt":(datetime.now(UTC)+timedelta(minutes=5)).isoformat()}
key = {"Idempotency-Key":uuid4().hex}
grant_url = INTERNAL + "/access-grants"
assert request(grant_url, method="POST", data={**grant_body,"ticketId":"unapproved"}, extra=key, token=admin)[0] == 403
created = request(grant_url, method="POST", data=grant_body, extra=key, token=admin)
assert created[0] == 201, created
assert request(grant_url, method="POST", data=grant_body, extra=key, token=admin) == created
assert request(grant_url, method="POST", data={**grant_body,"regionIds":[1]}, extra=key, token=admin)[0] == 409
grant_id = created[1]["data"]["grantId"]
code, listing = request(grant_url + "?" + urlencode({"principalId":analyst, "status":"active"}), token=admin)
assert code == 200 and listing["data"]["total"] == 1 and listing["data"]["list"][0]["grantId"] == grant_id, listing


assert request("/internal/analytics/pages")[0] == 401
assert request("/internal/analytics/pages?regionId=1", token=limited)[0] == 403
assert request("/internal/analytics/event-schemas", token=limited)[0] == 403
assert request("/internal/analytics/metadata?snapshotId=nonexistent", token=admin)[0] == 409

now = datetime.now(UTC)
device = {"installId": str(uuid4()), "identityEpoch": str(uuid4()), "platform": "ios", "appVersion": "1.2.0"}
status, payload = request("/analytics/anonymous-tokens", method="POST", data=device)
assert status == 201, payload
anonymous = payload["data"]["analyticsToken"]
session = str(uuid4())
page_id = str(uuid4())
enter_id, exit_id = str(uuid4()), str(uuid4())
start = now - timedelta(seconds=5)
common = {"schemaVersion": 1, "sessionId":session, "screenName":"DocPreview", "pageViewId":page_id}
batch = {
    **device, "batchId":str(uuid4()), "identityType":"anonymous", "regionId":1,
    "policyVersion":payload["data"]["policyVersion"],
    "events":[
        {**common,"eventId":enter_id,"eventName":"screen_view","sequence":1,
         "occurredAt":start.isoformat(),"properties":{"entrySource":"tab"}},
        {**common,"eventId":exit_id,"eventName":"screen_exit","sequence":2,
         "occurredAt":now.isoformat(),"properties":{"visibleMs":5000,"exitReason":"navigate"}},
    ],
}
status, uploaded = request("/analytics/events/report", method="POST", data=batch, anonymous=anonymous)
assert status == 200 and [row["status"] for row in uploaded["data"]["results"]] == ["accepted","accepted"], uploaded

from time import monotonic, sleep
deadline = monotonic() + 15
with clickhouse_client(settings) as ch:
    while monotonic() < deadline:
        count = ch.query("SELECT count() FROM events_raw WHERE event_id IN {ids:Array(UUID)}",
                         parameters={"ids":[enter_id,exit_id]}).first_row[0]
        if count == 2:
            break
        sleep(0.25)
    assert count == 2
date_from = start.astimezone(BUSINESS_TIMEZONE).date()
date_to = date_from + timedelta(days=2)
snapshot = build(settings, date_from, date_to)
assert snapshot
with psycopg.connect(settings.postgres_dsn.get_secret_value()) as pg:
    generation = pg.execute(
        "SELECT generation FROM analytics_report_snapshot_build WHERE snapshot_id=%s AND date_key=%s AND dataset='pageVisits'",
        (snapshot, date_from),
    ).fetchone()[0]
with clickhouse_client(settings) as ch:
    row = ch.query(
        "SELECT visible_ms,closure_status FROM fact_page_visit WHERE generation={g:UInt64} AND page_view_id={id:UUID}",
        parameters={"g":generation,"id":page_id},
    ).first_row
    assert tuple(row)==(5000,"closed"), row
status, overview = request("/internal/analytics/metadata", token=admin)
assert status == 200 and overview["data"]["snapshotId"] == snapshot, overview
status, result = request(
    f"/internal/analytics/pages?fromDate={date_from}&toDate={date_to}&screenName=DocPreview&snapshotId={snapshot}",
    token=admin,
)
assert status == 200 and result["data"]["total"] >= 1, result
assert result["data"]["list"][0]["status"] == "suppressed" and result["data"]["durationBuckets"][0]["count"] is None
limited_report = request(
    f"/internal/analytics/pages?fromDate={date_from}&toDate={date_to}&screenName=DocPreview&snapshotId={snapshot}",
    token=limited,
)
assert limited_report[0] == 200 and limited_report[1]["data"]["total"] == 0, limited_report

for endpoint in ("/event-schemas", "/event-schemas/screen_view/versions/1", "/collection-policies",
                 f"/collection-policies/{payload['data']['policyVersion']}", "/report-snapshots"):
    code,response = request("/internal/analytics"+endpoint,token=admin)
    assert code == 200, (endpoint,response)

draft_name = "smoke_" + uuid4().hex
body = {"eventName":draft_name, "schemaVersion":1, "source":"app",
        "propertySchemaText":json.dumps({"type":"object","additionalProperties":False})}
status, data = request("/internal/analytics/event-schemas",method="POST",data=body,token=admin)
assert status == 201 and data["data"]["status"] == "draft", data
code,_ = request(f"/internal/analytics/event-schemas/{draft_name}/versions/1/status", method="PUT",
                 data={"status":"active","ticketId":ticket}, token=admin)
assert code == 409
code,_ = request("/internal/analytics/event-schemas/screen_view/versions/1/status", method="PUT",
                 data={"status":"active","ticketId":ticket}, token=admin)
assert code == 200

code, draft = request("/internal/analytics/collection-policies", method="POST", token=admin,
                      data={"enabled":True,"anonymousEnabled":True,"rolloutPercent":100,
                            "platforms":["ios","android"],"appVersions":[],"enabledEventNames":["screen_view"]})
assert code==201, draft
code,_ = request("/internal/analytics/collection-policies/current", method="PUT", token=admin,
                 data={"policyVersion":draft["data"]["policyVersion"],"expectedPolicyVersion":2147483647,"ticketId":ticket})
assert code==409

previous = build(settings,date_from,date_to)
assert previous and previous != snapshot
code, updated = request("/internal/analytics/report-snapshots/current",method="PUT",token=admin,
    data={"snapshotId":snapshot,"expectedSnapshotId":previous,"ticketId":ticket,"reasonCode":"lateData"})
assert code==200 and updated["data"]["snapshotId"]==snapshot, updated

revoked = request(f"{grant_url}/{grant_id}/status", method="PUT",
                  data={"status":"revoked","ticketId":ticket}, token=admin)
assert revoked[0] == 200, revoked
assert request(f"{grant_url}/{grant_id}/status", method="PUT",
               data={"status":"revoked","ticketId":ticket}, token=admin) == revoked
assert request(INTERNAL + "/pages?regionId=2", token=limited)[0] == 403
audit_url = INTERNAL + "/audit-records?" + urlencode({"ticketId":ticket, "operatorId":actor})
assert request(audit_url, token=limited)[0] == 403
status, records = request(audit_url, token=admin)
actions = [row["action"] for row in records["data"]["list"]]
assert status == 200 and actions.count("grantCreated") == actions.count("grantRevoked") == 1, records
with psycopg.connect(settings.postgres_dsn.get_secret_value()) as pg:
    pg.execute("UPDATE analytics_admin_account SET status='disabled' WHERE account_id=%s", (analyst,))
assert request(INTERNAL + "/pages", token=limited)[0] == 401
assert request(INTERNAL + "/sessions/current", method="DELETE", token=admin)[0] == 200
assert request(INTERNAL + "/sessions/current", method="DELETE", token=admin)[0] == 200
assert request(INTERNAL + "/metadata", token=admin)[0] == 401
with psycopg.connect(settings.postgres_dsn.get_secret_value()) as pg:
    pg.execute("UPDATE analytics_admin_account SET status='disabled' WHERE account_id=%s", (actor,))
print(json.dumps({"visibleMs":5000,"snapshotId":snapshot,
                  "checks":["collection","report","scoped access","configuration","rollback",
                            "login","logout","grant idempotency","revoke","audit","disable"]}))
