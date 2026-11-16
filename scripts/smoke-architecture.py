"""Run via Get-Content -Raw scripts/smoke-architecture.py | docker compose exec -T api python -."""
import json
from datetime import UTC, datetime, time, timedelta
from http.cookiejar import CookieJar
from secrets import token_urlsafe
from time import monotonic, sleep
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from redis import Redis
from psycopg.types.json import Jsonb

from ipaklink_analytics.application.build_pages import build
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.admin_sessions import PASSWORDS
from ipaklink_analytics.infrastructure.report_store import clickhouse_client
from ipaklink_analytics.infrastructure.quality_telemetry import enqueue_quality_alerts

settings=Settings()
assert settings.environment=="local"
client=build_opener(HTTPCookieProcessor(CookieJar()))
base="http://127.0.0.1:8000/ipaklink/v1"
today=datetime.now(UTC).astimezone(BUSINESS_TIMEZONE).date()
cohort=today-timedelta(days=4)
cohort_start=datetime.combine(cohort,time(10),BUSINESS_TIMEZONE).astimezone(UTC)
suffix=uuid4().hex
app_version="8."+suffix[:10]
dsn=settings.postgres_dsn.get_secret_value()


def send(path,method="GET",body=None,token=None):
    headers={"Content-Type":"application/json"}
    if token:
        headers["X-Analytics-Token"]=token
    req=Request(base+path,method=method,data=json.dumps(body).encode() if body is not None else None,headers=headers)
    with client.open(req,timeout=15) as response:
        return json.load(response)["data"]


def event(name,at,sequence,session,properties):
    return {"eventId":str(uuid4()),"eventName":name,"schemaVersion":1,"occurredAt":at.isoformat(),
            "sequence":sequence,"sessionId":session,"properties":properties}


def seed(index):
    device={"installId":str(uuid4()),"identityEpoch":str(uuid4()),"platform":"ios","appVersion":app_version}
    issued=send("/analytics/anonymous-tokens","POST",device)
    token=issued["analyticsToken"]
    session=str(uuid4())
    task=str(uuid4())
    translation={"taskId":task,"mode":"text","sourceLang":"zh","targetLang":"uz"}
    items=[
        event("app_session_start",cohort_start,1,session,{"launchSource":"coldStart"}),
        event("translate_started",cohort_start+timedelta(milliseconds=10),2,session,translation),
    ]
    outcome="success" if index<8 else "failed" if index<12 else "cancelled" if index<15 else None
    if outcome:
        terminal={**translation,"outcome":outcome,"latencyMs":120}
        if outcome=="failed":
            terminal["errorCode"]="networkTimeout"
            terminal["latencyMs"]=800
        items.append(event("translate_completed",cohort_start+timedelta(milliseconds=130),3,session,terminal))
    if index==0:
        items.append(event("translate_started",cohort_start+timedelta(seconds=1),4,session,{**translation,"mode":"invalid"}))
    batch={**device,"batchId":str(uuid4()),"identityType":"anonymous","regionId":1,
           "policyVersion":issued["policyVersion"],"events":items}
    result=send("/analytics/events/report","POST",batch,token)
    statuses=[item["status"] for item in result["results"]]
    expect=["accepted"]*(len(items)-(index==0))+(["rejected"] if index==0 else [])
    assert statuses==expect,statuses
    accepted=[item["eventId"] for item,state in zip(items,statuses,strict=True) if state=="accepted"]
    if index==0:
        retry=send("/analytics/events/report","POST",{**batch,"batchId":str(uuid4())},token)
        assert [row["status"] for row in retry["results"]]==["duplicate","duplicate","duplicate","rejected"],retry
    if index<10:
        revisit=event("app_session_start",cohort_start+timedelta(days=1),1,str(uuid4()),{"launchSource":"foreground"})
        result=send("/analytics/events/report","POST",{**batch,"batchId":str(uuid4()),"regionId":2,"events":[revisit]},token)
        assert result["results"][0]["status"]=="accepted",result
        accepted.append(revisit["eventId"])
    return accepted


event_ids=[identifier for index in range(20) for identifier in seed(index)]
with clickhouse_client(settings) as ch:
    deadline=monotonic()+20
    while monotonic()<deadline:
        count=ch.query("SELECT uniqExact(event_id) FROM events_raw WHERE event_id IN {ids:Array(UUID)}",
                       parameters={"ids":event_ids}).first_row[0]
        if count==len(event_ids):
            break
        sleep(0.2)
    assert count==len(event_ids),count

snapshot=build(settings,today-timedelta(days=7),today+timedelta(days=1))
assert snapshot
with psycopg.connect(dsn) as pg:
    builds=dict(pg.execute("SELECT dataset,generation FROM analytics_report_snapshot_build WHERE snapshot_id=%s AND date_key=%s",
                           (snapshot,cohort)).fetchall())
    assert {"translateTasks","actorActivity","retention","dailyMetrics"}<=builds.keys(),builds
with clickhouse_client(settings) as ch:
    counts=ch.query(
        """SELECT count(),countIf(outcome='success'),countIf(outcome='failed'),
                  countIf(outcome='cancelled'),countIf(terminal_at IS NULL)
           FROM fact_translate_task WHERE generation={generation:UInt64} AND app_version={version:String}""",
        parameters={"generation":builds["translateTasks"],"version":app_version},
    ).first_row
    assert tuple(counts)==(20,8,4,3,5),counts
    active=ch.query(
        """SELECT business_date,uniqExact(actor_key) FROM fact_actor_active_day
           WHERE generation={generation:UInt64} AND app_version={version:String}
           GROUP BY business_date ORDER BY business_date""",
        parameters={"generation":builds["actorActivity"],"version":app_version},
    ).result_rows
    assert [row[1] for row in active]==[20,10],active
    daily=ch.query(
        """SELECT numerator,denominator FROM agg_metric_day
           WHERE generation={generation:UInt64} AND business_date={day:Date} AND metric_code='translateSuccessRate'""",
        parameters={"generation":builds["dailyMetrics"],"day":cohort},
    ).first_row
    expected=ch.query(
        """SELECT countIf(outcome='success'),countIf(outcome IN ('success','failed'))
           FROM fact_translate_task WHERE generation={generation:UInt64} AND business_date={day:Date}""",
        parameters={"generation":builds["translateTasks"],"day":cohort},
    ).first_row
    assert tuple(daily)==tuple(expected) and daily[0]>=8 and daily[1]>=12,(daily,expected)

account="acct_"+suffix
username="architecture."+suffix
password=token_urlsafe(26)
with psycopg.connect(dsn) as pg:
    pg.execute("INSERT INTO analytics_admin_account(account_id,username,display_name,password_hash) VALUES (%s,%s,%s,%s)",
               (account,username,"Architecture smoke",PASSWORDS.hash(password)))
    pg.execute("INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,expires_at) VALUES (%s,%s,'platformAdmin',%s,now()+interval '5 minutes')",
               (uuid4(),account,Jsonb({"allRegions":True})))
prefix="/internal/analytics"
try:
    send(prefix+"/sessions","POST",{"username":username,"password":password})
    selected={"fromDate":cohort,"toDate":cohort+timedelta(days=1),"snapshotId":snapshot,"appVersion":app_version}
    retention_url=prefix+"/retention?"+urlencode({**selected,"regionId":1,"offsetDays":"1,7"})
    pattern=f"analytics:report:{settings.environment}:v1:*"
    with Redis.from_url(settings.redis_url.get_secret_value()) as redis:
        before=set(redis.scan_iter(match=pattern))
        retention=send(retention_url)
        entered=set(redis.scan_iter(match=pattern))-before
        assert len(entered)==1,entered
        key=next(iter(entered))
        assert 0<redis.ttl(key)<=70 and send(retention_url)==retention
    result=retention["list"][0]
    assert result["cohortSize"]==20 and result["d1RetainedCount"]==10 and result["d1Rate"]==0.5,result
    assert result["d1Mature"] is True and result["d7Mature"] is False and result["d30Mature"] is None,result
    translation=send(prefix+"/features?"+urlencode({**selected,"feature":"translate"}))["summary"]
    assert next(x["value"] for x in translation if x["metricCode"]=="translateSuccessRate")==8/12,translation
    assert next(x["value"] for x in translation if x["metricCode"]=="translateP95Ms")==120,translation
    global_overview=send(prefix+"/overview?"+urlencode({
        "fromDate":cohort,"toDate":cohort+timedelta(days=1),"snapshotId":snapshot,"includePrevious":"false",
    }))
    point=next(x for x in global_overview["series"] if x["metricCode"]=="translateSuccessRate")
    assert point["status"]=="suppressed" or (point["numerator"],point["denominator"])==tuple(daily),point
    quality=send(prefix+"/quality?"+urlencode({"fromDate":today,"toDate":today+timedelta(days=1),
                                              "snapshotId":snapshot,"appVersion":app_version}))
    assert quality["rejected"]==2 and quality["duplicates"]==3,quality
    global_quality=send(prefix+"/quality?"+urlencode({"fromDate":today,"toDate":today+timedelta(days=1),
                                                     "snapshotId":snapshot}))
    assert global_quality["lagRecords"] is not None and global_quality["stalePartitions"] is not None,global_quality
    with psycopg.connect(dsn,row_factory=dict_row) as pg:
        stamps=pg.execute(
            """INSERT INTO analytics_kafka_lag_sample(sampled_at,topic,partition,next_offset,high_offset,stalled)
               SELECT clock_timestamp()+make_interval(secs=>second),'smoke.test',0,0,%s,true
               FROM generate_series(1,2) AS second RETURNING sampled_at""",
            (settings.quality_lag_threshold,),
        ).fetchall()
        from hashlib import sha256
        window=int(max(s["sampled_at"] for s in stamps).timestamp())//300
        digest=sha256(f"kafkaLag:{window}".encode()).hexdigest()
        enqueue_quality_alerts(settings,pg)
        enqueue_quality_alerts(settings,pg)
        assert pg.execute("SELECT count(*) AS total FROM analytics_alert_delivery WHERE alert_key=%s",
                          (digest,)).fetchone()["total"]==1
        pg.rollback()
    print(json.dumps({"translation":list(counts),"active":[20,10],"d1Retention":result["d1Rate"],
                      "dayAggregation":list(daily),"cache":"verified","alertDeduplication":"verified",
                      "quality":{"rejected":2,"duplicates":3,"kafkaLag":global_quality["lagRecords"]},
                      "snapshotId":snapshot}))
finally:
    send(prefix+"/sessions/current","DELETE")
    with psycopg.connect(dsn) as pg:
        pg.execute("UPDATE analytics_admin_account SET status='disabled' WHERE account_id=%s",(account,))
