"""Run via Get-Content -Raw scripts/smoke-remaining.py | docker compose exec -T api python -."""
import json
import subprocess
import sys
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
from pymongo import MongoClient

from ipaklink_analytics.application.build_pages import build
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.admin_sessions import PASSWORDS
from ipaklink_analytics.infrastructure import export_objects
from ipaklink_analytics.infrastructure.report_store import clickhouse_client

settings=Settings()
assert settings.environment=="local"
dsn=settings.postgres_dsn.get_secret_value()
origin="http://127.0.0.1:8000"
prefix="/ipaklink/v1/internal/analytics"
client=build_opener(HTTPCookieProcessor(CookieJar()))
suffix=uuid4().hex
version="7."+suffix[:10]
account="acct_"+suffix
username="fullsmoke."+suffix
ticket="SMOKE-"+suffix
password=token_urlsafe(28)
checks=set()


def request(path,method="GET",body=None,headers=None):
    req=Request(origin+path,method=method,data=json.dumps(body).encode() if body is not None else None,
                headers={"Content-Type":"application/json",**(headers or {})})
    try:
        response=client.open(req,timeout=20)
    except HTTPError as error:
        response=error
    with response:
        return response.status,(json.load(response) if response.headers.get_content_type()=="application/json" else response.read())


def api(identifier,path,method="GET",body=None,headers=None,expected=200):
    for _ in range(6):
        status,result=request(prefix+path,method,body,headers)
        if status!=429:
            break
        sleep(1)
    assert status==expected and result["code"]==status,(identifier,status,result)
    checks.add(identifier)
    return result["data"]


def wait(path,identifier,wanted):
    deadline=monotonic()+35
    while monotonic()<deadline:
        result=api(identifier,path)
        if result["status"] in {"succeeded","failed"}:
            assert result["status"]==wanted,(path,result)
            return result
        sleep(1)
    raise AssertionError((path,result))


def create_account():
    with psycopg.connect(dsn) as pg:
        pg.execute("INSERT INTO analytics_admin_account(account_id,username,display_name,password_hash) VALUES (%s,%s,%s,%s)",
                   (account,username,"Full smoke",PASSWORDS.hash(password)))
        for code in (None,ticket):
            pg.execute(
                """INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,ticket_id,expires_at)
                   VALUES(%s,%s,'platformAdmin',%s,%s,now()+interval '10 minutes')""",
                (uuid4(),account,Jsonb({"allRegions":True}),code),
            )


def emit_search(now):
    device={"installId":str(uuid4()),"identityEpoch":str(uuid4()),"platform":"ios","appVersion":version}
    status,issued=request("/ipaklink/v1/analytics/anonymous-tokens","POST",device)
    assert status==201,(status,issued)
    session=str(uuid4())
    search=str(uuid4())
    page=str(uuid4())
    template={"schemaVersion":1,"sessionId":session,"screenName":"GlobalSearch","pageViewId":page}
    fields=[
        ("app_session_start",{"launchSource":"coldStart"}),
        ("search_submit",{"searchId":search,"scope":"company","queryLengthBucket":"1-3","filterCount":0}),
        ("search_result",{"searchId":search,"scope":"company","outcome":"success","resultCount":3,"latencyMs":70}),
        ("search_result_open",{"searchId":search,"targetType":"company","targetId":"demo_1","position":1}),
        ("entity_detail_view",{"searchId":search,"targetType":"company","targetId":"demo_1","companyId":"demo_1","entrySource":"search"}),
    ]
    events=[{**template,"eventName":name,"eventId":str(uuid4()),"sequence":index,"properties":properties,
             "occurredAt":(now+timedelta(milliseconds=100*index)).isoformat()}
            for index,(name,properties) in enumerate(fields,1)]
    body={**device,"batchId":str(uuid4()),"identityType":"anonymous","regionId":1,
          "policyVersion":issued["data"]["policyVersion"],"events":events}
    status,result=request("/ipaklink/v1/analytics/events/report","POST",body,{"X-Analytics-Token":issued["data"]["analyticsToken"]})
    assert status==200 and all(row["status"]=="accepted" for row in result["data"]["results"]),result
    return session,search,[row["eventId"] for row in events]


create_account()
now=datetime.now(UTC)-timedelta(seconds=2)
today=now.astimezone(BUSINESS_TIMEZONE).date()
end=today+timedelta(days=1)
try:
    api("46","/sessions","POST",{"username":username,"password":password},expected=201)
    version_filter=urlencode({"fromDate":today,"toDate":end,"appVersion":version})
    sessions=[emit_search(now) for _ in range(20)]
    ids=[item for _,_,events in sessions for item in events]
    with clickhouse_client(settings) as ch:
        deadline=monotonic()+20
        while monotonic()<deadline:
            count=ch.query("SELECT uniqExact(event_id) FROM events_raw WHERE event_id IN {ids:Array(UUID)}",parameters={"ids":ids}).first_row[0]
            if count==len(ids):
                break
            sleep(0.2)
        assert count==len(ids),count
        partition,offset=ch.query(
            "SELECT kafka_partition,kafka_offset FROM events_raw WHERE event_id={id:UUID} LIMIT 1",
            parameters={"id":ids[0]},
        ).first_row
    snapshot=build(settings,today-timedelta(days=1),end)
    assert snapshot
    query="?"+version_filter+"&snapshotId="+snapshot
    assert next(x["value"] for x in api("11","/overview"+query)["cards"] if x["metricCode"]=="dau")==20
    feature=api("13","/features"+query+"&feature=search&groupBy=scope")
    assert feature["total"]>=1
    open_rate=next(row for row in feature["summary"] if row["metricCode"]=="searchOpenRate")
    assert (open_rate["value"],open_rate["numerator"],open_rate["denominator"])==(1.0,20,20),open_rate
    funnel_counts=[s["count"] for s in api("14","/funnels/F02"+query+"&searchScope=company")["steps"]]
    fallback_counts=[s["count"] for s in api("14","/funnels/F02"+query+"&searchScope=company&targetType=company")["steps"]]
    assert fallback_counts==funnel_counts
    with psycopg.connect(dsn) as pg:
        generation=pg.execute(
            """SELECT generation FROM analytics_report_snapshot_build
               WHERE snapshot_id=%s AND dataset='searches' AND date_key=%s""",(snapshot,today),
        ).fetchone()[0]
    with clickhouse_client(settings) as ch:
        fact_counts=ch.query(
            """SELECT count(), countIf(outcome='success' AND result_count>0), count(first_open_at), count(detail_at)
               FROM fact_search
               WHERE generation={generation:UInt64} AND business_date={date:Date}
                 AND app_version={version:String} AND search_id IN {searches:Array(UUID)}""",
            parameters={"generation":generation,"date":today,"version":version,
                        "searches":[search for _,search,_ in sessions]},
        ).first_row
    assert list(fact_counts)==funnel_counts==[20,20,20,20],(fact_counts,funnel_counts)
    assert api("14","/funnels/F01"+query)["entityUnit"]=="actor"
    cohort=api("15","/retention"+query+"&offsetDays=1")["list"][0]
    assert cohort["cohortSize"]==20 and cohort["d1Mature"] is False and cohort["d7Mature"] is None,cohort
    timeline=api("16",f"/sessions/{sessions[0][0]}?ticketId={ticket}&snapshotId={snapshot}")
    assert timeline["total"]==5 and len(timeline["list"])==5
    quality=api("17","/quality"+query+"&source=app")
    assert quality["accepted"]==100 and quality["list"][0]["warehouseCount"]==100,quality
    key={"Idempotency-Key":suffix}
    base={"fromDate":str(today),"toDate":str(end)}
    data={**base,"snapshotId":snapshot,"reportCode":"features","format":"csv","appVersion":version}
    export=api("24","/export-jobs","POST",data,key,202)
    assert api("24","/export-jobs","POST",data,key,202)==export
    detail=wait("/export-jobs/"+export["jobId"],"25","succeeded")
    status,artifact=request(detail["downloadUrl"])
    assert status==200 and b"searchSubmitted" in artifact,(status,artifact[:150])
    assert request(detail["downloadUrl"].split("signature=",1)[0]+"signature="+"a"*64)[0]==403
    if settings.export_bucket:
        with psycopg.connect(dsn) as pg:
            ref=pg.execute("SELECT artifact_ref FROM analytics_export_job WHERE job_id=%s",
                           (export["jobId"],)).fetchone()[0]
            assert ref.startswith("s3:exports/"+export["jobId"]+"/"),ref
            obj=export_objects.get(settings,ref[3:])
            with obj["Body"] as stream:
                assert stream.read()==artifact
            pg.execute("UPDATE analytics_export_job SET created_at=now()-interval '2 days',expires_at=now()-interval '1 day' WHERE job_id=%s",
                       (export["jobId"],))
        assert request(detail["downloadUrl"])[0]==410
        deadline=monotonic()+12
        while monotonic()<deadline:
            remaining=export_objects.get(settings,ref[3:])
            if remaining is None:
                break
            remaining["Body"].close()
            sleep(0.5)
        assert remaining is None,ref

    repair_body={**base,"type":"rebuild","reasonCode":"missingData","ticketId":ticket,"dryRun":True}
    repair=api("21","/repair-jobs","POST",repair_body,key)
    done=wait("/repair-jobs/"+repair["jobId"],"22","succeeded")
    assert done["publishedSnapshotId"] is None and done["scannedCount"]>=100,done
    replay=api("21","/repair-jobs","POST",{**repair_body,"type":"replay","topic":"analytics.event",
               "partition":partition,"startOffset":str(offset),"endOffsetExclusive":str(offset+1),"dryRun":False},
               {"Idempotency-Key":suffix+"replay"})
    replay_result=wait("/repair-jobs/"+replay["jobId"],"22","succeeded")
    assert replay_result["scannedCount"]==1 and replay_result["publishedSnapshotId"],replay_result
    bad=api("21","/repair-jobs","POST",{**repair_body,"type":"replay","topic":"analytics.event","partition":0,
              "startOffset":"9999999","endOffsetExclusive":"10000000"},{"Idempotency-Key":suffix+"fail"})
    failed=wait("/repair-jobs/"+bad["jobId"],"22","failed")
    assert failed["errorCode"]=="offsetOutOfRange",failed
    assert any(r["jobId"]==bad["jobId"] for r in api("32","/repair-jobs?type=replay&status=failed")["list"])
    assert api("33","/repair-jobs/"+bad["jobId"]+"/failures")["list"][0]["errorCode"]=="offsetOutOfRange"
    assert any(r["factCode"]=="repairFailure" for r in api("42","/alert-deliveries?channelCode=localLog")["list"])

    dataset="smoke"+suffix[:12]
    input_ref="input_"+suffix
    with MongoClient(settings.mongo_uri.get_secret_value(),uuidRepresentation="standard") as mongo:
        db=mongo[settings.mongo_database]
        dataset_id=db.corpus_datasets.insert_one({"datasetCode":dataset,"revision":1,"purposeCode":"testing",
            "allowedLanguages":["zh","uz"],"policyRef":"synthetic","retentionDays":1,"status":"active",
            "createdAt":datetime.now(UTC)}).inserted_id
        db.corpus_import_inputs.insert_one({"_id":input_ref,"datasetId":dataset_id,"purposeCode":"testing",
            "createdAt":datetime.now(UTC),"items":[{"sourceRef":"synthetic","sourceLang":"zh","targetLang":"uz","text":"test phrase"}]})
    spec={"datasetCode":dataset,"datasetRevision":1,"type":"import","purposeCode":"testing","ticketId":ticket,"inputRef":input_ref}
    corpus=api("39","/corpus/jobs","POST",spec,key,202)
    assert api("39","/corpus/jobs","POST",spec,key,202)==corpus
    assert wait("/corpus/jobs/"+corpus["jobRef"],"19","succeeded")["acceptedCount"]==1
    row=api("40",f"/corpus/items?datasetCode={dataset}&datasetRevision=1&status=pending")["list"][0]
    review={"status":"approved","expectedItemRevision":row["itemRevision"],"reasonCode":"reviewPassed","ticketId":ticket}
    path=f'/corpus/items/{row["itemId"]}/review-state'
    reviewed=api("41",path,"PUT",review)
    assert reviewed["itemRevision"]==2 and api("41",path,"PUT",review)==reviewed
    snap=build(settings,today,end)
    assert snap
    results=api("18","/corpus/summary?"+urlencode({"fromDate":today,"toDate":end,"datasetCode":dataset,"snapshotId":snap}))
    assert results["list"][0]["itemCount"]==1 and results["list"][0]["status"]=="approved",results
    sync_input="review_"+suffix
    with MongoClient(settings.mongo_uri.get_secret_value(),uuidRepresentation="standard") as mongo:
        mongo[settings.mongo_database].corpus_import_inputs.insert_one({
            "_id":sync_input,"datasetId":dataset_id,"purposeCode":"testing","createdAt":datetime.now(UTC),
            "items":[{"itemId":row["itemId"],"expectedItemRevision":2,"status":"rejected","reasonCode":"qualityIssue"},
                     {"itemId":row["itemId"],"expectedItemRevision":1,"status":"approved","reasonCode":"reviewPassed"}],
        })
    command=[sys.executable,"-m","ipaklink_analytics.application.queue_review_sync",account,dataset,
             "testing",ticket,sync_input,suffix+"review","1"]
    sync_id=subprocess.check_output(command,text=True,timeout=15).strip()
    assert subprocess.check_output(command,text=True,timeout=15).strip()==sync_id
    sync=wait("/corpus/jobs/"+sync_id,"19","succeeded")
    assert (sync["type"],sync["scannedCount"],sync["acceptedCount"],sync["rejectedCount"])==("reviewSync",2,1,1),sync
    synced=api("40",f"/corpus/items?datasetCode={dataset}&datasetRevision=1&status=rejected")["list"][0]
    assert synced["itemRevision"]==3,synced
    erased=api("39","/corpus/jobs","POST",{**spec,"type":"erase","inputRef":None,"sourceRef":"synthetic"},
               {"Idempotency-Key":suffix+"erase"},202)
    assert wait("/corpus/jobs/"+erased["jobRef"],"19","succeeded")["acceptedCount"]==1
    assert api("40",f"/corpus/items?datasetCode={dataset}&datasetRevision=1&status=removed")["total"]==1
    assert checks-{"46"}==set("11 13 14 15 16 17 18 19 21 22 24 25 32 33 39 40 41 42".split()),checks
    print(json.dumps({"verified":sorted(checks-{"46"}),"searchActors":20,"searchFacts":list(fact_counts),"qualityAccepted":100,"export":"S3 + expiry" if settings.export_bucket else "csv",
                      "corpus":["import","review","reviewSync","erase"],"repair":["dryRun","replay","failure","alert"]}))
finally:
    request(prefix+"/sessions/current","DELETE")
    with psycopg.connect(dsn) as pg:
        pg.execute("UPDATE analytics_admin_account SET status='disabled' WHERE account_id=%s",(account,))
