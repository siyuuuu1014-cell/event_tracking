"""Pipe this file to docker compose exec -T api python - for local business fact validation."""
import hashlib
import json
from datetime import UTC, datetime, timedelta
from http.cookiejar import CookieJar
from secrets import token_urlsafe
from time import monotonic, sleep
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener
from uuid import uuid4

import psycopg
from confluent_kafka import Producer
from psycopg.types.json import Jsonb

from ipaklink_analytics.application.build_pages import build
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE, encode
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.admin_sessions import PASSWORDS
from ipaklink_analytics.infrastructure.identity import IdentityService
from ipaklink_analytics.infrastructure.report_store import clickhouse_client

settings = Settings()
assert settings.environment=="local"
identity = IdentityService(settings)
now = datetime.now(UTC)
start_time = now-timedelta(days=2)
start = start_time.astimezone(BUSINESS_TIMEZONE).date()
end = start+timedelta(days=1)
version = "6."+uuid4().hex[:10]
tag = uuid4().hex[:16]
dsn = settings.postgres_dsn.get_secret_value()
publisher = Producer({"bootstrap.servers":settings.kafka_bootstrap_servers,"acks":"all","delivery.timeout.ms":5000})
ids, delivered = [], []


def publish(event, actor, props, at, source="server", sequence=1, session=None, page=None, event_id=None):
    event_id = event_id or str(uuid4())
    values = {"eventId":event_id,"eventName":event,"schemaVersion":1,"occurredAt":at.isoformat(),
              "businessDate":at.astimezone(BUSINESS_TIMEZONE).date().isoformat(),"source":source,
              "producer":"analytics-inquiry-adapter" if source=="server" else "analytics-collector",
              "identityType":"authenticated","actorKey":actor,"platform":"ios","appVersion":version,
              "regionId":1,"properties":props}
    meta = {"receivedAt":now.isoformat()}
    if source=="server":
        values["aggregateRef"] = props.get("inquiryRef",props.get("dialogRef",event_id))
    else:
        values.update(sessionId=session,sequence=sequence,pageViewId=page,screenName="FactoryDetail")
        meta.update(policyVersion=1,requestId=event_id,ingestBatchId=str(uuid4()))
    digest = hashlib.sha256(encode(values)).hexdigest()
    wire = encode({**values,**meta,"payloadHash":digest})
    publisher.produce("analytics.event",key=values.get("aggregateRef",session),value=wire,
                      headers={"analytics-signature":identity.opaque("kafka",wire.decode())},
                      on_delivery=lambda error,_:delivered.append(error))
    ids.append(event_id)
    return event_id


for index in range(20):
    user = identity.opaque("user",tag+str(index))
    other = identity.opaque("user",tag+"peer"+str(index))
    dialog = identity.opaque("dialog",tag+str(index))
    company = "company_"+tag
    session,page,flow = (str(uuid4()) for _ in range(3))
    request_id = "request_"+uuid4().hex
    inquiry = "inquiry_"+tag+str(index)
    base = {"companyId":company}
    publish("entity_detail_view",user,{**base,"targetType":"company","targetId":company,"entrySource":"search"},
            start_time,"app",1,session,page)
    publish("contact_supplier_click",user,{**base,"contactFlowId":flow},start_time+timedelta(seconds=1),"app",2,session,page)
    publish("inquiry_submit",user,{**base,"contactFlowId":flow,"clientRequestId":request_id},
            start_time+timedelta(seconds=2),"app",3,session,page)
    created = {**base,"inquiryRef":inquiry,"contactFlowId":flow,"clientRequestId":request_id}
    when = start_time+timedelta(seconds=3)
    created_id = publish("inquiry_created",user,created,when)
    if index==0:
        publish("inquiry_created",user,created,when,event_id=created_id)
    for action in ("add","remove"):
        publish("favorite_changed",user,{"targetType":"company","targetId":company,"action":action},when)
    publish("translate_sync_completed",user,{"recordRef":f"sync_{tag}{index}","mode":"text",
            "outcome":"failed" if index<5 else "success"},when)
    for ordinal,sender,kind in ((1,user,"human"),(2,user,"human"),(3,other,"system"),(4,other,"human")):
        publish("message_send_succeeded",sender,{"dialogRef":dialog,"messageRef":f"m{ordinal}_{tag}{index}",
                "messageType":"text","senderKind":kind},start_time+timedelta(seconds=ordinal))

extra = identity.opaque("user",tag+"unmatched")
publish("inquiry_created",extra,{"companyId":"company_"+tag,"inquiryRef":"unmatched_"+tag},start_time)
assert publisher.flush(15)==0 and len(delivered)==len(ids) and all(error is None for error in delivered),delivered
with clickhouse_client(settings) as ch:
    deadline=monotonic()+25
    while monotonic()<deadline:
        count=ch.query("SELECT uniqExact(event_id) FROM events_raw WHERE event_id IN {ids:Array(UUID)}",
                       parameters={"ids":list(set(ids))}).first_row[0]
        if count==len(set(ids)):
            break
        sleep(0.2)
    assert count==len(set(ids)),(count,len(set(ids)))

snapshot=build(settings,start,end)
assert snapshot
with psycopg.connect(dsn) as pg:
    frames=dict(pg.execute("SELECT dataset,generation FROM analytics_report_snapshot_build WHERE snapshot_id=%s AND date_key=%s",
                           (snapshot,start)).fetchall())
with clickhouse_client(settings) as ch:
    args={"tag":tag,"generation":frames["inquiryCreated"]}
    inquiries=ch.query(
        """SELECT count(),countIf(attribution_reason IS NULL),countIf(attribution_reason='missingFlow')
           FROM fact_inquiry_created WHERE generation={generation:UInt64} AND position(inquiry_ref,{tag:String})>0""",
        parameters=args,
    ).first_row
    assert tuple(inquiries)==(21,20,1),inquiries
    matched=ch.query(
        """SELECT countIf(attribution_status='matched') FROM fact_inquiry_flow
           WHERE generation={generation:UInt64} AND position(company_id,{tag:String})>0""",
        parameters={"tag":tag,"generation":frames["inquiryFlows"]},
    ).first_row[0]
    assert matched==20,matched
    replies=ch.query(
        """SELECT count(),countIf(mature AND latency_ms=3000) FROM fact_dialog_reply
           WHERE generation={generation:UInt64} AND position(anchor_message_ref,{tag:String})>0""",
        parameters={"tag":tag,"generation":frames["dialogReplies"]},
    ).first_row
    assert tuple(replies)==(20,20),replies

username="business."+tag
account="acct_"+uuid4().hex
password=token_urlsafe(29)
with psycopg.connect(dsn) as pg:
    pg.execute("INSERT INTO analytics_admin_account(account_id,username,display_name,password_hash) VALUES (%s,%s,%s,%s)",
               (account,username,"Business smoke",PASSWORDS.hash(password)))
    pg.execute("INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,expires_at) VALUES (%s,%s,'platformAdmin',%s,now()+interval '5 minutes')",
               (uuid4(),account,Jsonb({"allRegions":True})))

client=build_opener(HTTPCookieProcessor(CookieJar()))
origin="http://127.0.0.1:8000/ipaklink/v1/internal/analytics"


def api(path,method="GET",data=None):
    req=Request(origin+path,method=method,data=json.dumps(data).encode() if data else None,
                headers={"Content-Type":"application/json"})
    with client.open(req,timeout=15) as response:
        return json.load(response)["data"]


try:
    api("/sessions","POST",{"username":username,"password":password})
    query=urlencode({"fromDate":start,"toDate":end,"snapshotId":snapshot,"appVersion":version})
    chat={row["metricCode"]:row["value"] for row in api("/features?feature=chat&"+query)["summary"]}
    assert chat["messageSent"]==80 and chat["chatReplyRate24h"]==1 and chat["chatReplyP50Ms"]==3000,chat
    favorite={row["metricCode"]:row["value"] for row in api("/features?feature=favorite&"+query)["summary"]}
    assert favorite=={"favoriteAdded":20,"favoriteRemoved":20,"favoriteActors":20},favorite
    translate={row["metricCode"]:row for row in api("/features?feature=translate&"+query)["summary"]}
    sync=translate["translateSyncFailureRate"]
    assert (sync["value"],sync["numerator"],sync["denominator"])==(0.25,5,20),sync
    assert translate["translateSuccessRate"]["value"] is None,translate
    funnel=api("/funnels/F01?"+query)
    assert [step["count"] for step in funnel["steps"]]==[20,20,20,20] and funnel["businessCount"]==21,funnel
    print(json.dumps({"snapshotId":snapshot,"inquiries":inquiries,"flows":matched,"dialogReplies":replies,
                      "favorite":favorite,"translationSyncFailureRate":sync["value"],"serverAndAppEvents":len(set(ids))}))
finally:
    api("/sessions/current","DELETE")
    with psycopg.connect(dsn) as pg:
        pg.execute("UPDATE analytics_admin_account SET status='disabled' WHERE account_id=%s",(account,))
