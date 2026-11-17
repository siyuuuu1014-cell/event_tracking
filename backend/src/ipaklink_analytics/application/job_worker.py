import argparse
import csv
import json
from io import StringIO
import os
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from secrets import token_urlsafe
from time import monotonic, sleep
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

import psycopg
from confluent_kafka import Consumer, TopicPartition
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from redis import Redis

from ipaklink_analytics.api.admin_login import PREFIX
from ipaklink_analytics.api.jobs import REPORTS
from ipaklink_analytics.application.build_pages import build
from ipaklink_analytics.application.corpus_worker import drain_corpus, expire_corpus
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.application.subject_erasure import run_erasure
from ipaklink_analytics.config import Settings
from ipaklink_analytics.consumer import COLUMNS, event_row
from ipaklink_analytics.infrastructure.admin_sessions import COOKIE
from ipaklink_analytics.infrastructure import export_objects
from ipaklink_analytics.infrastructure.identity import IdentityService
from ipaklink_analytics.infrastructure.logging import get_logger
from ipaklink_analytics.infrastructure.report_store import clickhouse_client
from ipaklink_analytics.infrastructure.snapshot_outbox import SnapshotRelay
from ipaklink_analytics.infrastructure.subject_erasure import filter_rows, persist_events
from ipaklink_analytics.infrastructure.quality_telemetry import enqueue_quality_alerts, sample_kafka

TABLES = {"repair":"analytics_repair_job","export":"analytics_export_job"}


class JobProblem(Exception):
    def __init__(self, code, *, topic=None, partition=None, offset=None):
        self.code,self.topic,self.partition,self.offset = code,topic,partition,offset


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def claim(pg, kind):
    table=TABLES[kind]
    lease=uuid4()
    return pg.execute(
        f"""WITH next_job AS (
                SELECT job_id FROM {table}
                WHERE status='queued' OR (status='running' AND lease_until<=now())
                ORDER BY created_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1)
            UPDATE {table} j SET status='running',lease_token=%s,lease_until=now()+interval '10 minutes',
                   attempts=attempts+1 FROM next_job n WHERE j.job_id=n.job_id RETURNING j.*""",
        (lease,),
    ).fetchone()


def ticket_valid(pg, operator, ticket):
    return bool(pg.execute(
        """SELECT 1 FROM analytics_admin_account a
           JOIN analytics_access_grant g ON g.principal_id=a.account_id
           JOIN analytics_role r ON r.role_code=g.role_code AND r.status='active'
           WHERE a.account_id=%s AND a.status='active' AND g.role_code='platformAdmin'
           AND g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at>now())
           AND g.scope->'allRegions'='true'::jsonb AND g.ticket_id=%s
           AND g.scope->>'authorizationKind' IS DISTINCT FROM 'role'
           AND EXISTS (SELECT 1 FROM analytics_access_grant baseline WHERE baseline.principal_id=a.account_id
             AND baseline.role_code='platformAdmin' AND baseline.ticket_id IS NULL AND baseline.revoked_at IS NULL
             AND (baseline.expires_at IS NULL OR baseline.expires_at>now()) AND baseline.scope->'allRegions'='true'::jsonb)
           LIMIT 1""",(operator,ticket),
    ).fetchone())


def replay(settings, job, start, end):
    spec=job["scope"]
    topic,partition=spec["topic"],spec["partition"]
    begin,finish=map(int,(spec["startOffset"],spec["endOffsetExclusive"]))
    client=Consumer({"bootstrap.servers":settings.kafka_bootstrap_servers,
                     "group.id":"analytics-repair-"+job["job_id"],
                     "enable.auto.commit":False,"enable.auto.offset.store":False,"enable.partition.eof":True})
    identity=IdentityService(settings)
    rows,scanned=[],0
    current=begin
    try:
        source=TopicPartition(topic,partition,begin)
        low,high=client.get_watermark_offsets(source,timeout=5)
        if begin<low:
            raise JobProblem("offsetExpired",topic=topic,partition=partition,offset=begin)
        if finish>high:
            raise JobProblem("offsetOutOfRange",topic=topic,partition=partition,offset=finish)
        client.assign([source])
        deadline=monotonic()+60
        while current<finish and monotonic()<deadline:
            msg=client.poll(1)
            if msg is None:
                continue
            if msg.error():
                if msg.offset()>=finish:
                    break
                raise JobProblem("replayUnavailable",topic=topic,partition=partition,offset=current)
            current=msg.offset()+1
            if msg.offset()>=finish:
                break
            scanned+=1
            try:
                row=event_row(msg,identity)
            except (ValueError,TypeError,KeyError,AttributeError,OverflowError) as error:
                raise JobProblem("invalidEvent",topic=topic,partition=partition,offset=msg.offset()) from error
            if start<=row[5]<end:
                rows.append((*row[:-1],job["job_id"]))
        if current<finish:
            raise JobProblem("replayTimeout",topic=topic,partition=partition,offset=current)
    finally:
        client.close()
    if rows:
        with psycopg.connect(settings.postgres_dsn.get_secret_value(),autocommit=True) as pg:
            rows = filter_rows(pg,rows)
            if rows and not job["dry_run"]:
                with clickhouse_client(settings) as ch:
                    persist_events(pg,ch,rows,COLUMNS)
    return scanned,len({row[5] for row in rows})


def repair(settings,pg,job):
    if not ticket_valid(pg,job["operator_id"],job["ticket_id"]):
        raise JobProblem("permissionRevoked")
    start,end=(date.fromisoformat(job["scope"][name]) for name in ("fromDate","toDate"))
    if job["job_type"]=="replay":
        scanned,affected=replay(settings,job,start,end)
    else:
        with clickhouse_client(settings) as ch:
            scanned,affected=ch.query(
                "SELECT uniqExact(event_id),uniqExact(business_date) FROM events_raw "
                "WHERE business_date>={start:Date} AND business_date<{end:Date}",
                parameters={"start":start,"end":end},settings={"max_execution_time":10},
            ).first_row
    snapshot=None
    if not job["dry_run"]:
        snapshot=build(settings,start,end)
        if snapshot is None:
            raise JobProblem("builderBusy")
    return Jsonb({"scannedCount":scanned,"affectedDateCount":affected}),snapshot


def export_scope(pg,principal,report):
    roles=pg.execute(
        """SELECT g.role_code,g.scope FROM analytics_access_grant g
           JOIN analytics_admin_account a ON a.account_id=g.principal_id AND a.status='active'
           JOIN analytics_role r ON r.role_code=g.role_code AND r.status='active'
           WHERE g.principal_id=%s AND g.role_code IN ('analyst','platformAdmin')
             AND g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at>now())
             AND (g.ticket_id IS NULL OR g.scope->>'authorizationKind'='role')""",(principal,),
    ).fetchall()
    if any(r["role_code"]=="platformAdmin" and r["scope"].get("allRegions") is True for r in roles):
        return None
    if report=="quality":
        raise JobProblem("permissionRevoked")
    if any(r["scope"].get("allRegions") is True for r in roles):
        return None
    regions=sorted({v for r in roles for v in r["scope"].get("regionIds",[]) if type(v) is int and v>0})
    if not regions:
        raise JobProblem("permissionRevoked")
    return regions


def records(data):
    for key in ("cards","series","summary","list","steps","durationBuckets","attributionReasons"):
        for row in data.get(key,[]):
            yield {"section":key,**row}


def cell(value):
    if value is None:
        return ""
    if isinstance(value,(list,dict)):
        return json.dumps(value,ensure_ascii=False)
    if isinstance(value,str) and value.lstrip().startswith(("=","+","-","@")):
        return "'"+value
    return value


def run_export(settings,pg,redis,job):
    spec=job["query_spec"]
    code=spec["reportCode"]
    allowed=spec.get("allowedRegions")
    principal=job["requester_id"]
    if export_scope(pg,principal,code)!=allowed:
        raise JobProblem("permissionRevoked")
    if job["expires_at"]<=datetime.now(UTC):
        raise JobProblem("exportExpired")
    endpoint,defaults=REPORTS[code]
    paginated=code in {"pages","features","quality","retention","cardImpressions"}
    query={key:spec[key] for key in ("snapshotId","fromDate","toDate","platform","regionId","appVersion") if key in spec}
    query.update(defaults)
    token=token_urlsafe(32)
    redis_key=f"analytics:admin:session:{settings.environment}:"+sha256(token.encode()).hexdigest()
    redis.set(redis_key,principal,ex=180)
    opener=build_opener(NoRedirect())
    rows=[]
    try:
        deadline=monotonic()+120
        for page in range(1,102):
            if monotonic()>deadline:
                raise JobProblem("queryTimeout")
            if paginated:
                query.update(page=page,size=100)
            request=Request(f"{settings.job_api_origin}{PREFIX}{endpoint}?"+urlencode(query),
                            headers={"Cookie":f"{COOKIE}={token}"})
            with opener.open(request,timeout=12) as response:
                data=json.load(response)["data"]
            if page>1:
                data={"list":data.get("list",[])}
            rows.extend(records(data))
            if not paginated or (len(data.get("list",[]))<100) or len(rows)>10000:
                break
        if len(rows)>10000:
            raise JobProblem("exportTooLarge")
        if export_scope(pg,principal,code)!=allowed:
            raise JobProblem("permissionRevoked")
    finally:
        redis.delete(redis_key)
    fields=["section"]+sorted({key for row in rows for key in row if key!="section"})
    output=StringIO(newline="")
    writer=csv.DictWriter(output,fieldnames=fields)
    writer.writeheader()
    writer.writerows({key:cell(value) for key,value in row.items()} for row in rows)
    csv_bytes=output.getvalue().encode("utf-8-sig")
    if settings.export_bucket:
        key=f'exports/{job["job_id"]}/{job["lease_token"].hex}.csv'
        export_objects.put(settings,key,csv_bytes)
        return "s3:"+key
    settings.export_dir.mkdir(parents=True,exist_ok=True)
    artifact=job["job_id"]+".csv"
    temporary=settings.export_dir/(artifact+"."+job["lease_token"].hex)
    descriptor=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        with os.fdopen(descriptor,"wb") as stream:
            stream.write(csv_bytes)
        os.replace(temporary,settings.export_dir/artifact)
    finally:
        temporary.unlink(missing_ok=True)
    return artifact


def run_pg(settings,pg,redis,kind,logger):
    job=claim(pg,kind)
    if not job:
        return False
    table=TABLES[kind]
    try:
        if job["attempts"]>3:
            raise JobProblem("attemptsExhausted")
        if kind=="repair":
            result,snapshot=repair(settings,pg,job)
            update="result=%s,published_snapshot_id=%s"
            params=(result,snapshot)
        else:
            update="artifact_ref=%s"
            params=(run_export(settings,pg,redis,job),)
        completed=pg.execute(
            f"""UPDATE {table} SET {update},status='succeeded',finished_at=now(),lease_token=NULL,lease_until=NULL
                WHERE job_id=%s AND lease_token=%s""",(*params,job["job_id"],job["lease_token"]),
        )
        if kind=="export" and not completed.rowcount and params[0].startswith("s3:"):
            export_objects.delete(settings,params[0][3:])
    except Exception as error:
        problem=error if isinstance(error,JobProblem) else JobProblem("jobExecutionFailed")
        logger.error("job.failed",extra={"jobId":job["job_id"],"errorCode":problem.code,"errorType":type(error).__name__})
        with pg.transaction():
            changed=pg.execute(
                f"""UPDATE {table} SET status='failed',error_code=%s,finished_at=now(),lease_token=NULL,lease_until=NULL
                    WHERE job_id=%s AND lease_token=%s""",(problem.code,job["job_id"],job["lease_token"]),
            )
            if kind=="repair" and changed.rowcount:
                pg.execute(
                    """INSERT INTO analytics_repair_failure(job_id,topic,partition,offset_value,error_code)
                       VALUES(%s,%s,%s,%s,%s)""",(job["job_id"],problem.topic,problem.partition,
                                                str(problem.offset) if problem.offset is not None else None,problem.code),
                )
                pg.execute(
                    """INSERT INTO analytics_alert_delivery(delivery_id,alert_key,fact_code,channel_code,destination_ref)
                       VALUES(%s,%s,'repairFailure','localLog','analytics-jobs')
                       ON CONFLICT (alert_key,channel_code,destination_ref) DO NOTHING""",
                    (uuid4(),sha256(job["job_id"].encode()).hexdigest()),
                )
    return True


def deliver_local_alert(pg, logger):
    with pg.transaction():
        row=pg.execute(
            """SELECT delivery_id,fact_code FROM analytics_alert_delivery
               WHERE status='pending' AND channel_code='localLog'
               ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1""",
        ).fetchone()
        if row:
            logger.warning("quality.alert",extra={"deliveryId":str(row["delivery_id"]),"factCode":row["fact_code"]})
            pg.execute("UPDATE analytics_alert_delivery SET status='sent',sent_at=now(),attempts=attempts+1 WHERE delivery_id=%s",
                       (row["delivery_id"],))


def cleanup_exports(settings, pg):
    rows=pg.execute(
        """SELECT e.job_id,e.artifact_ref FROM analytics_export_job e
           JOIN analytics_report_snapshot s USING(snapshot_id)
           WHERE e.artifact_ref IS NOT NULL AND (e.expires_at<=now() OR s.status='expired') LIMIT 100""",
    ).fetchall()
    for row in rows:
        ref=row["artifact_ref"]
        if ref.startswith(f's3:exports/{row["job_id"]}/'):
            try:
                export_objects.delete(settings,ref[3:])
            except export_objects.ExportStorageError:
                continue
        elif ref == row["job_id"]+".csv":
            (settings.export_dir/ref).unlink(missing_ok=True)
        else:
            continue
        pg.execute("UPDATE analytics_export_job SET artifact_ref=NULL WHERE job_id=%s AND artifact_ref=%s",
                   (row["job_id"],ref))


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--once",action="store_true")
    options=parser.parse_args()
    settings=Settings()
    logger=get_logger(settings.log_dir,"analytics-jobs")
    with psycopg.connect(settings.postgres_dsn.get_secret_value(),autocommit=True,row_factory=dict_row) as pg, (
        Redis.from_url(settings.redis_url.get_secret_value(),socket_timeout=5)
    ) as redis, MongoClient(settings.mongo_uri.get_secret_value(),uuidRepresentation="standard",tz_aware=True,
                            serverSelectionTimeoutMS=3000,timeoutMS=10000) as mongo:
        db=mongo.get_database(settings.mongo_database,write_concern=WriteConcern("majority"))
        next_build=monotonic()+5
        next_quality=monotonic()
        next_expiry=monotonic()
        relay=SnapshotRelay(settings.kafka_bootstrap_servers)
        last_day=None
        while True:
            active=run_erasure(settings,pg,logger)
            active=run_pg(settings,pg,redis,"repair",logger) or active
            active=run_pg(settings,pg,redis,"export",logger) or active
            active=drain_corpus(settings,pg,db,logger,ticket_valid) or active
            active=relay.send_next(pg,logger) or active
            if monotonic()>=next_expiry:
                try:
                    count=expire_corpus(db,logger)
                    next_expiry=monotonic()+(2 if count==100 else 60)
                except Exception as error:
                    logger.warning("corpus.expiryFailed",extra={"errorType":type(error).__name__})
                    next_expiry=monotonic()+60
            deliver_local_alert(pg,logger)
            cleanup_exports(settings,pg)
            if monotonic()>=next_quality:
                try:
                    sample_kafka(settings,pg)
                    enqueue_quality_alerts(settings,pg)
                except Exception as error:
                    logger.warning("quality.kafkaSamplingFailed",extra={"errorType":type(error).__name__})
                next_quality=monotonic()+60
            if options.once:
                break
            if monotonic()>=next_build:
                today=datetime.now(UTC).astimezone(BUSINESS_TIMEZONE).date()
                try:
                    start=today-timedelta(days=30) if last_day is None else today-timedelta(days=7)
                    if build(settings,start,today+timedelta(days=1)):
                        last_day=today
                except Exception as error:
                    logger.error("snapshot.buildFailed",extra={"errorType":type(error).__name__})
                next_build=monotonic()+300
            if not active:
                sleep(2)


if __name__=="__main__":
    main()
