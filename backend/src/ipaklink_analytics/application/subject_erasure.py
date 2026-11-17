import argparse
import re
from datetime import timedelta
from uuid import uuid4

import psycopg

from ipaklink_analytics.application.build_pages import BUILDER_LOCK, PUBLISH_LOCK, build
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.report_store import clickhouse_client
from ipaklink_analytics.infrastructure.snapshot_outbox import INSERT as NOTICE_INSERT, TOPIC, publication
from ipaklink_analytics.infrastructure.subject_erasure import SUBJECT_DATA_LOCK

DERIVED = (
    "fact_page_visit", "fact_actor_active_day", "dim_actor_first_active", "fact_search",
    "fact_inquiry_flow", "fact_inquiry_created", "fact_dialog_reply", "fact_translate_task",
    "agg_metric_day", "agg_retention_day", "agg_quality_minute", "fact_reconciliation_day",
    "fact_card_impression",
)

RAW_SUBJECT = """
actor_key={key:String} OR anon_key={key:String} OR
(event_name='search_result_open' AND JSONExtractString(properties_json,'targetType')='user'
 AND JSONExtractString(properties_json,'targetId')={key:String})
"""


def enqueue(pg, actor_key, *, reapply=False):
    if re.fullmatch(r"[a-f0-9]{64}",actor_key) is None:
        raise ValueError("Expected the analytics actor key (64 lowercase hexadecimal digits)")
    with pg.transaction():
        pg.execute("SELECT pg_advisory_xact_lock(%s)",(PUBLISH_LOCK,))
        current = pg.execute(
            """SELECT job_id,status FROM analytics_subject_erasure
               WHERE actor_key=%s ORDER BY requested_at DESC LIMIT 1""",(actor_key,),
        ).fetchone()
        if current:
            job_id,status = current
            if status == "failed" or (reapply and status == "succeeded"):
                pg.execute("UPDATE analytics_subject_erasure SET status='queued',finished_at=NULL WHERE job_id=%s",
                           (job_id,))
            return job_id
        job_id = uuid4()
        pg.execute("INSERT INTO analytics_subject_erasure(job_id,actor_key) VALUES (%s,%s)",(job_id,actor_key))
    return job_id


def ranges(days):
    start = previous = None
    step = timedelta(days=1)
    for day in days:
        if start is not None and (day != previous+step or (day-start).days >= 31):
            yield start,previous+step
            start = None
        if start is None:
            start = day
        previous = day
    if start is not None:
        yield start,previous+step


def delete_subject(pg, ch, key):
    with pg.transaction():
        pg.execute("SELECT pg_advisory_xact_lock(%s)",(SUBJECT_DATA_LOCK,))
        ch.command("ALTER TABLE events_raw DELETE WHERE "+RAW_SUBJECT,
                   parameters={"key":key},settings={"mutations_sync":1})
        pg.execute(
            """DELETE FROM analytics_ingest_receipt
               WHERE producer_scope=%s OR (event_name='search_result_open'
                  AND canonical_payload#>>'{properties,targetType}'='user'
                  AND canonical_payload#>>'{properties,targetId}'=%s)""",(key,key),
        )


def rebuild_snapshots(settings, pg, ch):
    days = pg.execute(
        """SELECT DISTINCT b.date_key FROM analytics_report_snapshot_build b
           JOIN analytics_report_snapshot s USING(snapshot_id)
           WHERE s.status='published' ORDER BY b.date_key""",
    ).fetchall()
    for start,end in ranges(day for (day,) in days):
        if build(settings,start,end,connection=pg,notify=False) is None:
            raise RuntimeError("Snapshot builder is busy")

    generations = [gen for (gen,) in pg.execute(
        """SELECT DISTINCT b.generation FROM analytics_report_snapshot_build b
           JOIN analytics_report_snapshot s USING(snapshot_id) WHERE s.status='published'""",
    ).fetchall()]
    predicate = "generation NOT IN {generations:Array(UInt64)}" if generations else "1=1"
    args = {"generations":generations} if generations else {}
    for table in DERIVED:
        if ch.query(f"SELECT 1 FROM {table} WHERE {predicate} LIMIT 1",parameters=args).result_rows:
            ch.command(f"ALTER TABLE {table} DELETE WHERE {predicate}",
                       parameters=args,settings={"mutations_sync":1})


def finish(pg, job_id):
    with pg.transaction():
        pg.execute("SELECT pg_advisory_xact_lock(%s)",(PUBLISH_LOCK,))
        pg.execute(
            "UPDATE analytics_report_snapshot SET status='expired',expires_at=now() WHERE status='archived'"
        )
        pg.execute(
            """UPDATE analytics_outbox o SET status='failed',lease_token=NULL,lease_until=NULL
               FROM analytics_report_snapshot s WHERE o.aggregate_id=s.snapshot_id
               AND o.topic=%s AND o.status='pending' AND s.status='expired'""",(TOPIC,),
        )
        pg.execute(
            "UPDATE analytics_subject_erasure SET status='succeeded',finished_at=now() WHERE job_id=%s",(job_id,),
        )
        if pg.execute("SELECT 1 FROM analytics_subject_erasure WHERE status<>'succeeded' LIMIT 1").fetchone():
            return
        snapshot = pg.execute(
            """SELECT snapshot_id,generated_at,source_watermark,metric_version
               FROM analytics_report_snapshot WHERE status='published'"""
        ).fetchone()
        if snapshot:
            pg.execute(NOTICE_INSERT,publication(*snapshot))


def run_erasure(settings, worker_pg, logger):
    if not worker_pg.execute(
        "SELECT 1 FROM analytics_subject_erasure WHERE status IN ('queued','running') LIMIT 1"
    ).fetchone():
        return False
    # An interrupted job remains running and the next worker can safely repeat its steps.
    with psycopg.connect(settings.postgres_dsn.get_secret_value(),autocommit=True) as pg:
        if not pg.execute("SELECT pg_try_advisory_lock(%s)",(BUILDER_LOCK,)).fetchone()[0]:
            return False
        job = pg.execute(
            """SELECT job_id,actor_key FROM analytics_subject_erasure WHERE status IN ('queued','running')
               ORDER BY requested_at,job_id LIMIT 1""",
        ).fetchone()
        if job is None:
            return False
        job_id,key = job
        pg.execute("UPDATE analytics_subject_erasure SET status='running' WHERE job_id=%s",(job_id,))
        try:
            with clickhouse_client(settings,read_timeout=120) as ch:
                delete_subject(pg,ch,key)
                rebuild_snapshots(settings,pg,ch)
            finish(pg,job_id)
        except Exception as error:
            pg.execute(
                "UPDATE analytics_subject_erasure SET status='failed',finished_at=now() WHERE job_id=%s",(job_id,),
            )
            logger.error("subjectErasure.failed",extra={"jobId":str(job_id),"errorType":type(error).__name__})
        else:
            logger.info("subjectErasure.completed",extra={"jobId":str(job_id)})
        return True


def main():
    parser = argparse.ArgumentParser(description="Queue an existing analytics actor key for erasure")
    parser.add_argument("actor_key")
    parser.add_argument("--reapply",action="store_true",help="Reapply a completed erasure after restoring data")
    args = parser.parse_args()
    try:
        with psycopg.connect(Settings().postgres_dsn.get_secret_value(),autocommit=True) as pg:
            print(enqueue(pg,args.actor_key,reapply=args.reapply))
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
