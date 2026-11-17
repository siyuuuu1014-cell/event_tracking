import argparse
import json
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from ipaklink_analytics.application.activity_facts import (
    ACTIVITY_DATASET, RETENTION_DATASET, TRANSLATE_DATASET, TRANSLATE_INSERT, fill_activity,
)
from ipaklink_analytics.application.business_facts import CHAT_DATASET, FLOW_DATASET, INQUIRY_DATASET, build_business
from ipaklink_analytics.application.card_reports import CARD_DATASET, CARD_INSERT
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.application.quality_facts import QUALITY_DATASET, RECONCILIATION_DATASET, build_quality
from ipaklink_analytics.application.search_facts import SEARCH_DATASET, SEARCH_INSERT
from ipaklink_analytics.application.warehouse_metrics import DAILY_INSERT, METRIC_DATASET
from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.snapshot_outbox import INSERT as NOTICE_INSERT, publication
from ipaklink_analytics.infrastructure.logging import get_logger
from ipaklink_analytics.infrastructure.report_store import DATASET, clickhouse_client

BUILDER_LOCK = 345972152
PUBLISH_LOCK = 345972153
BUILT_DATASETS = (DATASET, CARD_DATASET, SEARCH_DATASET, TRANSLATE_DATASET, ACTIVITY_DATASET, RETENTION_DATASET, METRIC_DATASET,
                  CHAT_DATASET, FLOW_DATASET, INQUIRY_DATASET, QUALITY_DATASET, RECONCILIATION_DATASET)

PAGE_INSERT = """
INSERT INTO fact_page_visit
    (generation,business_date,page_view_id,session_id,actor_key,screen_name,entry_source,
     entry_at,exit_at,visible_ms,exit_reason,closure_status,is_session_exit,platform,app_version,region_id)
WITH events AS
(
    SELECT * FROM
    (
        SELECT * FROM events_raw
        WHERE ingested_at <= {watermark:DateTime64(3, 'UTC')}
          AND arrayExists(c -> c.1=topic AND c.2=kafka_partition AND kafka_offset<=c.3,
                {watermarks:Array(Tuple(String,UInt32,UInt64))})
          AND source='app' AND actor_key IS NOT NULL AND session_id IS NOT NULL
        ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
        LIMIT 1 BY event_id
    )
),
grouped AS
(
    SELECT assumeNotNull(actor_key) AS actor, assumeNotNull(session_id) AS session,
           assumeNotNull(page_view_id) AS visit,
           argMinIf(tuple(business_date,occurred_at,ifNull(screen_name,''),
                    JSONExtractString(properties_json,'entrySource'),
                    ifNull(platform,'unknown'),ifNull(app_version,'unknown'),ifNull(region_id,0),
                    ifNull(sequence,0),event_id),
                    tuple(occurred_at,ifNull(sequence,0),event_id), event_name='screen_view') AS entry,
           argMinIf(tuple(occurred_at,JSONExtract(properties_json,'visibleMs','Nullable(UInt64)'),
                    JSONExtractString(properties_json,'exitReason')),
                    tuple(occurred_at,ifNull(sequence,0),event_id), event_name='screen_exit') AS closing,
           countIf(event_name='screen_exit') AS exits
    FROM events
    WHERE page_view_id IS NOT NULL AND event_name IN ('screen_view','screen_exit')
    GROUP BY actor,session,visit
    HAVING countIf(event_name='screen_view') > 0
),
visits AS
(
    SELECT *,
       row_number() OVER (PARTITION BY actor,session ORDER BY entry.2 DESC,entry.8 DESC,entry.9 DESC) AS reverse_visit
    FROM grouped
),
sessions AS
(
    SELECT assumeNotNull(actor_key) AS actor, assumeNotNull(session_id) AS session,
           max(occurred_at) AS last_activity
    FROM events GROUP BY actor,session
)
SELECT {generation:UInt64}, entry.1, visit, v.session, v.actor, entry.3, entry.4, entry.2,
       if(exits>0 AND closing.1>=entry.2,closing.1,NULL),
       if(exits>0 AND closing.1>=entry.2 AND closing.2<=28800000,closing.2,NULL),
       if(exits>0 AND closing.1>=entry.2,closing.3,NULL),
       if(exits>0 AND closing.1>=entry.2,'closed','open'),
       if(last_activity<=({watermark:DateTime64(3, 'UTC')} - INTERVAL 30 MINUTE), reverse_visit=1, NULL),
       entry.5, entry.6, entry.7
FROM visits AS v INNER JOIN sessions AS s ON v.actor=s.actor AND v.session=s.session
WHERE entry.1 >= {fromDate:Date} AND entry.1 < {toDate:Date}
SETTINGS max_execution_time=60
"""


def build(settings: Settings, start: date, end: date, *, connection=None, notify=True) -> str | None:
    days = (end - start).days
    if not 0 < days <= 31:
        raise ValueError("Page build date range must cover 1–31 days")
    dates = [start + timedelta(days=i) for i in range(days)]
    # The dedicated session lock keeps builders serialized without a PG transaction during CH work.
    session = (psycopg.connect(settings.postgres_dsn.get_secret_value(),autocommit=True)
               if connection is None else nullcontext(connection))
    with session as pg:
        if connection is None and not pg.execute("SELECT pg_try_advisory_lock(%s)", (BUILDER_LOCK,)).fetchone()[0]:
            return None
        generation = pg.execute("SELECT nextval('analytics_report_generation')").fetchone()[0]
        now = datetime.now(UTC)
        watermark = now.replace(microsecond=now.microsecond // 1000 * 1000)
        as_of = watermark.astimezone(BUSINESS_TIMEZONE).date()
        retention_start = min(start,as_of-timedelta(days=31)) if end>=as_of else start
        retention_days = [retention_start+timedelta(days=i) for i in range((end-retention_start).days)]
        history_start = min(retention_start,as_of-timedelta(days=90))
        snapshot = f"snap_{uuid4().hex}"
        with clickhouse_client(settings) as ch:
            caps = ch.query(
                """SELECT topic,kafka_partition,max(kafka_offset) FROM events_raw
                   WHERE ingested_at <= {watermark:DateTime64(3, 'UTC')}
                   GROUP BY topic,kafka_partition ORDER BY topic,kafka_partition""",
                parameters={"watermark": watermark},
            ).result_rows
            offsets = Jsonb({"topics": [
                {"topic": topic, "partition": partition, "offset": str(offset)}
                for topic, partition, offset in caps
            ], "ingestedThrough": watermark.isoformat()})
            with pg.transaction(), pg.cursor() as cursor:
                cursor.executemany(
                    """INSERT INTO analytics_report_build (dataset,date_key,generation,source_offsets)
                       VALUES (%s,%s,%s,%s)""",
                    [(dataset, day, generation, offsets) for dataset in BUILT_DATASETS
                     for day in (retention_days if dataset == RETENTION_DATASET else dates)],
                )
            try:
                parameters = {
                    "watermark": watermark, "generation": generation,
                    "fromDate": start, "toDate": end, "watermarks": caps,
                }
                if caps:
                    ch.command(PAGE_INSERT, parameters=parameters)
                    ch.command(CARD_INSERT, parameters=parameters)
                    ch.command(SEARCH_INSERT, parameters=parameters)
                    ch.command(TRANSLATE_INSERT, parameters=parameters)
                    fill_activity(ch, {
                        **parameters, "historyStart":history_start,"observationEnd":as_of+timedelta(days=1),
                        "asOf":as_of,"retentionStart":retention_start,
                    })
                    with clickhouse_client(settings) as source:
                        build_business(source,ch,parameters)
                ch.command(DAILY_INSERT, parameters={
                    "generation":generation,"fromDate":start,"toDate":end,"asOf":as_of,
                })
                stats = ch.query(
                    """SELECT business_date,count(),uniqExact(page_view_id) FROM fact_page_visit
                       WHERE generation={generation:UInt64} GROUP BY business_date""",
                    parameters={"generation": generation},
                ).result_rows
                if any(total != unique for _, total, unique in stats):
                    raise RuntimeError("Page generation has duplicate visits")
                card_stats = ch.query(
                    """SELECT count(),uniqExact(actor_key,impression_id) FROM fact_card_impression
                       WHERE generation={generation:UInt64}""", parameters={"generation": generation},
                ).first_row
                if card_stats[0] != card_stats[1]:
                    raise RuntimeError("Card generation has duplicate impressions")
                search_stats = ch.query(
                    """SELECT count(),uniqExact(business_date,actor_key,search_id),
                              countIf(first_result_at<submitted_at OR first_open_at<first_result_at
                                      OR detail_at<first_open_at)
                       FROM fact_search WHERE generation={generation:UInt64}""",
                    parameters={"generation": generation},
                ).first_row
                if search_stats[0] != search_stats[1] or search_stats[2]:
                    raise RuntimeError("Search generation failed uniqueness or ordering checks")
                invalid = ch.query(
                    """SELECT countIf(terminal_at<started_at),
                              count()-uniqExact(business_date,actor_key,task_id)
                       FROM fact_translate_task WHERE generation={generation:UInt64}""",
                    parameters={"generation":generation},
                ).first_row
                if any(invalid):
                    raise RuntimeError("Translation generation failed uniqueness or ordering checks")
                for table, unique in (
                    ("fact_inquiry_created","inquiry_ref"),
                    ("fact_inquiry_flow","business_date,actor_key,contact_flow_id"),
                    ("fact_dialog_reply","dialog_ref,anchor_message_ref"),
                ):
                    distinct = ch.query(
                        f"SELECT count()-uniqExact({unique}) FROM {table} WHERE generation={{generation:UInt64}}",
                        parameters={"generation":generation},
                    ).first_row[0]
                    if distinct:
                        raise RuntimeError(f"{table} generated duplicate business facts")
                build_quality(pg,ch,parameters)
                publish(pg, snapshot, generation, watermark,notify=notify)
            except Exception:
                pg.execute(
                    "UPDATE analytics_report_build SET status='failed' WHERE dataset=ANY(%s) AND generation=%s",
                    (list(BUILT_DATASETS), generation),
                )
                raise
    return snapshot


def publish(pg, snapshot, generation, watermark, *, notify=True):
    with pg.transaction():
        pg.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLISH_LOCK,))
        pg.execute(
            """UPDATE analytics_report_build SET status='verified',built_at=now()
               WHERE dataset=ANY(%s) AND generation=%s""",
            (list(BUILT_DATASETS), generation),
        )
        prev = pg.execute(
            "SELECT snapshot_id FROM analytics_report_snapshot WHERE status='published' FOR UPDATE"
        ).fetchone()
        pg.execute(
            "INSERT INTO analytics_report_snapshot (snapshot_id,metric_version,source_watermark) VALUES (%s,1,%s)",
            (snapshot, watermark),
        )
        if prev:
            pg.execute(
                """INSERT INTO analytics_report_snapshot_build
                   SELECT %s,s.dataset,s.date_key,s.generation FROM analytics_report_snapshot_build s
                   WHERE s.snapshot_id=%s AND NOT EXISTS (
                       SELECT 1 FROM analytics_report_build b
                       WHERE b.generation=%s AND b.dataset=s.dataset AND b.date_key=s.date_key
                   )""",
                (snapshot, prev[0], generation),
            )
            pg.execute(
                "UPDATE analytics_report_snapshot SET status='archived',expires_at=now()+interval '7 days' WHERE snapshot_id=%s",
                (prev[0],),
            )
        pg.execute(
            """INSERT INTO analytics_report_snapshot_build
               SELECT %s,dataset,date_key,generation FROM analytics_report_build
               WHERE dataset=ANY(%s) AND generation=%s""",
            (snapshot, list(BUILT_DATASETS), generation),
        )
        generated = pg.execute(
            "UPDATE analytics_report_snapshot SET status='published' WHERE snapshot_id=%s RETURNING generated_at",
            (snapshot,),
        ).fetchone()[0]
        if notify:
            pg.execute(NOTICE_INSERT,publication(snapshot,generated,watermark))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and publish analytics facts and retention")
    parser.add_argument("--from-date", type=date.fromisoformat)
    parser.add_argument("--to-date", type=date.fromisoformat)
    args = parser.parse_args()
    if (args.from_date is None) != (args.to_date is None):
        parser.error("Supply both dates or neither")
    settings = Settings()
    logger = get_logger(settings.log_dir, "analytics-report-builder")
    today = datetime.now(UTC).astimezone(BUSINESS_TIMEZONE).date()
    try:
        snapshot = build(settings, args.from_date or today - timedelta(days=7),
                         args.to_date or today + timedelta(days=1))
    except Exception as error:
        logger.error("report.buildFailed", extra={"errorType": type(error).__name__})
        raise
    print(json.dumps({"snapshotId": snapshot}))
    logger.info("report.snapshotPublished" if snapshot else "report.builderBusy")


if __name__ == "__main__":
    main()
