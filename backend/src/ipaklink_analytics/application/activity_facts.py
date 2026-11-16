from ipaklink_analytics.application.event_reports import ACTIVE, ratio

TRANSLATE_DATASET = "translateTasks"
ACTIVITY_DATASET = "actorActivity"
RETENTION_DATASET = "retention"

TRANSLATE_INSERT = """
INSERT INTO fact_translate_task
    (generation,business_date,actor_key,task_id,mode,source_lang,target_lang,started_at,terminal_at,
     outcome,latency_ms,error_code,platform,app_version,region_id)
WITH captured AS
(
    SELECT * FROM events_raw
    WHERE business_date >= subtractDays({fromDate:Date},1)
      AND business_date < addDays({toDate:Date},1)
      AND ingested_at <= {watermark:DateTime64(3,'UTC')}
      AND arrayExists(c -> c.1=topic AND c.2=kafka_partition AND kafka_offset<=c.3,
            {watermarks:Array(Tuple(String,UInt32,UInt64))})
      AND source='app' AND schema_version=1 AND actor_key IS NOT NULL
      AND event_name IN ('translate_started','translate_completed')
    ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
    LIMIT 1 BY event_id
),
decoded AS
(
    SELECT *, toUUIDOrNull(JSONExtractString(properties_json,'taskId')) AS task_ref
    FROM captured WHERE task_ref IS NOT NULL
),
grouped AS
(
    SELECT assumeNotNull(actor_key) AS actor,assumeNotNull(task_ref) AS task,
           arraySort(row -> (row.2,row.3,row.4),groupArray(tuple(
               toString(event_name),occurred_at,ifNull(sequence,0),event_id,business_date,
               JSONExtractString(properties_json,'mode'),
               JSONExtractString(properties_json,'sourceLang'),
               JSONExtractString(properties_json,'targetLang'),
               nullIf(JSONExtractString(properties_json,'outcome'),''),
               JSONExtract(properties_json,'latencyMs','Nullable(UInt64)'),
               nullIf(JSONExtractString(properties_json,'errorCode'),''),
               ifNull(platform,'unknown'),ifNull(app_version,'unknown'),ifNull(region_id,0)
           ))) AS events
    FROM decoded GROUP BY actor,task HAVING countIf(event_name='translate_started')>0
),
anchored AS
(
    SELECT *,arrayFirst(row -> row.1='translate_started',events) AS start
    FROM grouped WHERE start.5 >= {fromDate:Date} AND start.5 < {toDate:Date}
),
resolved AS
(
    SELECT *,arrayFirst(row -> row.1='translate_completed'
           AND row.6=start.6 AND row.7=start.7 AND row.8=start.8
           AND (row.2,row.3,row.4)>=(start.2,start.3,start.4)
           AND row.2<=start.2+INTERVAL 1 DAY,events) AS terminal
    FROM anchored
)
SELECT {generation:UInt64},start.5,actor,task,start.6,start.7,start.8,start.2,
       if(terminal.1='',NULL,terminal.2),terminal.9,terminal.10,terminal.11,start.12,start.13,start.14
FROM resolved SETTINGS max_execution_time=60
"""

_ACTIVITY_INPUT = """
SELECT event_id,business_date,occurred_at,assumeNotNull(actor_key) AS actor,identity_type,
       ifNull(platform,'unknown') AS device_platform,ifNull(app_version,'unknown') AS version,ifNull(region_id,0) AS region
FROM events_raw
WHERE business_date >= {historyStart:Date}
  AND business_date < {observationEnd:Date}
  AND occurred_at <= {watermark:DateTime64(3,'UTC')}
  AND ingested_at <= {watermark:DateTime64(3,'UTC')}
  AND arrayExists(c -> c.1=topic AND c.2=kafka_partition AND kafka_offset<=c.3,
      {watermarks:Array(Tuple(String,UInt32,UInt64))})
  AND source='app' AND actor_key IS NOT NULL AND event_name IN {activeNames:Array(String)}
ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
LIMIT 1 BY event_id
"""

ACTIVITY_INSERT = """
INSERT INTO fact_actor_active_day
    (generation,business_date,actor_key,identity_type,platform,app_version,region_id,first_activity_at)
WITH active AS (""" + _ACTIVITY_INPUT + """)
SELECT {generation:UInt64},business_date,actor,identity_type,device_platform,version,region,min(occurred_at)
FROM active
WHERE business_date >= {fromDate:Date} AND business_date < {toDate:Date}
GROUP BY business_date,actor,identity_type,device_platform,version,region
SETTINGS max_execution_time=60
"""

FIRST_ACTIVE_INSERT = """
INSERT INTO dim_actor_first_active
    (generation,actor_key,first_business_date,first_event_id,platform,app_version,region_id,history_truncated)
WITH active AS (""" + _ACTIVITY_INPUT + """),
firsts AS (
    SELECT actor,argMin(tuple(business_date,event_id,device_platform,version,region),tuple(occurred_at,event_id)) AS first
    FROM active GROUP BY actor
)
SELECT {generation:UInt64},actor,first.1,first.2,first.3,first.4,first.5,first.1<={historyStart:Date}
FROM firsts
SETTINGS max_execution_time=60
"""

RETENTION_INSERT = """
INSERT INTO agg_retention_day
    (generation,cohort_date,offset_day,cohort_dim,cohort_size,retained_count,mature,expected_mature_date)
WITH active AS (""" + _ACTIVITY_INPUT + """),
visits AS
(
    SELECT actor,groupUniqArray(business_date) AS days
    FROM active GROUP BY actor
),
cohorts AS
(
    SELECT assumeNotNull(f.first_business_date) AS cohort,f.actor_key,
           toJSONString(map('platform',toString(f.platform),'appVersion',f.app_version,'regionId',toString(f.region_id))) AS dim,
           v.days
    FROM dim_actor_first_active f INNER JOIN visits v ON f.actor_key=v.actor
    WHERE f.generation={generation:UInt64} AND NOT f.history_truncated
      AND f.first_business_date>={retentionStart:Date} AND f.first_business_date<{toDate:Date}
),
observations AS
(
    SELECT cohort,dim,days,arrayJoin([toUInt8(1),toUInt8(7),toUInt8(30)]) AS offset_day,
           addDays(cohort,offset_day) AS observation,addDays(observation,1) AS mature_date
    FROM cohorts
)
SELECT {generation:UInt64},cohort,offset_day,dim,count(),
       if(mature_date<={asOf:Date},countIf(has(days,observation)),NULL),mature_date<={asOf:Date},mature_date
FROM observations
GROUP BY cohort,dim,offset_day,observation,mature_date
SETTINGS max_execution_time=60
"""


def fill_activity(ch, parameters):
    parameters = {**parameters, "activeNames": sorted(ACTIVE)}
    ch.command(ACTIVITY_INSERT, parameters=parameters)
    ch.command(FIRST_ACTIVE_INSERT, parameters=parameters)
    ch.command(RETENTION_INSERT, parameters=parameters)

def query_retention(ch, pairs, platform, region, version, allowed_regions, offsets):
    params = {"pairs":pairs,"offsets":sorted(offsets)}
    filters = []
    dimension_filters = []
    dims = (
        ("platform", "String", platform, "platform", "JSONExtractString(cohort_dim,'platform')"),
        ("region", "UInt32", region, "region_id", "toUInt32OrZero(JSONExtractString(cohort_dim,'regionId'))"),
        ("version", "String", version, "app_version", "JSONExtractString(cohort_dim,'appVersion')"),
        ("allow", "Array(UInt32)", sorted(allowed_regions) if allowed_regions is not None else None,
         "region_id", "toUInt32OrZero(JSONExtractString(cohort_dim,'regionId'))"),
    )
    for key, kind, value, field, expression in dims:
        if value is not None:
            operator = "IN" if kind.startswith("Array") else "="
            parameter = "{" + key + ":" + kind + "}"
            filters.append(f"{expression} {operator} {parameter}")
            dimension_filters.append(f"{field} {operator} {parameter}")
            params[key] = value
    condition = " AND " + " AND ".join(filters) if filters else ""
    dimension_condition = " AND " + " AND ".join(dimension_filters) if dimension_filters else ""
    snapshots = "{pairs:Array(Tuple(Date,UInt64))}"
    rows = ch.query(
        f"""SELECT cohort_date,offset_day,sum(cohort_size),sum(retained_count),
                   min(mature),max(expected_mature_date)
            FROM agg_retention_day WHERE (cohort_date,generation) IN {snapshots}{condition}
              AND offset_day IN {{offsets:Array(UInt8)}}
            GROUP BY cohort_date,offset_day ORDER BY cohort_date,offset_day""",
        parameters=params,settings={"max_execution_time":5},
    ).result_rows
    unknown = ch.query(
        f"""SELECT uniqExact(actor_key) FROM dim_actor_first_active WHERE history_truncated
             AND (assumeNotNull(first_business_date),generation) IN {snapshots}{dimension_condition}""",
        parameters=params,settings={"max_execution_time":5},
    ).first_row[0]
    cohorts = {}
    for date,offset,cohort_size,retained,mature,expected in rows:
        if date not in cohorts:
            row = {"cohortDate":date,"cohortSize":cohort_size if cohort_size>=20 else None}
            for period in (1,7,30):
                for field in ("RetainedCount","Rate","Mature","ExpectedMatureDate"):
                    row[f"d{period}{field}"] = None
            cohorts[date] = row
        allowed_count = retained if cohort_size>=20 and mature else None
        cohorts[date].update({
            f"d{offset}RetainedCount":allowed_count,
            f"d{offset}Rate":ratio(allowed_count,cohort_size) if allowed_count is not None else None,
            f"d{offset}Mature":bool(mature),
            f"d{offset}ExpectedMatureDate":expected,
        })
    return list(cohorts.values()), unknown if unknown>=20 or unknown==0 else None
