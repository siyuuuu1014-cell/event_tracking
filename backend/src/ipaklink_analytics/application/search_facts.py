SEARCH_DATASET = "searches"

SEARCH_INSERT = """
INSERT INTO fact_search
    (generation,business_date,actor_key,search_id,scope,submitted_at,first_result_at,outcome,
     result_count,error_code,latency_ms,first_open_at,detail_at,matched_target_type,
     matched_target_id,platform,app_version,region_id)
WITH source_events AS
(
    SELECT * FROM events_raw
    WHERE business_date >= subtractDays({fromDate:Date},1)
      AND business_date < addDays({toDate:Date},1)
      AND ingested_at <= {watermark:DateTime64(3, 'UTC')}
      AND arrayExists(c -> c.1=topic AND c.2=kafka_partition AND kafka_offset<=c.3,
            {watermarks:Array(Tuple(String,UInt32,UInt64))})
      AND source='app' AND schema_version=1 AND actor_key IS NOT NULL
      AND event_name IN ('search_submit','search_result','search_result_open','entity_detail_view')
    ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
    LIMIT 1 BY event_id
),
decoded AS
(
    SELECT *, toUUIDOrNull(JSONExtractString(properties_json,'searchId')) AS search_ref
    FROM source_events
    WHERE search_ref IS NOT NULL
),
per_search AS
(
    SELECT assumeNotNull(actor_key) AS actor, assumeNotNull(search_ref) AS search_id,
           arraySort(row -> (row.2,row.3,row.4),groupArray(tuple(
               toString(event_name),occurred_at,ifNull(sequence,0),event_id,business_date,
               JSONExtractString(properties_json,'scope'),
               JSONExtractString(properties_json,'outcome'),
               JSONExtract(properties_json,'resultCount','Nullable(UInt32)'),
               nullIf(JSONExtractString(properties_json,'errorCode'),''),
               JSONExtract(properties_json,'latencyMs','Nullable(UInt64)'),
               nullIf(JSONExtractString(properties_json,'targetType'),''),
               nullIf(JSONExtractString(properties_json,'targetId'),''),
               ifNull(platform,'unknown'),ifNull(app_version,'unknown'),ifNull(region_id,0)
           ))) AS events
    FROM decoded GROUP BY actor,search_id
    HAVING countIf(event_name='search_submit')>0
),
anchored AS
(
    SELECT *,arrayFirst(row -> row.1='search_submit',events) AS submission
    FROM per_search
    WHERE submission.5 >= {fromDate:Date} AND submission.5 < {toDate:Date}
),
resolved AS
(
    SELECT *,arrayFirst(row -> row.1='search_result' AND row.6=submission.6
           AND (row.2,row.3,row.4)>=(submission.2,submission.3,submission.4)
           AND row.2 <= submission.2+INTERVAL 15 MINUTE,events) AS terminal
    FROM anchored
),
opened AS
(
    SELECT *,arrayFirst(row -> row.1='search_result_open' AND row.11 IS NOT NULL AND row.12 IS NOT NULL
           AND terminal.7='success' AND terminal.8>0
           AND (submission.6='all' OR row.11=submission.6)
           AND (row.2,row.3,row.4)>=(terminal.2,terminal.3,terminal.4)
           AND row.2 <= submission.2+INTERVAL 15 MINUTE,events) AS opening
    FROM resolved
),
completed AS
(
    SELECT *,arrayFirst(row -> row.1='entity_detail_view' AND opening.1!=''
           AND row.11=opening.11 AND row.12=opening.12
           AND (row.2,row.3,row.4)>=(opening.2,opening.3,opening.4)
           AND row.2 <= submission.2+INTERVAL 15 MINUTE,events) AS detail
    FROM opened
)
SELECT {generation:UInt64},submission.5,actor,search_id,submission.6,submission.2,
       if(terminal.1='',NULL,terminal.2),nullIf(terminal.7,''),terminal.8,terminal.9,terminal.10,
       if(opening.1='',NULL,opening.2),if(detail.1='',NULL,detail.2),opening.11,opening.12,
       submission.13,submission.14,submission.15
FROM completed
SETTINGS max_execution_time=60
"""
