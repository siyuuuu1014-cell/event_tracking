CARD_DATASET = "cardImpressions"

CARD_INSERT = """
INSERT INTO fact_card_impression
    (generation,business_date,impression_id,actor_key,page_view_id,list_request_id,placement_code,
     target_type,target_id,position,visible_ms,exposed_at,first_click_at,platform,app_version,region_id)
WITH source_events AS
(
    SELECT * FROM events_raw
    WHERE ingested_at <= {watermark:DateTime64(3, 'UTC')}
      AND arrayExists(c -> c.1=topic AND c.2=kafka_partition AND kafka_offset<=c.3,
            {watermarks:Array(Tuple(String,UInt32,UInt64))})
      AND source='app' AND schema_version=1 AND actor_key IS NOT NULL
      AND session_id IS NOT NULL AND page_view_id IS NOT NULL
      AND event_name IN ('card_impression','card_impression_click')
    ORDER BY received_at,topic,kafka_partition,kafka_offset,ingested_at,payload_hash
    LIMIT 1 BY event_id
),
decoded AS
(
    SELECT event_id, event_name, business_date, occurred_at, ifNull(sequence,0) AS seq,
           assumeNotNull(actor_key) AS actor, assumeNotNull(session_id) AS session,
           assumeNotNull(page_view_id) AS page_view,
           toUUIDOrNull(JSONExtractString(properties_json,'impressionId')) AS impression,
           toUUIDOrNull(JSONExtractString(properties_json,'listRequestId')) AS list_request,
           JSONExtractString(properties_json,'placementCode') AS placement,
           JSONExtractString(properties_json,'targetType') AS target_kind,
           JSONExtractString(properties_json,'targetId') AS target_ref,
           JSONExtractUInt(properties_json,'position') AS slot,
           JSONExtractUInt(properties_json,'visibleMs') AS duration,
           ifNull(platform,'unknown') AS device_platform, ifNull(app_version,'unknown') AS version,
           ifNull(region_id,0) AS region
    FROM source_events
    WHERE impression IS NOT NULL AND list_request IS NOT NULL AND slot BETWEEN 1 AND 2147483647
      AND placement IN ('homeRecommendation','searchResults') AND target_kind IN ('company','product')
      AND notEmpty(target_ref)
),
by_instance AS
(
    SELECT * FROM decoded
    WHERE event_name='card_impression' AND duration BETWEEN 1000 AND 28800000
    ORDER BY occurred_at,seq,event_id
    LIMIT 1 BY actor,session,page_view,list_request,placement,target_kind,target_ref,slot
),
exposures AS
(
    SELECT * FROM by_instance
    ORDER BY occurred_at,seq,event_id LIMIT 1 BY actor,impression
),
clicks AS
(
    SELECT e.actor AS actor, e.impression AS impression, min(c.occurred_at) AS first_click
    FROM exposures e INNER JOIN decoded c ON e.actor=c.actor AND e.session=c.session
        AND e.page_view=c.page_view AND e.impression=c.impression AND e.list_request=c.list_request
        AND e.placement=c.placement AND e.target_kind=c.target_kind
        AND e.target_ref=c.target_ref AND e.slot=c.slot
    WHERE c.event_name='card_impression_click' AND c.occurred_at>=e.occurred_at AND c.seq>e.seq
      AND e.business_date >= {fromDate:Date} AND e.business_date < {toDate:Date}
    GROUP BY e.actor,e.impression
)
SELECT {generation:UInt64},e.business_date,assumeNotNull(e.impression),e.actor,e.page_view,
       assumeNotNull(e.list_request),e.placement,e.target_kind,e.target_ref,e.slot,e.duration,e.occurred_at,
       c.first_click,e.device_platform,e.version,e.region
FROM exposures e LEFT JOIN clicks c ON e.actor=c.actor AND e.impression=c.impression
WHERE e.business_date >= {fromDate:Date} AND e.business_date < {toDate:Date}
SETTINGS max_execution_time=60, join_use_nulls=1
"""

GROUP_BY = {"placement": "placement_code", "targetType": "target_type"}


def query_cards(client, pairs, filters, group_by, page, size):
    clauses = ["(business_date,generation) IN {pairs:Array(Tuple(Date,UInt64))}"]
    parameters = {"pairs": pairs, "size": size, "offset": (page - 1) * size}
    for column, column_type, value in filters:
        if value is not None:
            operator = "IN" if column_type.startswith("Array") else "="
            clauses.append(column + " " + operator + " {" + column + ":" + column_type + "}")
            parameters[column] = value
    where = " AND ".join(clauses)
    group = GROUP_BY[group_by]
    result = client.query(
        f"""SELECT {group} AS groupKey, count() AS impressions, count(first_click_at) AS clickedImpressions,
                   clickedImpressions / impressions AS ctr, uniqExact(actor_key) AS actors
            FROM fact_card_impression WHERE {where}
            GROUP BY {group} ORDER BY groupKey LIMIT {{size:UInt32}} OFFSET {{offset:UInt64}}""",
        parameters=parameters, settings={"max_execution_time": 5},
    )
    rows = list(result.named_results())
    total = client.query(
        f"SELECT uniqExact({group}) FROM fact_card_impression WHERE {where}",
        parameters=parameters, settings={"max_execution_time": 5},
    ).first_row[0]
    for row in rows:
        if row.pop("actors") < 20:
            row.update(impressions=None, clickedImpressions=None, ctr=None, status="suppressed")
        else:
            row["status"] = "ready"
    return rows, total
