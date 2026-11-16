CREATE VIEW IF NOT EXISTS events_canonical AS
SELECT *
FROM
(
    SELECT *
    FROM events_raw
    ORDER BY received_at, topic, kafka_partition, kafka_offset, ingested_at, payload_hash
    LIMIT 1 BY event_id
);

CREATE TABLE IF NOT EXISTS fact_page_visit (
    generation UInt64,
    business_date Date,
    page_view_id UUID,
    session_id UUID,
    actor_key String,
    screen_name LowCardinality(String),
    entry_source LowCardinality(String),
    entry_at DateTime64(3, 'UTC'),
    exit_at Nullable(DateTime64(3, 'UTC')),
    visible_ms Nullable(UInt64),
    exit_reason Nullable(String),
    closure_status LowCardinality(String),
    is_session_exit Nullable(Bool),
    platform LowCardinality(String) DEFAULT 'unknown',
    app_version String DEFAULT 'unknown',
    region_id UInt32 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, page_view_id);

CREATE TABLE IF NOT EXISTS fact_actor_active_day (
    generation UInt64,
    business_date Date,
    actor_key String,
    identity_type LowCardinality(String),
    platform LowCardinality(String) DEFAULT 'unknown',
    app_version String DEFAULT 'unknown',
    region_id UInt32 DEFAULT 0,
    first_activity_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, actor_key, platform, app_version, region_id);

CREATE TABLE IF NOT EXISTS dim_actor_first_active (
    generation UInt64,
    actor_key String,
    first_business_date Nullable(Date),
    first_event_id Nullable(UUID),
    platform LowCardinality(String) DEFAULT 'unknown',
    app_version String DEFAULT 'unknown',
    region_id UInt32 DEFAULT 0,
    history_truncated Bool DEFAULT false
)
ENGINE = MergeTree
ORDER BY (generation, actor_key);

CREATE TABLE IF NOT EXISTS dim_entity_safe (
    target_type LowCardinality(String),
    target_id String,
    effective_at DateTime64(3, 'UTC'),
    valid_until Nullable(DateTime64(3, 'UTC')),
    region_id UInt32 DEFAULT 0,
    category_code Nullable(String),
    industry_code Nullable(String)
)
ENGINE = MergeTree
ORDER BY (target_type, target_id, effective_at);

CREATE TABLE IF NOT EXISTS fact_search (
    generation UInt64,
    business_date Date,
    actor_key String,
    search_id UUID,
    scope LowCardinality(String),
    submitted_at DateTime64(3, 'UTC'),
    first_result_at Nullable(DateTime64(3, 'UTC')),
    outcome Nullable(String),
    result_count Nullable(UInt32),
    error_code Nullable(String),
    latency_ms Nullable(UInt64),
    first_open_at Nullable(DateTime64(3, 'UTC')),
    detail_at Nullable(DateTime64(3, 'UTC')),
    matched_target_type Nullable(String),
    matched_target_id Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, actor_key, search_id);

CREATE TABLE IF NOT EXISTS fact_inquiry_flow (
    generation UInt64,
    business_date Date,
    actor_key String,
    page_view_id UUID,
    contact_flow_id UUID,
    company_id String,
    product_id Nullable(String),
    detail_at DateTime64(3, 'UTC'),
    contact_at DateTime64(3, 'UTC'),
    client_request_id Nullable(String),
    submit_at Nullable(DateTime64(3, 'UTC')),
    inquiry_ref Nullable(String),
    created_at Nullable(DateTime64(3, 'UTC')),
    attribution_status LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, actor_key, contact_flow_id);

CREATE TABLE IF NOT EXISTS fact_inquiry_created (
    generation UInt64,
    business_date Date,
    inquiry_ref String,
    actor_key String,
    submitted_at DateTime64(3, 'UTC'),
    company_id String,
    product_id Nullable(String),
    contact_flow_id Nullable(UUID),
    attribution_reason Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, inquiry_ref);

CREATE TABLE IF NOT EXISTS fact_dialog_reply (
    generation UInt64,
    business_date Date,
    dialog_ref String,
    anchor_message_ref String,
    sender_actor_key String,
    anchor_at DateTime64(3, 'UTC'),
    reply_at Nullable(DateTime64(3, 'UTC')),
    latency_ms Nullable(UInt64),
    mature Bool DEFAULT false
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, dialog_ref, anchor_message_ref);

CREATE TABLE IF NOT EXISTS fact_translate_task (
    generation UInt64,
    business_date Date,
    actor_key String,
    task_id UUID,
    mode LowCardinality(String),
    source_lang LowCardinality(String) DEFAULT 'und',
    target_lang LowCardinality(String) DEFAULT 'und',
    started_at DateTime64(3, 'UTC'),
    terminal_at Nullable(DateTime64(3, 'UTC')),
    outcome Nullable(String),
    latency_ms Nullable(UInt64),
    error_code Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, actor_key, task_id);

CREATE TABLE IF NOT EXISTS agg_metric_day (
    generation UInt64,
    business_date Date,
    metric_code LowCardinality(String),
    metric_version UInt32,
    dim_set LowCardinality(String),
    dim_values String,
    value_count Nullable(UInt64),
    numerator Nullable(UInt64),
    denominator Nullable(UInt64),
    provisional Bool DEFAULT false
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, metric_code, metric_version, dim_set, dim_values);

CREATE TABLE IF NOT EXISTS agg_retention_day (
    generation UInt64,
    cohort_date Date,
    offset_day UInt8,
    cohort_dim String,
    cohort_size UInt64,
    retained_count Nullable(UInt64),
    mature Bool DEFAULT false,
    expected_mature_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, generation, offset_day, cohort_dim);

CREATE TABLE IF NOT EXISTS agg_quality_minute (
    generation UInt64,
    bucket_at DateTime64(3, 'UTC'),
    service LowCardinality(String),
    source LowCardinality(String) DEFAULT 'unknown',
    app_version String DEFAULT 'unknown',
    platform LowCardinality(String) DEFAULT 'unknown',
    accepted UInt64 DEFAULT 0,
    rejected UInt64 DEFAULT 0,
    rate_limited UInt64 DEFAULT 0,
    duplicate UInt64 DEFAULT 0,
    late UInt64 DEFAULT 0,
    dlq UInt64 DEFAULT 0,
    lag_records Nullable(UInt64),
    p95_ingest_delay_ms Nullable(UInt64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_at)
ORDER BY (bucket_at, generation, service, source, app_version, platform);

CREATE TABLE IF NOT EXISTS fact_reconciliation_day (
    generation UInt64,
    business_date Date,
    fact_code LowCardinality(String),
    source LowCardinality(String),
    source_count UInt64,
    warehouse_count UInt64,
    delta Int64,
    unattributed_count Nullable(UInt64),
    source_watermark DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, fact_code, source);

CREATE TABLE IF NOT EXISTS fact_card_impression (
    generation UInt64,
    business_date Date,
    impression_id UUID,
    actor_key String,
    page_view_id UUID,
    list_request_id UUID,
    placement_code LowCardinality(String),
    target_type LowCardinality(String),
    target_id String,
    position UInt32,
    visible_ms UInt64,
    exposed_at DateTime64(3, 'UTC'),
    first_click_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(business_date)
ORDER BY (business_date, generation, impression_id);
