CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events_raw (
    event_id UUID,
    event_name LowCardinality(String),
    schema_version UInt16,
    occurred_at DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    business_date Date,
    source LowCardinality(String),
    producer LowCardinality(String),
    actor_key Nullable(String),
    anon_key Nullable(String),
    identity_type LowCardinality(String),
    session_id Nullable(UUID),
    page_view_id Nullable(UUID),
    screen_name Nullable(String),
    sequence Nullable(UInt32),
    platform Nullable(String),
    app_version Nullable(String),
    app_language Nullable(String),
    region_id Nullable(UInt32),
    aggregate_ref Nullable(String),
    policy_version Nullable(UInt32),
    request_id Nullable(String),
    ingest_batch_id Nullable(UUID),
    payload_hash FixedString(64),
    properties_json String,
    topic LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset UInt64,
    broker_at Nullable(DateTime64(3, 'UTC')),
    ingested_at DateTime64(3, 'UTC'),
    replay_run_id Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, topic, kafka_partition, kafka_offset, event_id)
TTL toDate(received_at) + INTERVAL 90 DAY DELETE;

