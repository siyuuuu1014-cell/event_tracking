CREATE TABLE analytics_quality_observation (
    observation_key varchar(128) PRIMARY KEY,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    platform varchar(16),
    app_version varchar(40),
    rejected integer NOT NULL DEFAULT 0 CHECK (rejected>=0),
    duplicates integer NOT NULL DEFAULT 0 CHECK (duplicates>=0),
    rate_limited integer NOT NULL DEFAULT 0 CHECK (rate_limited>=0),
    retry integer NOT NULL DEFAULT 0 CHECK (retry>=0),
    dlq integer NOT NULL DEFAULT 0 CHECK (dlq>=0)
);
CREATE INDEX analytics_quality_observation_date ON analytics_quality_observation(recorded_at);

CREATE TABLE analytics_kafka_lag_sample (
    sampled_at timestamptz NOT NULL,
    topic varchar(249) NOT NULL,
    partition integer NOT NULL CHECK (partition>=0),
    next_offset bigint NOT NULL CHECK (next_offset>=0),
    high_offset bigint NOT NULL CHECK (high_offset>=next_offset),
    stalled boolean,
    PRIMARY KEY (sampled_at,topic,partition)
);
CREATE INDEX analytics_kafka_lag_partition ON analytics_kafka_lag_sample(topic,partition,sampled_at DESC);
