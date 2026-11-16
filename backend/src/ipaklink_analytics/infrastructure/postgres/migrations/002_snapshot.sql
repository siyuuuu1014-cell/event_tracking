CREATE TABLE analytics_report_build (
    dataset varchar(80) NOT NULL,
    date_key date NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    source_offsets jsonb NOT NULL,
    checksum char(64),
    status varchar(16) NOT NULL DEFAULT 'building' CHECK (status IN ('building', 'verified', 'failed')),
    reconciliation jsonb,
    built_at timestamptz,
    PRIMARY KEY (dataset, date_key, generation)
);

CREATE TABLE analytics_report_snapshot (
    snapshot_id varchar(80) PRIMARY KEY,
    metric_version integer NOT NULL CHECK (metric_version > 0),
    generated_at timestamptz NOT NULL DEFAULT now(),
    source_watermark timestamptz NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'archived', 'expired')),
    expires_at timestamptz
);

CREATE UNIQUE INDEX analytics_report_snapshot_single_published
    ON analytics_report_snapshot ((true)) WHERE status = 'published';

CREATE TABLE analytics_report_snapshot_build (
    snapshot_id varchar(80) NOT NULL REFERENCES analytics_report_snapshot(snapshot_id),
    dataset varchar(80) NOT NULL,
    date_key date NOT NULL,
    generation bigint NOT NULL,
    PRIMARY KEY (snapshot_id, dataset, date_key),
    FOREIGN KEY (dataset, date_key, generation)
        REFERENCES analytics_report_build(dataset, date_key, generation)
);

CREATE INDEX analytics_report_snapshot_build_reverse
    ON analytics_report_snapshot_build (dataset, date_key, generation);

