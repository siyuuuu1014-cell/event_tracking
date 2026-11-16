CREATE TABLE analytics_repair_failure (
    failure_id bigserial PRIMARY KEY,
    job_id varchar(80) NOT NULL REFERENCES analytics_repair_job(job_id),
    topic varchar(249),
    partition integer,
    offset_value varchar(20),
    error_code varchar(80) NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX analytics_repair_failure_job ON analytics_repair_failure(job_id,failure_id);
CREATE INDEX analytics_ingest_receipt_published ON analytics_ingest_receipt(published_at)
    WHERE state='published';
