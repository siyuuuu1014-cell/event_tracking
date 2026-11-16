ALTER TABLE fact_translate_task
    ADD COLUMN IF NOT EXISTS platform LowCardinality(String) DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS app_version String DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS region_id UInt32 DEFAULT 0;
