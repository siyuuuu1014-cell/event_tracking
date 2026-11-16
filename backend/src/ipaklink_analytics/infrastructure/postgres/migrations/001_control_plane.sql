CREATE TABLE analytics_event_schema (
    event_name varchar(80) NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    source varchar(12) NOT NULL CHECK (source IN ('app', 'server')),
    property_schema jsonb NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'retired')),
    created_at timestamptz NOT NULL DEFAULT now(),
    activated_at timestamptz,
    PRIMARY KEY (event_name, schema_version)
);

CREATE TABLE analytics_collection_policy (
    policy_version integer PRIMARY KEY CHECK (policy_version > 0),
    rules jsonb NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'retired')),
    effective_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (status <> 'active' OR effective_at IS NOT NULL)
);

CREATE UNIQUE INDEX analytics_collection_policy_single_active
    ON analytics_collection_policy ((true)) WHERE status = 'active';

CREATE TABLE analytics_ingest_receipt (
    event_id uuid PRIMARY KEY,
    event_name varchar(80) NOT NULL,
    schema_version integer NOT NULL,
    policy_version integer NOT NULL REFERENCES analytics_collection_policy(policy_version),
    producer_scope varchar(128) NOT NULL,
    subject_epoch uuid NOT NULL,
    payload_hash char(64) NOT NULL,
    canonical_payload jsonb NOT NULL,
    canonical_time timestamptz NOT NULL,
    first_received_at timestamptz NOT NULL DEFAULT now(),
    state varchar(16) NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'published')),
    lease_token uuid,
    lease_until timestamptz,
    published_at timestamptz,
    expires_at timestamptz NOT NULL,
    FOREIGN KEY (event_name, schema_version)
        REFERENCES analytics_event_schema(event_name, schema_version)
);

CREATE INDEX analytics_ingest_receipt_expiry ON analytics_ingest_receipt(expires_at);

INSERT INTO analytics_collection_policy (policy_version, rules, status, effective_at)
VALUES (1, '{"enabled":false,"anonymous_enabled":false,"platforms":["android","ios"]}', 'active', now());

