CREATE TABLE analytics_outbox (
    event_id uuid PRIMARY KEY,
    aggregate_id text NOT NULL,
    topic varchar(249) NOT NULL,
    payload_safe jsonb NOT NULL CHECK (jsonb_typeof(payload_safe) = 'object'),
    status varchar(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','published','failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX analytics_outbox_claim
    ON analytics_outbox (available_at,event_id) WHERE status = 'pending';

CREATE TABLE analytics_metric_def (
    metric_code varchar(80) NOT NULL,
    metric_version integer NOT NULL CHECK (metric_version > 0),
    dimension_set jsonb NOT NULL,
    query_template_key varchar(80) NOT NULL,
    definition jsonb NOT NULL CHECK (jsonb_typeof(definition) = 'object'),
    status varchar(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active','retired')),
    PRIMARY KEY (metric_code,metric_version)
);

CREATE TABLE analytics_repair_job (
    job_id varchar(80) PRIMARY KEY,
    job_type varchar(16) NOT NULL CHECK (job_type IN ('rebuild','replay')),
    operator_id varchar(128) NOT NULL,
    idempotency_key varchar(128) NOT NULL,
    request_hash char(64) NOT NULL,
    scope jsonb NOT NULL CHECK (jsonb_typeof(scope) = 'object'),
    ticket_id varchar(80) NOT NULL,
    dry_run boolean NOT NULL DEFAULT false,
    status varchar(16) NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','failed')),
    result jsonb,
    published_snapshot_id varchar(80) REFERENCES analytics_report_snapshot(snapshot_id) ON DELETE RESTRICT,
    error_code varchar(80),
    lease_token uuid,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (operator_id,idempotency_key),
    CHECK (NOT dry_run OR published_snapshot_id IS NULL),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX analytics_repair_job_status_date ON analytics_repair_job (status,created_at DESC,job_id);
CREATE INDEX analytics_repair_job_snapshot ON analytics_repair_job (published_snapshot_id);

CREATE TABLE analytics_role (
    role_code varchar(40) PRIMARY KEY,
    name varchar(80) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled'))
);

CREATE TABLE analytics_access_grant (
    grant_id uuid PRIMARY KEY,
    principal_id varchar(128) NOT NULL,
    role_code varchar(40) NOT NULL REFERENCES analytics_role(role_code) ON DELETE RESTRICT,
    scope jsonb NOT NULL CHECK (jsonb_typeof(scope) = 'object'),
    ticket_id varchar(80),
    expires_at timestamptz,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX analytics_access_grant_principal_role ON analytics_access_grant (principal_id,role_code);
CREATE INDEX analytics_access_grant_role ON analytics_access_grant (role_code);

CREATE TABLE analytics_admin_audit (
    audit_id uuid PRIMARY KEY,
    operator_id varchar(128) NOT NULL,
    action varchar(80) NOT NULL,
    ticket_id varchar(80),
    scope_safe jsonb NOT NULL CHECK (jsonb_typeof(scope_safe) = 'object'),
    result_code varchar(80) NOT NULL,
    request_id varchar(128) NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX analytics_admin_audit_operator_date ON analytics_admin_audit (operator_id,occurred_at DESC);
CREATE INDEX analytics_admin_audit_request ON analytics_admin_audit (request_id);
CREATE INDEX analytics_admin_audit_ticket ON analytics_admin_audit (ticket_id,occurred_at DESC)
    WHERE ticket_id IS NOT NULL;

CREATE TABLE analytics_subject_erasure (
    job_id uuid PRIMARY KEY,
    actor_key varchar(128) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','failed')),
    requested_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
);
CREATE INDEX analytics_subject_erasure_actor_status ON analytics_subject_erasure (actor_key,status);

CREATE TABLE analytics_export_job (
    job_id varchar(80) PRIMARY KEY,
    snapshot_id varchar(80) NOT NULL REFERENCES analytics_report_snapshot(snapshot_id) ON DELETE RESTRICT,
    requester_id varchar(128) NOT NULL,
    query_spec jsonb NOT NULL CHECK (jsonb_typeof(query_spec) = 'object'),
    status varchar(16) NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','failed')),
    artifact_ref text,
    request_id varchar(128) NOT NULL,
    error_code varchar(80),
    lease_token uuid,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    expires_at timestamptz NOT NULL,
    CHECK (expires_at > created_at),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL))
);
CREATE INDEX analytics_export_job_requester_date ON analytics_export_job (requester_id,created_at DESC);
CREATE INDEX analytics_export_job_expiry ON analytics_export_job (expires_at);
CREATE INDEX analytics_export_job_snapshot ON analytics_export_job (snapshot_id);

CREATE TABLE analytics_alert_delivery (
    delivery_id uuid PRIMARY KEY,
    alert_key char(64) NOT NULL,
    snapshot_id varchar(80) REFERENCES analytics_report_snapshot(snapshot_id) ON DELETE RESTRICT,
    fact_code varchar(80),
    channel_code varchar(40) NOT NULL,
    destination_ref varchar(128) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','sent','failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at timestamptz,
    UNIQUE (alert_key,channel_code,destination_ref)
);
CREATE INDEX analytics_alert_delivery_status_date ON analytics_alert_delivery (status,created_at DESC);
CREATE INDEX analytics_alert_delivery_snapshot ON analytics_alert_delivery (snapshot_id);

CREATE TABLE analytics_idempotency (
    principal_id varchar(128) NOT NULL,
    operation_code varchar(80) NOT NULL,
    idempotency_key varchar(128) NOT NULL,
    request_hash char(64) NOT NULL,
    resource_ref varchar(128) NOT NULL,
    response_code smallint NOT NULL CHECK (response_code BETWEEN 200 AND 299),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (principal_id,operation_code,idempotency_key)
);
CREATE INDEX analytics_idempotency_expiry ON analytics_idempotency (expires_at);
