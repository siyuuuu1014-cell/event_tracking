CREATE TABLE analytics_admin_account (
    account_id varchar(128) PRIMARY KEY,
    username varchar(64) NOT NULL UNIQUE CHECK (username ~ '^[a-z0-9][a-z0-9._-]{2,63}$'),
    display_name varchar(100) NOT NULL,
    password_hash text NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Preserve legacy development grants for inspection while enforcing the FK on every new grant.
ALTER TABLE analytics_access_grant ADD CONSTRAINT analytics_access_grant_account_fk
    FOREIGN KEY (principal_id) REFERENCES analytics_admin_account(account_id) NOT VALID;

INSERT INTO analytics_role(role_code,name) VALUES
    ('platformAdmin','Platform administrator'), ('operator','Operator'), ('analyst','Analyst')
ON CONFLICT (role_code) DO NOTHING;

CREATE TABLE analytics_access_grant_request (
    operator_id varchar(128) NOT NULL REFERENCES analytics_admin_account(account_id),
    idempotency_key varchar(128) NOT NULL,
    request_hash char(64) NOT NULL,
    grant_id uuid NOT NULL REFERENCES analytics_access_grant(grant_id),
    PRIMARY KEY (operator_id,idempotency_key)
);
