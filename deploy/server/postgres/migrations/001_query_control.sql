DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'candlescope_query_runtime') THEN
        CREATE ROLE candlescope_query_runtime
            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'candlescope_query_auditor') THEN
        CREATE ROLE candlescope_query_auditor
            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS candlescope_query_audit_head (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    last_audit_sequence BIGINT NOT NULL CHECK (last_audit_sequence >= 0),
    last_event_hash TEXT NOT NULL CHECK (last_event_hash ~ '^[0-9a-f]{64}$'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS candlescope_query_audit_event (
    audit_sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    schema_version TEXT NOT NULL,
    event_json JSONB NOT NULL,
    previous_hash TEXT NOT NULL CHECK (previous_hash ~ '^[0-9a-f]{64}$'),
    event_hash TEXT NOT NULL UNIQUE CHECK (event_hash ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS candlescope_query_hot_quarantine (
    backend_id TEXT PRIMARY KEY CHECK (btrim(backend_id) <> ''),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    active BOOLEAN NOT NULL DEFAULT FALSE,
    reason TEXT NULL,
    latched_by TEXT NULL,
    latched_at TIMESTAMPTZ NULL,
    cleared_by TEXT NULL,
    clear_reason_code TEXT NULL,
    cleared_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (generation = 0 AND reason IS NULL AND latched_by IS NULL
         AND latched_at IS NULL AND cleared_by IS NULL
         AND clear_reason_code IS NULL AND cleared_at IS NULL)
        OR generation > 0
    ),
    CHECK (
        NOT active
        OR (reason IS NOT NULL AND latched_by IS NOT NULL AND latched_at IS NOT NULL)
    )
);

INSERT INTO candlescope_query_audit_head (
    singleton,
    last_audit_sequence,
    last_event_hash
) VALUES (
    TRUE,
    0,
    '0000000000000000000000000000000000000000000000000000000000000000'
) ON CONFLICT (singleton) DO NOTHING;

INSERT INTO candlescope_query_hot_quarantine (backend_id)
VALUES ('clickhouse-market-events-v1')
ON CONFLICT (backend_id) DO NOTHING;

REVOKE ALL ON TABLE candlescope_query_schema_migration FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_query_audit_event FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_query_audit_head FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_query_hot_quarantine FROM PUBLIC;
REVOKE ALL ON SEQUENCE candlescope_query_audit_event_audit_sequence_seq FROM PUBLIC;

REVOKE ALL ON TABLE candlescope_query_schema_migration FROM candlescope_query_runtime;
REVOKE ALL ON TABLE candlescope_query_audit_event FROM candlescope_query_runtime;
REVOKE ALL ON TABLE candlescope_query_audit_head FROM candlescope_query_runtime;
REVOKE ALL ON TABLE candlescope_query_hot_quarantine FROM candlescope_query_runtime;
REVOKE ALL ON SEQUENCE candlescope_query_audit_event_audit_sequence_seq
    FROM candlescope_query_runtime;

GRANT USAGE ON SCHEMA public TO candlescope_query_runtime;
GRANT SELECT ON TABLE candlescope_query_schema_migration
    TO candlescope_query_runtime;
GRANT INSERT (event_id, schema_version, event_json, previous_hash, event_hash)
    ON TABLE candlescope_query_audit_event
    TO candlescope_query_runtime;
GRANT SELECT (audit_sequence) ON TABLE candlescope_query_audit_event
    TO candlescope_query_runtime;
GRANT USAGE ON SEQUENCE candlescope_query_audit_event_audit_sequence_seq
    TO candlescope_query_runtime;
GRANT SELECT, UPDATE ON TABLE candlescope_query_audit_head
    TO candlescope_query_runtime;
GRANT SELECT, UPDATE ON TABLE candlescope_query_hot_quarantine
    TO candlescope_query_runtime;

REVOKE ALL ON TABLE candlescope_query_schema_migration FROM candlescope_query_auditor;
REVOKE ALL ON TABLE candlescope_query_audit_event FROM candlescope_query_auditor;
REVOKE ALL ON TABLE candlescope_query_audit_head FROM candlescope_query_auditor;
REVOKE ALL ON TABLE candlescope_query_hot_quarantine FROM candlescope_query_auditor;

GRANT USAGE ON SCHEMA public TO candlescope_query_auditor;
GRANT SELECT ON TABLE candlescope_query_schema_migration
    TO candlescope_query_auditor;
GRANT SELECT ON TABLE candlescope_query_audit_event
    TO candlescope_query_auditor;
GRANT SELECT ON TABLE candlescope_query_audit_head
    TO candlescope_query_auditor;
GRANT SELECT ON TABLE candlescope_query_hot_quarantine
    TO candlescope_query_auditor;
