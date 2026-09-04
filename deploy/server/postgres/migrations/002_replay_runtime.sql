DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'candlescope_replay_runtime'
    ) THEN
        CREATE ROLE candlescope_replay_runtime
            NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END
$$;

DO $$
BEGIN
    IF to_regclass('public.candlescope_replay_session_lease') IS NULL THEN
        RAISE EXCEPTION 'candlescope_replay_session_lease is missing';
    END IF;
    IF (
        SELECT COUNT(*)
        FROM pg_attribute
        WHERE attrelid = 'public.candlescope_replay_session_lease'::regclass
          AND attnum > 0
          AND NOT attisdropped
    ) <> 12
    OR (
        SELECT COUNT(*)
        FROM pg_attribute
        WHERE attrelid = 'public.candlescope_replay_session_lease'::regclass
          AND attnum > 0
          AND NOT attisdropped
          AND attname IN (
            'session_id', 'worker_id', 'fencing_epoch', 'lease_token',
            'lease_expires_at', 'organization_id', 'workspace_id',
            'data_epoch', 'snapshot_version', 'manifest_uri',
            'manifest_sha256', 'updated_at'
          )
    ) <> 12 THEN
        RAISE EXCEPTION 'candlescope_replay_session_lease columns have drifted';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS candlescope_replay_schema_migration (
    version BIGINT PRIMARY KEY CHECK (version > 0),
    name TEXT NOT NULL UNIQUE CHECK (btrim(name) <> ''),
    sql_sha256 TEXT NOT NULL CHECK (sql_sha256 ~ '^[0-9a-f]{64}$'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    applied_by TEXT NOT NULL DEFAULT current_user CHECK (btrim(applied_by) <> '')
);

CREATE TABLE IF NOT EXISTS candlescope_replay_session (
    session_id TEXT PRIMARY KEY CHECK (btrim(session_id) <> ''),
    organization_id TEXT NOT NULL CHECK (btrim(organization_id) <> ''),
    workspace_id TEXT NOT NULL CHECK (btrim(workspace_id) <> ''),
    data_epoch TEXT NOT NULL CHECK (btrim(data_epoch) <> ''),
    snapshot_version BIGINT NOT NULL CHECK (snapshot_version > 0),
    manifest_uri TEXT NOT NULL CHECK (btrim(manifest_uri) <> ''),
    manifest_sha256 TEXT NOT NULL CHECK (length(manifest_sha256) = 64),
    replay_start_ms BIGINT NOT NULL CHECK (replay_start_ms >= 0),
    replay_end_time_ms BIGINT NOT NULL CHECK (replay_end_time_ms >= replay_start_ms),
    start_event_time_ms BIGINT NOT NULL CHECK (start_event_time_ms >= 0),
    end_event_time_ms BIGINT NOT NULL CHECK (end_event_time_ms >= start_event_time_ms),
    expected_first_agg_trade_id BIGINT NOT NULL CHECK (expected_first_agg_trade_id > 0),
    expected_last_agg_trade_id BIGINT NOT NULL
        CHECK (expected_last_agg_trade_id >= expected_first_agg_trade_id),
    row_count BIGINT NOT NULL CHECK (row_count > 0),
    spec_public_json JSONB NOT NULL,
    schema_version TEXT NOT NULL CHECK (btrim(schema_version) <> ''),
    code_version TEXT NOT NULL CHECK (btrim(code_version) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (spec_public_json ? 'lease'),
    CHECK (NOT (spec_public_json ? 'lease_token')),
    CHECK (NOT (spec_public_json -> 'lease' ? 'lease_token'))
);

CREATE TABLE IF NOT EXISTS candlescope_replay_session_state (
    session_id TEXT PRIMARY KEY
        REFERENCES candlescope_replay_session (session_id),
    revision BIGINT NOT NULL CHECK (revision >= 0),
    event_sequence BIGINT NOT NULL CHECK (event_sequence >= 0),
    command_log_offset BIGINT NOT NULL CHECK (command_log_offset >= 0),
    source_sequence BIGINT NOT NULL CHECK (source_sequence >= 0),
    state_hash TEXT NOT NULL CHECK (btrim(state_hash) <> ''),
    previous_hash TEXT NOT NULL CHECK (btrim(previous_hash) <> ''),
    state_json JSONB NOT NULL,
    checkpoint BYTEA NOT NULL,
    checkpoint_sha256 TEXT NOT NULL CHECK (checkpoint_sha256 ~ '^[0-9a-f]{64}$'),
    closed BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS candlescope_replay_mutation (
    mutation_id BIGINT GENERATED ALWAYS AS IDENTITY,
    session_id TEXT NOT NULL
        REFERENCES candlescope_replay_session (session_id),
    kind TEXT NOT NULL CHECK (btrim(kind) <> ''),
    command_id TEXT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    event_sequence BIGINT NOT NULL CHECK (event_sequence >= 0),
    command_log_offset BIGINT NOT NULL CHECK (command_log_offset >= 0),
    mutation_hash TEXT NOT NULL CHECK (mutation_hash ~ '^(sha256:)?[0-9a-f]{64}$'),
    previous_hash TEXT NOT NULL CHECK (btrim(previous_hash) <> ''),
    state_hash TEXT NOT NULL CHECK (btrim(state_hash) <> ''),
    payload_json JSONB NOT NULL,
    checkpoint BYTEA NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, mutation_id),
    UNIQUE (session_id, mutation_hash),
    CHECK (NOT (payload_json ? 'lease_token'))
);

CREATE TABLE IF NOT EXISTS candlescope_replay_command_result (
    session_id TEXT NOT NULL
        REFERENCES candlescope_replay_session (session_id),
    command_id TEXT NOT NULL CHECK (btrim(command_id) <> ''),
    fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^(sha256:)?[0-9a-f]{64}$'),
    accepted BOOLEAN NOT NULL,
    result_json JSONB NULL,
    error_code TEXT NULL,
    error_message TEXT NULL,
    mutation_hash TEXT NOT NULL CHECK (mutation_hash ~ '^(sha256:)?[0-9a-f]{64}$'),
    revision BIGINT NOT NULL CHECK (revision >= 0),
    event_sequence BIGINT NOT NULL CHECK (event_sequence >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, command_id)
);

CREATE TABLE IF NOT EXISTS candlescope_replay_event_outbox (
    session_id TEXT NOT NULL
        REFERENCES candlescope_replay_session (session_id),
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_json JSONB NOT NULL,
    mutation_hash TEXT NOT NULL CHECK (mutation_hash ~ '^(sha256:)?[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, sequence),
    CHECK (NOT (event_json ? 'lease_token'))
);

REVOKE ALL ON TABLE candlescope_replay_schema_migration FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_session FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_session_state FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_mutation FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_command_result FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_event_outbox FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_session_lease FROM PUBLIC;
REVOKE ALL ON SEQUENCE candlescope_replay_mutation_mutation_id_seq FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON TABLE candlescope_replay_session
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT, UPDATE ON TABLE candlescope_replay_session_state
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT ON TABLE candlescope_replay_mutation
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT ON TABLE candlescope_replay_command_result
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT ON TABLE candlescope_replay_event_outbox
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT, UPDATE ON TABLE candlescope_replay_session_lease
    TO candlescope_replay_runtime;
GRANT USAGE, SELECT ON SEQUENCE candlescope_replay_mutation_mutation_id_seq
    TO candlescope_replay_runtime;
GRANT SELECT ON TABLE candlescope_replay_schema_migration
    TO candlescope_replay_runtime;
