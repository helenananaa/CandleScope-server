DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'candlescope_replay_runtime'
    ) THEN
        RAISE EXCEPTION 'candlescope_replay_runtime is missing';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS candlescope_replay_scheduler_request (
    request_id TEXT PRIMARY KEY CHECK (btrim(request_id) <> ''),
    organization_id TEXT NOT NULL CHECK (btrim(organization_id) <> ''),
    workspace_id TEXT NOT NULL CHECK (btrim(workspace_id) <> ''),
    idempotency_key TEXT NOT NULL CHECK (btrim(idempotency_key) <> ''),
    payload_hash TEXT NOT NULL CHECK (btrim(payload_hash) <> ''),
    payload_json JSONB NOT NULL,
    priority INTEGER NOT NULL CHECK (priority >= 0 AND priority <= 100),
    state TEXT NOT NULL CHECK (state IN (
        'PENDING', 'ASSIGNED', 'STARTING', 'RUNNING',
        'CANCELLING', 'CANCELLED', 'FAILED', 'COMPLETED'
    )),
    session_id TEXT NULL,
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    timeout_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, idempotency_key),
    CHECK (NOT (payload_json ? 'lease_token'))
);

CREATE TABLE IF NOT EXISTS candlescope_replay_scheduler_worker (
    worker_id TEXT PRIMARY KEY CHECK (btrim(worker_id) <> ''),
    capacity INTEGER NOT NULL CHECK (capacity >= 0),
    active_sessions INTEGER NOT NULL CHECK (active_sessions >= 0),
    heartbeat_expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (active_sessions <= capacity)
);

CREATE TABLE IF NOT EXISTS candlescope_replay_scheduler_assignment (
    assignment_id TEXT PRIMARY KEY CHECK (btrim(assignment_id) <> ''),
    request_id TEXT NOT NULL UNIQUE
        REFERENCES candlescope_replay_scheduler_request (request_id),
    worker_id TEXT NOT NULL,
    session_id TEXT NOT NULL CHECK (btrim(session_id) <> ''),
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX IF NOT EXISTS candlescope_replay_active_session_attempt
    ON candlescope_replay_scheduler_assignment (session_id)
    WHERE active;

CREATE TABLE IF NOT EXISTS candlescope_replay_scheduler_command (
    session_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    worker_id TEXT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, command_id),
    CHECK (NOT (payload_json ? 'lease_token'))
);

CREATE TABLE IF NOT EXISTS candlescope_replay_scheduler_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (btrim(event_type) <> ''),
    organization_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    detail_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (NOT (detail_json ? 'lease_token')),
    CHECK (NOT (detail_json ? 'password'))
);

REVOKE ALL ON TABLE candlescope_replay_scheduler_request FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_scheduler_worker FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_scheduler_assignment FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_scheduler_command FROM PUBLIC;
REVOKE ALL ON TABLE candlescope_replay_scheduler_audit FROM PUBLIC;
REVOKE ALL ON SEQUENCE candlescope_replay_scheduler_audit_audit_id_seq FROM PUBLIC;

GRANT SELECT, INSERT, UPDATE ON TABLE candlescope_replay_scheduler_request
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT, UPDATE ON TABLE candlescope_replay_scheduler_worker
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT, UPDATE ON TABLE candlescope_replay_scheduler_assignment
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT, UPDATE ON TABLE candlescope_replay_scheduler_command
    TO candlescope_replay_runtime;
GRANT SELECT, INSERT ON TABLE candlescope_replay_scheduler_audit
    TO candlescope_replay_runtime;
GRANT USAGE, SELECT ON SEQUENCE candlescope_replay_scheduler_audit_audit_id_seq
    TO candlescope_replay_runtime;
