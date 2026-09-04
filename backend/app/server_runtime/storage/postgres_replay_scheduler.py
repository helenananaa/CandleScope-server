"""PostgreSQL replay scheduler store. SKIP LOCKED assignment, no Actors."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.server_runtime.replay_scheduler import (
    ReplayRequestState,
    ReplaySchedulerAssignment,
    ReplaySchedulerError,
    ReplaySchedulerRequest,
    validate_transition,
)

REQUEST_TABLE = "candlescope_replay_scheduler_request"
WORKER_TABLE = "candlescope_replay_scheduler_worker"
ASSIGNMENT_TABLE = "candlescope_replay_scheduler_assignment"
COMMAND_JOURNAL_TABLE = "candlescope_replay_scheduler_command"
COMMAND_RESULT_TABLE = "candlescope_replay_command_result"
SCHEDULER_MIGRATION_VERSION = 3
SCHEDULER_MIGRATION_NAME = "003_replay_scheduler"
SCHEDULER_MIGRATION_SHA256 = (
    "0c1af75fe415d4a6f06f1784254aa43a27db666444e8efabd314eccb62828cf9"
)
DEFAULT_SCHEDULER_MIGRATION_PATH = (
    Path(__file__).resolve().parents[4]
    / "deploy"
    / "server"
    / "postgres"
    / "migrations"
    / "003_replay_scheduler.sql"
)


class PostgresReplaySchedulerStore:
    def __init__(self, dsn: str) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be a non-blank PostgreSQL connection string")
        self._dsn = dsn.strip()

    def __repr__(self) -> str:
        return "PostgresReplaySchedulerStore(dsn=<redacted>)"

    async def create_request(
        self, request: ReplaySchedulerRequest
    ) -> ReplaySchedulerRequest:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                INSERT INTO {REQUEST_TABLE} (
                    request_id, organization_id, workspace_id, idempotency_key,
                    payload_hash, payload_json, priority, state, session_id,
                    attempt, timeout_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    to_timestamp(%s / 1000.0)
                )
                """,
                (
                    request.request_id,
                    request.organization_id,
                    request.workspace_id,
                    request.idempotency_key,
                    request.payload_hash,
                    Jsonb(dict(request.payload)),
                    request.priority,
                    request.state.value,
                    request.session_id,
                    request.attempt,
                    request.timeout_at_ms,
                ),
            )
            return request

    async def get_by_idempotency(
        self, organization_id: str, idempotency_key: str
    ) -> ReplaySchedulerRequest | None:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT * FROM {REQUEST_TABLE}
                WHERE organization_id = %s AND idempotency_key = %s
                """,
                (organization_id, idempotency_key),
            )
            row = await cursor.fetchone()
            return None if row is None else _row_to_request(row)

    async def get_request(self, request_id: str) -> ReplaySchedulerRequest | None:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"SELECT * FROM {REQUEST_TABLE} WHERE request_id = %s",
                (request_id,),
            )
            row = await cursor.fetchone()
            return None if row is None else _row_to_request(row)

    async def count_open(self, organization_id: str) -> tuple[int, int]:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT
                    COUNT(*) FILTER (WHERE state = 'PENDING') AS pending,
                    COUNT(*) FILTER (WHERE state IN (
                        'ASSIGNED', 'STARTING', 'RUNNING', 'CANCELLING'
                    )) AS active
                FROM {REQUEST_TABLE}
                WHERE organization_id = %s
                """,
                (organization_id,),
            )
            row = await cursor.fetchone()
            assert row is not None
            return int(row["pending"]), int(row["active"])

    async def heartbeat(self, worker_id: str, *, capacity: int, ttl_ms: int) -> None:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                INSERT INTO {WORKER_TABLE} (
                    worker_id, capacity, active_sessions, heartbeat_expires_at
                ) VALUES (
                    %s, %s, 0, clock_timestamp() + (%s || ' milliseconds')::interval
                )
                ON CONFLICT (worker_id) DO UPDATE SET
                    capacity = EXCLUDED.capacity,
                    heartbeat_expires_at = clock_timestamp()
                        + (%s || ' milliseconds')::interval,
                    updated_at = clock_timestamp()
                """,
                (worker_id, capacity, ttl_ms, ttl_ms),
            )

    async def claim_next(
        self, worker_id: str, *, now_ms: int
    ) -> ReplaySchedulerAssignment | None:
        del now_ms
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT worker_id, capacity, active_sessions
                FROM {WORKER_TABLE}
                WHERE worker_id = %s AND heartbeat_expires_at > clock_timestamp()
                FOR UPDATE
                """,
                (worker_id,),
            )
            worker = await cursor.fetchone()
            if worker is None or worker["active_sessions"] >= worker["capacity"]:
                return None
            await cursor.execute(
                f"""
                SELECT request_id, attempt, session_id, state
                FROM {REQUEST_TABLE}
                WHERE (
                    (state = 'PENDING' AND timeout_at > clock_timestamp())
                    OR (
                        state IN ('ASSIGNED', 'STARTING', 'RUNNING')
                        AND EXISTS (
                            SELECT 1 FROM {ASSIGNMENT_TABLE} assignment
                            WHERE assignment.request_id
                                = {REQUEST_TABLE}.request_id
                            AND assignment.active = FALSE
                        )
                    )
                )
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """
            )
            request = await cursor.fetchone()
            if request is None:
                return None
            session_id = str(
                request["session_id"] or f"sess-{request['request_id'][-12:]}"
            )
            assignment_id = f"asg-{request['request_id'][-12:]}"
            attempt = int(request["attempt"]) + 1
            await cursor.execute(
                f"""
                UPDATE {REQUEST_TABLE}
                SET state = 'ASSIGNED',
                    session_id = %s,
                    attempt = %s,
                    updated_at = clock_timestamp()
                WHERE request_id = %s
                """,
                (session_id, attempt, request["request_id"]),
            )
            await cursor.execute(
                f"""
                INSERT INTO {ASSIGNMENT_TABLE} (
                    assignment_id, request_id, worker_id, session_id, attempt, active
                ) VALUES (%s, %s, %s, %s, %s, TRUE)
                ON CONFLICT (request_id) DO UPDATE SET
                    worker_id = EXCLUDED.worker_id,
                    session_id = EXCLUDED.session_id,
                    attempt = EXCLUDED.attempt,
                    active = TRUE
                """,
                (
                    assignment_id,
                    request["request_id"],
                    worker_id,
                    session_id,
                    attempt,
                ),
            )
            await cursor.execute(
                f"""
                UPDATE {WORKER_TABLE}
                SET active_sessions = active_sessions + 1,
                    updated_at = clock_timestamp()
                WHERE worker_id = %s
                """,
                (worker_id,),
            )
            return ReplaySchedulerAssignment(
                assignment_id=assignment_id,
                request_id=request["request_id"],
                worker_id=worker_id,
                session_id=session_id,
                attempt=attempt,
            )

    async def transition(
        self,
        request_id: str,
        target: ReplayRequestState,
        *,
        now_ms: int,
    ) -> ReplaySchedulerRequest:
        del now_ms
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"SELECT * FROM {REQUEST_TABLE} WHERE request_id = %s FOR UPDATE",
                (request_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ReplaySchedulerError("SCHEDULER_NOT_FOUND", "request not found")
            current = _row_to_request(row)
            validate_transition(current.state, target)
            await cursor.execute(
                f"""
                UPDATE {REQUEST_TABLE}
                SET state = %s, updated_at = clock_timestamp()
                WHERE request_id = %s
                """,
                (target.value, request_id),
            )
            row["state"] = target.value
            return _row_to_request(row)

    async def expire_workers_and_timeouts(self, *, now_ms: int) -> int:
        del now_ms
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                UPDATE {ASSIGNMENT_TABLE} AS assignment
                SET active = FALSE
                FROM {WORKER_TABLE} AS worker
                WHERE assignment.worker_id = worker.worker_id
                  AND assignment.active IS TRUE
                  AND worker.heartbeat_expires_at <= clock_timestamp()
                """
            )
            orphaned = cursor.rowcount or 0
            await cursor.execute(
                f"""
                UPDATE {WORKER_TABLE}
                SET active_sessions = 0,
                    updated_at = clock_timestamp()
                WHERE heartbeat_expires_at <= clock_timestamp()
                """
            )
            await cursor.execute(
                f"""
                UPDATE {REQUEST_TABLE}
                SET state = 'FAILED', updated_at = clock_timestamp()
                WHERE state = 'PENDING' AND timeout_at <= clock_timestamp()
                """
            )
            return orphaned + (cursor.rowcount or 0)

    async def get_by_session(
        self, session_id: str
    ) -> ReplaySchedulerRequest | None:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"SELECT * FROM {REQUEST_TABLE} WHERE session_id = %s",
                (session_id,),
            )
            row = await cursor.fetchone()
            return None if row is None else _row_to_request(row)

    async def get_assignment(
        self, session_id: str
    ) -> ReplaySchedulerAssignment | None:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT assignment_id, request_id, worker_id, session_id, attempt,
                       active
                FROM {ASSIGNMENT_TABLE}
                WHERE session_id = %s
                """,
                (session_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return ReplaySchedulerAssignment(
                assignment_id=str(row["assignment_id"]),
                request_id=str(row["request_id"]),
                worker_id=str(row["worker_id"]),
                session_id=str(row["session_id"]),
                attempt=int(row["attempt"]),
                active=bool(row["active"]),
            )

    async def enqueue_command(
        self, session_id: str, payload: Mapping[str, object]
    ) -> None:
        command_id = str(payload["command_id"])
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                INSERT INTO {COMMAND_JOURNAL_TABLE} (
                    session_id, command_id, payload_json
                ) VALUES (%s, %s, %s)
                ON CONFLICT (session_id, command_id) DO NOTHING
                """,
                (session_id, command_id, Jsonb(dict(payload))),
            )

    async def list_commands(
        self, session_id: str
    ) -> tuple[Mapping[str, object], ...]:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT payload_json
                FROM {COMMAND_JOURNAL_TABLE}
                WHERE session_id = %s
                ORDER BY created_at ASC
                """,
                (session_id,),
            )
            rows = await cursor.fetchall()
            return tuple(dict(row["payload_json"]) for row in rows)

    async def live_worker_count(self) -> int:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM {WORKER_TABLE}
                WHERE heartbeat_expires_at > clock_timestamp()
                  AND capacity > 0
                """
            )
            row = await cursor.fetchone()
            assert row is not None
            return int(row["n"])

    async def drop_claim(self, assignment: ReplaySchedulerAssignment) -> None:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                UPDATE {ASSIGNMENT_TABLE}
                SET active = FALSE
                WHERE assignment_id = %s AND worker_id = %s AND active IS TRUE
                """,
                (assignment.assignment_id, assignment.worker_id),
            )
            if cursor.rowcount:
                await cursor.execute(
                    f"""
                    UPDATE {WORKER_TABLE}
                    SET active_sessions = GREATEST(active_sessions - 1, 0),
                        updated_at = clock_timestamp()
                    WHERE worker_id = %s
                    """,
                    (assignment.worker_id,),
                )

    async def assignment_is_recovering(self, session_id: str) -> bool:
        async with (
            await self._connect() as connection,
            connection.cursor() as cursor,
        ):
            await cursor.execute(
                f"""
                SELECT assignment.active, worker.heartbeat_expires_at
                FROM {ASSIGNMENT_TABLE} AS assignment
                LEFT JOIN {WORKER_TABLE} AS worker
                  ON worker.worker_id = assignment.worker_id
                WHERE assignment.session_id = %s
                """,
                (session_id,),
            )
            row = await cursor.fetchone()
            if row is None:
                return False
            if row["active"] is not True:
                return True
            expires = row["heartbeat_expires_at"]
            if expires is None:
                return True
            await cursor.execute("SELECT clock_timestamp() AS now")
            now_row = await cursor.fetchone()
            assert now_row is not None
            return expires <= now_row["now"]

    async def _connect(self) -> psycopg.AsyncConnection[dict[str, Any]]:
        return await psycopg.AsyncConnection.connect(
            self._dsn,
            row_factory=dict_row,
            application_name="candlescope-replay-scheduler",
        )


async def apply_scheduler_migration(dsn: str, *, migration_path: Path) -> None:
    raw = migration_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != SCHEDULER_MIGRATION_SHA256:
        raise ReplaySchedulerError(
            "SCHEDULER_MIGRATION_DRIFT",
            "scheduler migration checksum differs from the compiled contract",
        )
    async with (
        await psycopg.AsyncConnection.connect(dsn, row_factory=dict_row) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(raw.decode("utf-8"))
        await cursor.execute(
            """
            INSERT INTO candlescope_replay_schema_migration (version, name, sql_sha256)
            VALUES (%s, %s, %s)
            ON CONFLICT (version) DO NOTHING
            """,
            (
                SCHEDULER_MIGRATION_VERSION,
                SCHEDULER_MIGRATION_NAME,
                SCHEDULER_MIGRATION_SHA256,
            ),
        )


def _row_to_request(row: Mapping[str, Any]) -> ReplaySchedulerRequest:
    timeout = row["timeout_at"]
    created = row["created_at"]
    return ReplaySchedulerRequest(
        request_id=str(row["request_id"]),
        organization_id=str(row["organization_id"]),
        workspace_id=str(row["workspace_id"]),
        idempotency_key=str(row["idempotency_key"]),
        payload_hash=str(row["payload_hash"]),
        payload=dict(row["payload_json"]),
        priority=int(row["priority"]),
        state=ReplayRequestState(str(row["state"])),
        session_id=row["session_id"],
        attempt=int(row["attempt"]),
        timeout_at_ms=int(timeout.timestamp() * 1000),
        created_at_ms=int(created.timestamp() * 1000),
    )
