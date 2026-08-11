from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import tempfile
import time
import uuid
from pathlib import Path

import psycopg
import pytest
from app.server_runtime.query_audit_anchor import (
    ImmutableQueryAuditAnchorRepository,
    QueryAuditAnchorSigner,
)
from app.server_runtime.query_control import (
    ClearHotProjectionQuarantineCommand,
)
from app.server_runtime.query_migrations import (
    QUERY_AUDITOR_GROUP_ROLE,
    QUERY_CONTROL_MIGRATION_TABLE,
    QUERY_RUNTIME_GROUP_ROLE,
    PostgresQueryControlMigrator,
)
from app.server_runtime.query_security import QueryAuditEvent
from app.server_runtime.storage.postgres_query_control import (
    HOT_PROJECTION_QUARANTINE_TABLE,
    QUERY_AUDIT_EVENT_TABLE,
    QUERY_AUDIT_HEAD_TABLE,
    PostgresQueryAuditVerifier,
    PostgresQueryControlStore,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore
from psycopg import sql

POSTGRES_ADMIN_DSN = (
    "postgresql://candlescope:phase1i-local-only@localhost:15432/candlescope"
)
POSTGRES_RUNTIME_ROLE = "candlescope_query_app"
POSTGRES_RUNTIME_PASSWORD = "phase1i-runtime-local-only"
POSTGRES_RUNTIME_DSN = (
    "postgresql://candlescope_query_app:phase1i-runtime-local-only"
    "@localhost:15432/candlescope"
)
POSTGRES_AUDITOR_ROLE = "candlescope_query_audit_reader"
POSTGRES_AUDITOR_PASSWORD = "phase1i-auditor-local-only"
POSTGRES_AUDITOR_DSN = (
    "postgresql://candlescope_query_audit_reader:phase1i-auditor-local-only"
    "@localhost:15432/candlescope"
)
MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "deploy/server/postgres/migrations/001_query_control.sql"
)
COMPOSE_PATH = Path(__file__).resolve().parents[3] / "deploy/server/compose.phase1i.yml"
S3_ENDPOINT_URL = "http://localhost:19000"
S3_BUCKET = "candlescope-query-operations"
S3_ACCESS_KEY_ID = "candlescope"
S3_SECRET_ACCESS_KEY = "phase1i-local-secret"
HMAC_SECRET = b"phase1i-anchor-test-secret-is-at-least-32-bytes"
BACKEND_ID = "clickhouse-market-events-v1"


@pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1I_INTEGRATION") != "1",
    reason="requires the explicit Phase 1I PostgreSQL/MinIO stack",
)
def test_real_migration_privileges_anchor_and_backup_restore() -> None:
    asyncio.run(_run_gate())


async def _run_gate() -> None:
    _require_explicit_local_reset()
    docker = _required_executable("docker")
    await _reset_postgres()
    migrator = _migrator()
    first = await migrator.apply()
    replay = await migrator.apply()
    assert first.applied is True
    assert replay.applied is False

    runtime = PostgresQueryControlStore(
        POSTGRES_RUNTIME_DSN,
        instance_id="phase1i-runtime-a",
    )
    await runtime.start()
    try:
        await runtime.emit(
            _audit_event("phase1i-query-1", timestamp_ms=1_700_000_000_001)
        )
        await runtime.emit(
            _audit_event("phase1i-query-2", timestamp_ms=1_700_000_000_002)
        )
        latched = await runtime.latch("phase1i parity mismatch")
        assert latched.active is True
        assert latched.generation == 1
        cleared = await runtime.clear(
            ClearHotProjectionQuarantineCommand(
                expected_generation=1,
                principal="phase1i-operator",
                reason_code="projection_repaired",
                request_id="phase1i-clear-1",
                timestamp_ms=time.time_ns() // 1_000_000,
            )
        )
        assert cleared.active is False
    finally:
        await runtime.stop()

    await _assert_least_privilege()
    before_restore, final_state = await _verification_and_state("before-restore")
    assert before_restore.record_count == 4
    assert before_restore.head_audit_sequence == 4
    assert final_state.active is False
    assert final_state.generation == 1

    anchor_store = S3ImmutableObjectStore(
        endpoint_url=S3_ENDPOINT_URL,
        region="us-east-1",
        bucket=S3_BUCKET,
        prefix=f"query-audit-{uuid.uuid4()}",
        access_key_id=S3_ACCESS_KEY_ID,
        secret_access_key=S3_SECRET_ACCESS_KEY,
    )
    await anchor_store.ensure_bucket()
    anchors = ImmutableQueryAuditAnchorRepository(
        object_store=anchor_store,
        signer=QueryAuditAnchorSigner(
            key_id="phase1i-local-key",
            secret=HMAC_SECRET,
        ),
    )
    published = await anchors.publish(before_restore)
    assert await anchors.publish(before_restore) == published
    assert await anchors.verify(published.uri, database=before_restore) == published

    with tempfile.TemporaryDirectory(prefix="candlescope-phase1i-") as directory:
        dump_path = Path(directory) / "query-control.dump"
        await _run_backup(docker, dump_path)
        assert dump_path.stat().st_size > 0
        dump_sha256 = hashlib.sha256(dump_path.read_bytes()).hexdigest()
        assert len(dump_sha256) == 64

        await _drop_query_schema()
        recreated = await migrator.apply()
        assert recreated.applied is True
        await _truncate_recreated_schema()
        await _run_restore(docker, dump_path)

    after_restore, restored_state = await _verification_and_state("after-restore")
    assert after_restore == before_restore
    assert restored_state == final_state
    restored_anchor = await anchors.verify(
        published.uri,
        database=after_restore,
    )
    assert restored_anchor.content_sha256 == published.content_sha256

    restarted_runtime = PostgresQueryControlStore(
        POSTGRES_RUNTIME_DSN,
        instance_id="phase1i-runtime-restored",
    )
    await restarted_runtime.start()
    try:
        await restarted_runtime.emit(
            _audit_event("phase1i-query-after-restore", timestamp_ms=1_700_000_000_003)
        )
    finally:
        await restarted_runtime.stop()
    advanced, _ = await _verification_and_state("after-new-event")
    assert advanced.record_count == before_restore.record_count + 1
    assert advanced.head_audit_sequence == before_restore.head_audit_sequence + 1


def _migrator() -> PostgresQueryControlMigrator:
    return PostgresQueryControlMigrator(
        POSTGRES_ADMIN_DSN,
        migration_path=MIGRATION_PATH,
        runtime_login_role=POSTGRES_RUNTIME_ROLE,
        auditor_login_role=POSTGRES_AUDITOR_ROLE,
        backend_ids=(BACKEND_ID,),
    )


def _audit_event(request_id: str, *, timestamp_ms: int) -> QueryAuditEvent:
    return QueryAuditEvent(
        request_id=request_id,
        principal="phase1i-gateway",
        action="snapshot_market_event_query",
        outcome="success",
        status_code=200,
        timestamp_ms=timestamp_ms,
        latency_ms=3,
        backend="hot",
    )


async def _verification_and_state(label: str):
    verifier = PostgresQueryAuditVerifier(
        POSTGRES_AUDITOR_DSN,
        verifier_id=f"phase1i-{label}",
    )
    await verifier.start()
    try:
        return (
            await verifier.verify_audit_chain(max_records=100),
            await verifier.quarantine_status(BACKEND_ID),
        )
    finally:
        await verifier.stop()


async def _reset_postgres() -> None:
    await _drop_query_schema()
    async with await psycopg.AsyncConnection.connect(POSTGRES_ADMIN_DSN) as connection:
        async with connection.cursor() as cursor:
            for role_name, password in (
                (POSTGRES_RUNTIME_ROLE, POSTGRES_RUNTIME_PASSWORD),
                (POSTGRES_AUDITOR_ROLE, POSTGRES_AUDITOR_PASSWORD),
            ):
                await cursor.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname = %s",
                    (role_name,),
                )
                command = "CREATE" if await cursor.fetchone() is None else "ALTER"
                await cursor.execute(
                    sql.SQL(
                        f"{command} ROLE {{}} LOGIN PASSWORD {{}} INHERIT "
                        "NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOREPLICATION NOBYPASSRLS"
                    ).format(sql.Identifier(role_name), sql.Literal(password))
                )
            await cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
                ([QUERY_RUNTIME_GROUP_ROLE, QUERY_AUDITOR_GROUP_ROLE],),
            )
            for (group_role,) in await cursor.fetchall():
                for login_role in (POSTGRES_RUNTIME_ROLE, POSTGRES_AUDITOR_ROLE):
                    await cursor.execute(
                        sql.SQL("REVOKE {} FROM {}").format(
                            sql.Identifier(group_role),
                            sql.Identifier(login_role),
                        )
                    )


async def _drop_query_schema() -> None:
    async with await psycopg.AsyncConnection.connect(POSTGRES_ADMIN_DSN) as connection:
        for table in (
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
            QUERY_CONTROL_MIGRATION_TABLE,
        ):
            await connection.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table))
            )


async def _truncate_recreated_schema() -> None:
    async with await psycopg.AsyncConnection.connect(POSTGRES_ADMIN_DSN) as connection:
        await connection.execute(
            sql.SQL("TRUNCATE {}, {}, {}, {} RESTART IDENTITY").format(
                sql.Identifier(QUERY_AUDIT_EVENT_TABLE),
                sql.Identifier(QUERY_AUDIT_HEAD_TABLE),
                sql.Identifier(HOT_PROJECTION_QUARANTINE_TABLE),
                sql.Identifier(QUERY_CONTROL_MIGRATION_TABLE),
            )
        )


async def _assert_least_privilege() -> None:
    await _assert_denied(
        POSTGRES_RUNTIME_DSN,
        f"SELECT event_json FROM {QUERY_AUDIT_EVENT_TABLE}",
    )
    await _assert_denied(
        POSTGRES_RUNTIME_DSN,
        "CREATE TABLE phase1i_runtime_must_not_create (value INTEGER)",
    )
    await _assert_denied(
        POSTGRES_AUDITOR_DSN,
        f"UPDATE {QUERY_AUDIT_HEAD_TABLE} SET updated_at = clock_timestamp()",
    )
    async with await psycopg.AsyncConnection.connect(
        POSTGRES_RUNTIME_DSN
    ) as connection:
        await connection.execute(
            f"SELECT audit_sequence FROM {QUERY_AUDIT_EVENT_TABLE} LIMIT 1"
        )
    async with await psycopg.AsyncConnection.connect(
        POSTGRES_AUDITOR_DSN
    ) as connection:
        await connection.execute(f"SELECT event_json FROM {QUERY_AUDIT_EVENT_TABLE}")


async def _assert_denied(dsn: str, statement: str) -> None:
    async with await psycopg.AsyncConnection.connect(dsn) as connection:
        await connection.set_autocommit(True)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            await connection.execute(statement)


async def _run_backup(docker: str, dump_path: Path) -> None:
    arguments = _compose_exec(docker, "pg_dump") + [
        "--format=custom",
        "--data-only",
        "--no-owner",
        "--no-privileges",
        "--strict-names",
        "--username=candlescope",
        "--dbname=candlescope",
    ]
    for relation in (
        QUERY_CONTROL_MIGRATION_TABLE,
        QUERY_AUDIT_HEAD_TABLE,
        QUERY_AUDIT_EVENT_TABLE,
        "candlescope_query_audit_event_audit_sequence_seq",
        HOT_PROJECTION_QUARANTINE_TABLE,
    ):
        arguments.append(f"--table=public.{relation}")
    dump_path.write_bytes(await _run_process(*arguments))


async def _run_restore(docker: str, dump_path: Path) -> None:
    await _run_process(
        *_compose_exec(docker, "pg_restore"),
        "--data-only",
        "--no-owner",
        "--no-privileges",
        "--exit-on-error",
        "--single-transaction",
        "--username=candlescope",
        "--dbname=candlescope",
        stdin_data=dump_path.read_bytes(),
    )


def _compose_exec(docker: str, executable: str) -> list[str]:
    return [
        docker,
        "compose",
        "--file",
        str(COMPOSE_PATH),
        "exec",
        "--no-TTY",
        "postgres",
        executable,
    ]


async def _run_process(
    *arguments: str,
    stdin_data: bytes | None = None,
) -> bytes:
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdin=(asyncio.subprocess.PIPE if stdin_data is not None else None),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate(stdin_data)
    assert process.returncode == 0, {
        "arguments": [Path(arguments[0]).name, *arguments[1:]],
        "stdout": stdout.decode(),
        "stderr": stderr.decode(),
    }
    return stdout


def _required_executable(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise RuntimeError(f"required executable is missing: {name}")
    return path


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1I_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1I_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds its local PostgreSQL schema"
        )
