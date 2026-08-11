from __future__ import annotations

import asyncio
import os
import shutil
import time
import uuid
from pathlib import Path

import psycopg
import pytest
from app.server_runtime.query_audit_anchor import (
    ImmutableQueryAuditAnchorRepository,
    QueryAuditAnchorSigner,
)
from app.server_runtime.query_backup_catalog import (
    ImmutablePhysicalBackupCatalog,
    PhysicalBackupManifestSigner,
    PhysicalBackupRequest,
    parse_postgres_backup_manifest,
)
from app.server_runtime.query_migrations import (
    QUERY_AUDITOR_GROUP_ROLE,
    QUERY_CONTROL_MIGRATION_TABLE,
    QUERY_RUNTIME_GROUP_ROLE,
    PostgresQueryControlMigrator,
)
from app.server_runtime.query_security import QueryAuditEvent
from app.server_runtime.query_wal_archive import ImmutablePostgresWalArchive
from app.server_runtime.storage.postgres_query_control import (
    HOT_PROJECTION_QUARANTINE_TABLE,
    QUERY_AUDIT_EVENT_TABLE,
    QUERY_AUDIT_HEAD_TABLE,
    PostgresQueryAuditVerifier,
    PostgresQueryControlStore,
)
from app.server_runtime.storage.s3 import S3ImmutableObjectStore
from psycopg import sql

COMPOSE_PATH = Path(__file__).resolve().parents[3] / "deploy/server/compose.phase1j.yml"
MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "deploy/server/postgres/migrations/001_query_control.sql"
)
ADMIN_DSN = "postgresql://candlescope:phase1j-local-only@localhost:15432/candlescope"
RECOVERY_ADMIN_DSN = (
    "postgresql://candlescope:phase1j-local-only@localhost:15433/candlescope"
)
RUNTIME_ROLE = "candlescope_query_app"
RUNTIME_PASSWORD = "phase1j-runtime-local-only"
RUNTIME_DSN = (
    "postgresql://candlescope_query_app:phase1j-runtime-local-only"
    "@localhost:15432/candlescope"
)
RECOVERY_RUNTIME_DSN = (
    "postgresql://candlescope_query_app:phase1j-runtime-local-only"
    "@localhost:15433/candlescope"
)
AUDITOR_ROLE = "candlescope_query_audit_reader"
AUDITOR_PASSWORD = "phase1j-auditor-local-only"
AUDITOR_DSN = (
    "postgresql://candlescope_query_audit_reader:phase1j-auditor-local-only"
    "@localhost:15432/candlescope"
)
RECOVERY_AUDITOR_DSN = (
    "postgresql://candlescope_query_audit_reader:phase1j-auditor-local-only"
    "@localhost:15433/candlescope"
)
S3_ENDPOINT_URL = "http://localhost:19000"
S3_BUCKET = "candlescope-pitr-operations"
S3_ACCESS_KEY_ID = "candlescope"
S3_SECRET_ACCESS_KEY = "phase1j-local-secret"
CLUSTER_ID = "phase1j-primary"
AUDIT_HMAC_SECRET = b"phase1j-audit-anchor-secret-is-at-least-32-bytes"
BACKUP_HMAC_SECRET = b"phase1j-backup-catalog-secret-is-at-least-32-bytes"


@pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1J_INTEGRATION") != "1",
    reason="requires the explicit Phase 1J PostgreSQL/MinIO PITR stack",
)
def test_real_physical_backup_wal_archive_and_target_time_recovery() -> None:
    asyncio.run(_run_gate())


async def _run_gate() -> None:
    _require_explicit_local_reset()
    docker = _required_executable("docker")
    await _reset_postgres()
    await _migrator().apply()

    runtime = PostgresQueryControlStore(RUNTIME_DSN, instance_id="phase1j-primary")
    await runtime.start()
    try:
        await runtime.emit(_event("phase1j-base-event", timestamp_ms=1_700_000_000_001))
    finally:
        await runtime.stop()

    await _capture_base_backup(docker)
    await _run_compose(
        docker,
        "exec",
        "--no-TTY",
        "--user",
        "postgres",
        "postgres",
        "pg_verifybackup",
        "--no-parse-wal",
        "/base-backup/current",
    )

    runtime = PostgresQueryControlStore(RUNTIME_DSN, instance_id="phase1j-primary")
    await runtime.start()
    try:
        await runtime.emit(
            _event("phase1j-before-target", timestamp_ms=1_700_000_000_002)
        )
    finally:
        await runtime.stop()
    before_target = await _verify(AUDITOR_DSN, "phase1j-before-target")
    assert before_target.record_count == 2

    prefix = f"phase1j-{uuid.uuid4()}"
    object_store = _store(prefix)
    await object_store.ensure_bucket()
    anchors = ImmutableQueryAuditAnchorRepository(
        object_store=object_store,
        signer=QueryAuditAnchorSigner(
            key_id="phase1j-audit-key",
            secret=AUDIT_HMAC_SECRET,
        ),
    )
    anchor = await anchors.publish(before_target)

    target_time = await _database_target_time()
    await _write_container_file(
        docker,
        service="postgres",
        path="/base-backup/recovery_target_time",
        data=(target_time + "\n").encode(),
    )
    await asyncio.sleep(0.05)
    runtime = PostgresQueryControlStore(RUNTIME_DSN, instance_id="phase1j-primary")
    await runtime.start()
    try:
        await runtime.emit(
            _event("phase1j-after-target", timestamp_ms=1_700_000_000_003)
        )
    finally:
        await runtime.stop()
    primary_after_target = await _verify(AUDITOR_DSN, "phase1j-primary-tail")
    assert primary_after_target.record_count == 3
    await _force_and_wait_for_wal_archive()

    wal_archive = ImmutablePostgresWalArchive(
        object_store=object_store,
        cluster_id=CLUSTER_ID,
    )
    wal_filenames = await _container_filenames(docker, "/wal-archive")
    assert any(len(filename) == 24 for filename in wal_filenames)
    for filename in wal_filenames:
        local_data = await _read_container_file(
            docker,
            service="postgres",
            path=f"/wal-archive/{filename}",
        )
        archived = await wal_archive.archive(filename, local_data)
        restored, restored_data = await wal_archive.restore(filename)
        assert restored.sha256 == archived.sha256
        assert restored_data == local_data
        await _write_container_file(
            docker,
            service="postgres",
            path=f"/wal-archive/{filename}",
            data=restored_data,
        )

    artifacts = {
        name: await _read_container_file(
            docker,
            service="postgres",
            path=f"/base-backup/current/{name}",
        )
        for name in ("backup_manifest", "base.tar.gz", "pg_wal.tar.gz")
    }
    metadata = parse_postgres_backup_manifest(artifacts["backup_manifest"])
    backup_id = str(uuid.uuid4())
    wal_prefix_uri = (
        object_store.uri_for(wal_archive.key_for("000000010000000000000000")).rsplit(
            "/", 1
        )[0]
        + "/"
    )
    catalog = ImmutablePhysicalBackupCatalog(
        object_store=object_store,
        signer=PhysicalBackupManifestSigner(
            key_id="phase1j-backup-key",
            secret=BACKUP_HMAC_SECRET,
        ),
        max_artifact_bytes=64 * 1024 * 1024,
        max_total_bytes=128 * 1024 * 1024,
    )
    request = PhysicalBackupRequest(
        backup_id=backup_id,
        cluster_id=CLUSTER_ID,
        created_at_ms=time.time_ns() // 1_000_000,
        recovery_target_time=target_time,
        postgres_version="18.4",
        system_identifier=metadata.system_identifier,
        timeline=metadata.timeline,
        start_lsn=metadata.start_lsn,
        end_lsn=metadata.end_lsn,
        wal_archive_prefix_uri=wal_prefix_uri,
        audit_anchor_uri=anchor.uri,
        audit_anchor_sha256=anchor.content_sha256,
        audit_head_sequence=before_target.head_audit_sequence,
        audit_head_event_hash=before_target.head_event_hash,
        migration_version=before_target.migration_version,
        migration_sha256=before_target.migration_sha256,
    )
    published_backup = await catalog.publish(request, artifacts)
    assert published_backup.created_artifacts == 3
    verified_backup = await catalog.verify(
        published_backup.manifest_uri,
        expected_audit_anchor_uri=anchor.uri,
    )
    assert verified_backup.manifest_sha256 == published_backup.manifest_sha256
    assert verified_backup.manifest.request.recovery_target_time == target_time

    for artifact in verified_backup.manifest.artifacts:
        stored = await object_store.get_uri(artifact.uri)
        await _write_container_file(
            docker,
            service="postgres",
            path=f"/base-backup/current/{artifact.name}",
            data=stored.data,
        )

    await _run_compose(
        docker,
        "--profile",
        "recovery",
        "up",
        "--detach",
        "--wait",
        "postgres-recovery",
    )
    recovered = await _verify(RECOVERY_AUDITOR_DSN, "phase1j-recovered")
    assert recovered == before_target
    await anchors.verify(anchor.uri, database=recovered)
    request_ids = await _recovered_request_ids()
    assert "phase1j-before-target" in request_ids
    assert "phase1j-after-target" not in request_ids

    recovery_runtime = PostgresQueryControlStore(
        RECOVERY_RUNTIME_DSN,
        instance_id="phase1j-recovered-runtime",
    )
    await recovery_runtime.start()
    try:
        await recovery_runtime.emit(
            _event("phase1j-after-recovery", timestamp_ms=1_700_000_000_004)
        )
    finally:
        await recovery_runtime.stop()
    advanced = await _verify(RECOVERY_AUDITOR_DSN, "phase1j-recovered-tail")
    assert advanced.record_count == before_target.record_count + 1
    assert advanced.head_audit_sequence > before_target.head_audit_sequence
    async with await psycopg.AsyncConnection.connect(RECOVERY_ADMIN_DSN) as connection:
        in_recovery = await connection.execute("SELECT pg_is_in_recovery()")
        assert (await in_recovery.fetchone())[0] is False


def _migrator() -> PostgresQueryControlMigrator:
    return PostgresQueryControlMigrator(
        ADMIN_DSN,
        migration_path=MIGRATION_PATH,
        runtime_login_role=RUNTIME_ROLE,
        auditor_login_role=AUDITOR_ROLE,
        backend_ids=("clickhouse-market-events-v1",),
    )


def _event(request_id: str, *, timestamp_ms: int) -> QueryAuditEvent:
    return QueryAuditEvent(
        request_id=request_id,
        principal="phase1j-gateway",
        action="snapshot_market_event_query",
        outcome="success",
        status_code=200,
        timestamp_ms=timestamp_ms,
        latency_ms=2,
        backend="hot",
    )


async def _verify(dsn: str, verifier_id: str):
    verifier = PostgresQueryAuditVerifier(dsn, verifier_id=verifier_id)
    await verifier.start()
    try:
        return await verifier.verify_audit_chain(max_records=100)
    finally:
        await verifier.stop()


def _store(prefix: str) -> S3ImmutableObjectStore:
    return S3ImmutableObjectStore(
        endpoint_url=S3_ENDPOINT_URL,
        region="us-east-1",
        bucket=S3_BUCKET,
        prefix=prefix,
        access_key_id=S3_ACCESS_KEY_ID,
        secret_access_key=S3_SECRET_ACCESS_KEY,
        request_timeout_ms=30_000,
    )


async def _reset_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection:
        for table in (
            QUERY_AUDIT_EVENT_TABLE,
            QUERY_AUDIT_HEAD_TABLE,
            HOT_PROJECTION_QUARANTINE_TABLE,
            QUERY_CONTROL_MIGRATION_TABLE,
        ):
            await connection.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table))
            )
        async with connection.cursor() as cursor:
            for role_name, password in (
                (RUNTIME_ROLE, RUNTIME_PASSWORD),
                (AUDITOR_ROLE, AUDITOR_PASSWORD),
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
                for login_role in (RUNTIME_ROLE, AUDITOR_ROLE):
                    await cursor.execute(
                        sql.SQL("REVOKE {} FROM {}").format(
                            sql.Identifier(group_role),
                            sql.Identifier(login_role),
                        )
                    )


async def _capture_base_backup(docker: str) -> None:
    await _run_compose(
        docker,
        "exec",
        "--no-TTY",
        "--user",
        "postgres",
        "--env",
        "PGPASSWORD=phase1j-local-only",
        "postgres",
        "pg_basebackup",
        "--host=127.0.0.1",
        "--username=candlescope",
        "--pgdata=/base-backup/current",
        "--format=tar",
        "--gzip",
        "--wal-method=stream",
        "--checkpoint=fast",
        "--manifest-checksums=SHA256",
    )


async def _database_target_time() -> str:
    async with await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection:
        result = await connection.execute(
            "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', "
            "'YYYY-MM-DD HH24:MI:SS.US\"+00\"')"
        )
        row = await result.fetchone()
    assert row is not None
    return str(row[0])


async def _force_and_wait_for_wal_archive() -> None:
    async with await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection:
        result = await connection.execute(
            "SELECT pg_walfile_name(pg_current_wal_lsn())"
        )
        row = await result.fetchone()
        assert row is not None
        required_filename = row[0]
        await connection.execute("SELECT pg_switch_wal()")
    deadline = asyncio.get_running_loop().time() + 10
    while asyncio.get_running_loop().time() < deadline:
        async with await psycopg.AsyncConnection.connect(ADMIN_DSN) as connection:
            result = await connection.execute(
                "SELECT last_archived_wal, failed_count FROM pg_stat_archiver"
            )
            row = await result.fetchone()
        assert row is not None
        assert row[1] == 0
        if row[0] is not None and row[0] >= required_filename:
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("required WAL segment was not archived")


async def _recovered_request_ids() -> set[str]:
    async with await psycopg.AsyncConnection.connect(
        RECOVERY_AUDITOR_DSN
    ) as connection:
        result = await connection.execute(
            f"SELECT event_json ->> 'request_id' FROM {QUERY_AUDIT_EVENT_TABLE}"
        )
        return {row[0] for row in await result.fetchall()}


async def _container_filenames(docker: str, directory: str) -> tuple[str, ...]:
    output = await _run_compose(
        docker,
        "exec",
        "--no-TTY",
        "--user",
        "postgres",
        "postgres",
        "find",
        directory,
        "-maxdepth",
        "1",
        "-type",
        "f",
        "-printf",
        "%f\\n",
    )
    return tuple(sorted(output.decode().splitlines()))


async def _read_container_file(
    docker: str,
    *,
    service: str,
    path: str,
) -> bytes:
    return await _run_compose(
        docker,
        "exec",
        "--no-TTY",
        "--user",
        "postgres",
        service,
        "cat",
        path,
    )


async def _write_container_file(
    docker: str,
    *,
    service: str,
    path: str,
    data: bytes,
) -> None:
    await _run_compose(
        docker,
        "exec",
        "--no-TTY",
        "--user",
        "postgres",
        service,
        "dd",
        f"of={path}",
        "status=none",
        stdin_data=data,
    )


async def _run_compose(
    docker: str,
    *arguments: str,
    stdin_data: bytes | None = None,
) -> bytes:
    return await _run_process(
        docker,
        "compose",
        "--file",
        str(COMPOSE_PATH),
        *arguments,
        stdin_data=stdin_data,
    )


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
        "stdout": stdout.decode(errors="replace"),
        "stderr": stderr.decode(errors="replace"),
    }
    return stdout


def _required_executable(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise RuntimeError(f"required executable is missing: {name}")
    return path


def _require_explicit_local_reset() -> None:
    if os.environ.get("CANDLESCOPE_PHASE1J_ALLOW_TEST_RESET") != "1":
        raise RuntimeError(
            "CANDLESCOPE_PHASE1J_ALLOW_TEST_RESET=1 is required because the "
            "integration gate rebuilds PostgreSQL and recovery volumes"
        )
