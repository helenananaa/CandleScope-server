from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
from app.server_runtime.object_store import StoredObject
from app.server_runtime.query_audit_anchor import (
    ImmutableQueryAuditAnchorRepository,
    QueryAuditAnchorError,
    QueryAuditAnchorSigner,
)
from app.server_runtime.query_migrations import (
    QUERY_CONTROL_MIGRATION_SHA256,
    PostgresQueryControlMigrator,
    QueryControlMigrationDriftError,
)
from app.server_runtime.storage.postgres_query_control import (
    QueryAuditChainVerification,
)
from app.server_runtime.testing import InMemoryImmutableObjectStore

MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "deploy"
    / "server"
    / "postgres"
    / "migrations"
    / "001_query_control.sql"
)


def _verification(
    *,
    record_count: int = 7,
    head_audit_sequence: int = 7,
) -> QueryAuditChainVerification:
    return QueryAuditChainVerification(
        record_count=record_count,
        head_audit_sequence=head_audit_sequence,
        head_event_hash="a" * 64,
        head_updated_at_ms=1_700_000_000_123,
        migration_version=1,
        migration_sha256=QUERY_CONTROL_MIGRATION_SHA256,
    )


def _migrator(path: Path) -> PostgresQueryControlMigrator:
    return PostgresQueryControlMigrator(
        "postgresql://unused:unused@127.0.0.1:1/unused",
        migration_path=path,
        runtime_login_role="candlescope_query_app",
        auditor_login_role="candlescope_query_reader",
        backend_ids=("clickhouse-market-events-v1",),
    )


def test_query_control_migration_is_bound_to_exact_sql_bytes(tmp_path: Path) -> None:
    assert hashlib.sha256(MIGRATION_PATH.read_bytes()).hexdigest() == (
        QUERY_CONTROL_MIGRATION_SHA256
    )
    changed = tmp_path / "001_query_control.sql"
    changed.write_bytes(MIGRATION_PATH.read_bytes() + b"\n-- drift\n")
    with pytest.raises(QueryControlMigrationDriftError, match="checksum"):
        asyncio.run(_migrator(changed).apply())


def test_query_audit_anchor_is_deterministic_idempotent_and_key_scoped() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        repository = ImmutableQueryAuditAnchorRepository(
            object_store=store,
            signer=QueryAuditAnchorSigner(
                key_id="phase1i-key-2026-08",
                secret=b"phase1i-test-secret-is-at-least-32-bytes",
            ),
        )
        first = await repository.publish(_verification())
        replay = await repository.publish(_verification())
        assert replay == first
        assert "/phase1i-key-2026-08/" in first.uri
        verified = await repository.verify(first.uri, database=_verification())
        assert verified == first
        assert len(store.objects) == 1

    asyncio.run(run())


def test_query_audit_anchor_rejects_tamper_database_drift_and_key_conflict() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        repository = ImmutableQueryAuditAnchorRepository(
            object_store=store,
            signer=QueryAuditAnchorSigner(
                key_id="phase1i-key-2026-08",
                secret=b"phase1i-test-secret-is-at-least-32-bytes",
            ),
        )
        published = await repository.publish(_verification())
        with pytest.raises(QueryAuditAnchorError, match="database audit head"):
            await repository.verify(
                published.uri,
                database=_verification(record_count=6),
            )

        key = next(iter(store.objects))
        original = store.objects[key]
        tampered = original.data.replace(b'"record_count":"7"', b'"record_count":"8"')
        assert tampered != original.data
        store.objects[key] = StoredObject(
            data=tampered,
            metadata=original.metadata,
            content_type=original.content_type,
        )
        with pytest.raises(QueryAuditAnchorError, match="HMAC"):
            await repository.verify(published.uri)
        with pytest.raises(QueryAuditAnchorError, match="different bytes"):
            await repository.publish(_verification())

    asyncio.run(run())


def test_query_audit_anchor_rejects_noncanonical_json_and_wrong_key() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        signer = QueryAuditAnchorSigner(
            key_id="phase1i-key-a",
            secret=b"phase1i-test-secret-is-at-least-32-bytes",
        )
        repository = ImmutableQueryAuditAnchorRepository(
            object_store=store,
            signer=signer,
        )
        published = await repository.publish(_verification())
        wrong_key_repository = ImmutableQueryAuditAnchorRepository(
            object_store=store,
            signer=QueryAuditAnchorSigner(
                key_id="phase1i-key-b",
                secret=b"phase1i-test-secret-is-at-least-32-bytes",
            ),
        )
        with pytest.raises(QueryAuditAnchorError, match="key_id"):
            await wrong_key_repository.verify(published.uri)

        key = next(iter(store.objects))
        original = store.objects[key]
        store.objects[key] = StoredObject(
            data=original.data + b"\n",
            metadata=original.metadata,
            content_type=original.content_type,
        )
        with pytest.raises(QueryAuditAnchorError, match="canonical JSON"):
            await repository.verify(published.uri)

    asyncio.run(run())


def test_query_audit_anchor_requires_strong_secret_and_safe_key_id() -> None:
    with pytest.raises(ValueError, match="at least 32 bytes"):
        QueryAuditAnchorSigner(key_id="phase1i", secret=b"short")
    with pytest.raises(ValueError, match="lower-case safe ASCII"):
        QueryAuditAnchorSigner(key_id="Phase 1I", secret=b"x" * 32)
