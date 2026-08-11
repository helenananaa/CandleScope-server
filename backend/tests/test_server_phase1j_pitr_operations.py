from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import replace

import pytest
from app.server_runtime.object_store import StoredObject
from app.server_runtime.query_backup_catalog import (
    ImmutablePhysicalBackupCatalog,
    PhysicalBackupError,
    PhysicalBackupManifestSigner,
    PhysicalBackupRequest,
    parse_postgres_backup_manifest,
)
from app.server_runtime.query_wal_archive import (
    ArchivedWalObject,
    ImmutablePostgresWalArchive,
    WalArchiveError,
)
from app.server_runtime.testing import InMemoryImmutableObjectStore
from scripts import server_query_wal_archive as wal_archive_script
from scripts import server_query_wal_restore as wal_restore_script
from scripts.server_query_backup_publish import (
    _read_artifacts,
    _verify_postgres_backup,
)

SECRET = b"phase1j-backup-test-secret-is-at-least-32-bytes"


def _postgres_manifest() -> bytes:
    return json.dumps(
        {
            "PostgreSQL-Backup-Manifest-Version": 2,
            "System-Identifier": 7_672_760_263_611_183_147,
            "Files": [
                {
                    "Path": "PG_VERSION",
                    "Size": 3,
                    "Checksum-Algorithm": "SHA256",
                    "Checksum": hashlib.sha256(b"18\n").hexdigest(),
                }
            ],
            "WAL-Ranges": [
                {
                    "Timeline": 1,
                    "Start-LSN": "0/3000028",
                    "End-LSN": "0/3000120",
                }
            ],
            "Manifest-Checksum": "a" * 64,
        },
        separators=(",", ":"),
    ).encode()


def _artifacts() -> dict[str, bytes]:
    return {
        "backup_manifest": _postgres_manifest(),
        "base.tar.gz": b"base-tar-gzip-bytes",
        "pg_wal.tar.gz": b"wal-tar-gzip-bytes",
    }


def _request() -> PhysicalBackupRequest:
    return PhysicalBackupRequest(
        backup_id=str(uuid.UUID("11111111-2222-4333-8444-555555555555")),
        cluster_id="phase1j-primary",
        created_at_ms=1_700_000_000_000,
        recovery_target_time="2026-08-11 13:00:00.123456+00",
        postgres_version="18.4",
        system_identifier=7_672_760_263_611_183_147,
        timeline=1,
        start_lsn="0/3000028",
        end_lsn="0/3000120",
        wal_archive_prefix_uri=(
            "s3://candlescope-test/query-operations/wal/v1/phase1j-primary/"
        ),
        audit_anchor_uri=(
            "s3://candlescope-test/query-operations/anchors/v1/key/anchor.json"
        ),
        audit_anchor_sha256="b" * 64,
        audit_head_sequence=7,
        audit_head_event_hash="c" * 64,
        migration_version=1,
        migration_sha256="d" * 64,
    )


def _catalog(
    store: InMemoryImmutableObjectStore,
    *,
    max_artifact_bytes: int = 1024,
    max_total_bytes: int = 4096,
) -> ImmutablePhysicalBackupCatalog:
    return ImmutablePhysicalBackupCatalog(
        object_store=store,
        signer=PhysicalBackupManifestSigner(
            key_id="phase1j-backup-key",
            secret=SECRET,
        ),
        max_artifact_bytes=max_artifact_bytes,
        max_total_bytes=max_total_bytes,
    )


def test_postgres_manifest_and_backup_catalog_round_trip_are_deterministic() -> None:
    async def run() -> None:
        metadata = parse_postgres_backup_manifest(_postgres_manifest())
        assert metadata.system_identifier == 7_672_760_263_611_183_147
        assert metadata.timeline == 1
        assert metadata.start_lsn == "0/3000028"
        assert metadata.end_lsn == "0/3000120"
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        catalog = _catalog(store)
        first = await catalog.publish(_request(), _artifacts())
        replay = await catalog.publish(_request(), _artifacts())
        assert first.created_artifacts == 3
        assert first.manifest_created is True
        assert replay.created_artifacts == 0
        assert replay.manifest_created is False
        assert replay.manifest_sha256 == first.manifest_sha256
        verified = await catalog.verify(
            first.manifest_uri,
            expected_audit_anchor_uri=_request().audit_anchor_uri,
        )
        assert verified.manifest == first.manifest
        assert (
            verified.manifest.request.recovery_target_time
            == "2026-08-11 13:00:00.123456+00"
        )
        assert len(store.objects) == 4

    asyncio.run(run())


def test_backup_catalog_rejects_tamper_anchor_drift_and_artifact_bounds() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        catalog = _catalog(store)
        published = await catalog.publish(_request(), _artifacts())
        with pytest.raises(PhysicalBackupError, match="unexpected audit anchor"):
            await catalog.verify(
                published.manifest_uri,
                expected_audit_anchor_uri="s3://candlescope-test/wrong/anchor.json",
            )

        artifact_key = next(key for key in store.objects if key.endswith("base.tar.gz"))
        original = store.objects[artifact_key]
        store.objects[artifact_key] = StoredObject(
            data=original.data + b"tamper",
            metadata=original.metadata,
            content_type=original.content_type,
        )
        with pytest.raises(PhysicalBackupError, match="integrity"):
            await catalog.verify(published.manifest_uri)

        bounded = _catalog(store, max_artifact_bytes=4)
        with pytest.raises(PhysicalBackupError, match="exceed bounds"):
            await bounded.publish(_request(), _artifacts())

    asyncio.run(run())


def test_backup_catalog_rejects_manifest_hmac_and_postgres_metadata_drift() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        catalog = _catalog(store)
        published = await catalog.publish(_request(), _artifacts())
        manifest_key = next(
            key for key in store.objects if key.endswith("manifest.json")
        )
        original = store.objects[manifest_key]
        tampered = original.data.replace(b"phase1j-primary", b"phase1j-primary-x")
        store.objects[manifest_key] = StoredObject(
            data=tampered,
            metadata=original.metadata,
            content_type=original.content_type,
        )
        with pytest.raises(PhysicalBackupError, match="HMAC"):
            await catalog.verify(published.manifest_uri)

        changed = {
            **_artifacts(),
            "backup_manifest": _postgres_manifest().replace(b"3000120", b"4000120"),
        }
        with pytest.raises(PhysicalBackupError, match="differs"):
            await catalog.publish(_request(), changed)

    asyncio.run(run())


def test_wal_archive_is_immutable_bounded_and_restorable() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        archive = ImmutablePostgresWalArchive(
            object_store=store,
            cluster_id="phase1j-primary",
            max_object_bytes=32,
        )
        filename = "00000001000000000000000A"
        first = await archive.archive(filename, b"wal-bytes")
        replay = await archive.archive(filename, b"wal-bytes")
        restored, data = await archive.restore(filename)
        assert first.created is True
        assert replay.created is False
        assert restored.sha256 == first.sha256
        assert data == b"wal-bytes"

        key = next(iter(store.objects))
        original = store.objects[key]
        store.objects[key] = StoredObject(
            data=b"different",
            metadata=original.metadata,
            content_type=original.content_type,
        )
        with pytest.raises(WalArchiveError, match="different bytes"):
            await archive.archive(filename, b"wal-bytes")
        with pytest.raises(WalArchiveError, match="metadata"):
            await archive.restore(filename)
        with pytest.raises(WalArchiveError, match="bound"):
            await archive.archive("00000001000000000000000B", b"x" * 33)

    asyncio.run(run())


@pytest.mark.parametrize(
    "filename",
    [
        "00000001000000000000000A",
        "00000002.history",
        "00000001000000000000000A.00000028.backup",
    ],
)
def test_wal_archive_accepts_postgres_archive_filename_classes(filename: str) -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        archive = ImmutablePostgresWalArchive(
            object_store=store,
            cluster_id="phase1j-primary",
        )
        assert (await archive.archive(filename, b"x")).filename == filename

    asyncio.run(run())


def test_wal_archive_rejects_paths_and_lower_case_names() -> None:
    store = InMemoryImmutableObjectStore()
    archive = ImmutablePostgresWalArchive(
        object_store=store,
        cluster_id="phase1j-primary",
    )
    for filename in ("../escape", "00000001000000000000000a", "RECOVERYXLOG"):
        with pytest.raises(ValueError, match="archive filename"):
            archive.key_for(filename)


@pytest.mark.parametrize(
    "value",
    [
        "2026-08-11T13:00:00Z",
        "2026-08-11T13:00:00.123456Z",
        "2026-08-11T13:00:00.123456+08:00",
    ],
)
def test_backup_catalog_rejects_noncanonical_recovery_target(value: str) -> None:
    with pytest.raises(ValueError, match="recovery_target_time"):
        replace(_request(), recovery_target_time=value)


def test_backup_catalog_accepts_empty_audit_chain_head() -> None:
    request = replace(
        _request(),
        audit_head_sequence=0,
        audit_head_event_hash="0" * 64,
    )
    assert request.audit_head_sequence == 0


def test_backup_publisher_requires_exact_regular_artifacts(tmp_path) -> None:
    artifacts = _artifacts()
    for name, data in artifacts.items():
        (tmp_path / name).write_bytes(data)
    assert (
        _read_artifacts(
            tmp_path,
            max_artifact_bytes=1024,
            max_total_bytes=4096,
        )
        == artifacts
    )

    extra = tmp_path / "unexpected"
    extra.write_bytes(b"x")
    with pytest.raises(RuntimeError, match="exactly three"):
        _read_artifacts(
            tmp_path,
            max_artifact_bytes=1024,
            max_total_bytes=4096,
        )
    extra.unlink()

    artifact = tmp_path / "base.tar.gz"
    artifact.unlink()
    artifact.symlink_to(tmp_path / "backup_manifest")
    with pytest.raises(RuntimeError, match="non-symlink"):
        _read_artifacts(
            tmp_path,
            max_artifact_bytes=1024,
            max_total_bytes=4096,
        )


def test_backup_publisher_rejects_artifact_bounds(tmp_path) -> None:
    for name, data in _artifacts().items():
        (tmp_path / name).write_bytes(data)
    with pytest.raises(RuntimeError, match="outside configured bounds"):
        _read_artifacts(
            tmp_path,
            max_artifact_bytes=4,
            max_total_bytes=4096,
        )


def test_backup_publisher_verifies_the_exact_artifact_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    expected = tmp_path / "expected"
    expected.mkdir()
    for name, data in _artifacts().items():
        (expected / name).write_bytes(data)
    verifier = tmp_path / "pg_verifybackup"
    verifier.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        'test "$1" = "--no-parse-wal"\n'
        'test "$(find "$2" -maxdepth 1 -type f | wc -l)" -eq 3\n'
        'cmp "$2/backup_manifest" "$CANDLESCOPE_TEST_EXPECTED/backup_manifest"\n'
        'cmp "$2/base.tar.gz" "$CANDLESCOPE_TEST_EXPECTED/base.tar.gz"\n'
        'cmp "$2/pg_wal.tar.gz" "$CANDLESCOPE_TEST_EXPECTED/pg_wal.tar.gz"\n',
        encoding="utf-8",
    )
    verifier.chmod(0o700)
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_BACKUP_PG_VERIFYBACKUP_EXECUTABLE",
        str(verifier),
    )
    monkeypatch.setenv("CANDLESCOPE_TEST_EXPECTED", str(expected))
    asyncio.run(_verify_postgres_backup(_artifacts()))

    verifier.write_text("#!/bin/sh\nexit 7\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="pg_verifybackup rejected"):
        asyncio.run(_verify_postgres_backup(_artifacts()))


def test_wal_archive_adapter_checks_source_and_returns_receipt(
    tmp_path,
    monkeypatch,
) -> None:
    filename = "00000001000000000000000A"
    source = tmp_path / filename
    source.write_bytes(b"wal-bytes")

    class FakeArchive:
        async def archive(self, archived_name: str, data: bytes) -> ArchivedWalObject:
            assert archived_name == filename
            assert data == b"wal-bytes"
            return ArchivedWalObject(
                filename=archived_name,
                uri=f"s3://bucket/wal/{archived_name}",
                sha256=hashlib.sha256(data).hexdigest(),
                size_bytes=len(data),
                created=True,
            )

    monkeypatch.setattr(
        wal_archive_script, "_archive", lambda _max_bytes: FakeArchive()
    )
    result = asyncio.run(wal_archive_script.run(source, filename))
    assert result["created"] is True
    assert result["size_bytes"] == len(b"wal-bytes")

    with pytest.raises(RuntimeError, match="basename"):
        asyncio.run(wal_archive_script.run(source, filename[:-1] + "B"))
    monkeypatch.setenv("CANDLESCOPE_SERVER_QUERY_WAL_MAX_OBJECT_BYTES", "4")
    with pytest.raises(RuntimeError, match="configured bound"):
        asyncio.run(wal_archive_script.run(source, filename))


def test_wal_restore_adapter_is_root_confined_and_atomic(
    tmp_path,
    monkeypatch,
) -> None:
    filename = "00000001000000000000000A"
    recovery_root = tmp_path / "recovery"
    destination = recovery_root / "pg_wal" / filename
    destination.parent.mkdir(parents=True)
    monkeypatch.setenv(
        "CANDLESCOPE_SERVER_QUERY_WAL_RECOVERY_ROOT",
        str(recovery_root),
    )

    class FakeArchive:
        async def restore(
            self,
            restored_name: str,
        ) -> tuple[ArchivedWalObject, bytes]:
            data = b"restored-wal"
            return (
                ArchivedWalObject(
                    filename=restored_name,
                    uri=f"s3://bucket/wal/{restored_name}",
                    sha256=hashlib.sha256(data).hexdigest(),
                    size_bytes=len(data),
                    created=False,
                ),
                data,
            )

    monkeypatch.setattr(wal_restore_script, "_archive", lambda: FakeArchive())
    result = asyncio.run(wal_restore_script.run(filename, destination))
    assert destination.read_bytes() == b"restored-wal"
    assert destination.stat().st_mode & 0o777 == 0o600
    assert result["destination"] == str(destination)

    with pytest.raises(RuntimeError, match="escapes"):
        asyncio.run(wal_restore_script.run(filename, tmp_path / "escape"))
