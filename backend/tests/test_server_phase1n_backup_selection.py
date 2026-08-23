from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.server_runtime.query_backup_catalog import (
    PhysicalBackupArtifact,
    PhysicalBackupManifestSigner,
    PhysicalBackupRequest,
    PhysicalBackupWalSegment,
    PublishedPhysicalBackup,
)
from app.server_runtime.query_backup_history import (
    BackupRunHistorySigner,
    PublishedBackupRunHistory,
)
from app.server_runtime.query_backup_selection import (
    BackupJobReceipt,
    RecoverySelectionError,
    select_recovery_candidate,
)
from scripts import server_query_backup_select, server_query_backup_verify

CLUSTER_ID = "phase1n-primary"
SYSTEM_IDENTIFIER = 7_672_760_263_611_183_147
WAL_FILENAME = "000000010000000000000030"
WAL_PREFIX = f"s3://candlescope-test/wal/v1/{CLUSTER_ID}/"
SECRET = b"phase1n-backup-test-secret-is-at-least-32-bytes"
OLDER_ID = "11111111-2222-4333-8444-555555555555"
LATEST_ID = "22222222-3333-4444-8555-666666666666"


def test_job_receipt_parser_is_exact_canonical_and_duplicate_safe() -> None:
    published = _published(LATEST_ID, "2026-08-11 14:00:00.000000+00")
    receipt = _receipt(published)
    parsed = BackupJobReceipt.from_canonical_bytes(_receipt_bytes(receipt))
    assert parsed == receipt
    result = {
        **receipt.to_wire(),
        "schema_version": "candlescope.query-backup-job-result.v2",
        "success_history_schema_version": ("candlescope.query-backup-run-history.v1"),
        "success_history_uri": "s3://candlescope-test/backup-runs/run.json",
        "success_history_sha256": "9" * 64,
        "success_history_created": True,
    }
    assert BackupJobReceipt.from_canonical_bytes(_canonical_bytes(result)) == receipt

    wire = receipt.to_wire()
    wire["extra"] = True
    with pytest.raises(RecoverySelectionError, match="shape"):
        BackupJobReceipt.from_canonical_bytes(_canonical_bytes(wire))
    with pytest.raises(RecoverySelectionError, match="strict JSON"):
        BackupJobReceipt.from_canonical_bytes(b'{"run_id":"first","run_id":"second"}\n')
    with pytest.raises(RecoverySelectionError, match="canonical"):
        BackupJobReceipt.from_canonical_bytes(
            json.dumps(receipt.to_wire(), indent=2).encode() + b"\n"
        )
    wire = receipt.to_wire()
    wire["recovery_target_lsn"] = "00/03000200"
    with pytest.raises(RecoverySelectionError, match="fields"):
        BackupJobReceipt.from_canonical_bytes(_canonical_bytes(wire))


def test_selector_chooses_unique_fresh_latest_and_binds_scope() -> None:
    older = _published(OLDER_ID, "2026-08-11 13:00:00.000000+00")
    latest = _published(LATEST_ID, "2026-08-11 14:00:00.000000+00")
    now_ms = _timestamp_ms("2026-08-11 14:10:00.000000+00")
    first = select_recovery_candidate(
        ((_receipt(latest), latest), (_receipt(older), older)),
        expected_cluster_id=CLUSTER_ID,
        expected_system_identifier=SYSTEM_IDENTIFIER,
        expected_timeline=1,
        now_ms=now_ms,
        maximum_age_ms=3_600_000,
    )
    reordered = select_recovery_candidate(
        ((_receipt(older), older), (_receipt(latest), latest)),
        expected_cluster_id=CLUSTER_ID,
        expected_system_identifier=SYSTEM_IDENTIFIER,
        expected_timeline=1,
        now_ms=now_ms,
        maximum_age_ms=3_600_000,
    )
    assert first.receipt.backup_id == LATEST_ID
    assert first.age_ms == 600_000
    assert first.selection_scope_sha256 == reordered.selection_scope_sha256
    wire = first.to_wire()
    assert wire["latest_within_supplied_receipts"] is True
    assert wire["global_latest_proven"] is False
    assert wire["expected"] == {
        "cluster_id": CLUSTER_ID,
        "system_identifier": str(SYSTEM_IDENTIFIER),
        "timeline": 1,
    }


def test_selector_rejects_stale_future_ambiguous_and_wrong_identity() -> None:
    latest = _published(LATEST_ID, "2026-08-11 14:00:00.000000+00")
    receipt = _receipt(latest)
    with pytest.raises(RecoverySelectionError, match="stale"):
        select_recovery_candidate(
            ((receipt, latest),),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 16:00:00.000000+00"),
            maximum_age_ms=3_600_000,
        )
    with pytest.raises(RecoverySelectionError, match="future"):
        select_recovery_candidate(
            ((receipt, latest),),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 13:00:00.000000+00"),
            maximum_future_skew_ms=1_000,
        )

    same_time = _published(OLDER_ID, "2026-08-11 14:00:00.000000+00")
    with pytest.raises(RecoverySelectionError, match="ambiguous"):
        select_recovery_candidate(
            ((receipt, latest), (_receipt(same_time), same_time)),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
        )
    with pytest.raises(RecoverySelectionError, match="duplicated"):
        select_recovery_candidate(
            ((receipt, latest), (receipt, latest)),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
        )
    with pytest.raises(RecoverySelectionError, match="count exceeds"):
        select_recovery_candidate(
            ((receipt, latest), (_receipt(same_time), same_time)),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
            maximum_candidates=1,
        )
    with pytest.raises(RecoverySelectionError, match="another cluster"):
        select_recovery_candidate(
            ((receipt, latest),),
            expected_cluster_id="another-cluster",
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
        )
    with pytest.raises(RecoverySelectionError, match="another PostgreSQL"):
        select_recovery_candidate(
            ((receipt, latest),),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER + 1,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
        )
    with pytest.raises(RecoverySelectionError, match="another PostgreSQL timeline"):
        select_recovery_candidate(
            ((receipt, latest),),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=2,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
        )


def test_selector_rejects_receipt_manifest_and_job_interval_drift() -> None:
    published = _published(LATEST_ID, "2026-08-11 14:00:00.000000+00")
    receipt = _receipt(published)
    arguments = {
        "expected_cluster_id": CLUSTER_ID,
        "expected_system_identifier": SYSTEM_IDENTIFIER,
        "expected_timeline": 1,
        "now_ms": _timestamp_ms("2026-08-11 14:10:00.000000+00"),
    }
    with pytest.raises(RecoverySelectionError, match="differs"):
        select_recovery_candidate(
            ((replace(receipt, manifest_sha256="f" * 64), published),),
            **arguments,
        )
    with pytest.raises(RecoverySelectionError, match="created outside"):
        select_recovery_candidate(
            (
                (
                    replace(
                        receipt,
                        started_at_ms=published.manifest.request.created_at_ms + 1,
                    ),
                    published,
                ),
            ),
            **arguments,
        )


def test_selection_cli_uses_private_receipts_and_fully_verifies_selected(
    tmp_path,
    monkeypatch,
) -> None:
    older = _published(OLDER_ID, "2026-08-11 13:00:00.000000+00")
    latest = _published(LATEST_ID, "2026-08-11 14:00:00.000000+00")
    paths = tuple(
        _write_receipt(
            tmp_path / f"{item.manifest.request.backup_id}.json", _receipt(item)
        )
        for item in (older, latest)
    )
    catalog = _InspectCatalog((older, latest))
    monkeypatch.setattr(server_query_backup_verify, "backup_catalog", lambda: catalog)

    async def verify(**arguments):
        assert arguments["manifest_uri"] == latest.manifest_uri
        return _verified(latest)

    monkeypatch.setattr(server_query_backup_verify, "run", verify)
    result = asyncio.run(
        server_query_backup_select.run(
            receipt_paths=paths,
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
        )
    )
    assert catalog.inspected == [older.manifest_uri, latest.manifest_uri]
    assert result["status"] == "selected-and-fully-verified"
    assert result["selected_backup_fully_verified"] is True
    assert result["selected"]["backup_id"] == LATEST_ID

    paths[0].chmod(0o644)
    with pytest.raises(RecoverySelectionError, match="private"):
        asyncio.run(
            server_query_backup_select.run(
                receipt_paths=paths,
                expected_cluster_id=CLUSTER_ID,
                expected_system_identifier=SYSTEM_IDENTIFIER,
                expected_timeline=1,
                now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
            )
        )
    paths[0].chmod(0o600)
    symlink = tmp_path / "receipt-symlink.json"
    symlink.symlink_to(paths[0])
    with pytest.raises(RecoverySelectionError, match="private"):
        asyncio.run(
            server_query_backup_select.run(
                receipt_paths=(symlink,),
                expected_cluster_id=CLUSTER_ID,
                expected_system_identifier=SYSTEM_IDENTIFIER,
                expected_timeline=1,
                now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
            )
        )


def test_selection_cli_rejects_full_verification_drift(tmp_path, monkeypatch) -> None:
    published = _published(LATEST_ID, "2026-08-11 14:00:00.000000+00")
    older = _published(OLDER_ID, "2026-08-11 13:00:00.000000+00")
    paths = (
        _write_receipt(tmp_path / "older.json", _receipt(older)),
        _write_receipt(tmp_path / "latest.json", _receipt(published)),
    )
    monkeypatch.setattr(
        server_query_backup_verify,
        "backup_catalog",
        lambda: _InspectCatalog((older, published)),
    )
    verified_uris: list[str] = []

    async def verify(**arguments):
        verified_uris.append(arguments["manifest_uri"])
        return {**_verified(published), "manifest_sha256": "f" * 64}

    monkeypatch.setattr(server_query_backup_verify, "run", verify)
    with pytest.raises(RecoverySelectionError, match="full backup"):
        asyncio.run(
            server_query_backup_select.run(
                receipt_paths=paths,
                expected_cluster_id=CLUSTER_ID,
                expected_system_identifier=SYSTEM_IDENTIFIER,
                expected_timeline=1,
                now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
            )
        )
    assert verified_uris == [published.manifest_uri]


def test_selection_cli_accepts_signed_history_uris(monkeypatch) -> None:
    older = _published(OLDER_ID, "2026-08-11 13:00:00.000000+00")
    latest = _published(LATEST_ID, "2026-08-11 14:00:00.000000+00")
    histories = tuple(_history(item) for item in (older, latest))
    repository = _HistoryRepository(histories)
    monkeypatch.setattr(
        server_query_backup_select.server_query_backup_history,
        "history_repository",
        lambda: repository,
    )
    monkeypatch.setattr(
        server_query_backup_verify,
        "backup_catalog",
        lambda: _InspectCatalog((older, latest)),
    )

    async def verify(**arguments):
        assert arguments["manifest_uri"] == latest.manifest_uri
        return _verified(latest)

    monkeypatch.setattr(server_query_backup_verify, "run", verify)
    result = asyncio.run(
        server_query_backup_select.run(
            receipt_paths=(),
            history_uris=tuple(item.uri for item in histories),
            expected_cluster_id=CLUSTER_ID,
            expected_system_identifier=SYSTEM_IDENTIFIER,
            expected_timeline=1,
            now_ms=_timestamp_ms("2026-08-11 14:10:00.000000+00"),
        )
    )
    assert repository.verified == [item.uri for item in histories]
    assert result["selected"]["backup_id"] == LATEST_ID


class _HistoryRepository:
    def __init__(self, histories: tuple[PublishedBackupRunHistory, ...]) -> None:
        self._by_uri = {item.uri: item for item in histories}
        self.verified: list[str] = []

    async def verify(self, uri: str) -> PublishedBackupRunHistory:
        self.verified.append(uri)
        return self._by_uri[uri]


class _InspectCatalog:
    def __init__(self, published: tuple[PublishedPhysicalBackup, ...]) -> None:
        self._by_uri = {item.manifest_uri: item for item in published}
        self.inspected: list[str] = []

    async def inspect_signed_manifest(
        self,
        manifest_uri: str,
        *,
        expected_audit_anchor_uri: str,
    ) -> PublishedPhysicalBackup:
        self.inspected.append(manifest_uri)
        published = self._by_uri[manifest_uri]
        assert published.manifest.request.audit_anchor_uri == expected_audit_anchor_uri
        return published


def _published(backup_id: str, target_time: str) -> PublishedPhysicalBackup:
    target_ms = _timestamp_ms(target_time)
    request = PhysicalBackupRequest(
        backup_id=backup_id,
        cluster_id=CLUSTER_ID,
        created_at_ms=target_ms - 2_000,
        write_fence_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        write_fence_acquired_at_ms=target_ms - 3_000,
        recovery_target_time=target_time,
        recovery_target_lsn="0/3000200",
        postgres_version="18.4",
        system_identifier=SYSTEM_IDENTIFIER,
        timeline=1,
        start_lsn="0/3000028",
        end_lsn="0/3000120",
        wal_segment_size_bytes=1 << 20,
        wal_archive_prefix_uri=WAL_PREFIX,
        wal_coverage=(
            PhysicalBackupWalSegment(
                filename=WAL_FILENAME,
                uri=WAL_PREFIX + WAL_FILENAME,
                sha256="a" * 64,
                size_bytes=1 << 20,
            ),
        ),
        audit_anchor_uri=(f"s3://candlescope-test/anchors/v1/{backup_id}/anchor.json"),
        audit_anchor_sha256="b" * 64,
        audit_head_sequence=7,
        audit_head_event_hash="c" * 64,
        migration_version=1,
        migration_sha256="d" * 64,
    )
    prefix = f"s3://candlescope-test/backups/v3/{CLUSTER_ID}/{backup_id}"
    artifacts = tuple(
        PhysicalBackupArtifact(
            name=name,
            uri=f"{prefix}/artifacts/{name}",
            sha256=character * 64,
            size_bytes=10,
        )
        for name, character in (
            ("backup_manifest", "e"),
            ("base.tar.gz", "f"),
            ("pg_wal.tar.gz", "1"),
        )
    )
    manifest = PhysicalBackupManifestSigner(
        key_id="phase1n-key",
        secret=SECRET,
    ).sign(request, artifacts)
    manifest_bytes = manifest.canonical_bytes()
    return PublishedPhysicalBackup(
        manifest_uri=f"{prefix}/manifest.json",
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        manifest=manifest,
        created_artifacts=0,
        manifest_created=False,
    )


def _receipt(published: PublishedPhysicalBackup) -> BackupJobReceipt:
    request = published.manifest.request
    return BackupJobReceipt(
        run_id=request.backup_id,
        backup_id=request.backup_id,
        started_at_ms=request.created_at_ms - 60_000,
        completed_at_ms=request.created_at_ms + 60_000,
        duration_ms=120_000,
        manifest_uri=published.manifest_uri,
        manifest_sha256=published.manifest_sha256,
        audit_anchor_uri=request.audit_anchor_uri,
        audit_anchor_sha256=request.audit_anchor_sha256,
        recovery_target_time=request.recovery_target_time,
        recovery_target_lsn=request.recovery_target_lsn,
        recovery_target_wal_filename=request.wal_coverage[-1].filename,
        wal_coverage_segment_count=len(request.wal_coverage),
        write_fence_id=request.write_fence_id,
        operator_id="scheduled-backup",
    )


def _history(published: PublishedPhysicalBackup) -> PublishedBackupRunHistory:
    receipt = _receipt(published)
    history = BackupRunHistorySigner(
        key_id="phase1o-history-key",
        secret=b"phase1o-history-test-secret-is-at-least-32-bytes",
    ).sign(receipt, cluster_id=CLUSTER_ID)
    return PublishedBackupRunHistory(
        uri=(
            f"s3://candlescope-test/backup-runs/v1/{CLUSTER_ID}/"
            f"{receipt.completed_at_ms:020d}-{receipt.backup_id}.json"
        ),
        content_sha256=hashlib.sha256(history.canonical_bytes()).hexdigest(),
        history=history,
        created=False,
    )


def _verified(published: PublishedPhysicalBackup) -> dict[str, object]:
    request = published.manifest.request
    return {
        "backup_id": request.backup_id,
        "cluster_id": request.cluster_id,
        "system_identifier": str(request.system_identifier),
        "timeline": request.timeline,
        "manifest_uri": published.manifest_uri,
        "manifest_sha256": published.manifest_sha256,
        "audit_anchor_uri": request.audit_anchor_uri,
        "audit_anchor_sha256": request.audit_anchor_sha256,
        "recovery_target_time": request.recovery_target_time,
        "recovery_target_lsn": request.recovery_target_lsn,
        "recovery_target_wal_filename": request.wal_coverage[-1].filename,
        "write_fence_id": request.write_fence_id,
        "wal_coverage": [item.to_wire() for item in request.wal_coverage],
    }


def _write_receipt(path: Path, receipt: BackupJobReceipt) -> Path:
    path.write_bytes(_receipt_bytes(receipt))
    path.chmod(0o600)
    return path


def _receipt_bytes(receipt: BackupJobReceipt) -> bytes:
    return _canonical_bytes(receipt.to_wire())


def _canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _timestamp_ms(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f+00").replace(
        tzinfo=timezone.utc
    )
    return int(parsed.timestamp() * 1_000)
