from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from app.server_runtime.object_store import StoredObject
from app.server_runtime.query_backup_history import (
    BackupRunHistoryError,
    BackupRunHistorySigner,
    ImmutableBackupRunHistoryRepository,
    verify_success_cadence,
)
from app.server_runtime.query_backup_selection import BackupJobReceipt
from app.server_runtime.testing import InMemoryImmutableObjectStore
from scripts import server_query_backup_history

SECRET = b"phase1o-run-history-secret-is-at-least-32-bytes"
CLUSTER_ID = "phase1o-primary"
FIRST_ID = "11111111-2222-4333-8444-555555555555"
SECOND_ID = "22222222-3333-4444-8555-666666666666"
ROOT = Path(__file__).parents[2]


def test_signed_run_history_publish_verify_and_replay_are_deterministic() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        repository = _repository(store)
        receipt = _receipt(FIRST_ID, completed_at_ms=1_700_000_100_000)
        first = await repository.publish(receipt, cluster_id=CLUSTER_ID)
        replay = await repository.publish(receipt, cluster_id=CLUSTER_ID)
        verified = await repository.verify(first.uri)

        assert first.created is True
        assert replay.created is False
        assert replay.content_sha256 == first.content_sha256
        assert verified.history.receipt == receipt
        assert verified.history.cluster_id == CLUSTER_ID
        assert "/backup-runs/v1/phase1o-primary/" in first.uri
        assert len(store.objects) == 1

    asyncio.run(run())


def test_signed_run_history_rejects_hmac_canonical_uri_and_replay_drift() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        repository = _repository(store)
        receipt = _receipt(FIRST_ID, completed_at_ms=1_700_000_100_000)
        published = await repository.publish(receipt, cluster_id=CLUSTER_ID)
        key = next(iter(store.objects))
        original = store.objects[key]

        store.objects[key] = StoredObject(
            data=original.data.replace(b"phase1o-primary", b"phase1o-primary-x"),
            metadata=original.metadata,
            content_type=original.content_type,
        )
        with pytest.raises(BackupRunHistoryError, match="HMAC"):
            await repository.verify(published.uri)
        store.objects[key] = original

        alias = "backup-runs/v1/phase1o-primary/alias.json"
        store.objects[alias] = original
        with pytest.raises(BackupRunHistoryError, match="URI has drifted"):
            await repository.verify(store.uri_for(alias))

        changed = replace(receipt, operator_id="another-operator")
        changed_history = _signer().sign(changed, cluster_id=CLUSTER_ID)
        store.objects[key] = StoredObject(
            data=changed_history.canonical_bytes(),
            metadata=original.metadata,
            content_type=original.content_type,
        )
        with pytest.raises(BackupRunHistoryError, match="different bytes"):
            await repository.publish(receipt, cluster_id=CLUSTER_ID)

    asyncio.run(run())


def test_success_cadence_is_order_independent_and_includes_window_edges() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        repository = _repository(store)
        first = await repository.publish(
            _receipt(FIRST_ID, completed_at_ms=1_700_000_100_000),
            cluster_id=CLUSTER_ID,
        )
        second = await repository.publish(
            _receipt(SECOND_ID, completed_at_ms=1_700_000_200_000),
            cluster_id=CLUSTER_ID,
        )
        arguments = {
            "expected_cluster_id": CLUSTER_ID,
            "window_start_ms": 1_700_000_050_000,
            "window_end_ms": 1_700_000_250_000,
            "evaluated_at_ms": 1_700_000_250_000,
            "maximum_gap_ms": 100_000,
        }
        ordered = verify_success_cadence((first, second), **arguments)
        reversed_result = verify_success_cadence((second, first), **arguments)
        assert ordered.maximum_observed_gap_ms == 100_000
        assert ordered.scope_sha256 == reversed_result.scope_sha256
        wire = ordered.to_wire()
        assert wire["successful_run_count"] == 2
        assert wire["global_history_completeness_proven"] is False
        assert wire["scheduled_slot_execution_proven"] is False
        assert [item["backup_id"] for item in wire["runs"]] == [
            FIRST_ID,
            SECOND_ID,
        ]

    asyncio.run(run())


def test_success_cadence_rejects_gap_future_identity_duplicate_and_bounds() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        repository = _repository(store)
        first = await repository.publish(
            _receipt(FIRST_ID, completed_at_ms=1_700_000_100_000),
            cluster_id=CLUSTER_ID,
        )
        second = await repository.publish(
            _receipt(SECOND_ID, completed_at_ms=1_700_000_200_000),
            cluster_id=CLUSTER_ID,
        )
        base = {
            "expected_cluster_id": CLUSTER_ID,
            "window_start_ms": 1_700_000_050_000,
            "window_end_ms": 1_700_000_250_000,
            "evaluated_at_ms": 1_700_000_250_000,
        }
        with pytest.raises(BackupRunHistoryError, match="maximum gap"):
            verify_success_cadence((first,), maximum_gap_ms=100_000, **base)
        with pytest.raises(BackupRunHistoryError, match="future"):
            verify_success_cadence(
                (first,),
                maximum_gap_ms=200_000,
                maximum_future_skew_ms=1_000,
                **{**base, "evaluated_at_ms": 1_700_000_000_000},
            )
        with pytest.raises(BackupRunHistoryError, match="another cluster"):
            verify_success_cadence(
                (first,),
                maximum_gap_ms=200_000,
                **{**base, "expected_cluster_id": "another-cluster"},
            )
        with pytest.raises(BackupRunHistoryError, match="duplicated"):
            verify_success_cadence(
                (first, first),
                maximum_gap_ms=200_000,
                **base,
            )
        with pytest.raises(BackupRunHistoryError, match="count exceeds"):
            verify_success_cadence(
                (first, second),
                maximum_gap_ms=200_000,
                maximum_histories=1,
                **base,
            )

    asyncio.run(run())


def test_history_script_publishes_and_verifies_explicit_cadence(
    monkeypatch,
) -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        repository = _repository(store)
        monkeypatch.setattr(
            server_query_backup_history,
            "history_repository",
            lambda: repository,
        )
        monkeypatch.setenv(
            "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_CLUSTER_ID",
            CLUSTER_ID,
        )
        monkeypatch.setenv(
            "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_MAXIMUM_GAP_MS",
            "200000",
        )
        receipt = _receipt(FIRST_ID, completed_at_ms=1_700_000_100_000)
        publication = await server_query_backup_history.publish_receipt(
            receipt.to_wire()
        )
        cadence = await server_query_backup_history.verify_cadence(
            history_uris=(publication["history_uri"],),
            expected_cluster_id=CLUSTER_ID,
            window_start_ms=1_700_000_050_000,
            window_end_ms=1_700_000_200_000,
            evaluated_at_ms=1_700_000_200_000,
        )
        assert publication["created"] is True
        assert cadence["status"] == "success-cadence-within-explicit-window"
        assert cadence["maximum_observed_gap_ms"] == 100_000

    asyncio.run(run())


def test_history_cli_emits_sanitized_failure(monkeypatch, capsys) -> None:
    async def fail(**_arguments):
        raise RuntimeError("secret diagnostic must not be emitted")

    monkeypatch.setattr(server_query_backup_history, "verify_cadence", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "server_query_backup_history.py",
            "verify-cadence",
            "--history-uri",
            "s3://history/object.json",
            "--expected-cluster-id",
            CLUSTER_ID,
            "--window-start-ms",
            "1700000000000",
            "--window-end-ms",
            "1700000100000",
        ],
    )
    with pytest.raises(SystemExit) as captured:
        server_query_backup_history.main()
    assert captured.value.code == 1
    output = json.loads(capsys.readouterr().err)
    assert output == {
        "schema_version": "candlescope.query-backup-history-failure.v1",
        "status": "failed",
        "code": "BACKUP_HISTORY_OPERATION_FAILED",
    }


def test_systemd_environment_documents_independent_history_controls() -> None:
    environment = (ROOT / "deploy/server/systemd/query-backup.env.example").read_text(
        encoding="utf-8"
    )
    for setting in (
        "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_S3_ENDPOINT_URL",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_HMAC_KEY_ID",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_HMAC_SECRET_BASE64",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_CLUSTER_ID",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_MAXIMUM_HISTORIES=64",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_MAXIMUM_GAP_MS=108000000",
    ):
        assert setting in environment


def _repository(
    store: InMemoryImmutableObjectStore,
) -> ImmutableBackupRunHistoryRepository:
    return ImmutableBackupRunHistoryRepository(
        object_store=store,
        signer=_signer(),
    )


def _signer() -> BackupRunHistorySigner:
    return BackupRunHistorySigner(key_id="phase1o-history-key", secret=SECRET)


def _receipt(backup_id: str, *, completed_at_ms: int) -> BackupJobReceipt:
    return BackupJobReceipt(
        run_id=backup_id,
        backup_id=backup_id,
        started_at_ms=completed_at_ms - 60_000,
        completed_at_ms=completed_at_ms,
        duration_ms=60_000,
        manifest_uri=(
            f"s3://candlescope-test/backups/v3/{CLUSTER_ID}/{backup_id}/manifest.json"
        ),
        manifest_sha256="a" * 64,
        audit_anchor_uri=(f"s3://candlescope-test/anchors/v1/{backup_id}/anchor.json"),
        audit_anchor_sha256="b" * 64,
        recovery_target_time="2026-08-11 14:00:00.000000+00",
        recovery_target_lsn="0/3000200",
        recovery_target_wal_filename="000000010000000000000030",
        wal_coverage_segment_count=1,
        write_fence_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        operator_id="scheduled-backup",
    )
