from __future__ import annotations

import pytest
from app.deployment.profile import (
    DeploymentProfile,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.chain_health import (
    ChainHealthWireError,
    archive_health_from_wire,
    collector_health_from_wire,
    synthetic_publisher_collector_health,
    writer_health_from_wire,
)
from app.server_runtime.chain_reconciliation import (
    STATUS_CAUGHT_UP,
    ChainObservation,
    reconcile_chain,
)
from app.server_runtime.health import CollectorState
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)

NOW_MS = 1_700_000_200_000


def _snapshot() -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="phase1s-restart-epoch",
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256="a" * 64,
    )


def test_health_wires_round_trip_and_reject_key_drift() -> None:
    snapshot = _snapshot()
    collector = synthetic_publisher_collector_health(
        last_sequence=45,
        last_partition_offset=3,
        events_published=4,
        started_at_ms=NOW_MS,
    )
    writer = ClickHouseWriterHealth(
        state=ClickHouseWriterState.RUNNING,
        ready=True,
        reason="caught up",
        owner_id="writer-b",
        kafka_group_id="phase1s-writer",
        committed_next_offset=4,
        batches_committed=1,
        inserted_events=0,
        duplicate_events=4,
        conflict_events=0,
        started_at_ms=NOW_MS,
        updated_at_ms=NOW_MS,
        terminal_error=None,
    )
    archive = ArchiveWriterHealth(
        state=ArchiveWriterState.RUNNING,
        ready=True,
        reason="caught up",
        owner_id="archiver-b",
        kafka_group_id="phase1s-archive",
        data_epoch=snapshot.data_epoch,
        committed_next_offset=4,
        segments_committed=1,
        events_archived=4,
        current_snapshot=snapshot,
        started_at_ms=NOW_MS,
        updated_at_ms=NOW_MS,
        terminal_error=None,
    )
    assert collector_health_from_wire(collector.to_wire()) == collector
    assert writer_health_from_wire(writer.to_wire()) == writer
    assert archive_health_from_wire(archive.to_wire()) == archive
    drifted = dict(writer.to_wire())
    drifted["unexpected"] = True
    with pytest.raises(ChainHealthWireError, match="not exact"):
        writer_health_from_wire(drifted)
    missing = dict(archive.to_wire())
    missing.pop("current_snapshot")
    with pytest.raises(ChainHealthWireError, match="not exact"):
        archive_health_from_wire(missing)


def test_process_health_files_reconcile_after_restart_duplicates() -> None:
    snapshot = _snapshot()
    collector = synthetic_publisher_collector_health(
        last_sequence=45,
        last_partition_offset=3,
        events_published=4,
        started_at_ms=NOW_MS,
    )
    writer = writer_health_from_wire(
        {
            "state": "running",
            "ready": True,
            "reason": "restart replay committed",
            "owner_id": "writer-b",
            "kafka_group_id": "phase1s-writer",
            "committed_next_offset": 4,
            "batches_committed": 1,
            "inserted_events": 0,
            "duplicate_events": 4,
            "conflict_events": 0,
            "started_at_ms": NOW_MS,
            "updated_at_ms": NOW_MS,
            "terminal_error": None,
        }
    )
    archive = archive_health_from_wire(
        {
            "state": "running",
            "ready": True,
            "reason": "restart replay committed",
            "owner_id": "archiver-b",
            "kafka_group_id": "phase1s-archive",
            "data_epoch": snapshot.data_epoch,
            "committed_next_offset": 4,
            "segments_committed": 1,
            "events_archived": 4,
            "current_snapshot": {
                "data_epoch": snapshot.data_epoch,
                "snapshot_version": snapshot.snapshot_version,
                "manifest_uri": snapshot.manifest_uri,
                "manifest_sha256": snapshot.manifest_sha256,
            },
            "started_at_ms": NOW_MS,
            "updated_at_ms": NOW_MS,
            "terminal_error": None,
        }
    )
    result = reconcile_chain(
        ChainObservation(
            collector=collector,
            writer=writer,
            archive=archive,
            query_snapshot=snapshot,
            query_sequences=(42, 43, 44, 45),
            replay_snapshot=snapshot,
            replay_first_id=42,
            replay_last_id=45,
            replay_row_count=4,
        ),
        require_caught_up=True,
    )
    assert result.status == STATUS_CAUGHT_UP
    assert result.writer_duplicates == 4
    assert result.idempotent_replay_safe is True
    assert collector.source_health == "scripted_kafka_publisher"
    assert collector.state is CollectorState.LEADER


def test_server_profile_remains_fail_closed() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.profile is DeploymentProfile.SERVER
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
