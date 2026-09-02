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
    archive_health_from_wire,
    collector_health_from_wire,
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
from scripts.server_phase1t_collector_worker import parse_sequences

NOW_MS = 1_700_000_300_000


def test_parse_sequences_requires_a_bounded_contiguous_span() -> None:
    assert parse_sequences("42,43") == (42, 43)
    assert parse_sequences("42,43,44,45") == (42, 43, 44, 45)
    with pytest.raises(ValueError, match="contiguous"):
        parse_sequences("42,44")
    with pytest.raises(ValueError, match="blank"):
        parse_sequences("")
    with pytest.raises(ValueError, match="blank entry"):
        parse_sequences("42,")
    with pytest.raises(ValueError, match="positive"):
        parse_sequences("0,1")


def test_takeover_collector_health_reconciles_with_restarted_consumers() -> None:
    snapshot = MarketDataSnapshotRef(
        data_epoch="phase1t-takeover-epoch",
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256="b" * 64,
    )
    collector = collector_health_from_wire(
        {
            "state": "leader",
            "ready": True,
            "reason": "leader after takeover",
            "owner_id": "collector-process-b",
            "source_health": "connected",
            "producer_epoch": 1,
            "lease_expires_at_ms": NOW_MS + 1_200,
            "last_sequence": 45,
            "last_partition_offset": 3,
            "pending_event_id": None,
            "events_published": 2,
            "heartbeat_successes": 4,
            "heartbeat_failures": 0,
            "started_at_ms": NOW_MS,
            "updated_at_ms": NOW_MS,
            "terminal_error": None,
        }
    )
    writer = writer_health_from_wire(
        ClickHouseWriterHealth(
            state=ClickHouseWriterState.RUNNING,
            ready=True,
            reason="restart replay committed",
            owner_id="writer-b",
            kafka_group_id="phase1t-writer",
            committed_next_offset=4,
            batches_committed=1,
            inserted_events=0,
            duplicate_events=4,
            conflict_events=0,
            started_at_ms=NOW_MS,
            updated_at_ms=NOW_MS,
            terminal_error=None,
        ).to_wire()
    )
    archive = archive_health_from_wire(
        ArchiveWriterHealth(
            state=ArchiveWriterState.RUNNING,
            ready=True,
            reason="restart replay committed",
            owner_id="archiver-b",
            kafka_group_id="phase1t-archive",
            data_epoch=snapshot.data_epoch,
            committed_next_offset=4,
            segments_committed=1,
            events_archived=4,
            current_snapshot=snapshot,
            started_at_ms=NOW_MS,
            updated_at_ms=NOW_MS,
            terminal_error=None,
        ).to_wire()
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
    assert collector.state is CollectorState.LEADER
    assert collector.producer_epoch == 1
    assert collector.events_published == 2
    assert result.status == STATUS_CAUGHT_UP
    assert result.physical_next_offset == 4
    assert result.logical_last_sequence == 45


def test_server_profile_remains_fail_closed() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.profile is DeploymentProfile.SERVER
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
