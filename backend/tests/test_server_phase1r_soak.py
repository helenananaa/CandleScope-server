from __future__ import annotations

import asyncio
import json

import pytest
from app.deployment.profile import (
    DeploymentProfile,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.chain_reconciliation import (
    STATUS_CAUGHT_UP,
    STATUS_LAGGING,
    ChainObservation,
    ChainReconciliationError,
    reconcile_chain,
)
from app.server_runtime.health import CollectorHealth, CollectorState
from app.server_runtime.soak_rehearsal import (
    PUBLIC_SOAK_NOT_DELIVERED,
    public_soak_refusal,
    run_phase1r_rehearsal,
)
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)
from scripts import server_phase1r_soak

NOW_MS = 1_700_000_100_000


def _snapshot(version: int = 4, digest: str = "a" * 64) -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="phase1r-rehearsal",
        snapshot_version=version,
        manifest_uri=f"memory://archive/snapshot-{version}.json",
        manifest_sha256=digest,
    )


def _collector(**changes: object) -> CollectorHealth:
    values: dict[str, object] = {
        "state": CollectorState.LEADER,
        "ready": True,
        "reason": "ok",
        "owner_id": "collector-a",
        "source_health": "scripted",
        "producer_epoch": 0,
        "lease_expires_at_ms": NOW_MS + 15_000,
        "last_sequence": 45,
        "last_partition_offset": 3,
        "pending_event_id": None,
        "events_published": 4,
        "heartbeat_successes": 1,
        "heartbeat_failures": 0,
        "started_at_ms": NOW_MS,
        "updated_at_ms": NOW_MS,
        "terminal_error": None,
    }
    values.update(changes)
    return CollectorHealth(**values)  # type: ignore[arg-type]


def _writer(**changes: object) -> ClickHouseWriterHealth:
    values: dict[str, object] = {
        "state": ClickHouseWriterState.RUNNING,
        "ready": True,
        "reason": "ok",
        "owner_id": "writer-a",
        "kafka_group_id": "candlescope-clickhouse-writer-v1",
        "committed_next_offset": 4,
        "batches_committed": 1,
        "inserted_events": 4,
        "duplicate_events": 0,
        "conflict_events": 0,
        "started_at_ms": NOW_MS,
        "updated_at_ms": NOW_MS,
        "terminal_error": None,
    }
    values.update(changes)
    return ClickHouseWriterHealth(**values)  # type: ignore[arg-type]


def _archive(
    snapshot: MarketDataSnapshotRef | None = None, **changes: object
) -> ArchiveWriterHealth:
    selected = _snapshot() if snapshot is None else snapshot
    values: dict[str, object] = {
        "state": ArchiveWriterState.RUNNING,
        "ready": True,
        "reason": "ok",
        "owner_id": "archiver-a",
        "kafka_group_id": "candlescope-parquet-archiver-v1",
        "data_epoch": selected.data_epoch,
        "committed_next_offset": selected.snapshot_version,
        "segments_committed": 1,
        "events_archived": 4,
        "current_snapshot": selected,
        "started_at_ms": NOW_MS,
        "updated_at_ms": NOW_MS,
        "terminal_error": None,
    }
    values.update(changes)
    return ArchiveWriterHealth(**values)  # type: ignore[arg-type]


def _observation(**changes: object) -> ChainObservation:
    snapshot = _snapshot()
    values: dict[str, object] = {
        "collector": _collector(),
        "writer": _writer(),
        "archive": _archive(snapshot),
        "query_snapshot": snapshot,
        "query_sequences": (42, 43, 44, 45),
        "replay_snapshot": snapshot,
        "replay_first_id": 42,
        "replay_last_id": 45,
        "replay_row_count": 4,
    }
    values.update(changes)
    return ChainObservation(**values)  # type: ignore[arg-type]


def test_caught_up_chain_requires_matching_offsets_snapshot_and_replay_span() -> None:
    result = reconcile_chain(_observation(), require_caught_up=True)
    assert result.status == STATUS_CAUGHT_UP
    assert result.physical_next_offset == 4
    assert result.logical_last_sequence == 45
    assert result.query_sequences == (42, 43, 44, 45)
    wire = result.to_wire()
    assert wire["pid_alive_is_not_sufficient"] is True
    assert wire["snapshot"]["snapshot_version"] == 4


def test_lagging_writer_is_allowed_until_the_quiet_checkpoint() -> None:
    snapshot = _snapshot(version=2, digest="b" * 64)
    result = reconcile_chain(
        _observation(
            writer=_writer(committed_next_offset=2, inserted_events=2),
            archive=_archive(snapshot, events_archived=2),
            query_snapshot=snapshot,
            query_sequences=(42, 43),
            replay_snapshot=snapshot,
            replay_first_id=42,
            replay_last_id=43,
            replay_row_count=2,
        ),
        require_caught_up=False,
    )
    assert result.status == STATUS_LAGGING
    with pytest.raises(ChainReconciliationError, match="not caught up") as exc:
        reconcile_chain(
            _observation(
                writer=_writer(committed_next_offset=2, inserted_events=2),
                archive=_archive(snapshot, events_archived=2),
                query_snapshot=snapshot,
                query_sequences=(42, 43),
                replay_snapshot=snapshot,
                replay_first_id=42,
                replay_last_id=43,
                replay_row_count=2,
            ),
            require_caught_up=True,
        )
    assert exc.value.code == "OFFSET_MISMATCH"


def test_reconciler_fails_closed_on_pid_only_gap_drift_and_ahead_consumers() -> None:
    with pytest.raises(ChainReconciliationError) as pid_exc:
        reconcile_chain(
            _observation(
                collector=_collector(last_sequence=None, last_partition_offset=None)
            ),
            require_caught_up=True,
        )
    assert pid_exc.value.code == "PID_ONLY_HEALTH"

    with pytest.raises(ChainReconciliationError) as pending_exc:
        reconcile_chain(
            _observation(collector=_collector(pending_event_id="pending-event")),
            require_caught_up=True,
        )
    assert pending_exc.value.code == "COLLECTOR_NOT_PROGRESSING"

    with pytest.raises(ChainReconciliationError) as gap_exc:
        reconcile_chain(
            _observation(
                query_sequences=(42, 44, 45),
                replay_first_id=42,
                replay_last_id=45,
                replay_row_count=3,
            ),
            require_caught_up=True,
        )
    assert gap_exc.value.code == "QUERY_SEQUENCE_GAP"

    other = _snapshot(version=4, digest="c" * 64)
    with pytest.raises(ChainReconciliationError) as snapshot_exc:
        reconcile_chain(
            _observation(query_snapshot=other, replay_snapshot=other),
            require_caught_up=True,
        )
    assert snapshot_exc.value.code == "SNAPSHOT_DRIFT"

    with pytest.raises(ChainReconciliationError) as ahead_exc:
        reconcile_chain(
            _observation(writer=_writer(committed_next_offset=5)),
            require_caught_up=True,
        )
    assert ahead_exc.value.code == "CONSUMER_AHEAD_OF_COLLECTOR"

    with pytest.raises(ChainReconciliationError) as replay_exc:
        reconcile_chain(
            _observation(replay_row_count=3, replay_last_id=44),
            require_caught_up=True,
        )
    assert replay_exc.value.code == "REPLAY_SPAN_MISMATCH"

    with pytest.raises(ChainReconciliationError) as terminal_exc:
        reconcile_chain(
            _observation(
                writer=_writer(
                    state=ClickHouseWriterState.DEGRADED, terminal_error="boom"
                )
            ),
            require_caught_up=False,
        )
    assert terminal_exc.value.code == "TERMINAL_ERROR"


def test_in_process_rehearsal_archives_queries_replays_and_reconciles() -> None:
    async def run() -> None:
        result = await run_phase1r_rehearsal(clock_ms=NOW_MS)
        assert result["phase1r_passed"] is True
        assert result["mode"] == "rehearsal"
        assert result["twenty_four_hour_public_continuity"] is False
        assert result["main_fastapi_server_profile_unlocked"] is False
        assert result["idempotent_rearchive_verified"] is True
        assert result["replay_ids"] == [42, 43, 44, 45]
        assert result["reconciliation"]["status"] == STATUS_CAUGHT_UP
        assert result["reconciliation"]["physical_next_offset"] == 4
        assert result["snapshot_pin"]["snapshot_version"] == 4
        assert result["collector_process"] == "synthetic_from_published_records"

    asyncio.run(run())


def test_public_24h_mode_is_fail_closed_even_with_the_explicit_switch() -> None:
    denied = public_soak_refusal(allow_public_soak=False)
    assert denied["phase1r_passed"] is False
    assert denied["code"] == "PUBLIC_SOAK_NOT_AUTHORIZED"
    allowed = public_soak_refusal(allow_public_soak=True)
    assert allowed["phase1r_passed"] is False
    assert allowed["code"] == PUBLIC_SOAK_NOT_DELIVERED
    assert allowed["twenty_four_hour_public_continuity"] is False
    assert allowed["required_duration_ms"] == 24 * 60 * 60 * 1000


def test_cli_rehearsal_prints_caught_up_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert server_phase1r_soak.main(["rehearsal"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["phase1r_passed"] is True
    assert payload["reconciliation"]["status"] == STATUS_CAUGHT_UP


def test_cli_public_24h_refuses_to_claim_continuity(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert server_phase1r_soak.main(["public-24h"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["phase1r_passed"] is False
    assert payload["code"] == "PUBLIC_SOAK_NOT_AUTHORIZED"


def test_server_profile_remains_fail_closed() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.profile is DeploymentProfile.SERVER
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()


def test_ready_collector_without_durable_cursors_is_still_pid_only() -> None:
    observation = _observation(
        collector=_collector(ready=True, last_sequence=None, last_partition_offset=None)
    )
    with pytest.raises(ChainReconciliationError) as exc:
        reconcile_chain(observation, require_caught_up=True)
    assert exc.value.code == "PID_ONLY_HEALTH"
