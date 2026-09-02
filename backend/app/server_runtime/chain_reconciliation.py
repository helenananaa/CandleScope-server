"""Fail-closed vertical-chain reconciliation for Phase 1R soak evidence.

A process PID or a single health endpoint is not a soak success. The chain is
caught up only when collector, ClickHouse writer, Parquet archive, cold query
facts, and the Phase 1Q replay pin describe the same snapshot and contiguous
first-fact sequence span.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.health import CollectorHealth, CollectorState
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)

CHAIN_RECONCILIATION_SCHEMA_VERSION = "candlescope.server-chain-reconciliation.v1"
STATUS_CAUGHT_UP = "caught_up"
STATUS_LAGGING = "lagging"
_TERMINAL_COLLECTOR = {
    CollectorState.DEGRADED,
    CollectorState.FENCED,
}
_TERMINAL_WRITER = {ClickHouseWriterState.DEGRADED}
_TERMINAL_ARCHIVE = {ArchiveWriterState.DEGRADED}


class ChainReconciliationError(RuntimeError):
    """The observed server chain cannot be reconciled safely."""

    def __init__(
        self, code: str, message: str, *, details: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": CHAIN_RECONCILIATION_SCHEMA_VERSION,
            "status": "failed",
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


@dataclass(frozen=True, slots=True)
class ChainObservation:
    """One point-in-time view of the Phase 1 vertical data path."""

    collector: CollectorHealth
    writer: ClickHouseWriterHealth
    archive: ArchiveWriterHealth
    query_snapshot: MarketDataSnapshotRef
    query_sequences: tuple[int, ...]
    replay_snapshot: MarketDataSnapshotRef
    replay_first_id: int
    replay_last_id: int
    replay_row_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.collector, CollectorHealth):
            raise TypeError("collector must be CollectorHealth")
        if not isinstance(self.writer, ClickHouseWriterHealth):
            raise TypeError("writer must be ClickHouseWriterHealth")
        if not isinstance(self.archive, ArchiveWriterHealth):
            raise TypeError("archive must be ArchiveWriterHealth")
        if not isinstance(self.query_snapshot, MarketDataSnapshotRef):
            raise TypeError("query_snapshot must be a MarketDataSnapshotRef")
        if not isinstance(self.replay_snapshot, MarketDataSnapshotRef):
            raise TypeError("replay_snapshot must be a MarketDataSnapshotRef")
        sequences = tuple(self.query_sequences)
        if any(
            isinstance(item, bool) or not isinstance(item, int) or item < 1
            for item in sequences
        ):
            raise TypeError("query_sequences must contain positive integers")
        object.__setattr__(self, "query_sequences", sequences)
        object.__setattr__(
            self,
            "replay_first_id",
            _positive_int(self.replay_first_id, field="replay_first_id"),
        )
        object.__setattr__(
            self,
            "replay_last_id",
            _positive_int(self.replay_last_id, field="replay_last_id"),
        )
        object.__setattr__(
            self,
            "replay_row_count",
            _positive_int(self.replay_row_count, field="replay_row_count"),
        )


@dataclass(frozen=True, slots=True)
class ChainReconciliation:
    schema_version: str
    status: str
    physical_next_offset: int
    logical_last_sequence: int
    query_sequences: tuple[int, ...]
    snapshot: MarketDataSnapshotRef
    writer_duplicates: int
    writer_conflicts: int
    idempotent_replay_safe: bool

    def to_wire(self) -> dict[str, object]:
        snapshot = self.snapshot
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "physical_next_offset": self.physical_next_offset,
            "logical_last_sequence": self.logical_last_sequence,
            "query_sequences": list(self.query_sequences),
            "snapshot": {
                "data_epoch": snapshot.data_epoch,
                "snapshot_version": snapshot.snapshot_version,
                "manifest_uri": snapshot.manifest_uri,
                "manifest_sha256": snapshot.manifest_sha256,
            },
            "writer_duplicates": self.writer_duplicates,
            "writer_conflicts": self.writer_conflicts,
            "idempotent_replay_safe": self.idempotent_replay_safe,
            "pid_alive_is_not_sufficient": True,
        }


def reconcile_chain(
    observation: ChainObservation,
    *,
    require_caught_up: bool,
) -> ChainReconciliation:
    """Compare collector, writer, archive, cold query, and replay pin."""

    if not isinstance(observation, ChainObservation):
        raise TypeError("observation must be a ChainObservation")
    if not isinstance(require_caught_up, bool):
        raise TypeError("require_caught_up must be a boolean")
    _reject_terminal(observation)
    physical_next, logical_last = _progress_cursors(observation)
    _reject_consumers_ahead(observation, physical_next)
    sequences = _contiguous_sequences(observation.query_sequences)
    _reject_replay_and_query_drift(observation, sequences)
    snapshot = _require_matching_snapshot(observation)
    writer_offset = observation.writer.committed_next_offset
    archive_offset = observation.archive.committed_next_offset
    caught_up = (
        writer_offset == physical_next
        and archive_offset == physical_next
        and snapshot.snapshot_version == physical_next
        and sequences[-1] == logical_last
    )
    if require_caught_up and not caught_up:
        raise ChainReconciliationError(
            "OFFSET_MISMATCH",
            "the vertical chain is not caught up at the quiet checkpoint",
            details={
                "physical_next_offset": physical_next,
                "writer_committed_next_offset": writer_offset,
                "archive_committed_next_offset": archive_offset,
                "snapshot_version": snapshot.snapshot_version,
                "query_last_sequence": sequences[-1],
                "collector_last_sequence": logical_last,
            },
        )
    if (
        not caught_up
        and observation.collector.events_published > 0
        and (
            writer_offset is None
            or archive_offset is None
            or observation.archive.current_snapshot is None
        )
    ):
        raise ChainReconciliationError(
            "PID_ONLY_HEALTH",
            "published events exist but a consumer cursor is missing",
            details={
                "writer_committed_next_offset": writer_offset,
                "archive_committed_next_offset": archive_offset,
            },
        )
    return ChainReconciliation(
        schema_version=CHAIN_RECONCILIATION_SCHEMA_VERSION,
        status=STATUS_CAUGHT_UP if caught_up else STATUS_LAGGING,
        physical_next_offset=physical_next,
        logical_last_sequence=logical_last,
        query_sequences=sequences,
        snapshot=snapshot,
        writer_duplicates=observation.writer.duplicate_events,
        writer_conflicts=observation.writer.conflict_events,
        idempotent_replay_safe=observation.writer.conflict_events == 0,
    )


def _reject_terminal(observation: ChainObservation) -> None:
    if observation.collector.terminal_error is not None:
        raise ChainReconciliationError(
            "TERMINAL_ERROR",
            "collector health has a terminal error",
            details={"terminal_error": observation.collector.terminal_error},
        )
    if observation.writer.terminal_error is not None:
        raise ChainReconciliationError(
            "TERMINAL_ERROR",
            "ClickHouse writer health has a terminal error",
            details={"terminal_error": observation.writer.terminal_error},
        )
    if observation.archive.terminal_error is not None:
        raise ChainReconciliationError(
            "TERMINAL_ERROR",
            "archive writer health has a terminal error",
            details={"terminal_error": observation.archive.terminal_error},
        )
    if observation.collector.state in _TERMINAL_COLLECTOR:
        raise ChainReconciliationError(
            "COLLECTOR_NOT_PROGRESSING",
            "collector is fenced or degraded and cannot prove progress",
            details={"state": observation.collector.state.value},
        )
    if observation.writer.state in _TERMINAL_WRITER:
        raise ChainReconciliationError(
            "TERMINAL_ERROR",
            "ClickHouse writer is degraded",
            details={"state": observation.writer.state.value},
        )
    if observation.archive.state in _TERMINAL_ARCHIVE:
        raise ChainReconciliationError(
            "TERMINAL_ERROR",
            "archive writer is degraded",
            details={"state": observation.archive.state.value},
        )


def _progress_cursors(observation: ChainObservation) -> tuple[int, int]:
    last_offset = observation.collector.last_partition_offset
    last_sequence = observation.collector.last_sequence
    if last_offset is None or last_sequence is None:
        raise ChainReconciliationError(
            "PID_ONLY_HEALTH",
            "collector health does not prove a durable sequence and Kafka offset",
            details={
                "last_partition_offset": last_offset,
                "last_sequence": last_sequence,
                "events_published": observation.collector.events_published,
            },
        )
    if isinstance(last_offset, bool) or last_offset < 0:
        raise ChainReconciliationError(
            "PID_ONLY_HEALTH",
            "collector last_partition_offset is invalid",
        )
    if isinstance(last_sequence, bool) or last_sequence < 1:
        raise ChainReconciliationError(
            "PID_ONLY_HEALTH",
            "collector last_sequence is invalid",
        )
    if observation.collector.pending_event_id is not None:
        raise ChainReconciliationError(
            "COLLECTOR_NOT_PROGRESSING",
            "collector still has a pending unpublished or uncheckpointed event",
            details={"pending_event_id": observation.collector.pending_event_id},
        )
    return last_offset + 1, last_sequence


def _reject_consumers_ahead(
    observation: ChainObservation,
    physical_next: int,
) -> None:
    for name, offset in (
        ("writer", observation.writer.committed_next_offset),
        ("archive", observation.archive.committed_next_offset),
    ):
        if offset is None:
            continue
        if isinstance(offset, bool) or offset < 0:
            raise ChainReconciliationError(
                "OFFSET_MISMATCH",
                f"{name} committed_next_offset is invalid",
            )
        if offset > physical_next:
            raise ChainReconciliationError(
                "CONSUMER_AHEAD_OF_COLLECTOR",
                f"{name} committed next offset is ahead of the collector checkpoint",
                details={
                    "physical_next_offset": physical_next,
                    f"{name}_committed_next_offset": offset,
                },
            )


def _contiguous_sequences(sequences: tuple[int, ...]) -> tuple[int, ...]:
    if not sequences:
        raise ChainReconciliationError(
            "QUERY_SEQUENCE_GAP",
            "cold query returned no first-fact sequences",
        )
    expected = sequences[0]
    for value in sequences:
        if value != expected:
            raise ChainReconciliationError(
                "QUERY_SEQUENCE_GAP",
                "cold query first-fact sequences are not contiguous",
                details={"expected_sequence": expected, "actual_sequence": value},
            )
        expected += 1
    return sequences


def _reject_replay_and_query_drift(
    observation: ChainObservation,
    sequences: tuple[int, ...],
) -> None:
    expected_rows = sequences[-1] - sequences[0] + 1
    if (
        observation.replay_first_id != sequences[0]
        or observation.replay_last_id != sequences[-1]
        or observation.replay_row_count != expected_rows
        or observation.replay_row_count != len(sequences)
    ):
        raise ChainReconciliationError(
            "REPLAY_SPAN_MISMATCH",
            "Phase 1Q replay pin does not match cold query first facts",
            details={
                "query_first": sequences[0],
                "query_last": sequences[-1],
                "query_count": len(sequences),
                "replay_first_id": observation.replay_first_id,
                "replay_last_id": observation.replay_last_id,
                "replay_row_count": observation.replay_row_count,
            },
        )


def _require_matching_snapshot(
    observation: ChainObservation,
) -> MarketDataSnapshotRef:
    archive_snapshot = observation.archive.current_snapshot
    if archive_snapshot is None:
        raise ChainReconciliationError(
            "SNAPSHOT_DRIFT",
            "archive health has no current immutable snapshot",
        )
    if archive_snapshot != observation.query_snapshot:
        raise ChainReconciliationError(
            "SNAPSHOT_DRIFT",
            "cold query snapshot drifted from archive health",
            details={
                "archive_manifest_sha256": archive_snapshot.manifest_sha256,
                "query_manifest_sha256": observation.query_snapshot.manifest_sha256,
            },
        )
    if observation.query_snapshot != observation.replay_snapshot:
        raise ChainReconciliationError(
            "SNAPSHOT_DRIFT",
            "replay pin snapshot drifted from the cold query snapshot",
            details={
                "query_manifest_sha256": observation.query_snapshot.manifest_sha256,
                "replay_manifest_sha256": observation.replay_snapshot.manifest_sha256,
            },
        )
    if archive_snapshot.snapshot_version != observation.archive.committed_next_offset:
        raise ChainReconciliationError(
            "SNAPSHOT_DRIFT",
            "archive snapshot_version drifted from committed_next_offset",
            details={
                "snapshot_version": archive_snapshot.snapshot_version,
                "committed_next_offset": observation.archive.committed_next_offset,
            },
        )
    return archive_snapshot


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 1:
        raise ValueError(f"{field} must be greater than zero")
    return value


__all__ = [
    "CHAIN_RECONCILIATION_SCHEMA_VERSION",
    "STATUS_CAUGHT_UP",
    "STATUS_LAGGING",
    "ChainObservation",
    "ChainReconciliation",
    "ChainReconciliationError",
    "reconcile_chain",
]
