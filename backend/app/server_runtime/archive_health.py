"""Observable state for the Phase 1E Parquet archive writer."""

from __future__ import annotations

import enum
from dataclasses import dataclass

from app.server_contracts import MarketDataSnapshotRef


class ArchiveWriterState(str, enum.Enum):
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class ArchiveWriterHealth:
    state: ArchiveWriterState
    ready: bool
    reason: str
    owner_id: str
    kafka_group_id: str
    data_epoch: str
    committed_next_offset: int | None
    segments_committed: int
    events_archived: int
    current_snapshot: MarketDataSnapshotRef | None
    started_at_ms: int
    updated_at_ms: int
    terminal_error: str | None

    def to_wire(self) -> dict[str, object]:
        snapshot = self.current_snapshot
        return {
            "state": self.state.value,
            "ready": self.ready,
            "reason": self.reason,
            "owner_id": self.owner_id,
            "kafka_group_id": self.kafka_group_id,
            "data_epoch": self.data_epoch,
            "committed_next_offset": self.committed_next_offset,
            "segments_committed": self.segments_committed,
            "events_archived": self.events_archived,
            "current_snapshot": (
                None
                if snapshot is None
                else {
                    "data_epoch": snapshot.data_epoch,
                    "snapshot_version": snapshot.snapshot_version,
                    "manifest_uri": snapshot.manifest_uri,
                    "manifest_sha256": snapshot.manifest_sha256,
                }
            ),
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "terminal_error": self.terminal_error,
        }
