"""Observable state for the Phase 1D ClickHouse writer."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class ClickHouseWriterState(str, enum.Enum):
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class ClickHouseWriterHealth:
    state: ClickHouseWriterState
    ready: bool
    reason: str
    owner_id: str
    kafka_group_id: str
    committed_next_offset: int | None
    batches_committed: int
    inserted_events: int
    duplicate_events: int
    conflict_events: int
    started_at_ms: int
    updated_at_ms: int
    terminal_error: str | None

    def to_wire(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "ready": self.ready,
            "reason": self.reason,
            "owner_id": self.owner_id,
            "kafka_group_id": self.kafka_group_id,
            "committed_next_offset": self.committed_next_offset,
            "batches_committed": self.batches_committed,
            "inserted_events": self.inserted_events,
            "duplicate_events": self.duplicate_events,
            "conflict_events": self.conflict_events,
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "terminal_error": self.terminal_error,
        }
