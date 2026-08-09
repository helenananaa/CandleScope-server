"""Observable lifecycle state for the Phase 1C collector."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class CollectorState(str, enum.Enum):
    STARTING = "starting"
    STANDBY = "standby"
    LEADER = "leader"
    RECOVERING = "recovering"
    DEGRADED = "degraded"
    FENCED = "fenced"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class CollectorHealth:
    state: CollectorState
    ready: bool
    reason: str
    owner_id: str
    source_health: str | None
    producer_epoch: int | None
    lease_expires_at_ms: int | None
    last_sequence: int | None
    last_partition_offset: int | None
    pending_event_id: str | None
    events_published: int
    heartbeat_successes: int
    heartbeat_failures: int
    started_at_ms: int
    updated_at_ms: int
    terminal_error: str | None

    def to_wire(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "ready": self.ready,
            "reason": self.reason,
            "owner_id": self.owner_id,
            "source_health": self.source_health,
            "producer_epoch": self.producer_epoch,
            "lease_expires_at_ms": self.lease_expires_at_ms,
            "last_sequence": self.last_sequence,
            "last_partition_offset": self.last_partition_offset,
            "pending_event_id": self.pending_event_id,
            "events_published": self.events_published,
            "heartbeat_successes": self.heartbeat_successes,
            "heartbeat_failures": self.heartbeat_failures,
            "started_at_ms": self.started_at_ms,
            "updated_at_ms": self.updated_at_ms,
            "terminal_error": self.terminal_error,
        }
