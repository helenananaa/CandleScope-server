"""Loopback health contract for one Replay Worker."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class ReplayWorkerState(str, enum.Enum):
    STARTING = "starting"
    READY = "ready"
    RECOVERING = "recovering"
    FENCED = "fenced"
    STOPPING = "stopping"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class ReplayWorkerHealth:
    worker_id: str
    state: ReplayWorkerState
    ready: bool
    active_actors: int
    lease_expires_at_ms: int | None
    last_renew_success_at_ms: int | None
    last_mutation_at_ms: int | None
    last_checkpoint_at_ms: int | None
    checkpoint_age_ms: int | None
    recoveries: int
    recovery_failures: int
    fencing_conflicts: int
    last_error_code: str | None
    updated_at_ms: int

    def to_wire(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "state": self.state.value,
            "ready": self.ready,
            "active_actors": self.active_actors,
            "lease_expires_at_ms": self.lease_expires_at_ms,
            "last_renew_success_at_ms": self.last_renew_success_at_ms,
            "last_mutation_at_ms": self.last_mutation_at_ms,
            "last_checkpoint_at_ms": self.last_checkpoint_at_ms,
            "checkpoint_age_ms": self.checkpoint_age_ms,
            "recoveries": self.recoveries,
            "recovery_failures": self.recovery_failures,
            "fencing_conflicts": self.fencing_conflicts,
            "last_error_code": self.last_error_code,
            "updated_at_ms": self.updated_at_ms,
        }
