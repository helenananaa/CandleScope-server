"""Scheduler health wire contract."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReplaySchedulerHealth:
    ready: bool
    pending: int
    running: int
    last_scan_at_ms: int | None
    last_error_code: str | None

    def to_wire(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "pending": self.pending,
            "running": self.running,
            "last_scan_at_ms": self.last_scan_at_ms,
            "last_error_code": self.last_error_code,
        }
