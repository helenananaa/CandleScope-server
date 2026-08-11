"""Shared hot-projection control contracts for the Phase 1H query service."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

DEFAULT_HOT_BACKEND_ID = "clickhouse-market-events-v1"


class QueryControlError(RuntimeError):
    """Base class for durable query-control failures."""


class QueryControlUnavailableError(QueryControlError):
    """The shared control plane could not complete an operation."""


class HotProjectionNotQuarantinedError(QueryControlError):
    """A clear command targeted a backend that is not quarantined."""


class HotProjectionGenerationConflictError(QueryControlError):
    """A clear command used a stale quarantine generation."""


@dataclass(frozen=True, slots=True)
class HotProjectionControlState:
    backend_id: str
    generation: int
    active: bool
    reason: str | None
    latched_by: str | None
    latched_at_ms: int | None
    cleared_by: str | None
    clear_reason_code: str | None
    cleared_at_ms: int | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "backend_id",
            _required_text(self.backend_id, field="backend_id", max_length=128),
        )
        generation = _non_negative_int(self.generation, field="generation")
        object.__setattr__(self, "generation", generation)
        if not isinstance(self.active, bool):
            raise TypeError("active must be a boolean")
        for field in ("reason", "latched_by", "cleared_by", "clear_reason_code"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(
                    self,
                    field,
                    _required_text(value, field=field, max_length=256),
                )
        for field in ("latched_at_ms", "cleared_at_ms"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(
                    self,
                    field,
                    _non_negative_int(value, field=field),
                )
        if self.active and (
            generation == 0
            or self.reason is None
            or self.latched_by is None
            or self.latched_at_ms is None
        ):
            raise ValueError("active quarantine state is incomplete")
        if generation == 0 and any(
            value is not None
            for value in (
                self.reason,
                self.latched_by,
                self.latched_at_ms,
                self.cleared_by,
                self.clear_reason_code,
                self.cleared_at_ms,
            )
        ):
            raise ValueError("generation zero cannot contain quarantine history")

    @classmethod
    def initial(cls, backend_id: str) -> HotProjectionControlState:
        return cls(
            backend_id=backend_id,
            generation=0,
            active=False,
            reason=None,
            latched_by=None,
            latched_at_ms=None,
            cleared_by=None,
            clear_reason_code=None,
            cleared_at_ms=None,
        )

    def to_wire(self) -> dict[str, object]:
        return {
            "backend_id": self.backend_id,
            "generation": self.generation,
            "active": self.active,
            "reason": self.reason,
            "latched_by": self.latched_by,
            "latched_at_ms": self.latched_at_ms,
            "cleared_by": self.cleared_by,
            "clear_reason_code": self.clear_reason_code,
            "cleared_at_ms": self.cleared_at_ms,
        }


@dataclass(frozen=True, slots=True)
class ClearHotProjectionQuarantineCommand:
    expected_generation: int
    principal: str
    reason_code: str
    request_id: str
    timestamp_ms: int

    def __post_init__(self) -> None:
        generation = _positive_int(
            self.expected_generation,
            field="expected_generation",
        )
        object.__setattr__(self, "expected_generation", generation)
        for field, max_length in (
            ("principal", 128),
            ("reason_code", 128),
            ("request_id", 128),
        ):
            object.__setattr__(
                self,
                field,
                _required_text(
                    getattr(self, field),
                    field=field,
                    max_length=max_length,
                ),
            )
        if (
            not self.reason_code.isascii()
            or self.reason_code[0] not in "abcdefghijklmnopqrstuvwxyz0123456789"
            or not all(
                character in "abcdefghijklmnopqrstuvwxyz0123456789._:-"
                for character in self.reason_code
            )
        ):
            raise ValueError("reason_code must use lower-case safe ASCII")
        object.__setattr__(
            self,
            "timestamp_ms",
            _non_negative_int(self.timestamp_ms, field="timestamp_ms"),
        )


@runtime_checkable
class HotProjectionControlStore(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def status(self) -> HotProjectionControlState: ...

    async def latch(self, reason: str) -> HotProjectionControlState: ...

    async def clear(
        self,
        command: ClearHotProjectionQuarantineCommand,
    ) -> HotProjectionControlState: ...


class InProcessHotProjectionControlStore:
    """Historical single-process fallback used only by old regression gates."""

    def __init__(
        self,
        *,
        backend_id: str = DEFAULT_HOT_BACKEND_ID,
        instance_id: str = "in-process-query",
    ) -> None:
        self._backend_id = _required_text(
            backend_id,
            field="backend_id",
            max_length=128,
        )
        self._instance_id = _required_text(
            instance_id,
            field="instance_id",
            max_length=128,
        )
        self._state = HotProjectionControlState.initial(self._backend_id)
        self._started = False

    async def start(self) -> None:
        if self._started:
            raise QueryControlError("query control store is already started")
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def status(self) -> HotProjectionControlState:
        self._require_started()
        return self._state

    async def latch(self, reason: str) -> HotProjectionControlState:
        self._require_started()
        reason = _required_text(reason, field="reason", max_length=256)
        if self._state.active:
            return self._state
        now_ms = time.time_ns() // 1_000_000
        self._state = HotProjectionControlState(
            backend_id=self._backend_id,
            generation=self._state.generation + 1,
            active=True,
            reason=reason,
            latched_by=self._instance_id,
            latched_at_ms=now_ms,
            cleared_by=None,
            clear_reason_code=None,
            cleared_at_ms=None,
        )
        return self._state

    async def clear(
        self,
        command: ClearHotProjectionQuarantineCommand,
    ) -> HotProjectionControlState:
        self._require_started()
        if not isinstance(command, ClearHotProjectionQuarantineCommand):
            raise TypeError("command must be a ClearHotProjectionQuarantineCommand")
        if not self._state.active:
            raise HotProjectionNotQuarantinedError(
                "the hot projection is not quarantined"
            )
        if self._state.generation != command.expected_generation:
            raise HotProjectionGenerationConflictError(
                "the quarantine generation changed before clear"
            )
        self._state = HotProjectionControlState(
            backend_id=self._state.backend_id,
            generation=self._state.generation,
            active=False,
            reason=self._state.reason,
            latched_by=self._state.latched_by,
            latched_at_ms=self._state.latched_at_ms,
            cleared_by=command.principal,
            clear_reason_code=command.reason_code,
            cleared_at_ms=command.timestamp_ms,
        )
        return self._state

    def _require_started(self) -> None:
        if not self._started:
            raise QueryControlUnavailableError("query control store is not started")


def _required_text(value: object, *, field: str, max_length: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    value = value.strip()
    if len(value) > max_length:
        raise ValueError(f"{field} must contain at most {max_length} characters")
    return value


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _positive_int(value: object, *, field: str) -> int:
    value = _non_negative_int(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be positive")
    return value
