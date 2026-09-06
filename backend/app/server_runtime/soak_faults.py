"""One-way Phase 1AI fault injection state machine."""

from __future__ import annotations

import enum
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from app.server_runtime.public_soak_manifest import (
    FAULT_METHOD_TARGETS,
    REQUIRED_RUN_FAULT_METHODS,
    FaultSpec,
    PublicSoakManifest,
)

ClockMs = Callable[[], int]
Sleeper = Callable[[float], Awaitable[None]]
WRITER_PRECOMMIT_HOOK = "after_project_before_commit"
ARCHIVER_PRECOMMIT_HOOK = "after_archive_before_commit"
COLLECTOR_QUIET_HOLD = "collector.quiet.hold"
QUIET_HOLD_MAX_MS = 8_000


class FaultStatus(str, enum.Enum):
    PLANNED = "planned"
    TRIGGER_REQUESTED = "trigger_requested"
    TRIGGER_OBSERVED = "trigger_observed"
    RECOVERY_OBSERVED = "recovery_observed"
    QUIET_CHECKPOINT_VERIFIED = "quiet_checkpoint_verified"
    FAILED = "failed"


_ALLOWED: dict[FaultStatus, frozenset[FaultStatus]] = {
    FaultStatus.PLANNED: frozenset({FaultStatus.TRIGGER_REQUESTED, FaultStatus.FAILED}),
    FaultStatus.TRIGGER_REQUESTED: frozenset(
        {FaultStatus.TRIGGER_OBSERVED, FaultStatus.FAILED}
    ),
    FaultStatus.TRIGGER_OBSERVED: frozenset(
        {FaultStatus.RECOVERY_OBSERVED, FaultStatus.FAILED}
    ),
    FaultStatus.RECOVERY_OBSERVED: frozenset(
        {FaultStatus.QUIET_CHECKPOINT_VERIFIED, FaultStatus.FAILED}
    ),
    FaultStatus.QUIET_CHECKPOINT_VERIFIED: frozenset(),
    FaultStatus.FAILED: frozenset(),
}


class FaultMachineError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class QuietObservation:
    collector_durable_next_offset: int
    writer_committed_next_offset: int
    archiver_covered_next_offset: int
    query_snapshot: Mapping[str, Any]
    replay_pinned_snapshot: Mapping[str, Any]
    unresolved_gaps: int
    hash_conflicts: int
    producer_epoch_rollback: int


class FaultActuator(Protocol):
    async def trigger(self, spec: FaultSpec) -> None: ...

    async def trigger_observed(self, spec: FaultSpec) -> bool: ...

    async def recovery_observed(self, spec: FaultSpec) -> bool: ...

    async def quiet_observation(self) -> QuietObservation: ...

    async def arm_quiet_hold(self) -> None: ...

    async def clear_quiet_hold(self) -> None: ...


@dataclass
class FaultRecord:
    spec: FaultSpec
    status: FaultStatus = FaultStatus.PLANNED
    error_code: str | None = None
    events: list[dict[str, object]] = field(default_factory=list)

    def to_evidence(self) -> dict[str, object]:
        return {
            "fault_id": self.spec.fault_id,
            "method": self.spec.method,
            "target_role": self.spec.target_role,
            "status": self.status.value,
            "error_code": self.error_code,
            "events": list(self.events),
        }


class SoakFaultMachine:
    def __init__(
        self,
        manifest: PublicSoakManifest,
        actuator: FaultActuator,
        *,
        clock_ms: ClockMs,
        sleep: Sleeper,
        hook_dir: str | Path | None = None,
    ) -> None:
        self._manifest = manifest
        self._actuator = actuator
        self._clock_ms = clock_ms
        self._sleep = sleep
        self._hook_dir = Path(hook_dir) if hook_dir is not None else None
        self._started_at_ms: int | None = None
        self.records = [FaultRecord(spec) for spec in manifest.fault_plan]
        self._index = 0

    def remaining(self) -> tuple[FaultRecord, ...]:
        return tuple(
            item
            for item in self.records
            if item.status is not FaultStatus.QUIET_CHECKPOINT_VERIFIED
        )

    async def run_due(self, *, started_at_ms: int) -> FaultRecord | None:
        self._started_at_ms = started_at_ms
        if self._index >= len(self.records):
            return None
        current = self.records[self._index]
        if current.status is FaultStatus.FAILED:
            raise FaultMachineError(
                "FAULT_FAILED",
                "a previous fault failed; refusing to continue",
                details={"fault_id": current.spec.fault_id},
            )
        elapsed = self._clock_ms() - started_at_ms
        if elapsed < current.spec.scheduled_elapsed_ms:
            return None
        if current.status is FaultStatus.QUIET_CHECKPOINT_VERIFIED:
            self._index += 1
            return current
        await self._execute(current)
        if current.status is not FaultStatus.QUIET_CHECKPOINT_VERIFIED:
            raise FaultMachineError(
                current.error_code or "FAULT_FAILED",
                f"{current.spec.fault_id} did not complete",
                details={"status": current.status.value},
            )
        self._index += 1
        return current

    async def run_all(self, *, started_at_ms: int) -> list[FaultRecord]:
        completed: list[FaultRecord] = []
        while self._index < len(self.records):
            current = self.records[self._index]
            delay_ms = current.spec.scheduled_elapsed_ms - (
                self._clock_ms() - started_at_ms
            )
            if delay_ms > 0:
                await self._sleep(delay_ms / 1000)
            record = await self.run_due(started_at_ms=started_at_ms)
            if record is None:
                break
            completed.append(record)
        return completed

    async def _execute(self, record: FaultRecord) -> None:
        spec = record.spec
        try:
            self._transition(record, FaultStatus.TRIGGER_REQUESTED)
            await self._actuator.trigger(spec)
            await self._wait(
                record,
                spec.observation_timeout_ms,
                self._actuator.trigger_observed,
                FaultStatus.TRIGGER_OBSERVED,
                "TRIGGER_TIMEOUT",
            )
            await self._wait(
                record,
                spec.recovery_timeout_ms,
                self._actuator.recovery_observed,
                FaultStatus.RECOVERY_OBSERVED,
                "RECOVERY_TIMEOUT",
            )
            await self._wait_quiet(record)
            self._transition(record, FaultStatus.QUIET_CHECKPOINT_VERIFIED)
        except FaultMachineError as exc:
            record.error_code = exc.code
            if record.status is not FaultStatus.FAILED:
                self._transition(record, FaultStatus.FAILED)
            raise

    async def _wait(
        self,
        record: FaultRecord,
        timeout_ms: int,
        probe: Callable[[FaultSpec], Awaitable[bool]],
        success: FaultStatus,
        timeout_code: str,
    ) -> None:
        deadline = self._clock_ms() + timeout_ms
        while self._clock_ms() < deadline:
            if await probe(record.spec):
                self._transition(record, success)
                return
            await self._sleep(0.05)
        raise FaultMachineError(
            timeout_code,
            f"{record.spec.fault_id} timed out waiting for {success.value}",
            details={"fault_id": record.spec.fault_id},
        )

    async def _wait_quiet(self, record: FaultRecord) -> None:
        armed = False
        try:
            arm = getattr(self._actuator, "arm_quiet_hold", None)
            if callable(arm):
                await arm()
                armed = True
            timeout_ms = self._manifest.quiet_checkpoint_timeout_ms
            deadline = self._clock_ms() + timeout_ms
            last_error: FaultMachineError | None = None
            while self._clock_ms() < deadline:
                quiet = await self._actuator.quiet_observation()
                try:
                    verify_quiet_checkpoint(quiet)
                    return
                except FaultMachineError as exc:
                    last_error = exc
                    await self._sleep(0.05)
            raise last_error or FaultMachineError(
                "QUIET_CHECKPOINT_TIMEOUT",
                f"{record.spec.fault_id} quiet checkpoint did not converge",
            )
        finally:
            if armed:
                clear = getattr(self._actuator, "clear_quiet_hold", None)
                if callable(clear):
                    await clear()

    def _transition(self, record: FaultRecord, target: FaultStatus) -> None:
        allowed = _ALLOWED[record.status]
        if target not in allowed:
            raise FaultMachineError(
                "ILLEGAL_FAULT_TRANSITION",
                f"{record.status.value} cannot move to {target.value}",
            )
        record.status = target
        record.events.append(
            {
                "status": target.value,
                "at_ms": self._clock_ms(),
                "fault_id": record.spec.fault_id,
            }
        )


def verify_quiet_checkpoint(observation: QuietObservation) -> None:
    if not (
        observation.collector_durable_next_offset
        == observation.writer_committed_next_offset
        == observation.archiver_covered_next_offset
    ):
        raise FaultMachineError(
            "QUIET_CHECKPOINT_MISMATCH",
            "collector, writer, and archiver offsets are not equal",
            details={
                "collector": observation.collector_durable_next_offset,
                "writer": observation.writer_committed_next_offset,
                "archiver": observation.archiver_covered_next_offset,
            },
        )
    if dict(observation.query_snapshot) != dict(observation.replay_pinned_snapshot):
        raise FaultMachineError(
            "QUIET_CHECKPOINT_MISMATCH",
            "query snapshot does not equal the replay pin",
        )
    if observation.unresolved_gaps != 0:
        raise FaultMachineError("UNRESOLVED_GAPS", "unresolved gaps must be 0")
    if observation.hash_conflicts != 0:
        raise FaultMachineError("HASH_CONFLICT", "hash conflicts must be 0")
    if observation.producer_epoch_rollback != 0:
        raise FaultMachineError(
            "PRODUCER_EPOCH_ROLLBACK",
            "producer epoch rollback must be 0",
        )


def quiet_hold_path(directory: str | Path) -> Path:
    return Path(directory) / COLLECTOR_QUIET_HOLD


def arm_quiet_hold(directory: str | Path) -> Path:
    path = quiet_hold_path(directory)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("quiet", encoding="utf-8")
    return path


def clear_quiet_hold(directory: str | Path) -> None:
    try:
        quiet_hold_path(directory).unlink()
    except FileNotFoundError:
        return


async def hold_after_aligned_publish(
    directory: str | Path,
    *,
    last_offset: int,
    segment_event_count: int,
    clock_ms: ClockMs | None = None,
    sleep: Sleeper | None = None,
    max_hold_ms: int = QUIET_HOLD_MAX_MS,
) -> None:
    """Block after a complete archive segment while a quiet hold is armed.

    The collector keeps its lease heartbeat on another task. The hold is
    capped so a missed clear cannot stall the Binance websocket indefinitely.
    """

    if segment_event_count <= 0 or last_offset < 0:
        return
    if (last_offset + 1) % segment_event_count != 0:
        return
    path = quiet_hold_path(directory)
    if not path.exists():
        return
    clock = clock_ms or _clock_ms
    sleeper = sleep or _async_sleep
    deadline = clock() + max_hold_ms
    while path.exists() and clock() < deadline:
        await sleeper(0.05)


def _clock_ms() -> int:
    return time.time_ns() // 1_000_000


async def _async_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


def precommit_hook_name(method: str) -> str | None:
    if method == "writer_pre_commit_exit":
        return WRITER_PRECOMMIT_HOOK
    if method == "archiver_pre_commit_exit":
        return ARCHIVER_PRECOMMIT_HOOK
    return None


def arm_precommit_hook(directory: str | Path, spec: FaultSpec) -> Path:
    hook = precommit_hook_name(spec.method)
    if hook is None:
        raise FaultMachineError(
            "UNSUPPORTED_FAULT_METHOD",
            "only writer/archiver faults arm a pre-commit hook",
        )
    path = Path(directory) / f"{spec.target_role}.{hook}.arm"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(spec.fault_id, encoding="utf-8")
    return path


def required_methods_for_plan(plan: tuple[FaultSpec, ...]) -> tuple[str, ...]:
    methods = tuple(item.method for item in plan)
    for method in methods:
        if method not in FAULT_METHOD_TARGETS:
            raise FaultMachineError("UNSUPPORTED_FAULT_METHOD", method)
    return methods


__all__ = [
    "ARCHIVER_PRECOMMIT_HOOK",
    "COLLECTOR_QUIET_HOLD",
    "QUIET_HOLD_MAX_MS",
    "REQUIRED_RUN_FAULT_METHODS",
    "WRITER_PRECOMMIT_HOOK",
    "FaultActuator",
    "FaultMachineError",
    "FaultRecord",
    "FaultStatus",
    "QuietObservation",
    "SoakFaultMachine",
    "arm_precommit_hook",
    "arm_quiet_hold",
    "clear_quiet_hold",
    "hold_after_aligned_publish",
    "precommit_hook_name",
    "quiet_hold_path",
    "required_methods_for_plan",
    "verify_quiet_checkpoint",
]
