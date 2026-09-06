"""Snapshot-pinned Phase 1AI replay workload and command-id idempotency."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from app.replay.constants import REPLAY_PROTOCOL, CommandType, QualityMode, SourceKind
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.public_soak_manifest import (
    PublicSoakManifest,
)

QUEUED_STATES = frozenset({"PENDING", "QUEUED"})
ACTIVE_STATES = frozenset({"ASSIGNED", "STARTING", "RUNNING"})
FROZEN_IDEMPOTENT_COMMAND_ID = "phase1ai-replay-b-idempotent-step"


class ReplaySoakError(RuntimeError):
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


class SoakReplayTransport(Protocol):
    async def cold_query_snapshot(
        self, snapshot: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    async def live_worker_count(self) -> int: ...

    async def create_run(self, payload: Mapping[str, Any]) -> Mapping[str, Any]: ...

    async def get_run(self, run_id: str) -> Mapping[str, Any]: ...

    async def submit_command(
        self, session_id: str, command: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    async def get_command_result(
        self, session_id: str, command_id: str
    ) -> Mapping[str, Any]: ...

    def durable_command_count(self, command_id: str) -> int: ...


@dataclass(frozen=True, slots=True)
class SnapshotPin:
    snapshot: MarketDataSnapshotRef
    start_event_time_ms: int
    end_event_time_ms: int
    expected_first_agg_trade_id: int
    expected_last_agg_trade_id: int
    row_count: int

    def to_payload(self) -> dict[str, object]:
        return {
            "snapshot": {
                "data_epoch": self.snapshot.data_epoch,
                "snapshot_version": self.snapshot.snapshot_version,
                "manifest_uri": self.snapshot.manifest_uri,
                "manifest_sha256": self.snapshot.manifest_sha256,
            },
            "pin": {
                "start_event_time_ms": self.start_event_time_ms,
                "end_event_time_ms": self.end_event_time_ms,
                "expected_first_agg_trade_id": self.expected_first_agg_trade_id,
                "expected_last_agg_trade_id": self.expected_last_agg_trade_id,
                "row_count": self.row_count,
            },
        }


@dataclass(frozen=True, slots=True)
class ReplayTask:
    name: str
    run_id: str
    session_id: str | None
    state: str
    observed_queued: bool


@dataclass
class SoakReplayWorkload:
    pin: SnapshotPin
    replay_a: ReplayTask
    replay_b: ReplayTask
    replay_queued: ReplayTask
    idempotent_command_id: str = FROZEN_IDEMPOTENT_COMMAND_ID


@dataclass
class CommandObservation:
    command_id: str
    revision: int
    cursor: Mapping[str, Any]
    state_hash: str
    component_hash: str | None = None


class PublicSoakReplayDriver:
    def __init__(
        self,
        manifest: PublicSoakManifest,
        transport: SoakReplayTransport,
        *,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._manifest = manifest
        self._transport = transport
        self._sleep = sleep

    async def ensure_ready(self, pin: SnapshotPin) -> None:
        workers = await self._transport.live_worker_count()
        if workers < 2:
            raise ReplaySoakError(
                "WORKERS_NOT_LIVE",
                "two scoped Replay Workers must be live before creating tasks",
                details={"live_workers": workers},
            )
        queried = await self._transport.cold_query_snapshot(
            pin.to_payload()["snapshot"]
        )
        if queried.get("preference") not in {None, "cold"}:
            raise ReplaySoakError(
                "QUERY_NOT_COLD",
                "Query Service must use preference=cold",
            )
        if int(queried.get("snapshot_version") or 0) != pin.snapshot.snapshot_version:
            raise ReplaySoakError(
                "SNAPSHOT_MISMATCH",
                "cold Query snapshot does not match the pinned snapshot",
            )
        if str(queried.get("manifest_sha256") or "") != pin.snapshot.manifest_sha256:
            raise ReplaySoakError(
                "SNAPSHOT_MISMATCH",
                "cold Query manifest hash does not match the pinned snapshot",
            )

    async def create_three_tasks(self, pin: SnapshotPin) -> SoakReplayWorkload:
        await self.ensure_ready(pin)
        replay_a = await self._create("replay-a", pin, priority=10)
        replay_b = await self._create("replay-b", pin, priority=9)
        if replay_a.state not in ACTIVE_STATES or replay_b.state not in ACTIVE_STATES:
            # Capacity may still be assigning; re-read rather than assume.
            replay_a = await self._refresh(replay_a)
            replay_b = await self._refresh(replay_b)
        replay_queued = await self._create("replay-queued", pin, priority=0)
        replay_queued = await self._refresh(replay_queued)
        observed_queued = replay_queued.state in QUEUED_STATES
        if not observed_queued:
            raise ReplaySoakError(
                "QUEUED_NOT_OBSERVED",
                "queued replay was not observed in PENDING/QUEUED",
                details={"state": replay_queued.state},
            )
        if "query_path" in replay_a.run_id:
            raise ReplaySoakError("QUERY_PATH_FORBIDDEN", "run id leaked query_path")
        return SoakReplayWorkload(
            pin=pin,
            replay_a=replay_a,
            replay_b=replay_b,
            replay_queued=ReplayTask(
                name=replay_queued.name,
                run_id=replay_queued.run_id,
                session_id=replay_queued.session_id,
                state=replay_queued.state,
                observed_queued=True,
            ),
        )

    async def drive_commands(
        self, workload: SoakReplayWorkload
    ) -> dict[str, CommandObservation]:
        if workload.replay_a.session_id is None or workload.replay_b.session_id is None:
            raise ReplaySoakError(
                "SESSION_NOT_ASSIGNED",
                "active replays must have session ids before commands",
            )
        observed: dict[str, CommandObservation] = {}
        observed["replay-a-step"] = await self._command(
            workload.replay_a.session_id,
            _step_command(f"{workload.replay_a.run_id}-step"),
        )
        observed["replay-b-step"] = await self._command(
            workload.replay_b.session_id,
            _step_command(f"{workload.replay_b.run_id}-step"),
        )
        observed["replay-b-pause"] = await self._command(
            workload.replay_b.session_id,
            _typed_command(f"{workload.replay_b.run_id}-pause", CommandType.PAUSE),
        )
        observed["replay-b-resume"] = await self._command(
            workload.replay_b.session_id,
            _typed_command(f"{workload.replay_b.run_id}-resume", CommandType.PLAY),
        )
        queued = await self._refresh(workload.replay_queued)
        if queued.state not in QUEUED_STATES:
            raise ReplaySoakError(
                "QUEUED_NOT_OBSERVED",
                "queued replay left the queue before the active pair completed a command",
                details={"state": queued.state},
            )
        return observed

    async def probe_idempotency(
        self,
        workload: SoakReplayWorkload,
        *,
        first_submit: Callable[[], Awaitable[Mapping[str, Any]]] | None = None,
    ) -> CommandObservation:
        if workload.replay_b.session_id is None:
            raise ReplaySoakError(
                "SESSION_NOT_ASSIGNED",
                "replay-b must have a session id",
            )
        command = _step_command(workload.idempotent_command_id)
        if first_submit is not None:
            try:
                await first_submit()
            except TimeoutError:
                pass
        else:
            try:
                await self._transport.submit_command(
                    workload.replay_b.session_id, {**command, "simulate_timeout": True}
                )
            except TimeoutError:
                pass
        retry = await self._command(workload.replay_b.session_id, command)
        reread = await self._transport.get_command_result(
            workload.replay_b.session_id,
            workload.idempotent_command_id,
        )
        observation = _observation(reread, workload.idempotent_command_id)
        if observation.revision != retry.revision:
            raise ReplaySoakError(
                "COMMAND_NOT_IDEMPOTENT",
                "command revision advanced after retry",
            )
        if (
            observation.cursor != retry.cursor
            or observation.state_hash != retry.state_hash
        ):
            raise ReplaySoakError(
                "COMMAND_NOT_IDEMPOTENT",
                "cursor or state hash advanced after retry",
            )
        durable = self._transport.durable_command_count(workload.idempotent_command_id)
        if durable != 1:
            raise ReplaySoakError(
                "COMMAND_NOT_IDEMPOTENT",
                "PostgreSQL must contain exactly one durable command result",
                details={"durable_count": durable},
            )
        return observation

    async def _create(
        self, name: str, pin: SnapshotPin, *, priority: int
    ) -> ReplayTask:
        payload = build_run_payload(
            self._manifest, pin, idempotency_key=name, priority=priority
        )
        if "query_path" in payload:
            raise ReplaySoakError(
                "QUERY_PATH_FORBIDDEN",
                "server replay requests cannot select a local query path",
            )
        created = await self._transport.create_run(payload)
        return ReplayTask(
            name=name,
            run_id=str(created["run_id"]),
            session_id=_optional_text(created.get("session_id")),
            state=str(created["state"]),
            observed_queued=str(created["state"]) in QUEUED_STATES,
        )

    async def _refresh(self, task: ReplayTask) -> ReplayTask:
        body = await self._transport.get_run(task.run_id)
        return ReplayTask(
            name=task.name,
            run_id=task.run_id,
            session_id=_optional_text(body.get("session_id")) or task.session_id,
            state=str(body["state"]),
            observed_queued=str(body["state"]) in QUEUED_STATES,
        )

    async def _command(
        self, session_id: str, command: Mapping[str, Any]
    ) -> CommandObservation:
        body = await self._transport.submit_command(session_id, command)
        return _observation(body, str(command["command_id"]))


def parse_snapshot_pin(payload: Mapping[str, Any]) -> SnapshotPin:
    if "query_path" in payload or "query_path" in payload.get("snapshot", {}):
        raise ReplaySoakError(
            "QUERY_PATH_FORBIDDEN",
            "snapshot pin cannot include a client query_path",
        )
    snapshot_payload = payload.get("snapshot")
    if not isinstance(snapshot_payload, Mapping):
        raise ReplaySoakError("INVALID_SNAPSHOT", "snapshot object is required")
    if str(snapshot_payload.get("data_epoch") or "").lower() == "latest":
        raise ReplaySoakError(
            "LATEST_SNAPSHOT_FORBIDDEN",
            "latest snapshots are forbidden",
        )
    try:
        snapshot = MarketDataSnapshotRef(
            data_epoch=str(snapshot_payload["data_epoch"]),
            snapshot_version=int(snapshot_payload["snapshot_version"]),
            manifest_uri=str(snapshot_payload["manifest_uri"]),
            manifest_sha256=str(snapshot_payload["manifest_sha256"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ReplaySoakError("INVALID_SNAPSHOT", str(exc)) from exc
    pin_payload = payload.get("pin")
    if not isinstance(pin_payload, Mapping):
        raise ReplaySoakError("INVALID_SNAPSHOT", "pin object is required")
    return SnapshotPin(
        snapshot=snapshot,
        start_event_time_ms=int(pin_payload["start_event_time_ms"]),
        end_event_time_ms=int(pin_payload["end_event_time_ms"]),
        expected_first_agg_trade_id=int(pin_payload["expected_first_agg_trade_id"]),
        expected_last_agg_trade_id=int(pin_payload["expected_last_agg_trade_id"]),
        row_count=int(pin_payload["row_count"]),
    )


def build_run_payload(
    manifest: PublicSoakManifest,
    pin: SnapshotPin,
    *,
    idempotency_key: str,
    priority: int,
) -> dict[str, object]:
    body = pin.to_payload()
    body.update(
        {
            "idempotency_key": idempotency_key,
            "source_kind": SourceKind.AGG_TRADE.value,
            "organization_id": manifest.organization_id,
            "workspace_id": manifest.workspace_id,
            "priority": priority,
            "protocol": REPLAY_PROTOCOL,
            "quality_mode": QualityMode.EXACT.value,
            "replay_start_ms": pin.start_event_time_ms,
            "replay_end_time_ms": pin.end_event_time_ms,
        }
    )
    return body


def _step_command(command_id: str) -> dict[str, object]:
    return _typed_command(command_id, CommandType.STEP)


def _typed_command(command_id: str, command_type: CommandType) -> dict[str, object]:
    return {
        "protocol": REPLAY_PROTOCOL,
        "command_id": command_id,
        "client_instance_id": "phase1ai-soak",
        "expected_revision": 0,
        "type": command_type.value,
        "payload": {},
    }


def _observation(body: Mapping[str, Any], command_id: str) -> CommandObservation:
    cursor = body.get("cursor")
    if not isinstance(cursor, Mapping):
        cursor = {}
    return CommandObservation(
        command_id=command_id,
        revision=int(body.get("revision") or 0),
        cursor=dict(cursor),
        state_hash=str(body.get("state_hash") or ""),
        component_hash=(
            str(body["component_hash"]) if body.get("component_hash") else None
        ),
    )


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def new_command_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"


__all__ = [
    "FROZEN_IDEMPOTENT_COMMAND_ID",
    "CommandObservation",
    "PublicSoakReplayDriver",
    "ReplaySoakError",
    "ReplayTask",
    "SnapshotPin",
    "SoakReplayTransport",
    "SoakReplayWorkload",
    "build_run_payload",
    "parse_snapshot_pin",
]
