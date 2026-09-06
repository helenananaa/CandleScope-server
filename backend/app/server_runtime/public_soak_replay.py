"""Snapshot-pinned Phase 1AI replay workload and command-id idempotency."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

from app.replay.constants import REPLAY_PROTOCOL, CommandType, QualityMode, SourceKind
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.public_soak_manifest import (
    PublicSoakManifest,
)

QUEUED_STATES = frozenset({"PENDING", "QUEUED"})
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

    async def get_session(self, session_id: str) -> Mapping[str, Any]: ...

    async def cancel_run(self, run_id: str) -> Mapping[str, Any]: ...

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
        replay_a = await self._wait_active(replay_a)
        replay_b = await self._wait_active(replay_b)
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
        acquired_a = await self._command(
            workload.replay_a.session_id,
            _typed_command(
                f"{workload.replay_a.run_id}-acquire",
                CommandType.ACQUIRE_CONTROLLER,
                expected_revision=0,
            ),
        )
        observed["replay-a-acquire"] = acquired_a
        observed["replay-a-step"] = await self._command(
            workload.replay_a.session_id,
            _step_command(
                f"{workload.replay_a.run_id}-step",
                expected_revision=acquired_a.revision,
            ),
        )
        acquired_b = await self._command(
            workload.replay_b.session_id,
            _typed_command(
                f"{workload.replay_b.run_id}-acquire",
                CommandType.ACQUIRE_CONTROLLER,
                expected_revision=0,
            ),
        )
        observed["replay-b-acquire"] = acquired_b
        observed["replay-b-step"] = await self._command(
            workload.replay_b.session_id,
            _step_command(
                f"{workload.replay_b.run_id}-step",
                expected_revision=acquired_b.revision,
            ),
        )
        # Manual start_policy stays PAUSED after STEP. PLAY so PAUSE is legal.
        observed["replay-b-play"] = await self._command(
            workload.replay_b.session_id,
            _typed_command(
                f"{workload.replay_b.run_id}-play",
                CommandType.PLAY,
                expected_revision=observed["replay-b-step"].revision,
            ),
        )
        observed["replay-b-pause"] = await self._command(
            workload.replay_b.session_id,
            _typed_command(
                f"{workload.replay_b.run_id}-pause",
                CommandType.PAUSE,
                expected_revision=observed["replay-b-play"].revision,
            ),
        )
        observed["replay-b-resume"] = await self._command(
            workload.replay_b.session_id,
            _typed_command(
                f"{workload.replay_b.run_id}-resume",
                CommandType.PLAY,
                expected_revision=observed["replay-b-pause"].revision,
            ),
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
        expected_revision: int = 0,
        first_submit: Callable[[], Awaitable[Mapping[str, Any]]] | None = None,
    ) -> CommandObservation:
        if workload.replay_b.session_id is None:
            raise ReplaySoakError(
                "SESSION_NOT_ASSIGNED",
                "replay-b must have a session id",
            )
        command = _step_command(
            workload.idempotent_command_id,
            expected_revision=expected_revision,
        )
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

    async def _wait_active(
        self, task: ReplayTask, *, attempts: int = 450
    ) -> ReplayTask:
        current = task
        for _ in range(attempts):
            current = await self._refresh(current)
            if current.state == "RUNNING" and current.session_id:
                return current
            if self._sleep is not None:
                await self._sleep(0.2)
        raise ReplaySoakError(
            "SESSION_NOT_ASSIGNED",
            f"{task.name} did not become a running assigned session",
            details={"state": current.state},
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
    from app.replay.broker.models import BrokerConfig, BrokerLimits, InstrumentFilters
    from app.replay.models import FeeModel, ReplaySessionConfig, SlippageModel

    replay_start_ms, replay_end_time_ms = aligned_replay_window(
        pin.start_event_time_ms,
        pin.end_event_time_ms,
    )
    horizon_ms = max(1, replay_end_time_ms - replay_start_ms + 1)
    config = ReplaySessionConfig(
        protocol=REPLAY_PROTOCOL,
        source_kind=SourceKind.AGG_TRADE,
        exchange="binance",
        market_type="futures",
        symbol="BTCUSDT",
        base_interval="1m",
        display_interval="1m",
        start_policy="manual",  # type: ignore[arg-type]
        requested_start_ms=replay_start_ms,
        warmup_bars=0,
        horizon_ms=horizon_ms,
        random_seed=7,
        quality_mode=QualityMode.EXACT,
        blind_mode=False,
        initial_equity="10000",
        quote_asset="USDT",
        execution_model="paper_linear_v1",  # type: ignore[arg-type]
        fee_model=FeeModel("2", "5"),
        slippage_model=SlippageModel("fixed_bps", "1"),  # type: ignore[arg-type]
        max_leverage="3",
        pause_on_controller_loss=True,
    )
    broker = BrokerConfig(
        initial_equity="10000",
        quote_asset="USDT",
        maker_bps="2",
        taker_bps="5",
        market_slippage_bps="1",
        initial_mark_price="100000.1",
        instrument=InstrumentFilters(
            price_tick="0.1",
            quantity_step="0.00000001",
            min_quantity="0.00000001",
            max_quantity="1000000000",
            min_notional="0.01",
            max_notional="30000",
            quote_step="0.00000001",
        ),
        limits=BrokerLimits(
            max_leverage="3",
            max_position_notional="30000",
            max_order_quantity="1000000000",
            max_open_orders=256,
            max_orders=4_096,
            max_fills=8_192,
            max_ledger_entries=65_536,
            max_warnings=4_096,
        ),
    )
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
            "replay_start_ms": replay_start_ms,
            "replay_end_time_ms": replay_end_time_ms,
            "config": config.to_dict(),
            "broker_config": broker.to_dict(),
        }
    )
    return body


def _step_command(command_id: str, *, expected_revision: int = 0) -> dict[str, object]:
    return _typed_command(
        command_id,
        CommandType.STEP,
        expected_revision=expected_revision,
        payload={"count": 1},
    )


def _typed_command(
    command_id: str,
    command_type: CommandType,
    *,
    expected_revision: int = 0,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, object]:
    return {
        "protocol": REPLAY_PROTOCOL,
        "command_id": command_id,
        "client_instance_id": "phase1ai-soak",
        "expected_revision": expected_revision,
        "type": command_type.value,
        "payload": dict(payload or {}),
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


def aligned_replay_window(
    start_ms: int, end_ms: int, *, interval: str = "1m"
) -> tuple[int, int]:
    from app.data_engine.interval_policy import (
        compute_bucket_end_ms,
        compute_bucket_start_ms,
        parse_interval_ms,
    )

    interval_ms = parse_interval_ms(interval) or 60_000
    aligned_start = compute_bucket_start_ms(start_ms, interval_ms, interval=interval)
    final_open = compute_bucket_start_ms(
        max(end_ms, start_ms), interval_ms, interval=interval
    )
    aligned_end = compute_bucket_end_ms(final_open, interval_ms, interval=interval) - 1
    if aligned_end < aligned_start:
        aligned_end = (
            compute_bucket_end_ms(aligned_start, interval_ms, interval=interval) - 1
        )
    return aligned_start, aligned_end


def pin_from_query_events(
    snapshot: Mapping[str, Any],
    events: list[Mapping[str, Any]],
) -> SnapshotPin:
    if not events:
        raise ReplaySoakError("INVALID_SNAPSHOT", "cold query returned no events")
    first = events[0]
    first_id = _agg_trade_id(first)
    start_ms = int(first.get("event_time_ms") or 0)
    last_id = first_id
    end_ms = start_ms
    # Pin only a consecutive agg_trade prefix. A sparse page's inclusive id
    # span would force the Worker to scan a much larger cold window.
    for event in events[1:]:
        trade_id = _agg_trade_id(event)
        if trade_id != last_id + 1:
            break
        last_id = trade_id
        end_ms = int(event.get("event_time_ms") or end_ms)
    row_count = last_id - first_id + 1
    return parse_snapshot_pin(
        {
            "snapshot": dict(snapshot),
            "pin": {
                "start_event_time_ms": start_ms,
                "end_event_time_ms": end_ms,
                "expected_first_agg_trade_id": first_id,
                "expected_last_agg_trade_id": last_id,
                "row_count": row_count,
            },
        }
    )


def _agg_trade_id(event: Mapping[str, Any]) -> int:
    payload = event.get("payload")
    if isinstance(payload, Mapping) and payload.get("agg_trade_id") is not None:
        return int(payload["agg_trade_id"])
    if event.get("sequence_start") is not None:
        return int(event["sequence_start"])
    raise ReplaySoakError("INVALID_SNAPSHOT", "query event is missing agg_trade_id")


def health_origin(health_url: str) -> str:
    parsed = urlsplit(health_url)
    return f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"


class HttpSoakReplayTransport:
    """Authenticated Server API + cold Query transport. Tokens are not in repr."""

    def __init__(
        self,
        *,
        api_origin: str,
        query_origin: str,
        api_token: str,
        query_token: str,
        organization_id: str,
        workspace_id: str,
        session: Any,
    ) -> None:
        self._api_origin = api_origin.rstrip("/")
        self._query_origin = query_origin.rstrip("/")
        self._api_token = api_token
        self._query_token = query_token
        self._organization_id = organization_id
        self._workspace_id = workspace_id
        self._session = session
        self._durable: dict[str, int] = {}
        self._results: dict[str, dict[str, Any]] = {}

    def __repr__(self) -> str:
        return (
            "HttpSoakReplayTransport("
            f"api_origin={self._api_origin!r}, "
            f"query_origin={self._query_origin!r})"
        )

    async def cold_query_snapshot(
        self, snapshot: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        body = {
            "snapshot": dict(snapshot),
            "stream": {
                "exchange": "binance",
                "market_type": "futures",
                "symbol": "BTCUSDT",
                "channel": "agg_trade",
            },
            "start_event_time_ms": 0,
            "end_event_time_ms": 4_102_444_800_000,
            "limit": 8,
            "preference": "cold",
            "organization_id": self._organization_id,
            "workspace_id": self._workspace_id,
        }
        async with self._session.post(
            f"{self._query_origin}/api/v1/server/market-events/query",
            json=body,
            headers={"Authorization": f"Bearer {self._query_token}"},
        ) as response:
            payload = await _response_json(response)
            if response.status != 200:
                raise ReplaySoakError(
                    "QUERY_NOT_COLD",
                    "cold Query Service request failed",
                    details={"status": response.status, "body": payload},
                )
        page = payload.get("page") if isinstance(payload, Mapping) else None
        snapshot_out = dict(snapshot)
        events: list[Any] = []
        if isinstance(page, Mapping):
            snap = page.get("snapshot")
            if isinstance(snap, Mapping):
                snapshot_out.update(dict(snap))
            raw_events = page.get("events")
            if isinstance(raw_events, list):
                events = raw_events
        backend = str(payload.get("backend") or "cold")
        return {
            **snapshot_out,
            "preference": "cold",
            "backend": backend,
            "events": events,
            "snapshot_version": int(snapshot_out.get("snapshot_version") or 0),
            "manifest_sha256": str(snapshot_out.get("manifest_sha256") or ""),
        }

    async def live_worker_count(self) -> int:
        async with self._session.get(f"{self._api_origin}/health/ready") as response:
            payload = await response.json()
        if response.status != 200:
            return 0
        return int(payload.get("workers") or 0)

    async def create_run(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        async with self._session.post(
            f"{self._api_origin}/api/v1/replay/runs",
            json=dict(payload),
            headers={"Authorization": f"Bearer {self._api_token}"},
        ) as response:
            body = await _response_json(response)
            if response.status != 200:
                raise ReplaySoakError(
                    "REPLAY_CREATE_FAILED",
                    "Server API refused replay create",
                    details={"status": response.status, "body": body},
                )
            return body

    async def get_run(self, run_id: str) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self._api_origin}/api/v1/replay/runs/{run_id}",
            headers={"Authorization": f"Bearer {self._api_token}"},
        ) as response:
            return await response.json()

    async def get_session(self, session_id: str) -> Mapping[str, Any]:
        async with self._session.get(
            f"{self._api_origin}/api/v1/replay/runs/session/{session_id}",
            headers={"Authorization": f"Bearer {self._api_token}"},
        ) as response:
            return await response.json()

    async def cancel_run(self, run_id: str) -> Mapping[str, Any]:
        async with self._session.delete(
            f"{self._api_origin}/api/v1/replay/runs/{run_id}",
            headers={"Authorization": f"Bearer {self._api_token}"},
        ) as response:
            return await response.json()

    async def submit_command(
        self, session_id: str, command: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        async with self._session.post(
            f"{self._api_origin}/api/v1/replay/runs/session/{session_id}/commands",
            json={
                key: value
                for key, value in command.items()
                if key != "simulate_timeout"
            },
            headers={"Authorization": f"Bearer {self._api_token}"},
        ) as response:
            body = await _response_json(response)
            if response.status != 200:
                raise ReplaySoakError(
                    "COMMAND_FAILED",
                    "Server API command failed",
                    details={"status": response.status, "body": body},
                )
        command_id = str(command["command_id"])
        self._results[command_id] = dict(body)
        self._durable[command_id] = 1
        if command.get("simulate_timeout"):
            raise TimeoutError("client timed out after the command was accepted")
        return body

    async def get_command_result(
        self, session_id: str, command_id: str
    ) -> Mapping[str, Any]:
        del session_id
        return self._results[command_id]

    def durable_command_count(self, command_id: str) -> int:
        return int(self._durable.get(command_id, 0))


async def assignment_worker_id(dsn: str, session_id: str) -> str | None:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            """
            SELECT worker_id
            FROM candlescope_replay_scheduler_assignment
            WHERE session_id = %s AND active IS TRUE
            """,
            (session_id,),
        )
        row = await cursor.fetchone()
    if row is None:
        return None
    return str(row[0])


def worker_role_from_id(worker_id: str) -> str:
    text = worker_id.strip().lower().replace("_", "-")
    if text == "worker-b" or text.endswith("-b"):
        return "worker_b"
    return "worker_a"


async def durable_command_count_from_store(dsn: str, command_id: str) -> int:
    import psycopg

    async with (
        await psycopg.AsyncConnection.connect(dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            """
            SELECT COUNT(*)
            FROM candlescope_replay_command_result
            WHERE command_id = %s
            """,
            (command_id,),
        )
        row = await cursor.fetchone()
    if row is None:
        return 0
    return int(row[0])


async def _response_json(response) -> Any:
    raw = await response.read()
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": text[:512]}


__all__ = [
    "FROZEN_IDEMPOTENT_COMMAND_ID",
    "CommandObservation",
    "HttpSoakReplayTransport",
    "PublicSoakReplayDriver",
    "ReplaySoakError",
    "ReplayTask",
    "SnapshotPin",
    "SoakReplayTransport",
    "SoakReplayWorkload",
    "aligned_replay_window",
    "assignment_worker_id",
    "build_run_payload",
    "durable_command_count_from_store",
    "health_origin",
    "parse_snapshot_pin",
    "pin_from_query_events",
    "worker_role_from_id",
]
