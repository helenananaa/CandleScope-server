"""Leased in-process ReplaySessionActor composition. Not a Worker or API."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from app.data_engine.interval_policy import (
    compute_bucket_end_ms,
    compute_bucket_start_ms,
    parse_interval_ms,
)
from app.replay.actor import ActorMutation, ActorSnapshot, ReplaySessionActor
from app.replay.bars.builder import assess_bar_builder_capability
from app.replay.broker.models import PAPER_LINEAR_EXECUTION_MODE, BrokerConfig
from app.replay.canonical import canonical_sha256
from app.replay.commands import CommandResult
from app.replay.constants import (
    REPLAY_CORE_VERSION,
    QualityMode,
    SourceKind,
    StartPolicy,
)
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.replay.models import ReplayCommand, ReplayCursor, ReplaySessionConfig
from app.replay.session_factory import AggTradeReplaySessionFactory
from app.server_contracts import MarketEventQuery
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseFencedError,
    ReplaySessionLeaseStore,
    ReplaySessionSnapshotConflictError,
)
from app.server_runtime.replay_leased import load_leased_server_snapshot
from app.server_runtime.replay_snapshot import (
    DEFAULT_MAX_QUERY_PAGES,
    DEFAULT_MAX_SCAN_ROWS,
    DEFAULT_QUERY_PAGE_LIMIT,
    DEFAULT_READER_PAGE_ROWS,
    ReplayServerSnapshotPin,
)

SERVER_REPLAY_SESSION_SCHEMA_VERSION = "candlescope.server-replay-session.v1"
SERVER_REPLAY_SESSION_CODE_VERSION = REPLAY_CORE_VERSION
FIRST_SLICE_EXCHANGE = "binance"
FIRST_SLICE_MARKET_TYPE = "futures"
FIRST_SLICE_SYMBOL = "BTCUSDT"
FIRST_SLICE_INTERVALS = frozenset(
    {
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
    }
)
_MAX_COMMAND_QUEUE = 4_096
_MAX_EVENT_BUFFER = 65_536
_MAX_EMIT_FPS = 120
_MAX_CONTROLLER_TTL_SECONDS = 3_600.0
_MAX_CHECKPOINT_EVENT_INTERVAL = 1_000_000
_MAX_CHECKPOINT_VIRTUAL_MS = 7 * 86_400_000
_MAX_COMMAND_RECORDS = 65_536
_MAX_RECENT_CHECKPOINTS = 1_024
_MAX_CLOSED_BARS = 10_000
_MAX_SHUTDOWN_TIMEOUT_SECONDS = 60.0


class ServerReplaySessionError(RuntimeError):
    """Composition or durable-session failure that is safe to surface."""


class ServerReplayMutationIntegrityError(ServerReplaySessionError):
    """Same revision/sequence was committed with a different hash."""


@dataclass(frozen=True, slots=True)
class ReplayCallerScope:
    """Verified organization/workspace used to read a durable session."""

    organization_id: str
    workspace_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "organization_id",
            normalize_organization_id(self.organization_id),
        )
        object.__setattr__(
            self,
            "workspace_id",
            normalize_workspace_id(self.workspace_id),
        )


@dataclass(frozen=True, slots=True)
class ServerReplaySessionRecord:
    session_id: str
    spec_public_ref: Mapping[str, object]
    checkpoint: bytes
    state: Mapping[str, object]
    closed: bool
    mutation_count: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "spec_public_ref",
            MappingProxyType(dict(self.spec_public_ref)),
        )
        object.__setattr__(self, "state", MappingProxyType(dict(self.state)))
        object.__setattr__(self, "checkpoint", bytes(self.checkpoint))


@dataclass(frozen=True, slots=True)
class ServerReplayMutationCommit:
    session_id: str
    command_id: str | None
    duplicate: bool
    mutation_hash: str
    state: Mapping[str, object]
    result: Mapping[str, object] | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "state", MappingProxyType(dict(self.state)))
        if self.result is not None:
            object.__setattr__(self, "result", MappingProxyType(dict(self.result)))


@dataclass(frozen=True, slots=True)
class ServerReplayRecovery:
    session_id: str
    checkpoint: bytes
    mutations: tuple[Mapping[str, object], ...]
    state: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "checkpoint", bytes(self.checkpoint))
        object.__setattr__(self, "state", MappingProxyType(dict(self.state)))
        object.__setattr__(
            self,
            "mutations",
            tuple(MappingProxyType(dict(item)) for item in self.mutations),
        )


@runtime_checkable
class ServerReplaySessionStore(Protocol):
    """Durable session/mutation port. Implementations fence in one operation."""

    async def create_session(
        self,
        lease: ReplaySessionLease,
        spec: ServerReplaySessionSpec,
        initial_checkpoint: bytes,
        state: Mapping[str, object],
    ) -> ServerReplaySessionRecord: ...

    async def commit_mutation(
        self,
        lease: ReplaySessionLease,
        mutation: ActorMutation,
    ) -> ServerReplayMutationCommit: ...

    async def load_recovery(
        self,
        session_id: str,
        lease: ReplaySessionLease,
    ) -> ServerReplayRecovery: ...

    async def read_session(
        self,
        session_id: str,
        caller_scope: ReplayCallerScope,
    ) -> ServerReplaySessionRecord: ...

    async def close_session(
        self,
        lease: ReplaySessionLease,
        terminal_state: Mapping[str, object],
    ) -> ServerReplaySessionRecord: ...


@dataclass(frozen=True, slots=True)
class ServerReplaySessionSpec:
    """Strict first-slice server session start contract."""

    lease: ReplaySessionLease
    pin: ReplayServerSnapshotPin
    config: ReplaySessionConfig
    broker_config: BrokerConfig
    replay_start_ms: int
    replay_end_time_ms: int
    command_queue_size: int
    event_buffer_size: int
    max_emit_fps: int
    controller_ttl_seconds: float
    checkpoint_event_interval: int
    checkpoint_virtual_ms: int
    max_command_records: int = 4_096
    max_recent_checkpoints: int = 32
    max_closed_bars: int = 10_000
    shutdown_timeout_seconds: float = 5.0
    schema_version: str = SERVER_REPLAY_SESSION_SCHEMA_VERSION
    code_version: str = SERVER_REPLAY_SESSION_CODE_VERSION
    execution_mode: str = PAPER_LINEAR_EXECUTION_MODE

    def __post_init__(self) -> None:
        if self.schema_version != SERVER_REPLAY_SESSION_SCHEMA_VERSION:
            raise ValueError("server replay session schema_version has drifted")
        if self.code_version != SERVER_REPLAY_SESSION_CODE_VERSION:
            raise ValueError("server replay session code_version has drifted")
        if not isinstance(self.lease, ReplaySessionLease):
            raise TypeError("lease must be a ReplaySessionLease")
        if not isinstance(self.pin, ReplayServerSnapshotPin):
            raise TypeError("pin must be a ReplayServerSnapshotPin")
        if not isinstance(self.config, ReplaySessionConfig):
            raise TypeError("config must be ReplaySessionConfig")
        if not isinstance(self.broker_config, BrokerConfig):
            raise TypeError("broker_config must be BrokerConfig")
        if self.execution_mode != PAPER_LINEAR_EXECUTION_MODE:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_EXECUTION_MODEL,
                "unsupported replay execution model",
            )
        object.__setattr__(
            self,
            "replay_start_ms",
            _non_negative_int(self.replay_start_ms, field="replay_start_ms"),
        )
        object.__setattr__(
            self,
            "replay_end_time_ms",
            _non_negative_int(self.replay_end_time_ms, field="replay_end_time_ms"),
        )
        object.__setattr__(
            self,
            "command_queue_size",
            _bounded_int(
                self.command_queue_size,
                field="command_queue_size",
                upper=_MAX_COMMAND_QUEUE,
            ),
        )
        object.__setattr__(
            self,
            "event_buffer_size",
            _bounded_int(
                self.event_buffer_size,
                field="event_buffer_size",
                upper=_MAX_EVENT_BUFFER,
            ),
        )
        object.__setattr__(
            self,
            "max_emit_fps",
            _bounded_int(self.max_emit_fps, field="max_emit_fps", upper=_MAX_EMIT_FPS),
        )
        object.__setattr__(
            self,
            "controller_ttl_seconds",
            _bounded_float(
                self.controller_ttl_seconds,
                field="controller_ttl_seconds",
                upper=_MAX_CONTROLLER_TTL_SECONDS,
            ),
        )
        object.__setattr__(
            self,
            "checkpoint_event_interval",
            _bounded_int(
                self.checkpoint_event_interval,
                field="checkpoint_event_interval",
                upper=_MAX_CHECKPOINT_EVENT_INTERVAL,
            ),
        )
        object.__setattr__(
            self,
            "checkpoint_virtual_ms",
            _bounded_int(
                self.checkpoint_virtual_ms,
                field="checkpoint_virtual_ms",
                upper=_MAX_CHECKPOINT_VIRTUAL_MS,
            ),
        )
        object.__setattr__(
            self,
            "max_command_records",
            _bounded_int(
                self.max_command_records,
                field="max_command_records",
                upper=_MAX_COMMAND_RECORDS,
            ),
        )
        object.__setattr__(
            self,
            "max_recent_checkpoints",
            _bounded_int(
                self.max_recent_checkpoints,
                field="max_recent_checkpoints",
                upper=_MAX_RECENT_CHECKPOINTS,
            ),
        )
        object.__setattr__(
            self,
            "max_closed_bars",
            _bounded_int(
                self.max_closed_bars,
                field="max_closed_bars",
                upper=_MAX_CLOSED_BARS,
            ),
        )
        object.__setattr__(
            self,
            "shutdown_timeout_seconds",
            _bounded_float(
                self.shutdown_timeout_seconds,
                field="shutdown_timeout_seconds",
                upper=_MAX_SHUTDOWN_TIMEOUT_SECONDS,
            ),
        )
        self._validate_first_slice()
        self._validate_pins()
        self._validate_time_range()

    def _validate_first_slice(self) -> None:
        config = self.config
        if config.source_kind is not SourceKind.AGG_TRADE:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "first-slice server replay only accepts source_kind=agg_trade",
            )
        if config.quality_mode is not QualityMode.EXACT:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "first-slice server replay only accepts quality_mode=exact",
            )
        if config.blind_mode is not False:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "first-slice server replay requires blind_mode=false",
            )
        if config.warmup_bars != 0:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "first-slice server replay requires warmup_bars=0",
            )
        if (
            config.exchange != FIRST_SLICE_EXCHANGE
            or config.market_type != FIRST_SLICE_MARKET_TYPE
            or config.symbol != FIRST_SLICE_SYMBOL
        ):
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "first-slice server replay only accepts binance futures BTCUSDT",
            )
        if (
            config.base_interval not in FIRST_SLICE_INTERVALS
            or config.display_interval not in FIRST_SLICE_INTERVALS
        ):
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_INTERVAL,
                "requested interval is outside the first-slice allowlist",
            )
        capability = assess_bar_builder_capability(
            config.base_interval,
            config.display_interval,
        )
        if not capability.enabled:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_INTERVAL,
                "base/display intervals cannot be reconstructed exactly",
                details={"reason": capability.reason},
            )
        if config.start_policy is not StartPolicy.MANUAL:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "first-slice server replay requires start_policy=manual",
            )
        if config.requested_start_ms != self.replay_start_ms:
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "requested_start_ms must equal replay_start_ms",
            )

    def _validate_pins(self) -> None:
        if self.lease.snapshot != self.pin.snapshot:
            raise ReplaySessionSnapshotConflictError(
                "replay session snapshot pin does not match the fenced lease"
            )
        stream = self.pin.stream
        if (
            self.config.exchange != stream.exchange
            or self.config.market_type != stream.market_type
            or self.config.symbol != stream.symbol
        ):
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "session config market identity does not match the snapshot pin",
            )
        if self.lease.session_id.strip() == "":
            raise ValueError("session_id must be a non-blank string")

    def _validate_time_range(self) -> None:
        interval_ms = parse_interval_ms(self.config.base_interval)
        if interval_ms is None or interval_ms <= 0:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_INTERVAL,
                "base interval is invalid",
            )
        aligned_start = compute_bucket_start_ms(
            self.replay_start_ms,
            interval_ms,
            interval=self.config.base_interval,
        )
        if aligned_start != self.replay_start_ms:
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "replay_start_ms must align to the base interval",
            )
        final_open = compute_bucket_start_ms(
            self.replay_end_time_ms,
            interval_ms,
            interval=self.config.base_interval,
        )
        expected_end = (
            compute_bucket_end_ms(
                final_open,
                interval_ms,
                interval=self.config.base_interval,
            )
            - 1
        )
        if expected_end != self.replay_end_time_ms:
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "replay_end_time_ms must be the last millisecond of a complete "
                "base interval",
            )
        if self.replay_end_time_ms < self.replay_start_ms:
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "replay range is inverted",
            )
        expected_horizon = self.replay_end_time_ms - self.replay_start_ms + 1
        if self.config.horizon_ms != expected_horizon:
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "horizon_ms must equal the inclusive aligned replay window",
            )
        if self.pin.start_event_time_ms < self.replay_start_ms:
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "snapshot pin starts before the aligned replay window",
            )
        if self.pin.end_event_time_ms > self.replay_end_time_ms:
            raise ReplayDomainError(
                ReplayErrorCode.DATASET_MISMATCH,
                "snapshot pin ends after the aligned replay window",
            )

    def to_public_ref(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "code_version": self.code_version,
            "lease": self.lease.to_public_ref(),
            "snapshot": self.pin.to_public_ref(),
            "config": self.config.to_dict(),
            "broker_config": self.broker_config.to_dict(),
            "replay_start_ms": self.replay_start_ms,
            "replay_end_time_ms": self.replay_end_time_ms,
            "command_queue_size": self.command_queue_size,
            "event_buffer_size": self.event_buffer_size,
            "max_emit_fps": self.max_emit_fps,
            "checkpoint_event_interval": self.checkpoint_event_interval,
            "checkpoint_virtual_ms": self.checkpoint_virtual_ms,
            "max_command_records": self.max_command_records,
            "max_recent_checkpoints": self.max_recent_checkpoints,
            "max_closed_bars": self.max_closed_bars,
        }

    def __repr__(self) -> str:
        return (
            "ServerReplaySessionSpec("
            f"session_id={self.lease.session_id!r}, "
            f"worker_id={self.lease.worker_id!r}, "
            f"fencing_epoch={self.lease.fencing_epoch}, "
            f"replay_start_ms={self.replay_start_ms}, "
            f"replay_end_time_ms={self.replay_end_time_ms})"
        )


class ServerReplaySession:
    """One leased in-process actor. Callers never receive the naked actor."""

    def __init__(
        self,
        spec: ServerReplaySessionSpec,
        *,
        lease_store: ReplaySessionLeaseStore,
        session_store: ServerReplaySessionStore,
        query: MarketEventQuery,
        query_page_limit: int = DEFAULT_QUERY_PAGE_LIMIT,
        page_rows: int = DEFAULT_READER_PAGE_ROWS,
        max_scan_rows: int = DEFAULT_MAX_SCAN_ROWS,
        max_query_pages: int = DEFAULT_MAX_QUERY_PAGES,
    ) -> None:
        if not isinstance(spec, ServerReplaySessionSpec):
            raise TypeError("spec must be ServerReplaySessionSpec")
        if not hasattr(lease_store, "require_active"):
            raise TypeError("lease_store must implement require_active")
        if not hasattr(session_store, "commit_mutation"):
            raise TypeError("session_store must implement ServerReplaySessionStore")
        self._spec = spec
        self._lease = spec.lease
        self._lease_store = lease_store
        self._session_store = session_store
        self._query = query
        self._query_page_limit = query_page_limit
        self._page_rows = page_rows
        self._max_scan_rows = max_scan_rows
        self._max_query_pages = max_query_pages
        self._actor: ReplaySessionActor | None = None
        self._registered = False
        self._accepting = False
        self._closed = False

    @property
    def session_id(self) -> str:
        return self._lease.session_id

    def to_public_ref(self) -> dict[str, object]:
        return {
            "schema_version": SERVER_REPLAY_SESSION_SCHEMA_VERSION,
            "session_id": self.session_id,
            "registered": self._registered,
            "closed": self._closed,
            "lease": self._lease.to_public_ref(),
            "snapshot": self._spec.pin.to_public_ref(),
        }

    def __repr__(self) -> str:
        return (
            "ServerReplaySession("
            f"session_id={self.session_id!r}, "
            f"worker_id={self._lease.worker_id!r}, "
            f"registered={self._registered}, "
            f"closed={self._closed})"
        )

    async def start(self) -> dict[str, object]:
        if self._closed:
            raise ServerReplaySessionError("server replay session is closed")
        if self._registered:
            return await self.public_snapshot()
        spec = self._spec
        loaded = await load_leased_server_snapshot(
            self._lease_store,
            spec.lease,
            self._query,
            spec.pin,
            query_page_limit=self._query_page_limit,
            page_rows=self._page_rows,
            max_scan_rows=self._max_scan_rows,
            max_query_pages=self._max_query_pages,
        )
        self._lease = loaded.lease
        factory = AggTradeReplaySessionFactory()
        actor = factory.create_actor(
            session_id=spec.lease.session_id,
            config=spec.config,
            broker_config=spec.broker_config,
            reader=loaded.reader,
            replay_start_ms=spec.replay_start_ms,
            replay_end_time_ms=spec.replay_end_time_ms,
            warmup_bars=(),
            command_queue_size=spec.command_queue_size,
            event_buffer_size=spec.event_buffer_size,
            max_emit_fps=spec.max_emit_fps,
            controller_ttl_seconds=spec.controller_ttl_seconds,
            checkpoint_event_interval=spec.checkpoint_event_interval,
            checkpoint_virtual_ms=spec.checkpoint_virtual_ms,
            mutation_hook=self._persist_mutation,
            max_closed_bars=spec.max_closed_bars,
            execution_mode=spec.execution_mode,
            max_command_records=spec.max_command_records,
            max_recent_checkpoints=spec.max_recent_checkpoints,
        )
        self._actor = actor
        try:
            await actor.start()
            initial_checkpoint = actor.latest_checkpoint_blob()
            if initial_checkpoint is None:
                raise ServerReplaySessionError(
                    "new replay actor did not create an initial checkpoint"
                )
            state = await actor.durable_state()
            await self._session_store.create_session(
                self._lease,
                spec,
                initial_checkpoint,
                state,
            )
        except BaseException:
            await abort_unregistered_actor(actor)
            self._actor = None
            raise
        self._registered = True
        self._accepting = True
        return await actor.public_snapshot()

    async def submit(self, command: ReplayCommand) -> CommandResult:
        actor = self._require_live_actor()
        return await actor.submit(command)

    async def snapshot(self) -> ActorSnapshot:
        actor = self._require_live_actor()
        return await actor.snapshot()

    async def public_snapshot(self) -> dict[str, object]:
        actor = self._require_live_actor()
        return await actor.public_snapshot()

    async def durable_state(self) -> dict[str, object]:
        actor = self._require_live_actor()
        return await actor.durable_state()

    async def subscribe(
        self,
        *,
        after_sequence: int | None,
        max_pending: int,
    ):
        actor = self._require_live_actor()
        return await actor.subscribe(
            after_sequence=after_sequence,
            max_pending=max_pending,
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._accepting = False
        actor = self._actor
        if actor is None:
            self._closed = True
            return
        if not self._registered:
            await abort_unregistered_actor(actor)
            self._actor = None
            self._closed = True
            return
        try:
            await actor.shutdown(step_timeout=self._spec.shutdown_timeout_seconds)
            terminal = await actor.durable_state()
            await self._session_store.close_session(self._lease, terminal)
        except ReplaySessionLeaseFencedError:
            await abort_unregistered_actor(actor)
        except ReplayDomainError as exc:
            await abort_unregistered_actor(actor)
            if exc.code is not ReplayErrorCode.PERSISTENCE_DEGRADED:
                raise
        finally:
            self._closed = True
            self._registered = False

    def _require_live_actor(self) -> ReplaySessionActor:
        if self._closed or not self._accepting or not self._registered:
            raise ServerReplaySessionError(
                "server replay session is not accepting commands"
            )
        actor = self._actor
        if actor is None:
            raise ServerReplaySessionError("server replay session has no actor")
        return actor

    async def _persist_mutation(self, mutation: ActorMutation) -> None:
        await self._session_store.commit_mutation(self._lease, mutation)


async def abort_unregistered_actor(actor: ReplaySessionActor) -> None:
    """Physically stop an unpublished actor without a shutdown mutation."""

    task = actor.task
    if task is None:
        return
    if not task.done():
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return
    except Exception:  # noqa: BLE001
        # Physical cancel of an unpublished actor is the expected rollback.
        return


def command_result_payload(result: CommandResult) -> dict[str, object]:
    cursor = result.cursor
    return {
        "command_id": result.command_id,
        "revision": result.revision,
        "sequence": result.sequence,
        "state": result.state.value,
        "state_hash": result.state_hash,
        "cursor": {
            "virtual_time_ms": cursor.virtual_time_ms,
            "source_sequence": cursor.source_sequence,
            "last_base_bar_open_ms": cursor.last_base_bar_open_ms,
            "last_trade_time_ms": cursor.last_trade_time_ms,
            "last_agg_trade_id": cursor.last_agg_trade_id,
            "at_end": cursor.at_end,
        },
        "data": dict(result.data),
    }


def command_result_from_payload(payload: Mapping[str, object]) -> CommandResult:
    cursor_payload = payload["cursor"]
    if not isinstance(cursor_payload, Mapping):
        raise TypeError("command result cursor must be an object")
    return CommandResult(
        command_id=str(payload["command_id"]),
        revision=int(payload["revision"]),
        sequence=int(payload["sequence"]),
        state=str(payload["state"]),  # type: ignore[arg-type]
        state_hash=str(payload["state_hash"]),
        cursor=ReplayCursor(
            virtual_time_ms=int(cursor_payload["virtual_time_ms"]),
            source_sequence=int(cursor_payload["source_sequence"]),
            last_base_bar_open_ms=_optional_int(
                cursor_payload.get("last_base_bar_open_ms")
            ),
            last_trade_time_ms=_optional_int(cursor_payload.get("last_trade_time_ms")),
            last_agg_trade_id=_optional_int(cursor_payload.get("last_agg_trade_id")),
            at_end=bool(cursor_payload["at_end"]),
        ),
        data=dict(payload["data"]) if isinstance(payload.get("data"), Mapping) else {},
    )


def mutation_integrity_hash(mutation: ActorMutation) -> str:
    checkpoint = mutation.checkpoint
    payload = {
        "kind": mutation.kind,
        "session_id": mutation.session_id,
        "session_state": dict(mutation.session_state),
        "checkpoint_sha256": None
        if checkpoint is None
        else hashlib.sha256(checkpoint).hexdigest(),
        "events": [event.to_dict() for event in mutation.events],
        "source_events": [dict(event) for event in mutation.source_events],
        "component_state": dict(mutation.component_state),
        "command": None if mutation.command is None else mutation.command.to_dict(),
        "result": None
        if mutation.result is None
        else command_result_payload(mutation.result),
        "error": None
        if mutation.error is None
        else {
            "code": mutation.error.code.value,
            "message": mutation.error.message,
        },
    }
    return canonical_sha256(payload)


def _bounded_int(value: object, *, field: str, upper: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 1 or value > upper:
        raise ValueError(f"{field} must be between 1 and {upper}")
    return value


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _bounded_float(value: object, *, field: str, upper: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a number")
    number = float(value)
    if not (0.0 < number <= upper):
        raise ValueError(f"{field} must be between 0 exclusive and {upper}")
    return number


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("optional integer field is invalid")
    return value


__all__ = [
    "FIRST_SLICE_EXCHANGE",
    "FIRST_SLICE_INTERVALS",
    "FIRST_SLICE_MARKET_TYPE",
    "FIRST_SLICE_SYMBOL",
    "SERVER_REPLAY_SESSION_CODE_VERSION",
    "SERVER_REPLAY_SESSION_SCHEMA_VERSION",
    "ReplayCallerScope",
    "ServerReplayMutationCommit",
    "ServerReplayMutationIntegrityError",
    "ServerReplayRecovery",
    "ServerReplaySession",
    "ServerReplaySessionError",
    "ServerReplaySessionRecord",
    "ServerReplaySessionSpec",
    "ServerReplaySessionStore",
    "abort_unregistered_actor",
    "command_result_from_payload",
    "command_result_payload",
    "mutation_integrity_hash",
]
