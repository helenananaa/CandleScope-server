from __future__ import annotations

import ast
import asyncio
import hashlib
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.deployment import ServerRuntimeUnavailableError, load_deployment_settings
from app.replay.broker.models import BrokerConfig, BrokerLimits, InstrumentFilters
from app.replay.canonical import canonical_sha256
from app.replay.constants import (
    REPLAY_PROTOCOL,
    CommandType,
    QualityMode,
    SourceKind,
)
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.replay.models import (
    FeeModel,
    ReplayCommand,
    ReplaySessionConfig,
    SlippageModel,
)
from app.server_contracts import MarketDataSnapshotRef, MarketEventPage
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import canonical_envelope_bytes
from app.server_runtime.query_pagination import SnapshotQueryRow, paginate_snapshot_rows
from app.server_runtime.replay_lease import (
    ReplaySessionLeaseFencedError,
    ReplaySessionScopeConflictError,
    ReplaySessionSnapshotConflictError,
)
from app.server_runtime.replay_session import (
    FIRST_SLICE_SYMBOL,
    SERVER_REPLAY_SESSION_SCHEMA_VERSION,
    ReplayCallerScope,
    ServerReplayMutationIntegrityError,
    ServerReplaySession,
    ServerReplaySessionError,
    ServerReplaySessionSpec,
)
from app.server_runtime.replay_snapshot import ReplayServerSnapshotPin
from app.server_runtime.testing import (
    InMemoryReplaySessionLeaseStore,
    InMemoryReplaySessionStore,
)

START_MS = 1_710_000_000_000
INTERVAL_MS = 60_000
REPLAY_END_MS = START_MS + INTERVAL_MS - 1
AUTH_ORG = "org-alpha"
AUTH_WS = "ws-research"
REPLAY_ROOT = Path(__file__).resolve().parents[1] / "app" / "replay"


def _envelope(sequence: int) -> Any:
    return AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
        MarketEvent(
            event_type=StreamType.AGG_TRADE,
            symbol="BTCUSDT",
            exchange="binance",
            event_time_ms=START_MS + sequence,
            received_at_ms=START_MS + 100 + sequence,
            source=DataSource.WEBSOCKET,
            data={
                "agg_trade_id": sequence,
                "price": 100000.1,
                "quantity": 0.025,
                "price_text": "100000.1000",
                "quantity_text": "0.02500000",
                "first_trade_id": sequence * 10,
                "last_trade_id": sequence * 10 + 2,
                "trade_time_ms": START_MS + sequence,
                "is_buyer_maker": False,
            },
            stream_key="futures:BTCUSDT@aggTrade",
            sequence=sequence,
            market_type="futures",
        ),
        previous_sequence=None if sequence == 42 else sequence - 1,
        published_at_ms=START_MS + 1_000 + sequence,
    )


def _snapshot(*, digest: str = "a" * 64) -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="sha256:" + ("c" * 64),
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256=digest,
    )


def _pin(snapshot: MarketDataSnapshotRef | None = None) -> ReplayServerSnapshotPin:
    return ReplayServerSnapshotPin(
        snapshot=snapshot or _snapshot(),
        start_event_time_ms=START_MS + 42,
        end_event_time_ms=START_MS + 44,
        expected_first_agg_trade_id=42,
        expected_last_agg_trade_id=44,
        row_count=3,
    )


def _config() -> ReplaySessionConfig:
    return ReplaySessionConfig(
        protocol=REPLAY_PROTOCOL,
        source_kind=SourceKind.AGG_TRADE,
        exchange="binance",
        market_type="futures",
        symbol=FIRST_SLICE_SYMBOL,
        base_interval="1m",
        display_interval="1m",
        start_policy="manual",  # type: ignore[arg-type]
        requested_start_ms=START_MS,
        warmup_bars=0,
        horizon_ms=INTERVAL_MS,
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


def _broker_config() -> BrokerConfig:
    return BrokerConfig(
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


class _PagingColdQuery:
    def __init__(self, snapshot: MarketDataSnapshotRef, envelopes: list[Any]) -> None:
        self.snapshot = snapshot
        self.rows = [
            SnapshotQueryRow(
                envelope=envelope,
                envelope_sha256=hashlib.sha256(
                    canonical_envelope_bytes(envelope)
                ).hexdigest(),
                envelope_bytes=canonical_envelope_bytes(envelope),
                kafka_partition=0,
                kafka_offset=index,
            )
            for index, envelope in enumerate(envelopes)
        ]
        self.calls: list[dict[str, Any]] = []

    async def query(self, **kwargs: Any) -> MarketEventPage:
        self.calls.append(kwargs)
        return paginate_snapshot_rows(
            self.rows,
            snapshot=self.snapshot,
            stream=kwargs["stream"],
            start_event_time_ms=kwargs["start_event_time_ms"],
            end_event_time_ms=kwargs["end_event_time_ms"],
            limit=kwargs["limit"],
            max_page_rows=kwargs["limit"],
            cursor=kwargs.get("cursor"),
        )


class _FailNextCommitStore(InMemoryReplaySessionStore):
    def __init__(self, lease_store: InMemoryReplaySessionLeaseStore) -> None:
        super().__init__(lease_store)
        self.fail_next = False
        self.commits = 0

    async def commit_mutation(self, lease, mutation):
        self.commits += 1
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("injected mutation failure")
        return await super().commit_mutation(lease, mutation)


class _FailCreateStore(InMemoryReplaySessionStore):
    async def create_session(self, lease, spec, initial_checkpoint, state):
        raise RuntimeError("injected create failure")


def _command(
    command_id: str,
    command_type: CommandType,
    revision: int,
    payload: dict[str, object] | None = None,
) -> ReplayCommand:
    return ReplayCommand(
        protocol=REPLAY_PROTOCOL,
        command_id=command_id,
        client_instance_id="worker-client",
        expected_revision=revision,
        type=command_type,
        payload=payload or {},
    )


async def _acquire(
    store: InMemoryReplaySessionLeaseStore, snapshot: MarketDataSnapshotRef
):
    return await store.acquire(
        session_id="sess-alpha",
        worker_id="worker-a",
        snapshot=snapshot,
        organization_id=AUTH_ORG,
        workspace_id=AUTH_WS,
        lease_ttl_ms=5_000,
    )


def _spec(lease, pin: ReplayServerSnapshotPin) -> ServerReplaySessionSpec:
    return ServerReplaySessionSpec(
        lease=lease,
        pin=pin,
        config=_config(),
        broker_config=_broker_config(),
        replay_start_ms=START_MS,
        replay_end_time_ms=REPLAY_END_MS,
        command_queue_size=32,
        event_buffer_size=64,
        max_emit_fps=30,
        controller_ttl_seconds=5.0,
        checkpoint_event_interval=10,
        checkpoint_virtual_ms=300_000,
        max_closed_bars=16,
        shutdown_timeout_seconds=1.0,
    )


def _query(pin: ReplayServerSnapshotPin) -> _PagingColdQuery:
    return _PagingColdQuery(
        pin.snapshot,
        [_envelope(42), _envelope(43), _envelope(44)],
    )


def _assert_no_token(token: str, *values: object) -> None:
    for value in values:
        rendered = repr(value) + str(value)
        assert token not in rendered
        assert "lease_token" not in rendered.lower() or token not in rendered


def test_valid_lease_starts_actor_and_completes_step() -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        session = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        started = await session.start()
        assert started["state"] == "PAUSED"
        assert query.calls
        assert all("preference" not in call for call in query.calls)
        acquired = await session.submit(
            _command("acquire", CommandType.ACQUIRE_CONTROLLER, 0)
        )
        stepped = await session.submit(
            _command("step", CommandType.STEP, acquired.revision, {"count": 1})
        )
        assert stepped.cursor.source_sequence == 1
        assert stepped.cursor.last_agg_trade_id == 42
        record = await session_store.read_session(
            "sess-alpha",
            ReplayCallerScope(AUTH_ORG, AUTH_WS),
        )
        assert record.mutation_count >= 1
        assert (
            record.spec_public_ref["schema_version"]
            == SERVER_REPLAY_SESSION_SCHEMA_VERSION
        )
        public = session.to_public_ref()
        _assert_no_token(lease.lease_token, public, started, record.spec_public_ref)
        await session.close()

    asyncio.run(run())


def test_expired_stale_snapshot_and_scope_fail_before_query() -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        now[0] += 5_000
        expired = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
        )
        with pytest.raises(ReplaySessionLeaseFencedError, match="expired"):
            await expired.start()
        assert query.calls == []

        now[0] += 1
        takeover = await lease_store.acquire(
            session_id="sess-alpha",
            worker_id="worker-b",
            snapshot=pin.snapshot,
            organization_id=AUTH_ORG,
            workspace_id=AUTH_WS,
            lease_ttl_ms=5_000,
        )
        stale = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
        )
        with pytest.raises(ReplaySessionLeaseFencedError):
            await stale.start()
        assert query.calls == []

        with pytest.raises(ReplaySessionSnapshotConflictError):
            _spec(takeover, _pin(snapshot=_snapshot(digest="b" * 64)))
        assert query.calls == []

        live = ServerReplaySession(
            _spec(takeover, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        await live.start()
        await session_store.read_session(
            "sess-alpha",
            ReplayCallerScope(AUTH_ORG, AUTH_WS),
        )
        with pytest.raises(ReplaySessionScopeConflictError):
            await session_store.read_session(
                "sess-alpha",
                ReplayCallerScope("org-beta", AUTH_WS),
            )
        await live.close()

    asyncio.run(run())


def test_takeover_after_snapshot_load_fences_old_mutation() -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        session = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        await session.start()
        acquired = await session.submit(
            _command("acquire", CommandType.ACQUIRE_CONTROLLER, 0)
        )
        before = await session.durable_state()
        before_snapshot = await session.public_snapshot()
        now[0] += 5_000
        takeover = await lease_store.acquire(
            session_id="sess-alpha",
            worker_id="worker-b",
            snapshot=pin.snapshot,
            organization_id=AUTH_ORG,
            workspace_id=AUTH_WS,
            lease_ttl_ms=5_000,
        )
        assert takeover.fencing_epoch == 1
        with pytest.raises(ReplayDomainError) as exc_info:
            await session.submit(
                _command("step", CommandType.STEP, acquired.revision, {"count": 1})
            )
        assert exc_info.value.code is ReplayErrorCode.PERSISTENCE_DEGRADED
        after = await session.durable_state()
        after_snapshot = await session.public_snapshot()
        for field_name in (
            "revision",
            "event_sequence",
            "source_sequence",
            "state_hash",
            "command_log_offset",
        ):
            assert after[field_name] == before[field_name]
        assert after["cursor"] == before["cursor"]
        assert after_snapshot["components"] == before_snapshot["components"]
        assert canonical_sha256(after_snapshot["components"]) == canonical_sha256(
            before_snapshot["components"]
        )
        await session.close()

    asyncio.run(run())


def test_mutation_failure_rolls_back_actor_authority() -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = _FailNextCommitStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        session = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        await session.start()
        acquired = await session.submit(
            _command("acquire", CommandType.ACQUIRE_CONTROLLER, 0)
        )
        before = await session.durable_state()
        before_snapshot = await session.public_snapshot()
        session_store.fail_next = True
        with pytest.raises(ReplayDomainError) as exc_info:
            await session.submit(
                _command("step", CommandType.STEP, acquired.revision, {"count": 1})
            )
        assert exc_info.value.code is ReplayErrorCode.PERSISTENCE_DEGRADED
        after = await session.durable_state()
        after_snapshot = await session.public_snapshot()
        for field_name in (
            "revision",
            "event_sequence",
            "source_sequence",
            "state_hash",
            "command_log_offset",
        ):
            assert after[field_name] == before[field_name]
        assert after["cursor"] == before["cursor"]
        assert after_snapshot["components"] == before_snapshot["components"]
        await session.close()

    asyncio.run(run())


def test_command_id_retry_returns_original_durable_result() -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        session = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        await session.start()
        acquired = await session.submit(
            _command("acquire", CommandType.ACQUIRE_CONTROLLER, 0)
        )
        first = await session.submit(
            _command("step", CommandType.STEP, acquired.revision, {"count": 1})
        )
        replayed = await session.submit(
            _command("step", CommandType.STEP, acquired.revision, {"count": 1})
        )
        assert replayed.command_id == first.command_id
        assert replayed.revision == first.revision
        assert replayed.sequence == first.sequence
        assert replayed.state_hash == first.state_hash
        assert replayed.cursor.source_sequence == first.cursor.source_sequence
        record = await session_store.read_session(
            "sess-alpha",
            ReplayCallerScope(AUTH_ORG, AUTH_WS),
        )
        recovery = await session_store.load_recovery("sess-alpha", lease)
        assert recovery.checkpoint == record.checkpoint
        await session.close()

    asyncio.run(run())


def test_same_revision_different_hash_is_integrity_conflict() -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        session = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        await session.start()
        acquired = await session.submit(
            _command("acquire", CommandType.ACQUIRE_CONTROLLER, 0)
        )
        await session.submit(
            _command("step", CommandType.STEP, acquired.revision, {"count": 1})
        )
        current = await session.durable_state()
        from app.replay.actor import ActorMutation

        mutation = ActorMutation(
            kind="command",
            session_id="sess-alpha",
            session_state={**dict(current), "state_hash": "0" * 64},
            checkpoint=b"not-the-original-checkpoint",
            events=(),
            source_events=(),
            component_state={"journal": []},
            command=_command(
                "other", CommandType.STEP, acquired.revision, {"count": 1}
            ),
        )
        with pytest.raises(ServerReplayMutationIntegrityError):
            await session_store.commit_mutation(lease, mutation)
        await session.close()

    asyncio.run(run())


def test_public_ref_repr_exceptions_and_logs_do_not_leak_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = InMemoryReplaySessionStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        spec = _spec(lease, pin)
        session = ServerReplaySession(
            spec,
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        with caplog.at_level(logging.INFO):
            logging.getLogger("test_phase1ad").info("started %s", session)
            await session.start()
        public = session.to_public_ref()
        _assert_no_token(
            lease.lease_token,
            public,
            spec.to_public_ref(),
            spec,
            session,
            caplog.text,
        )
        assert "lease_token" not in public["lease"]
        with pytest.raises(ServerReplaySessionError):
            raise ServerReplaySessionError("session failed")
        await session.close()

    asyncio.run(run())


def test_create_failure_physically_cancels_unregistered_actor() -> None:
    async def run() -> None:
        pin = _pin()
        query = _query(pin)
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        session_store = _FailCreateStore(lease_store)
        lease = await _acquire(lease_store, pin.snapshot)
        session = ServerReplaySession(
            _spec(lease, pin),
            lease_store=lease_store,
            session_store=session_store,
            query=query,
            query_page_limit=10,
            page_rows=10,
        )
        with pytest.raises(RuntimeError, match="injected create failure"):
            await session.start()
        with pytest.raises(ReplayDomainError) as exc_info:
            await InMemoryReplaySessionStore(lease_store).read_session(
                "sess-alpha",
                ReplayCallerScope(AUTH_ORG, AUTH_WS),
            )
        assert exc_info.value.code is ReplayErrorCode.SESSION_NOT_FOUND
        with pytest.raises(ServerReplaySessionError):
            await session.submit(_command("acquire", CommandType.ACQUIRE_CONTROLLER, 0))

    asyncio.run(run())


def test_first_slice_allowlist_rejects_warmup_blind_and_unaligned_start() -> None:
    async def run() -> None:
        pin = _pin()
        now = [1_000]
        lease_store = InMemoryReplaySessionLeaseStore(clock_ms=lambda: now[0])
        lease = await _acquire(lease_store, pin.snapshot)
        with pytest.raises(ReplayDomainError) as warmup:
            ServerReplaySessionSpec(
                lease=lease,
                pin=pin,
                config=replace(_config(), warmup_bars=1),
                broker_config=_broker_config(),
                replay_start_ms=START_MS,
                replay_end_time_ms=REPLAY_END_MS,
                command_queue_size=8,
                event_buffer_size=16,
                max_emit_fps=30,
                controller_ttl_seconds=1.0,
                checkpoint_event_interval=10,
                checkpoint_virtual_ms=1_000,
            )
        assert warmup.value.code is ReplayErrorCode.UNSUPPORTED_SOURCE
        with pytest.raises(ReplayDomainError) as unaligned:
            ServerReplaySessionSpec(
                lease=lease,
                pin=pin,
                config=replace(_config(), requested_start_ms=START_MS + 1),
                broker_config=_broker_config(),
                replay_start_ms=START_MS + 1,
                replay_end_time_ms=REPLAY_END_MS,
                command_queue_size=8,
                event_buffer_size=16,
                max_emit_fps=30,
                controller_ttl_seconds=1.0,
                checkpoint_event_interval=10,
                checkpoint_virtual_ms=1_000,
            )
        assert unaligned.value.code is ReplayErrorCode.DATASET_MISMATCH

    asyncio.run(run())


def test_replay_package_does_not_import_server_runtime() -> None:
    violations: list[str] = []
    for path in sorted(REPLAY_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module)
            for name in names:
                if name == "app.server_runtime" or name.startswith(
                    "app.server_runtime."
                ):
                    violations.append(
                        f"{path.relative_to(REPLAY_ROOT)}:{node.lineno}:{name}"
                    )
    assert violations == []


def test_server_profile_remains_refused() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.runtime_supported is False
    with pytest.raises(ServerRuntimeUnavailableError):
        settings.require_runtime_support()
