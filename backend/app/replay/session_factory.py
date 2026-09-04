"""Storage-free aggTrade ReplaySessionActor factory.

Personal ReplayService and the server session composition both construct
actors through this type. It must not import app.server_runtime, SQLite, or
ReplaySettings.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence

from .actor import ActorMutation, ActorRecoveryTarget, ReplaySessionActor
from .bars.trade_builder import TradeReplayBarBuilder
from .broker.execution import ConservativeBarBroker
from .broker.models import PAPER_LINEAR_EXECUTION_MODE, BrokerConfig
from .constants import SourceKind
from .dataset import ReplayBar
from .errors import ReplayDomainError, ReplayErrorCode
from .models import ReplaySessionConfig
from .sources.trade_reader import ReplayTradePageReader
from .sources.trade_source import TradeReplaySource

AGG_TRADE_SESSION_FACTORY_SCHEMA_VERSION = (
    "candlescope.agg-trade-replay-session-factory.v1"
)


class AggTradeReplaySessionFactory:
    """Build a ConservativeBarBroker and ReplaySessionActor for aggTrade."""

    schema_version = AGG_TRADE_SESSION_FACTORY_SCHEMA_VERSION

    def create_broker(
        self,
        config: ReplaySessionConfig,
        broker_config: BrokerConfig,
        *,
        replay_start_ms: int,
        replay_end_time_ms: int,
        warmup_bars: Sequence[ReplayBar] = (),
        max_closed_bars: int,
        execution_mode: str = PAPER_LINEAR_EXECUTION_MODE,
    ) -> ConservativeBarBroker:
        self._require_agg_trade(config)
        if not isinstance(broker_config, BrokerConfig):
            raise TypeError("broker_config must be BrokerConfig")
        if (
            isinstance(max_closed_bars, bool)
            or not isinstance(max_closed_bars, int)
            or max_closed_bars < 1
        ):
            raise ValueError("max_closed_bars must be a positive integer")
        builder = TradeReplayBarBuilder(
            base_interval=config.base_interval,
            display_interval=config.display_interval,
            replay_start_ms=replay_start_ms,
            replay_end_time_ms=replay_end_time_ms,
            warmup_bars=warmup_bars,
            max_closed_bars=max_closed_bars,
        )
        return ConservativeBarBroker(
            config=broker_config,
            bar_builder=builder,
            execution_mode=execution_mode,
        )

    def create_actor(
        self,
        *,
        session_id: str,
        config: ReplaySessionConfig,
        broker_config: BrokerConfig | None = None,
        reducer: ConservativeBarBroker | None = None,
        reader: ReplayTradePageReader | None = None,
        source_factory: Callable[[], TradeReplaySource] | None = None,
        replay_start_ms: int,
        replay_end_time_ms: int,
        warmup_bars: Sequence[ReplayBar] = (),
        command_queue_size: int,
        event_buffer_size: int,
        max_emit_fps: int,
        controller_ttl_seconds: float,
        checkpoint_event_interval: int,
        checkpoint_virtual_ms: int,
        restore_checkpoint: bytes | None = None,
        retained_checkpoints: Sequence[tuple[bytes, bool]] = (),
        recovery_target: ActorRecoveryTarget | None = None,
        mutation_hook: Callable[[ActorMutation], Awaitable[None]] | None = None,
        time_offset_ms: int = 0,
        max_closed_bars: int | None = None,
        execution_mode: str = PAPER_LINEAR_EXECUTION_MODE,
        max_command_records: int = 4_096,
        max_recent_checkpoints: int = 32,
    ) -> ReplaySessionActor:
        self._require_agg_trade(config)
        if source_factory is None:
            if reader is None:
                raise TypeError("reader or source_factory is required")
            source_factory = self._reader_source_factory(
                reader,
                time_offset_ms=time_offset_ms,
                blind_mode=config.blind_mode,
            )
        elif reader is not None:
            raise TypeError("pass either reader or source_factory, not both")
        if reducer is None:
            if broker_config is None or max_closed_bars is None:
                raise TypeError(
                    "broker_config and max_closed_bars are required when "
                    "reducer is omitted"
                )
            reducer = self.create_broker(
                config,
                broker_config,
                replay_start_ms=replay_start_ms,
                replay_end_time_ms=replay_end_time_ms,
                warmup_bars=warmup_bars,
                max_closed_bars=max_closed_bars,
                execution_mode=execution_mode,
            )
        elif broker_config is not None:
            raise TypeError("pass either reducer or broker_config, not both")
        return ReplaySessionActor(
            session_id=session_id,
            config=config,
            source_factory=source_factory,
            initial_virtual_time_ms=replay_start_ms,
            command_queue_size=command_queue_size,
            event_buffer_size=event_buffer_size,
            max_emit_fps=max_emit_fps,
            controller_ttl_seconds=controller_ttl_seconds,
            checkpoint_event_interval=checkpoint_event_interval,
            checkpoint_virtual_ms=checkpoint_virtual_ms,
            reducer=reducer,
            restore_checkpoint=restore_checkpoint,
            retained_checkpoints=retained_checkpoints,
            mutation_hook=mutation_hook,
            recovery_target=recovery_target,
            max_command_records=max_command_records,
            max_recent_checkpoints=max_recent_checkpoints,
        )

    @staticmethod
    def _require_agg_trade(config: ReplaySessionConfig) -> None:
        if not isinstance(config, ReplaySessionConfig):
            raise TypeError("config must be ReplaySessionConfig")
        if config.source_kind is not SourceKind.AGG_TRADE:
            raise ReplayDomainError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "aggTrade session factory only accepts source_kind=agg_trade",
            )

    @staticmethod
    def _reader_source_factory(
        reader: ReplayTradePageReader,
        *,
        time_offset_ms: int,
        blind_mode: bool,
    ) -> Callable[[], TradeReplaySource]:
        def source_factory() -> TradeReplaySource:
            return TradeReplaySource(
                reader,
                time_offset_ms=time_offset_ms,
                blind_mode=blind_mode,
            )

        return source_factory


__all__ = [
    "AGG_TRADE_SESSION_FACTORY_SCHEMA_VERSION",
    "AggTradeReplaySessionFactory",
]
