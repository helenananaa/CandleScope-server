"""In-process Phase 1R vertical rehearsal: archive, cold query, replay, reconcile."""

from __future__ import annotations

import time

from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.data_engine.market_data import MarketChannel, MarketStreamKey
from app.replay.sources.trade_source import TradeReplaySource
from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.chain_reconciliation import (
    ChainObservation,
    ChainReconciliation,
    reconcile_chain,
)
from app.server_runtime.health import CollectorHealth, CollectorState
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.projection import (
    KafkaMarketEventRecord,
    decode_kafka_market_event,
)
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    PHASE1B_PARTITION_KEY,
    canonical_envelope_bytes,
)
from app.server_runtime.replay_snapshot import (
    ReplayServerSnapshotPin,
    ServerSnapshotTradeReader,
)
from app.server_runtime.storage.parquet_archive import (
    ImmutableParquetMarketEventArchive,
    ParquetMarketEventQuery,
)
from app.server_runtime.testing import (
    InMemoryImmutableObjectStore,
    InMemoryMarketEventProjector,
)
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)

SOAK_RESULT_SCHEMA_VERSION = "candlescope.server-phase1r-soak-result.v1"
SOAK_MODE_REHEARSAL = "rehearsal"
PUBLIC_SOAK_DURATION_MS = 24 * 60 * 60 * 1_000
PUBLIC_SOAK_ENV = "CANDLESCOPE_PHASE1R_ALLOW_PUBLIC_SOAK"
PUBLIC_SOAK_NOT_DELIVERED = "PUBLIC_SOAK_SUPERVISOR_NOT_DELIVERED"
_FROZEN_STREAM = MarketStreamKey.build(
    "binance",
    "futures",
    "BTCUSDT",
    MarketChannel.AGG_TRADE,
)
_START_MS = 1_700_000_000_000
_FIRST_SEQUENCE = 42
_EVENT_COUNT = 4


async def run_phase1r_rehearsal(*, clock_ms: int | None = None) -> dict[str, object]:
    """Archive one exact segment, load Phase 1Q replay, then reconcile caught-up."""

    started_at_ms = _clock_ms(clock_ms)
    records = _records()
    store = InMemoryImmutableObjectStore()
    archive = ImmutableParquetMarketEventArchive(
        object_store=store,
        segment_event_count=_EVENT_COUNT,
    )
    await archive.initialize()
    first_commit = await archive.append_records(records, data_epoch="phase1r-rehearsal")
    projector = InMemoryMarketEventProjector()
    await projector.start()
    first_batch = await projector.apply_batch(records)
    replay_commit = await archive.append_records(
        records, data_epoch="phase1r-rehearsal"
    )
    second_batch = await projector.apply_batch(records)
    if first_commit.commit.snapshot != replay_commit.commit.snapshot:
        raise RuntimeError("idempotent re-archive changed the immutable snapshot")
    if (
        first_batch.inserted_count != _EVENT_COUNT
        or second_batch.duplicate_count != _EVENT_COUNT
    ):
        raise RuntimeError("in-memory projector restart replay is not idempotent")

    snapshot = first_commit.commit.snapshot
    query = ParquetMarketEventQuery(archive=archive, max_page_rows=_EVENT_COUNT)
    await query.start()
    try:
        pin = ReplayServerSnapshotPin(
            snapshot=snapshot,
            stream=_FROZEN_STREAM,
            start_event_time_ms=_START_MS + _FIRST_SEQUENCE,
            end_event_time_ms=_START_MS + _FIRST_SEQUENCE + _EVENT_COUNT - 1,
            expected_first_agg_trade_id=_FIRST_SEQUENCE,
            expected_last_agg_trade_id=_FIRST_SEQUENCE + _EVENT_COUNT - 1,
            row_count=_EVENT_COUNT,
        )
        reader = await ServerSnapshotTradeReader.load(
            query,
            pin,
            query_page_limit=_EVENT_COUNT,
            page_rows=_EVENT_COUNT,
        )
    finally:
        await query.stop()
    source = TradeReplaySource(reader)
    trades = tuple(
        item
        for item in (source.next() for _ in range(_EVENT_COUNT))
        if item is not None
    )
    if tuple(item.agg_trade_id for item in trades) != tuple(
        _FIRST_SEQUENCE + index for index in range(_EVENT_COUNT)
    ):
        raise RuntimeError("Phase 1Q replay did not emit the frozen rehearsal span")
    if not source.exhausted():
        raise RuntimeError("Phase 1Q replay did not exhaust the frozen rehearsal span")

    last_offset = records[-1].offset
    last_sequence = _FIRST_SEQUENCE + _EVENT_COUNT - 1
    observation = ChainObservation(
        collector=_collector_health(
            last_offset=last_offset,
            last_sequence=last_sequence,
            started_at_ms=started_at_ms,
        ),
        writer=_writer_health(
            committed_next_offset=last_offset + 1,
            inserted_events=first_batch.inserted_count,
            duplicate_events=first_batch.duplicate_count + second_batch.duplicate_count,
            conflict_events=first_batch.conflict_count + second_batch.conflict_count,
            batches_committed=2,
            started_at_ms=started_at_ms,
        ),
        archive=_archive_health(
            snapshot=snapshot,
            committed_next_offset=last_offset + 1,
            events_archived=_EVENT_COUNT,
            started_at_ms=started_at_ms,
        ),
        query_snapshot=snapshot,
        query_sequences=tuple(item.agg_trade_id for item in trades),
        replay_snapshot=pin.snapshot,
        replay_first_id=pin.expected_first_agg_trade_id,
        replay_last_id=pin.expected_last_agg_trade_id,
        replay_row_count=pin.row_count,
    )
    reconciliation = reconcile_chain(observation, require_caught_up=True)
    finished_at_ms = _clock_ms(None if clock_ms is None else clock_ms + 1)
    return _rehearsal_result(
        reconciliation=reconciliation,
        snapshot_pin=pin.to_public_ref(),
        started_at_ms=started_at_ms,
        finished_at_ms=finished_at_ms,
        replay_ids=tuple(item.agg_trade_id for item in trades),
        object_replayed=not replay_commit.object_created,
    )


def public_soak_refusal(*, allow_public_soak: bool) -> dict[str, object]:
    """Refuse to claim a 24h public soak; the supervisor is not delivered."""

    code = (
        PUBLIC_SOAK_NOT_DELIVERED if allow_public_soak else "PUBLIC_SOAK_NOT_AUTHORIZED"
    )
    message = (
        "Phase 1R defines the 24h success contract but does not start Binance "
        "collectors, ClickHouse writers, or archive supervisors"
        if allow_public_soak
        else "CANDLESCOPE_PHASE1R_ALLOW_PUBLIC_SOAK=1 is required for public-24h"
    )
    return {
        "schema_version": SOAK_RESULT_SCHEMA_VERSION,
        "mode": "public_24h",
        "phase1r_passed": False,
        "code": code,
        "message": message,
        "required_duration_ms": PUBLIC_SOAK_DURATION_MS,
        "public_source_required": "binance",
        "process_restarts_required": [
            "collector",
            "clickhouse_writer",
            "parquet_archiver",
        ],
        "caught_up_reconciliation_required": True,
        "twenty_four_hour_public_continuity": False,
        "main_fastapi_server_profile_unlocked": False,
    }


def _records() -> tuple[KafkaMarketEventRecord, ...]:
    adapter = AggTradeEnvelopeAdapter(ProducerIdentity("phase1r-rehearsal", 0))
    records: list[KafkaMarketEventRecord] = []
    for index in range(_EVENT_COUNT):
        sequence = _FIRST_SEQUENCE + index
        envelope = adapter.adapt(
            _market_event(sequence),
            previous_sequence=None if sequence == _FIRST_SEQUENCE else sequence - 1,
            published_at_ms=_START_MS + 1_000 + sequence,
        )
        records.append(
            decode_kafka_market_event(
                topic=MARKET_EVENTS_TOPIC,
                partition=0,
                offset=index,
                key=PHASE1B_PARTITION_KEY.encode(),
                value=canonical_envelope_bytes(envelope),
                headers=(
                    ("event-id", envelope.event_id.encode()),
                    ("schema-version", envelope.schema_version.encode()),
                ),
            )
        )
    return tuple(records)


def _market_event(sequence: int) -> MarketEvent:
    return MarketEvent(
        event_type=StreamType.AGG_TRADE,
        symbol="BTCUSDT",
        exchange="binance",
        event_time_ms=_START_MS + sequence,
        received_at_ms=_START_MS + 100 + sequence,
        source=DataSource.WEBSOCKET,
        data={
            "agg_trade_id": sequence,
            "price": 100000.1,
            "quantity": 0.025,
            "price_text": "100000.1000",
            "quantity_text": "0.02500000",
            "first_trade_id": sequence * 10,
            "last_trade_id": sequence * 10 + 2,
            "trade_time_ms": _START_MS + sequence,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=sequence,
        market_type="futures",
    )


def _collector_health(
    *,
    last_offset: int,
    last_sequence: int,
    started_at_ms: int,
) -> CollectorHealth:
    return CollectorHealth(
        state=CollectorState.LEADER,
        ready=True,
        reason="rehearsal published a complete contiguous segment",
        owner_id="phase1r-rehearsal-collector",
        source_health="scripted",
        producer_epoch=0,
        lease_expires_at_ms=started_at_ms + 15_000,
        last_sequence=last_sequence,
        last_partition_offset=last_offset,
        pending_event_id=None,
        events_published=_EVENT_COUNT,
        heartbeat_successes=1,
        heartbeat_failures=0,
        started_at_ms=started_at_ms,
        updated_at_ms=started_at_ms,
        terminal_error=None,
    )


def _writer_health(
    *,
    committed_next_offset: int,
    inserted_events: int,
    duplicate_events: int,
    conflict_events: int,
    batches_committed: int,
    started_at_ms: int,
) -> ClickHouseWriterHealth:
    return ClickHouseWriterHealth(
        state=ClickHouseWriterState.RUNNING,
        ready=True,
        reason="rehearsal projector caught up after idempotent replay",
        owner_id="phase1r-rehearsal-writer",
        kafka_group_id="candlescope-clickhouse-writer-v1",
        committed_next_offset=committed_next_offset,
        batches_committed=batches_committed,
        inserted_events=inserted_events,
        duplicate_events=duplicate_events,
        conflict_events=conflict_events,
        started_at_ms=started_at_ms,
        updated_at_ms=started_at_ms,
        terminal_error=None,
    )


def _archive_health(
    *,
    snapshot: MarketDataSnapshotRef,
    committed_next_offset: int,
    events_archived: int,
    started_at_ms: int,
) -> ArchiveWriterHealth:
    return ArchiveWriterHealth(
        state=ArchiveWriterState.RUNNING,
        ready=True,
        reason="rehearsal published one immutable segment and manifest",
        owner_id="phase1r-rehearsal-archiver",
        kafka_group_id="candlescope-parquet-archiver-v1",
        data_epoch=snapshot.data_epoch,
        committed_next_offset=committed_next_offset,
        segments_committed=1,
        events_archived=events_archived,
        current_snapshot=snapshot,
        started_at_ms=started_at_ms,
        updated_at_ms=started_at_ms,
        terminal_error=None,
    )


def _rehearsal_result(
    *,
    reconciliation: ChainReconciliation,
    snapshot_pin: dict[str, object],
    started_at_ms: int,
    finished_at_ms: int,
    replay_ids: tuple[int, ...],
    object_replayed: bool,
) -> dict[str, object]:
    return {
        "schema_version": SOAK_RESULT_SCHEMA_VERSION,
        "mode": SOAK_MODE_REHEARSAL,
        "phase1r_passed": True,
        "collector_process": "synthetic_from_published_records",
        "duration_ms": finished_at_ms - started_at_ms,
        "required_duration_ms": 0,
        "public_source_used": False,
        "process_restarts_injected": 1,
        "idempotent_rearchive_verified": object_replayed,
        "replay_ids": list(replay_ids),
        "snapshot_pin": snapshot_pin,
        "reconciliation": reconciliation.to_wire(),
        "twenty_four_hour_public_continuity": False,
        "main_fastapi_server_profile_unlocked": False,
        "claims_not_made": [
            "Binance public 24 hour continuity",
            "collector, writer, and archiver process supervisors",
            "FastAPI server profile startup",
            "ReplayService composition-root wiring",
        ],
    }


def _clock_ms(value: int | None) -> int:
    if value is None:
        return int(time.time() * 1000)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("clock_ms must be a non-negative integer")
    return value


__all__ = [
    "PUBLIC_SOAK_DURATION_MS",
    "PUBLIC_SOAK_ENV",
    "PUBLIC_SOAK_NOT_DELIVERED",
    "SOAK_MODE_REHEARSAL",
    "SOAK_RESULT_SCHEMA_VERSION",
    "public_soak_refusal",
    "run_phase1r_rehearsal",
]
