"""Phase 1Q snapshot-pinned aggregate-trade reader for existing Replay sources.

The adapter binds one caller-supplied MarketDataSnapshotRef and the frozen
Phase 1A stream to cold MarketEventQuery pages, materializes a bounded exact
trade dataset, then serves the synchronous ReplayTradePageReader contract.
It never selects a latest snapshot, never reads hot ClickHouse, and never
falls back to live market data.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Self

from app.data_engine.market_data import MarketChannel, MarketStreamKey
from app.data_engine.storage.raw_trade_archive import RawAggTradeCursor
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.replay.sources.trade_reader import (
    ReplayTrade,
    ReplayTradePage,
    ReplayTradeSequencePage,
)
from app.server_contracts import (
    MarketDataSnapshotRef,
    MarketEventCursor,
    MarketEventEnvelopeV1,
    MarketEventQuery,
)
from app.server_runtime.adapters import BINANCE_AGG_TRADE_PAYLOAD_SCHEMA

REPLAY_SERVER_SNAPSHOT_SCHEMA_VERSION = "candlescope.replay-server-snapshot.v1"
SERVER_SNAPSHOT_SOURCE_QUALITY = "server_snapshot_manifest"
SERVER_SNAPSHOT_TRADE_SOURCE = "server_snapshot"
SERVER_SNAPSHOT_COMPLETENESS = "exact"
DEFAULT_QUERY_PAGE_LIMIT = 500
DEFAULT_READER_PAGE_ROWS = 500
DEFAULT_MAX_SCAN_ROWS = 100_000
DEFAULT_MAX_QUERY_PAGES = 256
MAX_READER_PAGE_ROWS = 50_000
_FROZEN_STREAM = MarketStreamKey.build(
    "binance",
    "futures",
    "BTCUSDT",
    MarketChannel.AGG_TRADE,
)


class ServerSnapshotReplayError(ReplayDomainError):
    """Fail-closed binding from an immutable server snapshot to Replay trades."""


@dataclass(frozen=True, slots=True)
class ReplayServerSnapshotPin:
    """Caller-owned exact snapshot, stream, time window, and trade-ID span."""

    snapshot: MarketDataSnapshotRef
    start_event_time_ms: int
    end_event_time_ms: int
    expected_first_agg_trade_id: int
    expected_last_agg_trade_id: int
    row_count: int
    stream: MarketStreamKey = _FROZEN_STREAM
    schema_version: str = REPLAY_SERVER_SNAPSHOT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REPLAY_SERVER_SNAPSHOT_SCHEMA_VERSION:
            raise ValueError(
                "replay server snapshot schema must be "
                f"{REPLAY_SERVER_SNAPSHOT_SCHEMA_VERSION}"
            )
        if not isinstance(self.snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        if not isinstance(self.stream, MarketStreamKey):
            raise TypeError("stream must be a MarketStreamKey")
        if self.stream != _FROZEN_STREAM:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.UNSUPPORTED_SOURCE,
                "Phase 1Q only accepts binance futures BTCUSDT agg_trade",
                details={"partition_key": self.stream.topic},
            )
        start_event_time_ms = _non_negative_int(
            self.start_event_time_ms,
            field="start_event_time_ms",
        )
        end_event_time_ms = _non_negative_int(
            self.end_event_time_ms,
            field="end_event_time_ms",
        )
        if end_event_time_ms < start_event_time_ms:
            raise ValueError("end_event_time_ms must not precede start_event_time_ms")
        object.__setattr__(self, "start_event_time_ms", start_event_time_ms)
        object.__setattr__(self, "end_event_time_ms", end_event_time_ms)
        first_id = _positive_int(
            self.expected_first_agg_trade_id,
            field="expected_first_agg_trade_id",
        )
        last_id = _positive_int(
            self.expected_last_agg_trade_id,
            field="expected_last_agg_trade_id",
        )
        if last_id < first_id:
            raise ValueError("expected_last_agg_trade_id cannot precede first id")
        object.__setattr__(self, "expected_first_agg_trade_id", first_id)
        object.__setattr__(self, "expected_last_agg_trade_id", last_id)
        row_count = _positive_int(self.row_count, field="row_count")
        if row_count != last_id - first_id + 1:
            raise ValueError("row_count must equal the inclusive aggregate-trade span")
        object.__setattr__(self, "row_count", row_count)

    def to_public_ref(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "data_epoch": self.snapshot.data_epoch,
            "snapshot_version": self.snapshot.snapshot_version,
            "manifest_uri": self.snapshot.manifest_uri,
            "manifest_sha256": self.snapshot.manifest_sha256,
            "stream": {
                "exchange": self.stream.exchange,
                "market_type": self.stream.market_type,
                "symbol": self.stream.symbol,
                "channel": self.stream.channel.value,
            },
            "start_event_time_ms": self.start_event_time_ms,
            "end_event_time_ms": self.end_event_time_ms,
            "expected_first_agg_trade_id": self.expected_first_agg_trade_id,
            "expected_last_agg_trade_id": self.expected_last_agg_trade_id,
            "row_count": self.row_count,
            "completeness": SERVER_SNAPSHOT_COMPLETENESS,
            "source_quality": SERVER_SNAPSHOT_SOURCE_QUALITY,
        }


@dataclass(frozen=True, slots=True)
class ReplayServerSnapshotDataset:
    """TradeReplaySource-facing exact dataset view over one frozen snapshot."""

    data_epoch: str
    exchange: str
    market_type: str
    symbol: str
    start_time_ms: int
    end_time_ms: int
    expected_first_agg_trade_id: int
    expected_last_agg_trade_id: int
    row_count: int
    completeness: str = SERVER_SNAPSHOT_COMPLETENESS
    source_quality: str = SERVER_SNAPSHOT_SOURCE_QUALITY

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "data_epoch", _required_text(self.data_epoch, "data_epoch")
        )
        object.__setattr__(self, "exchange", _required_text(self.exchange, "exchange"))
        object.__setattr__(
            self,
            "market_type",
            _required_text(self.market_type, "market_type"),
        )
        object.__setattr__(self, "symbol", _required_text(self.symbol, "symbol"))
        if self.completeness != SERVER_SNAPSHOT_COMPLETENESS:
            raise ValueError("server snapshot replay dataset must be exact")
        if self.source_quality != SERVER_SNAPSHOT_SOURCE_QUALITY:
            raise ValueError("server snapshot source quality is not exact")
        start_time_ms = _non_negative_int(self.start_time_ms, field="start_time_ms")
        end_time_ms = _non_negative_int(self.end_time_ms, field="end_time_ms")
        if end_time_ms < start_time_ms:
            raise ValueError("dataset trade-time bounds are inverted")
        object.__setattr__(self, "start_time_ms", start_time_ms)
        object.__setattr__(self, "end_time_ms", end_time_ms)
        first_id = _positive_int(
            self.expected_first_agg_trade_id,
            field="expected_first_agg_trade_id",
        )
        last_id = _positive_int(
            self.expected_last_agg_trade_id,
            field="expected_last_agg_trade_id",
        )
        row_count = _positive_int(self.row_count, field="row_count")
        if last_id < first_id or row_count != last_id - first_id + 1:
            raise ValueError("dataset aggregate-trade span is inconsistent")
        object.__setattr__(self, "expected_first_agg_trade_id", first_id)
        object.__setattr__(self, "expected_last_agg_trade_id", last_id)
        object.__setattr__(self, "row_count", row_count)


class ServerSnapshotTradeReader:
    """Frozen exact aggTrade dataset loaded from one cold snapshot query."""

    def __init__(
        self,
        pin: ReplayServerSnapshotPin,
        trades: tuple[ReplayTrade, ...],
        *,
        page_rows: int = DEFAULT_READER_PAGE_ROWS,
    ) -> None:
        if not isinstance(pin, ReplayServerSnapshotPin):
            raise TypeError("pin must be a ReplayServerSnapshotPin")
        verified = _verify_frozen_trades(pin, trades)
        page_rows = _positive_int(page_rows, field="page_rows")
        if page_rows > MAX_READER_PAGE_ROWS:
            raise ValueError(f"page_rows cannot exceed {MAX_READER_PAGE_ROWS}")
        self.snapshot_pin = pin
        self._trades = verified
        self.page_rows = page_rows
        self.dataset_ref = ReplayServerSnapshotDataset(
            data_epoch=pin.snapshot.data_epoch,
            exchange=pin.stream.exchange,
            market_type=pin.stream.market_type,
            symbol=pin.stream.symbol,
            start_time_ms=verified[0].trade_time_ms,
            end_time_ms=verified[-1].trade_time_ms,
            expected_first_agg_trade_id=pin.expected_first_agg_trade_id,
            expected_last_agg_trade_id=pin.expected_last_agg_trade_id,
            row_count=pin.row_count,
        )

    @classmethod
    async def load(
        cls,
        query: MarketEventQuery,
        pin: ReplayServerSnapshotPin,
        *,
        query_page_limit: int = DEFAULT_QUERY_PAGE_LIMIT,
        page_rows: int = DEFAULT_READER_PAGE_ROWS,
        max_scan_rows: int = DEFAULT_MAX_SCAN_ROWS,
        max_query_pages: int = DEFAULT_MAX_QUERY_PAGES,
    ) -> Self:
        if not isinstance(pin, ReplayServerSnapshotPin):
            raise TypeError("pin must be a ReplayServerSnapshotPin")
        if not callable(getattr(query, "query", None)):
            raise TypeError("query must implement MarketEventQuery")
        query_page_limit = _positive_int(query_page_limit, field="query_page_limit")
        max_scan_rows = _positive_int(max_scan_rows, field="max_scan_rows")
        max_query_pages = _positive_int(max_query_pages, field="max_query_pages")
        if pin.row_count > max_scan_rows:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.SCAN_LIMIT_EXCEEDED,
                "pinned aggregate-trade row count exceeds the scan budget",
                details={"row_count": pin.row_count, "max_scan_rows": max_scan_rows},
            )
        collected: list[ReplayTrade] = []
        cursor: MarketEventCursor | None = None
        pages = 0
        while True:
            pages += 1
            if pages > max_query_pages:
                raise ServerSnapshotReplayError(
                    ReplayErrorCode.SCAN_LIMIT_EXCEEDED,
                    "snapshot query exceeded the bounded page budget",
                    details={"max_query_pages": max_query_pages},
                )
            try:
                page = await query.query(
                    snapshot=pin.snapshot,
                    stream=pin.stream,
                    start_event_time_ms=pin.start_event_time_ms,
                    end_event_time_ms=pin.end_event_time_ms,
                    limit=query_page_limit,
                    cursor=cursor,
                )
            except ServerSnapshotReplayError:
                raise
            except Exception as exc:
                raise ServerSnapshotReplayError(
                    ReplayErrorCode.ARCHIVE_DEGRADED,
                    "cold snapshot query failed while loading replay trades",
                ) from exc
            _require_page_snapshot(page.snapshot, pin.snapshot)
            if not page.events and page.next_cursor is not None:
                raise ServerSnapshotReplayError(
                    ReplayErrorCode.DATASET_MISMATCH,
                    "snapshot query page made no progress",
                )
            for envelope in page.events:
                collected.append(replay_trade_from_envelope(pin, envelope))
                if len(collected) > max_scan_rows:
                    raise ServerSnapshotReplayError(
                        ReplayErrorCode.SCAN_LIMIT_EXCEEDED,
                        "snapshot query exceeded the scan budget before exhaustion",
                        details={"max_scan_rows": max_scan_rows},
                    )
            if page.next_cursor is None:
                break
            if page.next_cursor == cursor:
                raise ServerSnapshotReplayError(
                    ReplayErrorCode.DATASET_MISMATCH,
                    "snapshot query cursor did not advance",
                )
            if page.next_cursor.manifest_sha256 != pin.snapshot.manifest_sha256:
                raise ServerSnapshotReplayError(
                    ReplayErrorCode.DATASET_MISMATCH,
                    "snapshot query cursor escaped the pinned manifest",
                    details={
                        "pinned_manifest_sha256": pin.snapshot.manifest_sha256,
                        "cursor_manifest_sha256": page.next_cursor.manifest_sha256,
                    },
                )
            cursor = page.next_cursor
        return cls(pin, tuple(collected), page_rows=page_rows)

    @property
    def data_epoch(self) -> str:
        return self.dataset_ref.data_epoch

    def read_page(
        self,
        after: RawAggTradeCursor | None = None,
        *,
        limit: int | None = None,
    ) -> ReplayTradePage:
        if after is not None and not isinstance(after, RawAggTradeCursor):
            raise TypeError("after must be RawAggTradeCursor or None")
        page_limit = self.page_rows if limit is None else limit
        page_limit = _positive_int(page_limit, field="limit")
        if page_limit > self.page_rows:
            raise ValueError(f"limit must be between 1 and {self.page_rows}")
        start_index = self._start_index(after)
        if start_index == len(self._trades):
            return ReplayTradePage((), after, True, self.data_epoch)
        trades = self._trades[start_index : start_index + page_limit]
        exhausted = start_index + len(trades) >= len(self._trades)
        next_cursor = trades[-1].cursor if trades else after
        return ReplayTradePage(
            trades=trades,
            next_cursor=next_cursor,
            exhausted=exhausted,
            data_epoch=self.data_epoch,
        )

    def read_sequence_page(
        self,
        *,
        after_sequence: int,
        revealed_sequence: int,
        limit: int,
    ) -> ReplayTradeSequencePage:
        row_count = self.dataset_ref.row_count
        after_sequence = _non_negative_int(after_sequence, field="after_sequence")
        revealed_sequence = _non_negative_int(
            revealed_sequence,
            field="revealed_sequence",
        )
        limit = _positive_int(limit, field="limit")
        if not 0 <= after_sequence <= revealed_sequence <= row_count:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATASET_MISMATCH,
                "aggregate-trade revealed sequence bounds are invalid",
            )
        if limit > self.page_rows:
            raise ValueError(f"limit must be between 1 and {self.page_rows}")
        if after_sequence == revealed_sequence:
            return ReplayTradeSequencePage(
                trades=(),
                after_sequence=after_sequence,
                next_sequence=after_sequence,
                revealed_sequence=revealed_sequence,
                has_more=False,
                data_epoch=self.data_epoch,
            )
        expected_rows = min(limit, revealed_sequence - after_sequence)
        trades = self._trades[after_sequence : after_sequence + expected_rows]
        if len(trades) != expected_rows:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATA_GAP,
                "aggregate-trade revealed page is incomplete",
                details={"expected_rows": expected_rows, "actual_rows": len(trades)},
            )
        next_sequence = after_sequence + len(trades)
        return ReplayTradeSequencePage(
            trades=trades,
            after_sequence=after_sequence,
            next_sequence=next_sequence,
            revealed_sequence=revealed_sequence,
            has_more=next_sequence < revealed_sequence,
            data_epoch=self.data_epoch,
        )

    def _start_index(self, after: RawAggTradeCursor | None) -> int:
        first_id = self.dataset_ref.expected_first_agg_trade_id
        last_id = self.dataset_ref.expected_last_agg_trade_id
        if after is None:
            return 0
        if after.agg_trade_id < first_id - 1:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATASET_MISMATCH,
                "aggregate-trade cursor precedes the frozen dataset",
            )
        if after.agg_trade_id > last_id:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATASET_MISMATCH,
                "aggregate-trade cursor exceeds the frozen dataset",
            )
        if after.agg_trade_id == last_id:
            last = self._trades[-1]
            if after != last.cursor:
                raise ServerSnapshotReplayError(
                    ReplayErrorCode.DATASET_MISMATCH,
                    "aggregate-trade cursor time drifted from the frozen last trade",
                )
            return len(self._trades)
        if after.agg_trade_id == first_id - 1:
            return 0
        index = after.agg_trade_id - first_id
        current = self._trades[index]
        if after != current.cursor:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATASET_MISMATCH,
                "aggregate-trade cursor does not match the frozen trade",
            )
        return index + 1


def replay_trade_from_envelope(
    pin: ReplayServerSnapshotPin,
    envelope: MarketEventEnvelopeV1,
) -> ReplayTrade:
    """Map one canonical aggTrade envelope onto the Replay trade schema."""

    if not isinstance(envelope, MarketEventEnvelopeV1):
        raise TypeError("envelope must be a MarketEventEnvelopeV1")
    if envelope.stream != pin.stream:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot event stream escaped the pinned market stream",
            details={"partition_key": envelope.partition_key},
        )
    if envelope.payload_schema != BINANCE_AGG_TRADE_PAYLOAD_SCHEMA:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot event payload schema is not the frozen aggTrade contract",
            details={"payload_schema": envelope.payload_schema},
        )
    if not (pin.start_event_time_ms <= envelope.event_time_ms <= pin.end_event_time_ms):
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot event time escaped the pinned query window",
            details={"event_time_ms": envelope.event_time_ms},
        )
    payload = envelope.payload
    if not isinstance(payload, Mapping):
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot aggTrade payload must be an object",
        )
    agg_trade_id = _payload_int(payload, "agg_trade_id")
    if envelope.source_event_id != str(agg_trade_id):
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot source_event_id drifted from agg_trade_id",
        )
    if envelope.sequence_start != agg_trade_id or envelope.sequence_end != agg_trade_id:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot sequence identity drifted from agg_trade_id",
        )
    first_trade_id = _payload_int(payload, "first_trade_id")
    last_trade_id = _payload_int(payload, "last_trade_id")
    price = _payload_decimal_text(payload, "price")
    quantity = _payload_decimal_text(payload, "quantity")
    buyer_is_maker = payload.get("buyer_is_maker")
    if not isinstance(buyer_is_maker, bool):
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot aggTrade buyer_is_maker must be a boolean",
        )
    try:
        return ReplayTrade(
            exchange=pin.stream.exchange,
            market_type=pin.stream.market_type,
            symbol=pin.stream.symbol,
            agg_trade_id=agg_trade_id,
            first_trade_id=first_trade_id,
            last_trade_id=last_trade_id,
            price=price,
            quantity=quantity,
            quote_quantity=_quote_quantity(price, quantity),
            trade_time_ms=_payload_int(payload, "trade_time_ms"),
            is_buyer_maker=buyer_is_maker,
            source=SERVER_SNAPSHOT_TRADE_SOURCE,
        )
    except (TypeError, ValueError) as exc:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot aggTrade payload violates replay trade schema",
        ) from exc


def _verify_frozen_trades(
    pin: ReplayServerSnapshotPin,
    trades: tuple[ReplayTrade, ...] | list[ReplayTrade],
) -> tuple[ReplayTrade, ...]:
    frozen = tuple(trades)
    if any(not isinstance(item, ReplayTrade) for item in frozen):
        raise TypeError("trades must contain ReplayTrade values")
    if len(frozen) != pin.row_count:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATA_GAP,
            "snapshot query row count drifted from the pinned span",
            details={"expected_rows": pin.row_count, "actual_rows": len(frozen)},
        )
    expected_id = pin.expected_first_agg_trade_id
    previous: RawAggTradeCursor | None = None
    for trade in frozen:
        if (
            trade.exchange,
            trade.market_type,
            trade.symbol,
            trade.source,
        ) != (
            pin.stream.exchange,
            pin.stream.market_type,
            pin.stream.symbol,
            SERVER_SNAPSHOT_TRADE_SOURCE,
        ):
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATASET_MISMATCH,
                "frozen replay trade identity escaped the pinned snapshot",
            )
        if trade.agg_trade_id != expected_id:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATA_GAP,
                "snapshot query lost aggregate-trade continuity",
                details={"expected_agg_trade_id": expected_id},
            )
        if previous is not None and trade.cursor <= previous:
            raise ServerSnapshotReplayError(
                ReplayErrorCode.DATASET_MISMATCH,
                "snapshot query cursor moved backward or repeated",
            )
        previous = trade.cursor
        expected_id += 1
    if frozen[-1].agg_trade_id != pin.expected_last_agg_trade_id:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATA_GAP,
            "snapshot query ended before the pinned last aggregate-trade id",
        )
    return frozen


def _require_page_snapshot(
    actual: MarketDataSnapshotRef,
    expected: MarketDataSnapshotRef,
) -> None:
    if actual != expected:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot query returned a different immutable snapshot",
            details={
                "pinned_manifest_sha256": expected.manifest_sha256,
                "returned_manifest_sha256": actual.manifest_sha256,
            },
        )


def _quote_quantity(price: str, quantity: str) -> str:
    product = Decimal(price) * Decimal(quantity)
    if not product.is_finite() or product <= 0:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            "snapshot aggTrade quote quantity must be positive and finite",
        )
    normalized = format(product, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def _payload_int(payload: Mapping[str, Any], field: str) -> int:
    if field not in payload:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            f"snapshot aggTrade payload is missing {field}",
        )
    value = payload[field]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            f"snapshot aggTrade {field} must be a non-negative integer",
        )
    return value


def _payload_decimal_text(payload: Mapping[str, Any], field: str) -> str:
    if field not in payload:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            f"snapshot aggTrade payload is missing {field}",
        )
    value = payload[field]
    if not isinstance(value, str) or not value.strip():
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            f"snapshot aggTrade {field} must be an exact decimal string",
        )
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            f"snapshot aggTrade {field} must be a finite Decimal",
        ) from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ServerSnapshotReplayError(
            ReplayErrorCode.DATASET_MISMATCH,
            f"snapshot aggTrade {field} must be positive and finite",
        )
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} cannot be blank")
    return value.strip()


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _positive_int(value: object, *, field: str) -> int:
    number = _non_negative_int(value, field=field)
    if number < 1:
        raise ValueError(f"{field} must be greater than zero")
    return number


__all__ = [
    "DEFAULT_MAX_QUERY_PAGES",
    "DEFAULT_MAX_SCAN_ROWS",
    "DEFAULT_QUERY_PAGE_LIMIT",
    "DEFAULT_READER_PAGE_ROWS",
    "REPLAY_SERVER_SNAPSHOT_SCHEMA_VERSION",
    "SERVER_SNAPSHOT_COMPLETENESS",
    "SERVER_SNAPSHOT_SOURCE_QUALITY",
    "SERVER_SNAPSHOT_TRADE_SOURCE",
    "ReplayServerSnapshotDataset",
    "ReplayServerSnapshotPin",
    "ServerSnapshotReplayError",
    "ServerSnapshotTradeReader",
    "replay_trade_from_envelope",
]
