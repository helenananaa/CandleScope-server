"""Strict Phase 1A adapter for one Binance futures aggregate-trade stream."""

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.data_engine.market_data import DeliveryClass, MarketChannel, MarketStreamKey
from app.server_contracts import MarketEventEnvelopeV1
from app.server_runtime.producer_identity import ProducerIdentity

BINANCE_AGG_TRADE_PAYLOAD_SCHEMA = "binance.agg-trade.normalized.v1"
_EVENT_ID_NAMESPACE = uuid.UUID("62e155eb-25ea-5f44-ad41-9189ee138506")
_SUPPORTED_SOURCES = {
    DataSource.WEBSOCKET,
    DataSource.HTTP,
    DataSource.HTTP_BACKFILL,
}


class UnsupportedMarketEventError(ValueError):
    """Raised when a MarketEvent is outside the frozen Phase 1A stream."""


@dataclass(frozen=True, slots=True)
class _AggTradeFields:
    agg_trade_id: int
    price_text: str
    quantity_text: str
    first_trade_id: int
    last_trade_id: int
    trade_time_ms: int
    buyer_is_maker: bool

    def to_payload(self) -> dict[str, Any]:
        return {
            "agg_trade_id": self.agg_trade_id,
            "price": self.price_text,
            "quantity": self.quantity_text,
            "first_trade_id": self.first_trade_id,
            "last_trade_id": self.last_trade_id,
            "trade_time_ms": self.trade_time_ms,
            "buyer_is_maker": self.buyer_is_maker,
        }


@dataclass(frozen=True, slots=True)
class AggTradeEnvelopeAdapter:
    """Convert the one Phase 1A MarketEvent shape into the durable envelope."""

    producer: ProducerIdentity

    def __post_init__(self) -> None:
        if not isinstance(self.producer, ProducerIdentity):
            raise TypeError("producer must be a ProducerIdentity")

    def sequence(self, event: MarketEvent) -> int:
        """Return the validated exchange continuity key for one event."""

        return self._extract(event).agg_trade_id

    def adapt(
        self,
        event: MarketEvent,
        *,
        published_at_ms: int,
        previous_sequence: int | None,
    ) -> MarketEventEnvelopeV1:
        fields = self._extract(event)
        published_at_ms = _non_negative_int(
            published_at_ms,
            field="published_at_ms",
        )
        if published_at_ms < event.received_at_ms:
            raise ValueError("published_at_ms cannot precede received_at_ms")
        if previous_sequence is not None:
            previous_sequence = _non_negative_int(
                previous_sequence,
                field="previous_sequence",
            )
            if previous_sequence >= fields.agg_trade_id:
                raise ValueError("previous_sequence must precede agg_trade_id")

        stream = MarketStreamKey.build(
            "binance",
            "futures",
            "BTCUSDT",
            MarketChannel.AGG_TRADE,
        )
        source_event_id = str(fields.agg_trade_id)
        event_id = str(
            uuid.uuid5(
                _EVENT_ID_NAMESPACE,
                f"{stream.topic}\n{source_event_id}",
            )
        )
        return MarketEventEnvelopeV1.build(
            event_id=event_id,
            stream=stream,
            delivery_class=DeliveryClass.APPEND,
            source=event.source.value,
            source_event_id=source_event_id,
            sequence_start=fields.agg_trade_id,
            sequence_end=fields.agg_trade_id,
            previous_sequence=previous_sequence,
            producer_id=self.producer.producer_id,
            producer_epoch=self.producer.producer_epoch,
            event_time_ms=_non_negative_int(
                event.event_time_ms,
                field="event_time_ms",
            ),
            received_at_ms=_non_negative_int(
                event.received_at_ms,
                field="received_at_ms",
            ),
            published_at_ms=published_at_ms,
            payload_schema=BINANCE_AGG_TRADE_PAYLOAD_SCHEMA,
            payload=fields.to_payload(),
        )

    @staticmethod
    def _extract(event: MarketEvent) -> _AggTradeFields:
        if not isinstance(event, MarketEvent):
            raise TypeError("event must be a MarketEvent")
        if event.event_type is not StreamType.AGG_TRADE:
            raise UnsupportedMarketEventError("Phase 1A only accepts aggTrade events")
        if event.exchange.strip().lower() != "binance":
            raise UnsupportedMarketEventError("Phase 1A only accepts Binance events")
        if event.market_type.strip().lower() != "futures":
            raise UnsupportedMarketEventError(
                "Phase 1A only accepts Binance futures events"
            )
        if event.symbol.strip().upper() != "BTCUSDT":
            raise UnsupportedMarketEventError("Phase 1A only accepts BTCUSDT")
        if event.source not in _SUPPORTED_SOURCES:
            raise UnsupportedMarketEventError(
                "Phase 1A requires websocket or HTTP recovery provenance"
            )

        data = event.data
        if not isinstance(data, dict):
            raise TypeError("event.data must be a dict")
        agg_trade_id = _data_int(data, "agg_trade_id")
        if event.sequence != agg_trade_id:
            raise ValueError("event.sequence must equal agg_trade_id")
        first_trade_id = _data_int(data, "first_trade_id")
        last_trade_id = _data_int(data, "last_trade_id")
        if last_trade_id < first_trade_id:
            raise ValueError("last_trade_id cannot precede first_trade_id")
        price_text = _decimal_text(data, "price", positive=True)
        quantity_text = _decimal_text(data, "quantity", positive=True)
        buyer_is_maker = data.get("is_buyer_maker")
        if not isinstance(buyer_is_maker, bool):
            raise TypeError("is_buyer_maker must be a boolean")
        return _AggTradeFields(
            agg_trade_id=agg_trade_id,
            price_text=price_text,
            quantity_text=quantity_text,
            first_trade_id=first_trade_id,
            last_trade_id=last_trade_id,
            trade_time_ms=_data_int(data, "trade_time_ms"),
            buyer_is_maker=buyer_is_maker,
        )


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _data_int(data: dict[str, Any], field: str) -> int:
    if field not in data:
        raise ValueError(f"event.data is missing {field}")
    return _non_negative_int(data[field], field=field)


def _decimal_text(
    data: dict[str, Any],
    field: str,
    *,
    positive: bool,
) -> str:
    text_field = f"{field}_text"
    value = data.get(text_field)
    if not isinstance(value, str):
        raise TypeError(f"{text_field} must be an exact decimal string")
    text = value.strip()
    if not text:
        raise ValueError(f"{text_field} cannot be blank")
    try:
        decimal_value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{text_field} must be a decimal string") from exc
    if not decimal_value.is_finite():
        raise ValueError(f"{text_field} must be finite")
    if positive and decimal_value <= 0:
        raise ValueError(f"{text_field} must be positive")

    legacy_value = data.get(field)
    if isinstance(legacy_value, bool) or not isinstance(legacy_value, (int, float)):
        raise TypeError(f"{field} must remain numeric for personal compatibility")
    if not math.isfinite(float(legacy_value)):
        raise ValueError(f"{field} must be finite")
    if Decimal(str(legacy_value)) != decimal_value:
        raise ValueError(f"{text_field} does not match {field}")
    return text
