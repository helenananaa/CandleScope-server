"""Adapters from personal-runtime domain models to server wire contracts."""

from .market_event import (
    BINANCE_AGG_TRADE_PAYLOAD_SCHEMA,
    AggTradeEnvelopeAdapter,
    UnsupportedMarketEventError,
)

__all__ = [
    "BINANCE_AGG_TRADE_PAYLOAD_SCHEMA",
    "AggTradeEnvelopeAdapter",
    "UnsupportedMarketEventError",
]
