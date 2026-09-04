"""Deterministic MarketEventQuery over a frozen aggTrade envelope file."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_contracts import MarketDataSnapshotRef, MarketEventPage
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.producer_identity import ProducerIdentity
from app.server_runtime.publishers import canonical_envelope_bytes
from app.server_runtime.query_pagination import SnapshotQueryRow, paginate_snapshot_rows


class FrozenAggTradeQuery:
    def __init__(self, path: Path) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        snapshot = payload["snapshot"]
        self.snapshot = MarketDataSnapshotRef(
            data_epoch=str(snapshot["data_epoch"]),
            snapshot_version=int(snapshot["snapshot_version"]),
            manifest_uri=str(snapshot["manifest_uri"]),
            manifest_sha256=str(snapshot["manifest_sha256"]),
        )
        start_ms = int(payload["start_ms"])
        envelopes = [
            AggTradeEnvelopeAdapter(ProducerIdentity("collector-a", 0)).adapt(
                MarketEvent(
                    event_type=StreamType.AGG_TRADE,
                    symbol="BTCUSDT",
                    exchange="binance",
                    event_time_ms=start_ms + sequence,
                    received_at_ms=start_ms + 100 + sequence,
                    source=DataSource.WEBSOCKET,
                    data={
                        "agg_trade_id": sequence,
                        "price": 100000.1,
                        "quantity": 0.025,
                        "price_text": "100000.1000",
                        "quantity_text": "0.02500000",
                        "first_trade_id": sequence * 10,
                        "last_trade_id": sequence * 10 + 2,
                        "trade_time_ms": start_ms + sequence,
                        "is_buyer_maker": False,
                    },
                    stream_key="futures:BTCUSDT@aggTrade",
                    sequence=sequence,
                    market_type="futures",
                ),
                previous_sequence=None
                if sequence == payload["sequences"][0]
                else sequence - 1,
                published_at_ms=start_ms + 1_000 + sequence,
            )
            for sequence in payload["sequences"]
        ]
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
