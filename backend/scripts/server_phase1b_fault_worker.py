"""Publish one staged event, then exit before checkpointing its Kafka receipt."""

from __future__ import annotations

import argparse
import asyncio
import json

from app.data_engine.ingestion.models import DataSource, MarketEvent, StreamType
from app.server_runtime import ProducerIdentity
from app.server_runtime.adapters import AggTradeEnvelopeAdapter
from app.server_runtime.publishers import (
    PHASE1B_PARTITION_KEY,
    KafkaMarketEventPublisher,
)
from app.server_runtime.storage import PostgresStreamLeaseStore

EVENT_TIME_MS = 1_700_000_000_010
RECEIVED_AT_MS = 1_700_000_000_020
PUBLISHED_AT_MS = 1_700_000_000_030


def _event() -> MarketEvent:
    return MarketEvent(
        event_type=StreamType.AGG_TRADE,
        symbol="BTCUSDT",
        exchange="binance",
        event_time_ms=EVENT_TIME_MS,
        received_at_ms=RECEIVED_AT_MS,
        source=DataSource.WEBSOCKET,
        data={
            "agg_trade_id": 42,
            "price": 100000.1,
            "quantity": 0.025,
            "price_text": "100000.1000",
            "quantity_text": "0.02500000",
            "first_trade_id": 420,
            "last_trade_id": 422,
            "trade_time_ms": EVENT_TIME_MS - 10,
            "is_buyer_maker": False,
        },
        stream_key="futures:BTCUSDT@aggTrade",
        sequence=42,
        market_type="futures",
    )


async def _run(args: argparse.Namespace) -> None:
    store = PostgresStreamLeaseStore(args.postgres_dsn)
    publisher = KafkaMarketEventPublisher(
        bootstrap_servers=args.bootstrap_servers,
        client_id=f"{args.owner_id}-fault-worker",
    )
    await publisher.start()
    try:
        lease = await store.acquire(
            partition_key=PHASE1B_PARTITION_KEY,
            owner_id=args.owner_id,
            lease_ttl_ms=args.lease_ttl_ms,
        )
        envelope = AggTradeEnvelopeAdapter(
            ProducerIdentity(lease.owner_id, lease.producer_epoch)
        ).adapt(
            _event(),
            previous_sequence=lease.last_sequence,
            published_at_ms=PUBLISHED_AT_MS,
        )
        lease = await store.stage_pending(lease, envelope)
        receipt = await publisher.publish((envelope,))
        print(
            json.dumps(
                {
                    "event_id": envelope.event_id,
                    "producer_epoch": envelope.producer_epoch,
                    "partition_offset": dict(receipt.partition_offsets)[
                        PHASE1B_PARTITION_KEY
                    ],
                    "pending_event_id": lease.pending_envelope.event_id,
                },
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        await publisher.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres-dsn", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--lease-ttl-ms", required=True, type=int)
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
