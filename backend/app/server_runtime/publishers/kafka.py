"""Kafka-compatible durable publisher for the frozen Phase 1B market stream."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Self

import rfc8785
from aiokafka import AIOKafkaProducer

from app.server_contracts import MarketEventEnvelopeV1, PublishReceipt

MARKET_EVENTS_TOPIC = "candlescope.market-events.v1"
PHASE1B_PARTITION_KEY = "binance:futures:BTCUSDT@agg_trade"


class KafkaPublisherError(RuntimeError):
    """Base class for fail-closed Kafka publisher failures."""


class KafkaPublisherStateError(KafkaPublisherError):
    """Raised when publication is attempted outside a valid lifecycle."""


class KafkaTopicContractError(KafkaPublisherError):
    """Raised when the physical topic does not have exactly one partition."""


class KafkaPublishReceiptError(KafkaPublisherError):
    """Raised when the broker acknowledgement violates the frozen contract."""


def canonical_envelope_bytes(envelope: MarketEventEnvelopeV1) -> bytes:
    """Encode every retry to byte-identical RFC 8785 JSON."""

    if not isinstance(envelope, MarketEventEnvelopeV1):
        raise TypeError("envelope must be a MarketEventEnvelopeV1")
    return rfc8785.dumps(envelope.to_wire())


class KafkaMarketEventPublisher:
    """Publish one logical stream to partition zero with strong broker acks."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | Sequence[str],
        client_id: str,
        connection_options: Mapping[str, Any] | None = None,
        producer_factory: Callable[..., Any] = AIOKafkaProducer,
    ) -> None:
        if isinstance(bootstrap_servers, str):
            if not bootstrap_servers.strip():
                raise ValueError("bootstrap_servers cannot be blank")
            normalized_servers: str | tuple[str, ...] = bootstrap_servers.strip()
        else:
            normalized_servers = tuple(bootstrap_servers)
            if not normalized_servers or any(
                not isinstance(item, str) or not item.strip()
                for item in normalized_servers
            ):
                raise ValueError("bootstrap_servers must contain non-blank strings")
        if not isinstance(client_id, str) or not client_id.strip():
            raise ValueError("client_id must be a non-blank string")
        options = dict(connection_options or {})
        forbidden = {
            "acks",
            "bootstrap_servers",
            "client_id",
            "enable_idempotence",
            "key_serializer",
            "value_serializer",
        }
        conflict = forbidden.intersection(options)
        if conflict:
            raise ValueError(f"connection_options cannot override {sorted(conflict)}")
        self._bootstrap_servers = normalized_servers
        self._client_id = client_id.strip()
        self._connection_options = options
        self._producer_factory = producer_factory
        self._producer: Any | None = None
        self._lock = asyncio.Lock()

    @property
    def started(self) -> bool:
        return self._producer is not None

    async def start(self) -> None:
        async with self._lock:
            if self._producer is not None:
                raise KafkaPublisherStateError("publisher is already started")
            producer = self._producer_factory(
                bootstrap_servers=self._bootstrap_servers,
                client_id=self._client_id,
                acks="all",
                enable_idempotence=True,
                **self._connection_options,
            )
            await producer.start()
            try:
                partitions = await producer.partitions_for(MARKET_EVENTS_TOPIC)
                if partitions != {0}:
                    raise KafkaTopicContractError(
                        f"{MARKET_EVENTS_TOPIC!r} must exist with exactly partition 0; "
                        f"received {sorted(partitions) if partitions else partitions}"
                    )
            except BaseException:
                await producer.stop()
                raise
            self._producer = producer

    async def stop(self) -> None:
        async with self._lock:
            producer = self._producer
            self._producer = None
            if producer is not None:
                await producer.stop()

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def publish(
        self,
        events: Sequence[MarketEventEnvelopeV1],
    ) -> PublishReceipt:
        batch = tuple(events)
        if not batch:
            raise ValueError("event batches cannot be empty")
        if len(batch) != 1:
            raise ValueError("Phase 1B publisher only accepts one event per call")
        if any(not isinstance(event, MarketEventEnvelopeV1) for event in batch):
            raise TypeError("events must contain MarketEventEnvelopeV1 values")
        if any(event.partition_key != PHASE1B_PARTITION_KEY for event in batch):
            raise ValueError(
                "Phase 1B publisher only accepts the frozen aggTrade stream"
            )

        async with self._lock:
            producer = self._producer
            if producer is None:
                raise KafkaPublisherStateError("publisher is not started")
            last_offset: int | None = None
            for event in batch:
                metadata = await producer.send_and_wait(
                    MARKET_EVENTS_TOPIC,
                    value=canonical_envelope_bytes(event),
                    key=event.partition_key.encode("utf-8"),
                    partition=0,
                    headers=[
                        ("event-id", event.event_id.encode("ascii")),
                        ("schema-version", event.schema_version.encode("ascii")),
                    ],
                )
                if metadata.topic != MARKET_EVENTS_TOPIC or metadata.partition != 0:
                    raise KafkaPublishReceiptError(
                        "broker acknowledged the wrong topic or partition"
                    )
                offset = metadata.offset
                if (
                    isinstance(offset, bool)
                    or not isinstance(offset, int)
                    or offset < 0
                ):
                    raise KafkaPublishReceiptError(
                        "broker acknowledgement has an invalid offset"
                    )
                if last_offset is not None and offset <= last_offset:
                    raise KafkaPublishReceiptError(
                        "broker offsets did not advance inside the batch"
                    )
                last_offset = offset
            if last_offset is None:  # pragma: no cover - non-empty batch invariant
                raise RuntimeError("publisher accepted no Kafka records")
            return PublishReceipt(
                accepted_count=len(batch),
                partition_offsets=((PHASE1B_PARTITION_KEY, last_offset),),
            )
