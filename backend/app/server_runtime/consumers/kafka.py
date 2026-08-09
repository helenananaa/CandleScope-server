"""Manual-commit Kafka consumer for frozen server projection boundaries."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from app.server_runtime.projection import (
    KafkaMarketEventRecord,
    decode_kafka_market_event,
    require_contiguous_records,
)
from app.server_runtime.publishers import MARKET_EVENTS_TOPIC


class KafkaConsumerStateError(RuntimeError):
    """The consumer was used outside its valid lifecycle."""


class KafkaConsumerTopicError(RuntimeError):
    """The physical topic no longer matches the Phase 1D contract."""


class KafkaConsumerOffsetError(RuntimeError):
    """The consumed offset stream has a gap or an unexpected starting point."""


@runtime_checkable
class KafkaMarketEventConsumer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def poll(
        self,
        *,
        timeout_ms: int,
        max_records: int,
    ) -> tuple[KafkaMarketEventRecord, ...]: ...

    async def commit_through(self, record: KafkaMarketEventRecord) -> None: ...


class KafkaMarketEventBatchConsumer:
    """Read partition zero and expose at most one uncommitted batch."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | Sequence[str],
        group_id: str,
        client_id: str,
        exact_batch_size: int | None = None,
        connection_options: Mapping[str, Any] | None = None,
        consumer_factory: Callable[..., Any] = AIOKafkaConsumer,
    ) -> None:
        servers = _servers(bootstrap_servers)
        group_id = _required_text(group_id, field="group_id")
        client_id = _required_text(client_id, field="client_id")
        options = dict(connection_options or {})
        forbidden = {
            "auto_offset_reset",
            "bootstrap_servers",
            "client_id",
            "enable_auto_commit",
            "group_id",
            "isolation_level",
        }
        conflict = forbidden.intersection(options)
        if conflict:
            raise ValueError(f"connection_options cannot override {sorted(conflict)}")
        self._bootstrap_servers = servers
        self._group_id = group_id
        self._client_id = client_id
        self._exact_batch_size = (
            None
            if exact_batch_size is None
            else _positive_int(exact_batch_size, field="exact_batch_size")
        )
        self._connection_options = options
        self._consumer_factory = consumer_factory
        self._consumer: Any | None = None
        self._lock = asyncio.Lock()
        self._topic_partition = TopicPartition(MARKET_EVENTS_TOPIC, 0)
        self._next_expected_offset: int | None = None
        self._pending_commit_offset: int | None = None
        self._buffer: list[KafkaMarketEventRecord] = []

    @property
    def started(self) -> bool:
        return self._consumer is not None

    async def start(self) -> None:
        async with self._lock:
            if self._consumer is not None:
                raise KafkaConsumerStateError("consumer is already started")
            consumer = self._consumer_factory(
                MARKET_EVENTS_TOPIC,
                bootstrap_servers=self._bootstrap_servers,
                group_id=self._group_id,
                client_id=self._client_id,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                isolation_level="read_committed",
                **self._connection_options,
            )
            try:
                await consumer.start()
                partitions = consumer.partitions_for_topic(MARKET_EVENTS_TOPIC)
                if partitions != {0}:
                    raise KafkaConsumerTopicError(
                        f"{MARKET_EVENTS_TOPIC!r} must have exactly partition 0; "
                        f"received {sorted(partitions) if partitions else partitions}"
                    )
            except BaseException:
                await consumer.stop()
                raise
            self._consumer = consumer
            self._next_expected_offset = None
            self._pending_commit_offset = None
            self._buffer.clear()

    async def stop(self) -> None:
        async with self._lock:
            consumer = self._consumer
            self._consumer = None
            self._next_expected_offset = None
            self._pending_commit_offset = None
            self._buffer.clear()
            if consumer is not None:
                await consumer.stop()

    async def poll(
        self,
        *,
        timeout_ms: int,
        max_records: int,
    ) -> tuple[KafkaMarketEventRecord, ...]:
        timeout_ms = _positive_int(timeout_ms, field="timeout_ms")
        max_records = _positive_int(max_records, field="max_records")
        if self._exact_batch_size is not None and max_records != self._exact_batch_size:
            raise ValueError("max_records must equal the configured exact_batch_size")
        consumer = self._require_consumer()
        if self._pending_commit_offset is not None:
            raise KafkaConsumerStateError(
                "previous batch must be committed before polling again"
            )
        requested_records = (
            max_records
            if self._exact_batch_size is None
            else self._exact_batch_size - len(self._buffer)
        )
        batches = await consumer.getmany(
            timeout_ms=timeout_ms,
            max_records=requested_records,
        )
        unexpected = [
            key
            for key, values in batches.items()
            if values and key != self._topic_partition
        ]
        if unexpected:
            raise KafkaConsumerTopicError(
                f"consumer returned unexpected partitions: {unexpected}"
            )
        decoded = tuple(
            decode_kafka_market_event(
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                key=record.key,
                value=record.value,
                headers=record.headers,
            )
            for record in batches.get(self._topic_partition, ())
        )
        if decoded:
            require_contiguous_records(decoded)
            expected = self._next_expected_offset
            if expected is None:
                committed = await consumer.committed(self._topic_partition)
                expected = 0 if committed is None else committed
            if decoded[0].offset != expected:
                raise KafkaConsumerOffsetError(
                    "market-event offset stream is not contiguous: "
                    f"expected {expected}, received {decoded[0].offset}"
                )
            self._next_expected_offset = decoded[-1].offset + 1
        if self._exact_batch_size is None:
            if decoded:
                self._pending_commit_offset = self._next_expected_offset
            return decoded
        self._buffer.extend(decoded)
        if len(self._buffer) < self._exact_batch_size:
            return ()
        if len(self._buffer) != self._exact_batch_size:
            raise KafkaConsumerStateError("exact batch buffer exceeded its boundary")
        result = tuple(self._buffer)
        self._buffer.clear()
        self._pending_commit_offset = self._next_expected_offset
        return result

    async def commit_through(self, record: KafkaMarketEventRecord) -> None:
        if not isinstance(record, KafkaMarketEventRecord):
            raise TypeError("record must be a KafkaMarketEventRecord")
        if (record.topic, record.partition) != (
            self._topic_partition.topic,
            self._topic_partition.partition,
        ):
            raise ValueError("record does not belong to the frozen topic partition")
        consumer = self._require_consumer()
        next_offset = record.offset + 1
        if self._pending_commit_offset != next_offset:
            raise KafkaConsumerOffsetError(
                "commit must cover the complete most recently polled batch"
            )
        await consumer.commit({self._topic_partition: next_offset})
        self._pending_commit_offset = None

    def _require_consumer(self) -> Any:
        consumer = self._consumer
        if consumer is None:
            raise KafkaConsumerStateError("consumer is not started")
        return consumer


def _servers(value: str | Sequence[str]) -> str | tuple[str, ...]:
    if isinstance(value, str):
        return _required_text(value, field="bootstrap_servers")
    servers = tuple(value)
    if not servers:
        raise ValueError("bootstrap_servers cannot be empty")
    return tuple(
        _required_text(server, field="bootstrap_servers") for server in servers
    )


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value
