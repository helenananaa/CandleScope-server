"""Validated Kafka-to-ClickHouse projection contracts for Phase 1D."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise
from typing import Protocol, runtime_checkable

from app.server_contracts import MarketEventEnvelopeV1
from app.server_runtime.publishers import (
    MARKET_EVENTS_TOPIC,
    PHASE1B_PARTITION_KEY,
    canonical_envelope_bytes,
)


class MarketEventRecordError(ValueError):
    """A Kafka record does not satisfy the frozen event-log contract."""


class ProjectionIntegrityError(RuntimeError):
    """Stored identity state is internally contradictory."""


@dataclass(frozen=True, slots=True)
class KafkaMarketEventRecord:
    topic: str
    partition: int
    offset: int
    envelope: MarketEventEnvelopeV1
    envelope_sha256: str
    envelope_bytes: bytes

    def __post_init__(self) -> None:
        if self.topic != MARKET_EVENTS_TOPIC:
            raise MarketEventRecordError("record belongs to the wrong Kafka topic")
        if self.partition != 0:
            raise MarketEventRecordError("record belongs to the wrong Kafka partition")
        if isinstance(self.offset, bool) or not isinstance(self.offset, int):
            raise TypeError("offset must be an integer")
        if self.offset < 0:
            raise MarketEventRecordError("offset must be non-negative")
        if not isinstance(self.envelope, MarketEventEnvelopeV1):
            raise TypeError("envelope must be a MarketEventEnvelopeV1")
        if self.envelope.partition_key != PHASE1B_PARTITION_KEY:
            raise MarketEventRecordError("record carries the wrong logical partition")
        if not isinstance(self.envelope_bytes, bytes):
            raise TypeError("envelope_bytes must be bytes")
        if canonical_envelope_bytes(self.envelope) != self.envelope_bytes:
            raise MarketEventRecordError(
                "envelope_bytes are not the canonical bytes of the envelope"
            )
        expected = hashlib.sha256(self.envelope_bytes).hexdigest()
        if self.envelope_sha256 != expected:
            raise MarketEventRecordError("envelope_sha256 does not match record bytes")


@dataclass(frozen=True, slots=True)
class ProjectionBatchResult:
    inserted_count: int
    duplicate_count: int
    conflict_count: int
    first_offset: int
    last_offset: int

    def __post_init__(self) -> None:
        for field in (
            "inserted_count",
            "duplicate_count",
            "conflict_count",
            "first_offset",
            "last_offset",
        ):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field} must be an integer")
            if value < 0:
                raise ValueError(f"{field} must be non-negative")
        if self.last_offset < self.first_offset:
            raise ValueError("last_offset must not precede first_offset")
        expected = self.last_offset - self.first_offset + 1
        if self.inserted_count + self.duplicate_count + self.conflict_count != expected:
            raise ValueError("projection counts must cover every contiguous record")


@runtime_checkable
class MarketEventProjector(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def apply_batch(
        self,
        records: Sequence[KafkaMarketEventRecord],
    ) -> ProjectionBatchResult: ...


def decode_kafka_market_event(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    headers: Sequence[tuple[str, bytes | None]],
) -> KafkaMarketEventRecord:
    """Parse one record and reject any wire drift before storage."""

    if topic != MARKET_EVENTS_TOPIC:
        raise MarketEventRecordError("record belongs to the wrong Kafka topic")
    if partition != 0:
        raise MarketEventRecordError("record belongs to the wrong Kafka partition")
    expected_key = PHASE1B_PARTITION_KEY.encode("utf-8")
    if key != expected_key:
        raise MarketEventRecordError("record key does not match the logical partition")
    if not isinstance(value, bytes) or not value:
        raise MarketEventRecordError("record value must be non-empty bytes")
    try:
        wire = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MarketEventRecordError("record value is not valid UTF-8 JSON") from exc
    try:
        envelope = MarketEventEnvelopeV1.from_wire(wire)
    except (TypeError, ValueError) as exc:
        raise MarketEventRecordError("record envelope is invalid") from exc
    if canonical_envelope_bytes(envelope) != value:
        raise MarketEventRecordError("record envelope is not canonical RFC 8785 JSON")

    header_map: dict[str, bytes | None] = {}
    for name, header_value in headers:
        if name in header_map:
            raise MarketEventRecordError(f"record repeats header {name!r}")
        header_map[name] = header_value
    if set(header_map) != {"event-id", "schema-version"}:
        raise MarketEventRecordError("record headers do not match the frozen contract")
    if header_map["event-id"] != envelope.event_id.encode("ascii"):
        raise MarketEventRecordError("event-id header does not match the envelope")
    if header_map["schema-version"] != envelope.schema_version.encode("ascii"):
        raise MarketEventRecordError(
            "schema-version header does not match the envelope"
        )

    return KafkaMarketEventRecord(
        topic=topic,
        partition=partition,
        offset=offset,
        envelope=envelope,
        envelope_sha256=hashlib.sha256(value).hexdigest(),
        envelope_bytes=value,
    )


def require_contiguous_records(
    records: Sequence[KafkaMarketEventRecord],
) -> tuple[KafkaMarketEventRecord, ...]:
    batch = tuple(records)
    if not batch:
        raise ValueError("projection batch cannot be empty")
    for previous, current in pairwise(batch):
        if current.offset != previous.offset + 1:
            raise MarketEventRecordError(
                f"Kafka offsets are not contiguous: {previous.offset}->{current.offset}"
            )
    return batch
