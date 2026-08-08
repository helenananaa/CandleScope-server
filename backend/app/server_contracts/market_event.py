"""Durable market-event wire contract used at process boundaries.

The existing ingestion MarketEvent remains the in-process normalized
model. Phase 1 will add an explicit adapter from that model into this strict,
immutable, versioned envelope before publishing to the server event log.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import rfc8785

from app.data_engine.market_data import DeliveryClass, MarketStreamKey

MARKET_EVENT_ENVELOPE_SCHEMA_VERSION = "market-event-envelope.v1"
PAYLOAD_CANONICALIZATION = "rfc8785"
_WIRE_FIELDS = {
    "schema_version",
    "event_id",
    "partition_key",
    "stream",
    "delivery_class",
    "source",
    "source_event_id",
    "sequence_start",
    "sequence_end",
    "previous_sequence",
    "producer_id",
    "producer_epoch",
    "event_time_ms",
    "received_at_ms",
    "published_at_ms",
    "payload_schema",
    "payload_canonicalization",
    "payload_sha256",
    "payload",
}
_STREAM_WIRE_FIELDS = {"exchange", "market_type", "symbol", "channel", "params"}


def _required_text(value: object, *, field: str, lowercase: bool = False) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} cannot be blank")
    return normalized.lower() if lowercase else normalized


def _non_negative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _materialize_json(value: Any, *, path: str = "payload") -> Any:
    if isinstance(value, Mapping):
        materialized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} object keys must be strings")
            materialized[key] = _materialize_json(item, path=f"{path}.{key}")
        return materialized
    elif isinstance(value, (list, tuple)):
        return [
            _materialize_json(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    return value


def canonical_payload_bytes(payload: Mapping[str, Any]) -> bytes:
    """Serialize one payload as RFC 8785 JSON Canonicalization Scheme bytes."""

    if not isinstance(payload, Mapping):
        raise TypeError("payload must be a JSON object")
    materialized = _materialize_json(payload)
    try:
        return rfc8785.dumps(materialized)
    except (rfc8785.CanonicalizationError, TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("payload must conform to RFC 8785/I-JSON") from exc


def canonical_payload_sha256(payload: Mapping[str, Any]) -> str:
    """Hash a payload using its RFC 8785 canonical UTF-8 representation."""

    return hashlib.sha256(canonical_payload_bytes(payload)).hexdigest()


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType(
            {key: _freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class MarketEventEnvelopeV1:
    """Immutable event-log payload with explicit identity and lineage."""

    event_id: str
    stream: MarketStreamKey
    delivery_class: DeliveryClass
    source: str
    producer_id: str
    producer_epoch: int
    event_time_ms: int
    received_at_ms: int
    published_at_ms: int
    payload_schema: str
    payload_sha256: str
    payload: Mapping[str, Any]
    source_event_id: str | None = None
    sequence_start: int | None = None
    sequence_end: int | None = None
    previous_sequence: int | None = None
    schema_version: str = MARKET_EVENT_ENVELOPE_SCHEMA_VERSION
    payload_canonicalization: str = PAYLOAD_CANONICALIZATION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_EVENT_ENVELOPE_SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must be {MARKET_EVENT_ENVELOPE_SCHEMA_VERSION!r}"
            )
        if self.payload_canonicalization != PAYLOAD_CANONICALIZATION:
            raise ValueError(
                f"payload_canonicalization must be {PAYLOAD_CANONICALIZATION!r}"
            )
        try:
            normalized_event_id = str(uuid.UUID(self.event_id))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("event_id must be a UUID") from exc
        object.__setattr__(self, "event_id", normalized_event_id)
        if not isinstance(self.stream, MarketStreamKey):
            raise TypeError("stream must be a MarketStreamKey")

        delivery_class = self.delivery_class
        if isinstance(delivery_class, str):
            try:
                delivery_class = DeliveryClass(delivery_class.strip().lower())
            except ValueError as exc:
                raise ValueError("delivery_class is unsupported") from exc
        if not isinstance(delivery_class, DeliveryClass):
            raise TypeError("delivery_class must be a DeliveryClass or string")
        object.__setattr__(self, "delivery_class", delivery_class)

        object.__setattr__(
            self,
            "source",
            _required_text(self.source, field="source", lowercase=True),
        )
        object.__setattr__(
            self,
            "producer_id",
            _required_text(self.producer_id, field="producer_id"),
        )
        object.__setattr__(
            self,
            "payload_schema",
            _required_text(self.payload_schema, field="payload_schema"),
        )
        object.__setattr__(
            self,
            "producer_epoch",
            _non_negative_integer(self.producer_epoch, field="producer_epoch"),
        )
        for field in ("event_time_ms", "received_at_ms", "published_at_ms"):
            object.__setattr__(
                self,
                field,
                _non_negative_integer(getattr(self, field), field=field),
            )

        source_event_id = self.source_event_id
        if source_event_id is not None:
            source_event_id = _required_text(source_event_id, field="source_event_id")
            object.__setattr__(self, "source_event_id", source_event_id)

        for field in ("sequence_start", "sequence_end", "previous_sequence"):
            value = getattr(self, field)
            if value is not None:
                object.__setattr__(
                    self,
                    field,
                    _non_negative_integer(value, field=field),
                )
        if self.sequence_end is not None and self.sequence_start is None:
            raise ValueError("sequence_end requires sequence_start")
        if (
            self.sequence_start is not None
            and self.sequence_end is not None
            and self.sequence_end < self.sequence_start
        ):
            raise ValueError(
                "sequence_end must be greater than or equal to sequence_start"
            )
        if self.delivery_class is DeliveryClass.ORDERED_DELTA and (
            self.sequence_start is None or self.sequence_end is None
        ):
            raise ValueError("ordered_delta events require a sequence range")
        if (
            self.delivery_class
            in {
                DeliveryClass.APPEND,
                DeliveryClass.ORDERED_DELTA,
            }
            and self.source_event_id is None
            and self.sequence_start is None
        ):
            raise ValueError(
                "append and ordered_delta events require a durable source identity"
            )

        canonical_bytes = canonical_payload_bytes(self.payload)
        materialized_payload = json.loads(canonical_bytes.decode("utf-8"))
        object.__setattr__(self, "payload", _freeze_json(materialized_payload))
        expected_hash = hashlib.sha256(canonical_bytes).hexdigest()
        supplied_hash = _required_text(
            self.payload_sha256,
            field="payload_sha256",
            lowercase=True,
        )
        if supplied_hash != expected_hash:
            raise ValueError("payload_sha256 does not match canonical payload")
        object.__setattr__(self, "payload_sha256", supplied_hash)

    @property
    def partition_key(self) -> str:
        """Stable routing key; one logical stream is never randomly split."""

        return self.stream.topic

    @classmethod
    def build(
        cls,
        *,
        stream: MarketStreamKey,
        delivery_class: DeliveryClass | str,
        source: str,
        producer_id: str,
        producer_epoch: int,
        event_time_ms: int,
        received_at_ms: int,
        published_at_ms: int,
        payload_schema: str,
        payload: Mapping[str, Any],
        event_id: str | None = None,
        source_event_id: str | None = None,
        sequence_start: int | None = None,
        sequence_end: int | None = None,
        previous_sequence: int | None = None,
    ) -> MarketEventEnvelopeV1:
        """Create a validated envelope and compute its integrity hash."""

        return cls(
            event_id=str(uuid.uuid4()) if event_id is None else event_id,
            stream=stream,
            delivery_class=delivery_class,  # type: ignore[arg-type]
            source=source,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            event_time_ms=event_time_ms,
            received_at_ms=received_at_ms,
            published_at_ms=published_at_ms,
            payload_schema=payload_schema,
            payload_sha256=canonical_payload_sha256(payload),
            payload=payload,
            source_event_id=source_event_id,
            sequence_start=sequence_start,
            sequence_end=sequence_end,
            previous_sequence=previous_sequence,
        )

    def to_wire(self) -> dict[str, Any]:
        """Return the strict JSON object carried by the event log."""

        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "partition_key": self.partition_key,
            "stream": self.stream.to_dict(),
            "delivery_class": self.delivery_class.value,
            "source": self.source,
            "source_event_id": self.source_event_id,
            "sequence_start": self.sequence_start,
            "sequence_end": self.sequence_end,
            "previous_sequence": self.previous_sequence,
            "producer_id": self.producer_id,
            "producer_epoch": self.producer_epoch,
            "event_time_ms": self.event_time_ms,
            "received_at_ms": self.received_at_ms,
            "published_at_ms": self.published_at_ms,
            "payload_schema": self.payload_schema,
            "payload_canonicalization": self.payload_canonicalization,
            "payload_sha256": self.payload_sha256,
            "payload": _thaw_json(self.payload),
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, Any]) -> MarketEventEnvelopeV1:
        """Parse a wire object, rejecting missing or unknown fields."""

        if not isinstance(value, Mapping):
            raise TypeError("market event wire value must be an object")
        fields = set(value)
        missing = _WIRE_FIELDS - fields
        unknown = fields - _WIRE_FIELDS
        if missing:
            raise ValueError(
                f"market event wire value is missing fields: {sorted(missing)}"
            )
        if unknown:
            raise ValueError(
                f"market event wire value has unknown fields: {sorted(unknown)}"
            )
        stream_value = value["stream"]
        if not isinstance(stream_value, Mapping):
            raise TypeError("stream must be an object")
        stream_fields = set(stream_value)
        stream_missing = _STREAM_WIRE_FIELDS - stream_fields
        stream_unknown = stream_fields - _STREAM_WIRE_FIELDS
        if stream_missing:
            raise ValueError(f"stream is missing fields: {sorted(stream_missing)}")
        if stream_unknown:
            raise ValueError(f"stream has unknown fields: {sorted(stream_unknown)}")
        stream = MarketStreamKey.build(
            stream_value.get("exchange"),  # type: ignore[arg-type]
            stream_value.get("market_type"),  # type: ignore[arg-type]
            stream_value.get("symbol"),  # type: ignore[arg-type]
            stream_value.get("channel"),  # type: ignore[arg-type]
            stream_value.get("params"),
        )
        if value["partition_key"] != stream.topic:
            raise ValueError("partition_key does not match the canonical stream key")
        payload = value["payload"]
        if not isinstance(payload, Mapping):
            raise TypeError("payload must be an object")
        return cls(
            schema_version=value["schema_version"],  # type: ignore[arg-type]
            event_id=value["event_id"],  # type: ignore[arg-type]
            stream=stream,
            delivery_class=value["delivery_class"],  # type: ignore[arg-type]
            source=value["source"],  # type: ignore[arg-type]
            source_event_id=value["source_event_id"],  # type: ignore[arg-type]
            sequence_start=value["sequence_start"],  # type: ignore[arg-type]
            sequence_end=value["sequence_end"],  # type: ignore[arg-type]
            previous_sequence=value["previous_sequence"],  # type: ignore[arg-type]
            producer_id=value["producer_id"],  # type: ignore[arg-type]
            producer_epoch=value["producer_epoch"],  # type: ignore[arg-type]
            event_time_ms=value["event_time_ms"],  # type: ignore[arg-type]
            received_at_ms=value["received_at_ms"],  # type: ignore[arg-type]
            published_at_ms=value["published_at_ms"],  # type: ignore[arg-type]
            payload_schema=value["payload_schema"],  # type: ignore[arg-type]
            payload_canonicalization=value["payload_canonicalization"],  # type: ignore[arg-type]
            payload_sha256=value["payload_sha256"],  # type: ignore[arg-type]
            payload=payload,
        )
