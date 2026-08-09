"""Immutable, versioned Parquet manifest contract for the server archive."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import rfc8785

from .ports import MarketDataSnapshotRef

MARKET_DATA_MANIFEST_SCHEMA_VERSION = "market-data-manifest.v1"
MARKET_EVENT_PARQUET_SCHEMA_VERSION = "market-event-parquet.v1"
_MANIFEST_FIELDS = {
    "schema_version",
    "data_epoch",
    "snapshot_version",
    "parent_snapshot",
    "segment",
}
_SEGMENT_FIELDS = {
    "object_uri",
    "content_sha256",
    "byte_size",
    "format",
    "parquet_schema_version",
    "parquet_schema_sha256",
    "row_count",
    "partition_key",
    "kafka_topic",
    "kafka_partition",
    "first_kafka_offset",
    "last_kafka_offset",
    "start_event_time_ms",
    "end_event_time_ms",
    "sequence_start",
    "sequence_end",
    "envelope_sha256s",
}
_SNAPSHOT_FIELDS = {
    "data_epoch",
    "snapshot_version",
    "manifest_uri",
    "manifest_sha256",
}


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _positive_int(value: object, *, field: str) -> int:
    value = _non_negative_int(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be positive")
    return value


def _sha256(value: object, *, field: str) -> str:
    value = _required_text(value, field=field).lower()
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value


@dataclass(frozen=True, slots=True)
class ParquetArchiveSegmentV1:
    object_uri: str
    content_sha256: str
    byte_size: int
    parquet_schema_sha256: str
    row_count: int
    partition_key: str
    kafka_topic: str
    kafka_partition: int
    first_kafka_offset: int
    last_kafka_offset: int
    start_event_time_ms: int
    end_event_time_ms: int
    envelope_sha256s: tuple[str, ...]
    sequence_start: int | None = None
    sequence_end: int | None = None
    format: str = "parquet"
    parquet_schema_version: str = MARKET_EVENT_PARQUET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for field in ("object_uri", "partition_key", "kafka_topic"):
            object.__setattr__(
                self,
                field,
                _required_text(getattr(self, field), field=field),
            )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, field="content_sha256"),
        )
        object.__setattr__(
            self,
            "parquet_schema_sha256",
            _sha256(self.parquet_schema_sha256, field="parquet_schema_sha256"),
        )
        if self.format != "parquet":
            raise ValueError("format must be 'parquet'")
        if self.parquet_schema_version != MARKET_EVENT_PARQUET_SCHEMA_VERSION:
            raise ValueError(
                f"parquet_schema_version must be {MARKET_EVENT_PARQUET_SCHEMA_VERSION!r}"
            )
        object.__setattr__(
            self, "byte_size", _positive_int(self.byte_size, field="byte_size")
        )
        object.__setattr__(
            self, "row_count", _positive_int(self.row_count, field="row_count")
        )
        for field in (
            "kafka_partition",
            "first_kafka_offset",
            "last_kafka_offset",
            "start_event_time_ms",
            "end_event_time_ms",
        ):
            object.__setattr__(
                self,
                field,
                _non_negative_int(getattr(self, field), field=field),
            )
        if self.last_kafka_offset < self.first_kafka_offset:
            raise ValueError("last_kafka_offset must not precede first_kafka_offset")
        if self.row_count != self.last_kafka_offset - self.first_kafka_offset + 1:
            raise ValueError("row_count must cover every Kafka offset in the segment")
        if self.end_event_time_ms < self.start_event_time_ms:
            raise ValueError("end_event_time_ms must not precede start_event_time_ms")
        if (self.sequence_start is None) != (self.sequence_end is None):
            raise ValueError(
                "sequence_start and sequence_end must be supplied together"
            )
        if self.sequence_start is not None and self.sequence_end is not None:
            object.__setattr__(
                self,
                "sequence_start",
                _non_negative_int(self.sequence_start, field="sequence_start"),
            )
            object.__setattr__(
                self,
                "sequence_end",
                _non_negative_int(self.sequence_end, field="sequence_end"),
            )
            if self.sequence_end < self.sequence_start:
                raise ValueError("sequence_end must not precede sequence_start")
        hashes = tuple(
            _sha256(value, field="envelope_sha256s item")
            for value in self.envelope_sha256s
        )
        if len(hashes) != self.row_count:
            raise ValueError("envelope_sha256s must contain one hash per row")
        object.__setattr__(self, "envelope_sha256s", hashes)

    def to_wire(self) -> dict[str, Any]:
        return {
            "object_uri": self.object_uri,
            "content_sha256": self.content_sha256,
            "byte_size": self.byte_size,
            "format": self.format,
            "parquet_schema_version": self.parquet_schema_version,
            "parquet_schema_sha256": self.parquet_schema_sha256,
            "row_count": self.row_count,
            "partition_key": self.partition_key,
            "kafka_topic": self.kafka_topic,
            "kafka_partition": self.kafka_partition,
            "first_kafka_offset": self.first_kafka_offset,
            "last_kafka_offset": self.last_kafka_offset,
            "start_event_time_ms": self.start_event_time_ms,
            "end_event_time_ms": self.end_event_time_ms,
            "sequence_start": self.sequence_start,
            "sequence_end": self.sequence_end,
            "envelope_sha256s": list(self.envelope_sha256s),
        }

    @classmethod
    def from_wire(cls, value: Mapping[str, Any]) -> ParquetArchiveSegmentV1:
        _require_exact_fields(value, _SEGMENT_FIELDS, field="segment")
        return cls(**{field: value[field] for field in _SEGMENT_FIELDS})  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MarketDataManifestV1:
    data_epoch: str
    snapshot_version: int
    segment: ParquetArchiveSegmentV1
    parent_snapshot: MarketDataSnapshotRef | None = None
    schema_version: str = MARKET_DATA_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MARKET_DATA_MANIFEST_SCHEMA_VERSION:
            raise ValueError(
                f"schema_version must be {MARKET_DATA_MANIFEST_SCHEMA_VERSION!r}"
            )
        object.__setattr__(
            self,
            "data_epoch",
            _required_text(self.data_epoch, field="data_epoch"),
        )
        object.__setattr__(
            self,
            "snapshot_version",
            _positive_int(self.snapshot_version, field="snapshot_version"),
        )
        if not isinstance(self.segment, ParquetArchiveSegmentV1):
            raise TypeError("segment must be a ParquetArchiveSegmentV1")
        if self.snapshot_version != self.segment.last_kafka_offset + 1:
            raise ValueError("snapshot_version must equal last_kafka_offset + 1")
        parent = self.parent_snapshot
        if self.segment.first_kafka_offset == 0:
            if parent is not None:
                raise ValueError("the first archive segment cannot have a parent")
        else:
            if not isinstance(parent, MarketDataSnapshotRef):
                raise ValueError(
                    "non-initial archive segments require a parent snapshot"
                )
            if parent.data_epoch != self.data_epoch:
                raise ValueError("parent snapshot belongs to another data epoch")
            if parent.snapshot_version != self.segment.first_kafka_offset:
                raise ValueError(
                    "parent snapshot_version must equal first_kafka_offset"
                )

    def to_wire(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "data_epoch": self.data_epoch,
            "snapshot_version": self.snapshot_version,
            "parent_snapshot": (
                None
                if self.parent_snapshot is None
                else {
                    "data_epoch": self.parent_snapshot.data_epoch,
                    "snapshot_version": self.parent_snapshot.snapshot_version,
                    "manifest_uri": self.parent_snapshot.manifest_uri,
                    "manifest_sha256": self.parent_snapshot.manifest_sha256,
                }
            ),
            "segment": self.segment.to_wire(),
        }

    def canonical_bytes(self) -> bytes:
        return rfc8785.dumps(self.to_wire())

    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def from_wire(cls, value: Mapping[str, Any]) -> MarketDataManifestV1:
        _require_exact_fields(value, _MANIFEST_FIELDS, field="manifest")
        parent_value = value["parent_snapshot"]
        parent: MarketDataSnapshotRef | None
        if parent_value is None:
            parent = None
        else:
            if not isinstance(parent_value, Mapping):
                raise TypeError("parent_snapshot must be an object or null")
            _require_exact_fields(
                parent_value, _SNAPSHOT_FIELDS, field="parent_snapshot"
            )
            parent = MarketDataSnapshotRef(
                data_epoch=parent_value["data_epoch"],  # type: ignore[arg-type]
                snapshot_version=parent_value["snapshot_version"],  # type: ignore[arg-type]
                manifest_uri=parent_value["manifest_uri"],  # type: ignore[arg-type]
                manifest_sha256=parent_value["manifest_sha256"],  # type: ignore[arg-type]
            )
        segment_value = value["segment"]
        if not isinstance(segment_value, Mapping):
            raise TypeError("segment must be an object")
        return cls(
            schema_version=value["schema_version"],  # type: ignore[arg-type]
            data_epoch=value["data_epoch"],  # type: ignore[arg-type]
            snapshot_version=value["snapshot_version"],  # type: ignore[arg-type]
            parent_snapshot=parent,
            segment=ParquetArchiveSegmentV1.from_wire(segment_value),
        )


def parse_manifest_bytes(value: bytes) -> MarketDataManifestV1:
    import json

    if not isinstance(value, bytes) or not value:
        raise ValueError("manifest must be non-empty bytes")
    try:
        wire = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("manifest is not valid UTF-8 JSON") from exc
    if not isinstance(wire, Mapping):
        raise TypeError("manifest must be a JSON object")
    manifest = MarketDataManifestV1.from_wire(wire)
    if manifest.canonical_bytes() != value:
        raise ValueError("manifest is not canonical RFC 8785 JSON")
    return manifest


def _require_exact_fields(
    value: Mapping[str, Any],
    expected: set[str],
    *,
    field: str,
) -> None:
    actual = set(value)
    missing = expected - actual
    unknown = actual - expected
    if missing:
        raise ValueError(f"{field} is missing fields: {sorted(missing)}")
    if unknown:
        raise ValueError(f"{field} has unknown fields: {sorted(unknown)}")
