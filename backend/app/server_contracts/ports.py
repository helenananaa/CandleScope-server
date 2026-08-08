"""Transport-neutral Phase 0 ports for the future distributed data spine."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from app.data_engine.market_data import MarketStreamKey

from .market_event import MarketEventEnvelopeV1


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field} cannot be blank")
    return normalized


def _non_negative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must be non-negative")
    return value


def _sha256(value: object, *, field: str) -> str:
    normalized = _required_text(value, field=field).lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return normalized


@dataclass(frozen=True, slots=True)
class MarketDataSnapshotRef:
    """Immutable query identity for one versioned data epoch manifest."""

    data_epoch: str
    snapshot_version: int
    manifest_uri: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "data_epoch",
            _required_text(self.data_epoch, field="data_epoch"),
        )
        snapshot_version = _non_negative_integer(
            self.snapshot_version,
            field="snapshot_version",
        )
        if snapshot_version == 0:
            raise ValueError("snapshot_version must be greater than zero")
        object.__setattr__(self, "snapshot_version", snapshot_version)
        object.__setattr__(
            self,
            "manifest_uri",
            _required_text(self.manifest_uri, field="manifest_uri"),
        )
        object.__setattr__(
            self,
            "manifest_sha256",
            _sha256(self.manifest_sha256, field="manifest_sha256"),
        )


@dataclass(frozen=True, slots=True)
class MarketEventCursor:
    """Opaque page cursor cryptographically bound to one snapshot manifest."""

    value: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _required_text(self.value, field="cursor"))
        object.__setattr__(
            self,
            "manifest_sha256",
            _sha256(self.manifest_sha256, field="cursor.manifest_sha256"),
        )


@dataclass(frozen=True, slots=True)
class MarketEventRange:
    """Auditable coverage summary for one logical market stream."""

    partition_key: str
    start_event_time_ms: int
    end_event_time_ms: int
    event_count: int
    sequence_start: int | None = None
    sequence_end: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "partition_key",
            _required_text(self.partition_key, field="partition_key"),
        )
        for field in ("start_event_time_ms", "end_event_time_ms", "event_count"):
            object.__setattr__(
                self,
                field,
                _non_negative_integer(getattr(self, field), field=field),
            )
        if self.end_event_time_ms < self.start_event_time_ms:
            raise ValueError(
                "end_event_time_ms must be greater than or equal to start_event_time_ms"
            )
        if (self.sequence_start is None) != (self.sequence_end is None):
            raise ValueError(
                "sequence_start and sequence_end must be supplied together"
            )
        if self.sequence_start is not None and self.sequence_end is not None:
            object.__setattr__(
                self,
                "sequence_start",
                _non_negative_integer(self.sequence_start, field="sequence_start"),
            )
            object.__setattr__(
                self,
                "sequence_end",
                _non_negative_integer(self.sequence_end, field="sequence_end"),
            )
            if self.sequence_end < self.sequence_start:
                raise ValueError(
                    "sequence_end must be greater than or equal to sequence_start"
                )
        if self.event_count == 0 and self.sequence_start is not None:
            raise ValueError("an empty event range cannot declare sequence coverage")


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    """Acknowledgement returned only after the event log accepts a batch."""

    accepted_count: int
    partition_offsets: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class ArchiveCommit:
    """Immutable manifest commit, independent of S3/MinIO clients."""

    accepted_count: int
    snapshot: MarketDataSnapshotRef
    object_uri: str
    content_sha256: str
    covered_ranges: tuple[MarketEventRange, ...]

    def __post_init__(self) -> None:
        accepted_count = _non_negative_integer(
            self.accepted_count,
            field="accepted_count",
        )
        if accepted_count == 0:
            raise ValueError("archive commits must contain at least one event")
        object.__setattr__(self, "accepted_count", accepted_count)
        if not isinstance(self.snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        object.__setattr__(
            self,
            "object_uri",
            _required_text(self.object_uri, field="object_uri"),
        )
        object.__setattr__(
            self,
            "content_sha256",
            _sha256(self.content_sha256, field="content_sha256"),
        )
        ranges = tuple(self.covered_ranges)
        if not ranges:
            raise ValueError("covered_ranges cannot be empty")
        if any(not isinstance(item, MarketEventRange) for item in ranges):
            raise TypeError("covered_ranges must contain MarketEventRange values")
        if any(item.event_count == 0 for item in ranges):
            raise ValueError("archive covered ranges cannot be empty")
        if sum(item.event_count for item in ranges) != accepted_count:
            raise ValueError("accepted_count must equal covered range event counts")
        object.__setattr__(self, "covered_ranges", ranges)


@dataclass(frozen=True, slots=True)
class MarketEventPage:
    """Bounded query result pinned to one immutable snapshot manifest."""

    snapshot: MarketDataSnapshotRef
    events: tuple[MarketEventEnvelopeV1, ...]
    covered_range: MarketEventRange
    next_cursor: MarketEventCursor | None

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        events = tuple(self.events)
        if any(not isinstance(item, MarketEventEnvelopeV1) for item in events):
            raise TypeError("events must contain MarketEventEnvelopeV1 values")
        object.__setattr__(self, "events", events)
        if not isinstance(self.covered_range, MarketEventRange):
            raise TypeError("covered_range must be a MarketEventRange")
        if self.covered_range.event_count != len(events):
            raise ValueError("covered_range.event_count must equal the page size")
        if any(
            event.partition_key != self.covered_range.partition_key for event in events
        ):
            raise ValueError("page events must match covered_range.partition_key")
        if any(
            not (
                self.covered_range.start_event_time_ms
                <= event.event_time_ms
                <= self.covered_range.end_event_time_ms
            )
            for event in events
        ):
            raise ValueError("page events must be inside covered_range event time")
        if self.next_cursor is not None:
            if not isinstance(self.next_cursor, MarketEventCursor):
                raise TypeError("next_cursor must be a MarketEventCursor")
            if self.next_cursor.manifest_sha256 != self.snapshot.manifest_sha256:
                raise ValueError("next_cursor must be bound to the page snapshot")


@runtime_checkable
class MarketEventPublisher(Protocol):
    """Append ordered batches to the durable logical event log."""

    async def publish(
        self,
        events: Sequence[MarketEventEnvelopeV1],
    ) -> PublishReceipt: ...


@runtime_checkable
class MarketEventArchive(Protocol):
    """Write immutable event batches to the cold archive."""

    async def append(
        self,
        events: Sequence[MarketEventEnvelopeV1],
        *,
        data_epoch: str,
        snapshot_version: int,
    ) -> ArchiveCommit: ...


@runtime_checkable
class MarketEventQuery(Protocol):
    """Read one stream by event time without exposing a vendor client."""

    async def query(
        self,
        *,
        snapshot: MarketDataSnapshotRef,
        stream: MarketStreamKey,
        start_event_time_ms: int,
        end_event_time_ms: int,
        limit: int,
        cursor: MarketEventCursor | None = None,
    ) -> MarketEventPage: ...
