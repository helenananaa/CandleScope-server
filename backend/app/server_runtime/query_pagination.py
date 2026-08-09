"""Backend-independent canonical fact projection and snapshot pagination."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass

import rfc8785

from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import (
    MarketDataSnapshotRef,
    MarketEventCursor,
    MarketEventEnvelopeV1,
    MarketEventPage,
    MarketEventRange,
)
from app.server_runtime.publishers import canonical_envelope_bytes

MARKET_EVENT_CURSOR_SCHEMA_VERSION = "market-event-cursor.v1"
_CURSOR_FIELDS = {
    "schema_version",
    "manifest_sha256",
    "partition_key",
    "start_event_time_ms",
    "end_event_time_ms",
    "next_index",
}


class SnapshotQueryIntegrityError(RuntimeError):
    """A snapshot row or cursor cannot be reconciled safely."""


class SnapshotQueryCursorError(SnapshotQueryIntegrityError):
    """A client cursor is malformed or bound to another immutable query."""


@dataclass(frozen=True, slots=True)
class SnapshotQueryRow:
    envelope: MarketEventEnvelopeV1
    envelope_sha256: str
    envelope_bytes: bytes
    kafka_partition: int
    kafka_offset: int

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, MarketEventEnvelopeV1):
            raise TypeError("envelope must be a MarketEventEnvelopeV1")
        if not isinstance(self.envelope_bytes, bytes):
            raise TypeError("envelope_bytes must be bytes")
        if canonical_envelope_bytes(self.envelope) != self.envelope_bytes:
            raise SnapshotQueryIntegrityError(
                "snapshot row envelope bytes are not canonical"
            )
        expected = hashlib.sha256(self.envelope_bytes).hexdigest()
        if self.envelope_sha256 != expected:
            raise SnapshotQueryIntegrityError(
                "snapshot row envelope SHA-256 does not match its bytes"
            )
        for field in ("kafka_partition", "kafka_offset"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{field} must be an integer")
            if value < 0:
                raise ValueError(f"{field} must be non-negative")


@dataclass(frozen=True, slots=True)
class CanonicalFactRows:
    rows: tuple[SnapshotQueryRow, ...]
    exact_duplicate_count: int
    integrity_conflict_count: int


def canonical_fact_rows(rows: Sequence[SnapshotQueryRow]) -> CanonicalFactRows:
    """Apply the same first-envelope-wins identity semantics as Phase 1D."""

    ordered = sorted(rows, key=lambda row: (row.kafka_partition, row.kafka_offset))
    coordinates = [(row.kafka_partition, row.kafka_offset) for row in ordered]
    if len(coordinates) != len(set(coordinates)):
        raise SnapshotQueryIntegrityError(
            "snapshot contains duplicate Kafka coordinates"
        )
    identities: dict[tuple[str, str], SnapshotQueryRow] = {}
    facts: list[SnapshotQueryRow] = []
    duplicates = 0
    conflicts = 0
    for row in ordered:
        identity = (row.envelope.partition_key, row.envelope.event_id)
        existing = identities.get(identity)
        if existing is None:
            identities[identity] = row
            facts.append(row)
        elif (
            existing.envelope_sha256 == row.envelope_sha256
            and existing.envelope_bytes == row.envelope_bytes
        ):
            duplicates += 1
        else:
            conflicts += 1
    facts.sort(
        key=lambda row: (
            row.envelope.event_time_ms,
            row.kafka_partition,
            row.kafka_offset,
        )
    )
    return CanonicalFactRows(
        rows=tuple(facts),
        exact_duplicate_count=duplicates,
        integrity_conflict_count=conflicts,
    )


def paginate_snapshot_rows(
    rows: Sequence[SnapshotQueryRow],
    *,
    snapshot: MarketDataSnapshotRef,
    stream: MarketStreamKey,
    start_event_time_ms: int,
    end_event_time_ms: int,
    limit: int,
    max_page_rows: int,
    cursor: MarketEventCursor | None,
) -> MarketEventPage:
    if not isinstance(snapshot, MarketDataSnapshotRef):
        raise TypeError("snapshot must be a MarketDataSnapshotRef")
    if not isinstance(stream, MarketStreamKey):
        raise TypeError("stream must be a MarketStreamKey")
    start_event_time_ms = _non_negative_int(
        start_event_time_ms,
        field="start_event_time_ms",
    )
    end_event_time_ms = _non_negative_int(
        end_event_time_ms,
        field="end_event_time_ms",
    )
    if end_event_time_ms < start_event_time_ms:
        raise ValueError("end_event_time_ms must not precede start_event_time_ms")
    limit = _positive_int(limit, field="limit")
    max_page_rows = _positive_int(max_page_rows, field="max_page_rows")
    if limit > max_page_rows:
        raise ValueError(f"limit cannot exceed {max_page_rows}")
    start_index = _decode_cursor(
        cursor,
        snapshot=snapshot,
        partition_key=stream.topic,
        start_event_time_ms=start_event_time_ms,
        end_event_time_ms=end_event_time_ms,
    )
    selected = [
        row
        for row in rows
        if row.envelope.partition_key == stream.topic
        and start_event_time_ms <= row.envelope.event_time_ms <= end_event_time_ms
    ]
    selected.sort(
        key=lambda row: (
            row.envelope.event_time_ms,
            row.kafka_partition,
            row.kafka_offset,
        )
    )
    if start_index > len(selected):
        raise SnapshotQueryIntegrityError(
            "cursor points beyond the immutable query result"
        )
    page_rows = selected[start_index : start_index + limit]
    next_index = start_index + len(page_rows)
    next_cursor = (
        _encode_cursor(
            snapshot=snapshot,
            partition_key=stream.topic,
            start_event_time_ms=start_event_time_ms,
            end_event_time_ms=end_event_time_ms,
            next_index=next_index,
        )
        if next_index < len(selected)
        else None
    )
    events = tuple(row.envelope for row in page_rows)
    return MarketEventPage(
        snapshot=snapshot,
        events=events,
        covered_range=_event_range(
            partition_key=stream.topic,
            events=events,
            empty_bounds=(start_event_time_ms, end_event_time_ms),
        ),
        next_cursor=next_cursor,
    )


def _event_range(
    *,
    partition_key: str,
    events: Sequence[MarketEventEnvelopeV1],
    empty_bounds: tuple[int, int],
) -> MarketEventRange:
    if not events:
        return MarketEventRange(
            partition_key=partition_key,
            start_event_time_ms=empty_bounds[0],
            end_event_time_ms=empty_bounds[1],
            event_count=0,
        )
    times = [event.event_time_ms for event in events]
    sequence_values = [(event.sequence_start, event.sequence_end) for event in events]
    complete = all(
        start is not None and end is not None for start, end in sequence_values
    )
    return MarketEventRange(
        partition_key=partition_key,
        start_event_time_ms=min(times),
        end_event_time_ms=max(times),
        event_count=len(events),
        sequence_start=(
            min(start for start, _ in sequence_values if start is not None)
            if complete
            else None
        ),
        sequence_end=(
            max(end for _, end in sequence_values if end is not None)
            if complete
            else None
        ),
    )


def _encode_cursor(
    *,
    snapshot: MarketDataSnapshotRef,
    partition_key: str,
    start_event_time_ms: int,
    end_event_time_ms: int,
    next_index: int,
) -> MarketEventCursor:
    wire = {
        "schema_version": MARKET_EVENT_CURSOR_SCHEMA_VERSION,
        "manifest_sha256": snapshot.manifest_sha256,
        "partition_key": partition_key,
        "start_event_time_ms": start_event_time_ms,
        "end_event_time_ms": end_event_time_ms,
        "next_index": next_index,
    }
    value = base64.urlsafe_b64encode(rfc8785.dumps(wire)).decode("ascii").rstrip("=")
    return MarketEventCursor(
        value=value,
        manifest_sha256=snapshot.manifest_sha256,
    )


def _decode_cursor(
    cursor: MarketEventCursor | None,
    *,
    snapshot: MarketDataSnapshotRef,
    partition_key: str,
    start_event_time_ms: int,
    end_event_time_ms: int,
) -> int:
    if cursor is None:
        return 0
    if not isinstance(cursor, MarketEventCursor):
        raise TypeError("cursor must be a MarketEventCursor")
    if cursor.manifest_sha256 != snapshot.manifest_sha256:
        raise SnapshotQueryCursorError("cursor belongs to another manifest")
    try:
        padding = "=" * (-len(cursor.value) % 4)
        raw = base64.b64decode(
            cursor.value + padding,
            altchars=b"-_",
            validate=True,
        )
        wire = json.loads(raw.decode("utf-8"))
        if not isinstance(wire, dict) or set(wire) != _CURSOR_FIELDS:
            raise ValueError("cursor fields drifted")
        if rfc8785.dumps(wire) != raw:
            raise ValueError("cursor is not canonical")
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise SnapshotQueryCursorError("cursor is malformed") from exc
    expected = {
        "schema_version": MARKET_EVENT_CURSOR_SCHEMA_VERSION,
        "manifest_sha256": snapshot.manifest_sha256,
        "partition_key": partition_key,
        "start_event_time_ms": start_event_time_ms,
        "end_event_time_ms": end_event_time_ms,
    }
    if any(wire.get(name) != value for name, value in expected.items()):
        raise SnapshotQueryCursorError(
            "cursor query binding does not match the request"
        )
    return _non_negative_int(wire.get("next_index"), field="cursor.next_index")


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
