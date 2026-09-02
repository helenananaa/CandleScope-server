"""Strict health-wire codecs for Phase 1S process-restart reconciliation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.server_contracts import MarketDataSnapshotRef
from app.server_runtime.archive_health import ArchiveWriterHealth, ArchiveWriterState
from app.server_runtime.health import CollectorHealth, CollectorState
from app.server_runtime.writer_health import (
    ClickHouseWriterHealth,
    ClickHouseWriterState,
)

_COLLECTOR_FIELDS = frozenset(
    {
        "state",
        "ready",
        "reason",
        "owner_id",
        "source_health",
        "producer_epoch",
        "lease_expires_at_ms",
        "last_sequence",
        "last_partition_offset",
        "pending_event_id",
        "events_published",
        "heartbeat_successes",
        "heartbeat_failures",
        "started_at_ms",
        "updated_at_ms",
        "terminal_error",
    }
)
_WRITER_FIELDS = frozenset(
    {
        "state",
        "ready",
        "reason",
        "owner_id",
        "kafka_group_id",
        "committed_next_offset",
        "batches_committed",
        "inserted_events",
        "duplicate_events",
        "conflict_events",
        "started_at_ms",
        "updated_at_ms",
        "terminal_error",
    }
)
_ARCHIVE_FIELDS = frozenset(
    {
        "state",
        "ready",
        "reason",
        "owner_id",
        "kafka_group_id",
        "data_epoch",
        "committed_next_offset",
        "segments_committed",
        "events_archived",
        "current_snapshot",
        "started_at_ms",
        "updated_at_ms",
        "terminal_error",
    }
)
_SNAPSHOT_FIELDS = frozenset(
    {"data_epoch", "snapshot_version", "manifest_uri", "manifest_sha256"}
)


class ChainHealthWireError(ValueError):
    """A process health snapshot is not a strict role wire document."""


def collector_health_from_wire(payload: Mapping[str, Any]) -> CollectorHealth:
    values = _exact_object(payload, _COLLECTOR_FIELDS, role="collector")
    return CollectorHealth(
        state=_enum(CollectorState, values["state"], field="collector.state"),
        ready=_bool(values["ready"], field="collector.ready"),
        reason=_text(values["reason"], field="collector.reason"),
        owner_id=_text(values["owner_id"], field="collector.owner_id"),
        source_health=_optional_text(
            values["source_health"], field="collector.source_health"
        ),
        producer_epoch=_optional_int(
            values["producer_epoch"], field="collector.producer_epoch"
        ),
        lease_expires_at_ms=_optional_int(
            values["lease_expires_at_ms"],
            field="collector.lease_expires_at_ms",
        ),
        last_sequence=_optional_int(
            values["last_sequence"], field="collector.last_sequence"
        ),
        last_partition_offset=_optional_int(
            values["last_partition_offset"],
            field="collector.last_partition_offset",
        ),
        pending_event_id=_optional_text(
            values["pending_event_id"],
            field="collector.pending_event_id",
        ),
        events_published=_non_negative_int(
            values["events_published"],
            field="collector.events_published",
        ),
        heartbeat_successes=_non_negative_int(
            values["heartbeat_successes"],
            field="collector.heartbeat_successes",
        ),
        heartbeat_failures=_non_negative_int(
            values["heartbeat_failures"],
            field="collector.heartbeat_failures",
        ),
        started_at_ms=_non_negative_int(
            values["started_at_ms"], field="collector.started_at_ms"
        ),
        updated_at_ms=_non_negative_int(
            values["updated_at_ms"], field="collector.updated_at_ms"
        ),
        terminal_error=_optional_text(
            values["terminal_error"],
            field="collector.terminal_error",
        ),
    )


def writer_health_from_wire(payload: Mapping[str, Any]) -> ClickHouseWriterHealth:
    values = _exact_object(payload, _WRITER_FIELDS, role="writer")
    return ClickHouseWriterHealth(
        state=_enum(ClickHouseWriterState, values["state"], field="writer.state"),
        ready=_bool(values["ready"], field="writer.ready"),
        reason=_text(values["reason"], field="writer.reason"),
        owner_id=_text(values["owner_id"], field="writer.owner_id"),
        kafka_group_id=_text(values["kafka_group_id"], field="writer.kafka_group_id"),
        committed_next_offset=_optional_int(
            values["committed_next_offset"],
            field="writer.committed_next_offset",
        ),
        batches_committed=_non_negative_int(
            values["batches_committed"],
            field="writer.batches_committed",
        ),
        inserted_events=_non_negative_int(
            values["inserted_events"],
            field="writer.inserted_events",
        ),
        duplicate_events=_non_negative_int(
            values["duplicate_events"],
            field="writer.duplicate_events",
        ),
        conflict_events=_non_negative_int(
            values["conflict_events"],
            field="writer.conflict_events",
        ),
        started_at_ms=_non_negative_int(
            values["started_at_ms"], field="writer.started_at_ms"
        ),
        updated_at_ms=_non_negative_int(
            values["updated_at_ms"], field="writer.updated_at_ms"
        ),
        terminal_error=_optional_text(
            values["terminal_error"], field="writer.terminal_error"
        ),
    )


def archive_health_from_wire(payload: Mapping[str, Any]) -> ArchiveWriterHealth:
    values = _exact_object(payload, _ARCHIVE_FIELDS, role="archive")
    snapshot_payload = values["current_snapshot"]
    snapshot = (
        None if snapshot_payload is None else _snapshot_from_wire(snapshot_payload)
    )
    return ArchiveWriterHealth(
        state=_enum(ArchiveWriterState, values["state"], field="archive.state"),
        ready=_bool(values["ready"], field="archive.ready"),
        reason=_text(values["reason"], field="archive.reason"),
        owner_id=_text(values["owner_id"], field="archive.owner_id"),
        kafka_group_id=_text(values["kafka_group_id"], field="archive.kafka_group_id"),
        data_epoch=_text(values["data_epoch"], field="archive.data_epoch"),
        committed_next_offset=_optional_int(
            values["committed_next_offset"],
            field="archive.committed_next_offset",
        ),
        segments_committed=_non_negative_int(
            values["segments_committed"],
            field="archive.segments_committed",
        ),
        events_archived=_non_negative_int(
            values["events_archived"],
            field="archive.events_archived",
        ),
        current_snapshot=snapshot,
        started_at_ms=_non_negative_int(
            values["started_at_ms"], field="archive.started_at_ms"
        ),
        updated_at_ms=_non_negative_int(
            values["updated_at_ms"], field="archive.updated_at_ms"
        ),
        terminal_error=_optional_text(
            values["terminal_error"], field="archive.terminal_error"
        ),
    )


def synthetic_publisher_collector_health(
    *,
    last_sequence: int,
    last_partition_offset: int,
    events_published: int,
    started_at_ms: int,
) -> CollectorHealth:
    """Health for a scripted Kafka publisher used as the 1S collector stand-in."""

    last_sequence = _non_negative_int(last_sequence, field="last_sequence")
    last_partition_offset = _non_negative_int(
        last_partition_offset,
        field="last_partition_offset",
    )
    events_published = _non_negative_int(events_published, field="events_published")
    started_at_ms = _non_negative_int(started_at_ms, field="started_at_ms")
    if last_sequence < 1:
        raise ValueError("last_sequence must be greater than zero")
    return CollectorHealth(
        state=CollectorState.LEADER,
        ready=True,
        reason="scripted Kafka publisher completed a contiguous segment",
        owner_id="phase1s-scripted-publisher",
        source_health="scripted_kafka_publisher",
        producer_epoch=0,
        lease_expires_at_ms=started_at_ms + 15_000,
        last_sequence=last_sequence,
        last_partition_offset=last_partition_offset,
        pending_event_id=None,
        events_published=events_published,
        heartbeat_successes=1,
        heartbeat_failures=0,
        started_at_ms=started_at_ms,
        updated_at_ms=started_at_ms,
        terminal_error=None,
    )


def _snapshot_from_wire(payload: object) -> MarketDataSnapshotRef:
    values = _exact_object(payload, _SNAPSHOT_FIELDS, role="snapshot")
    return MarketDataSnapshotRef(
        data_epoch=_text(values["data_epoch"], field="snapshot.data_epoch"),
        snapshot_version=_non_negative_int(
            values["snapshot_version"],
            field="snapshot.snapshot_version",
        ),
        manifest_uri=_text(values["manifest_uri"], field="snapshot.manifest_uri"),
        manifest_sha256=_text(
            values["manifest_sha256"], field="snapshot.manifest_sha256"
        ),
    )


def _exact_object(
    payload: object,
    fields: frozenset[str],
    *,
    role: str,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise ChainHealthWireError(f"{role} health wire must be an object")
    values = dict(payload)
    extra = set(values) - fields
    missing = fields - set(values)
    if extra or missing:
        raise ChainHealthWireError(
            f"{role} health wire keys are not exact",
        )
    return values


def _enum(enum_type: type[Any], value: object, *, field: str) -> Any:
    if not isinstance(value, str) or not value.strip():
        raise ChainHealthWireError(f"{field} must be a non-blank string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ChainHealthWireError(
            f"{field} is not a known {enum_type.__name__}"
        ) from exc


def _bool(value: object, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise ChainHealthWireError(f"{field} must be a boolean")
    return value


def _text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ChainHealthWireError(f"{field} must be a non-blank string")
    return value


def _optional_text(value: object, *, field: str) -> str | None:
    if value is None:
        return None
    return _text(value, field=field)


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ChainHealthWireError(f"{field} must be an integer")
    if value < 0:
        raise ChainHealthWireError(f"{field} must be non-negative")
    return value


def _optional_int(value: object, *, field: str) -> int | None:
    if value is None:
        return None
    return _non_negative_int(value, field=field)


__all__ = [
    "ChainHealthWireError",
    "archive_health_from_wire",
    "collector_health_from_wire",
    "synthetic_publisher_collector_health",
    "writer_health_from_wire",
]
