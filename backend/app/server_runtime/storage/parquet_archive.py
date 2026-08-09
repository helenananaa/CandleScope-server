"""Immutable Parquet segments, manifest chains, and snapshot-pinned queries."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from itertools import pairwise

import pyarrow as pa
import pyarrow.parquet as pq
import rfc8785

from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import (
    MARKET_EVENT_PARQUET_SCHEMA_VERSION,
    ArchiveCommit,
    MarketDataManifestV1,
    MarketDataSnapshotRef,
    MarketEventCursor,
    MarketEventEnvelopeV1,
    MarketEventPage,
    MarketEventRange,
    ParquetArchiveSegmentV1,
    parse_manifest_bytes,
)
from app.server_runtime.object_store import (
    ImmutableObjectStore,
    ObjectNotFoundError,
)
from app.server_runtime.projection import (
    KafkaMarketEventRecord,
    require_contiguous_records,
)
from app.server_runtime.publishers import canonical_envelope_bytes
from app.server_runtime.query_pagination import (
    SnapshotQueryCursorError,
    SnapshotQueryIntegrityError,
    SnapshotQueryRow,
    canonical_fact_rows,
    paginate_snapshot_rows,
)

_DATA_EPOCH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_PARQUET_SCHEMA_DEFINITION = {
    "schema_version": MARKET_EVENT_PARQUET_SCHEMA_VERSION,
    "columns": [
        ["kafka_topic", "utf8", False],
        ["kafka_partition", "uint16", False],
        ["kafka_offset", "uint64", False],
        ["event_id", "utf8", False],
        ["partition_key", "utf8", False],
        ["event_time_ms", "uint64", False],
        ["sequence_start", "uint64", True],
        ["sequence_end", "uint64", True],
        ["envelope_sha256", "utf8", False],
        ["envelope_json", "utf8", False],
    ],
}
PARQUET_SCHEMA_SHA256 = hashlib.sha256(
    rfc8785.dumps(_PARQUET_SCHEMA_DEFINITION)
).hexdigest()
_ARROW_SCHEMA = pa.schema(
    [
        pa.field("kafka_topic", pa.string(), nullable=False),
        pa.field("kafka_partition", pa.uint16(), nullable=False),
        pa.field("kafka_offset", pa.uint64(), nullable=False),
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("partition_key", pa.string(), nullable=False),
        pa.field("event_time_ms", pa.uint64(), nullable=False),
        pa.field("sequence_start", pa.uint64(), nullable=True),
        pa.field("sequence_end", pa.uint64(), nullable=True),
        pa.field("envelope_sha256", pa.string(), nullable=False),
        pa.field("envelope_json", pa.string(), nullable=False),
    ],
    metadata={
        b"candlescope.schema_version": MARKET_EVENT_PARQUET_SCHEMA_VERSION.encode(
            "ascii"
        ),
        b"candlescope.schema_sha256": PARQUET_SCHEMA_SHA256.encode("ascii"),
    },
)


class ParquetArchiveError(RuntimeError):
    """Base class for fail-closed immutable archive errors."""


class ParquetArchiveConflictError(ParquetArchiveError):
    """An immutable key already contains different logical content."""


class ParquetArchiveIntegrityError(ParquetArchiveError):
    """A manifest, Parquet object, or cursor failed integrity validation."""


class ParquetArchiveCursorError(ParquetArchiveIntegrityError):
    """A client cursor is malformed or bound to another snapshot."""


@dataclass(frozen=True, slots=True)
class ArchivedMarketEventRow:
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    envelope_sha256: str
    envelope: MarketEventEnvelopeV1


@dataclass(frozen=True, slots=True)
class ArchiveAppendResult:
    commit: ArchiveCommit
    first_offset: int
    last_offset: int
    object_created: bool
    manifest_created: bool


class MarketEventParquetCodec:
    """Encode and verify the frozen market-event Parquet schema."""

    def encode(self, records: Sequence[KafkaMarketEventRecord]) -> bytes:
        batch = require_contiguous_records(records)
        rows = [
            {
                "kafka_topic": record.topic,
                "kafka_partition": record.partition,
                "kafka_offset": record.offset,
                "event_id": record.envelope.event_id,
                "partition_key": record.envelope.partition_key,
                "event_time_ms": record.envelope.event_time_ms,
                "sequence_start": record.envelope.sequence_start,
                "sequence_end": record.envelope.sequence_end,
                "envelope_sha256": record.envelope_sha256,
                "envelope_json": record.envelope_bytes.decode("utf-8"),
            }
            for record in batch
        ]
        table = pa.Table.from_pylist(rows, schema=_ARROW_SCHEMA)
        sink = pa.BufferOutputStream()
        pq.write_table(
            table,
            sink,
            compression="zstd",
            version="2.6",
            data_page_version="2.0",
            use_dictionary=False,
            write_statistics=True,
            row_group_size=len(rows),
        )
        return sink.getvalue().to_pybytes()

    def decode(self, data: bytes) -> tuple[ArchivedMarketEventRow, ...]:
        if not isinstance(data, bytes) or not data:
            raise ParquetArchiveIntegrityError("Parquet object is empty")
        try:
            table = pq.read_table(pa.BufferReader(data))
        except (pa.ArrowException, OSError, ValueError) as exc:
            raise ParquetArchiveIntegrityError("Parquet object cannot be read") from exc
        if not table.schema.equals(_ARROW_SCHEMA, check_metadata=True):
            raise ParquetArchiveIntegrityError("Parquet schema has drifted")
        decoded: list[ArchivedMarketEventRow] = []
        for row in table.to_pylist():
            try:
                envelope_bytes = str(row["envelope_json"]).encode("utf-8")
                wire = json.loads(envelope_bytes.decode("utf-8"))
                envelope = MarketEventEnvelopeV1.from_wire(wire)
                if canonical_envelope_bytes(envelope) != envelope_bytes:
                    raise ValueError("envelope JSON is not canonical")
                envelope_sha256 = _sha256(
                    row["envelope_sha256"],
                    field="envelope_sha256",
                )
                if hashlib.sha256(envelope_bytes).hexdigest() != envelope_sha256:
                    raise ValueError("envelope SHA-256 mismatch")
                if envelope.event_id != row["event_id"]:
                    raise ValueError("event_id column mismatch")
                if envelope.partition_key != row["partition_key"]:
                    raise ValueError("partition_key column mismatch")
                if envelope.event_time_ms != row["event_time_ms"]:
                    raise ValueError("event_time_ms column mismatch")
                if envelope.sequence_start != row["sequence_start"]:
                    raise ValueError("sequence_start column mismatch")
                if envelope.sequence_end != row["sequence_end"]:
                    raise ValueError("sequence_end column mismatch")
                decoded.append(
                    ArchivedMarketEventRow(
                        kafka_topic=_required_text(
                            row["kafka_topic"],
                            field="kafka_topic",
                        ),
                        kafka_partition=_non_negative_int(
                            row["kafka_partition"],
                            field="kafka_partition",
                        ),
                        kafka_offset=_non_negative_int(
                            row["kafka_offset"],
                            field="kafka_offset",
                        ),
                        envelope_sha256=envelope_sha256,
                        envelope=envelope,
                    )
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ParquetArchiveIntegrityError(
                    "Parquet row violates the frozen archive contract"
                ) from exc
        return tuple(decoded)

    def validate_records(
        self,
        data: bytes,
        records: Sequence[KafkaMarketEventRecord],
    ) -> tuple[ArchivedMarketEventRow, ...]:
        batch = tuple(records)
        rows = self.decode(data)
        if len(rows) != len(batch):
            raise ParquetArchiveConflictError(
                "existing Parquet segment has a different row count"
            )
        for row, record in zip(rows, batch, strict=True):
            if (
                row.kafka_topic != record.topic
                or row.kafka_partition != record.partition
                or row.kafka_offset != record.offset
                or row.envelope_sha256 != record.envelope_sha256
                or row.envelope != record.envelope
            ):
                raise ParquetArchiveConflictError(
                    "existing Parquet segment contains different market events"
                )
        return rows


class ImmutableParquetMarketEventArchive:
    """Publish fixed-size offset-aligned Parquet segments and manifest links."""

    def __init__(
        self,
        *,
        object_store: ImmutableObjectStore,
        segment_event_count: int = 10_000,
        max_manifest_depth: int = 100_000,
        codec: MarketEventParquetCodec | None = None,
    ) -> None:
        if not isinstance(object_store, ImmutableObjectStore):
            raise TypeError("object_store must implement ImmutableObjectStore")
        self._store = object_store
        self._segment_event_count = _positive_int(
            segment_event_count,
            field="segment_event_count",
        )
        self._max_manifest_depth = _positive_int(
            max_manifest_depth,
            field="max_manifest_depth",
        )
        self._codec = codec or MarketEventParquetCodec()

    async def initialize(self) -> None:
        await self._store.ensure_bucket()

    async def check_ready(self) -> None:
        await self._store.check_bucket()

    async def append_records(
        self,
        records: Sequence[KafkaMarketEventRecord],
        *,
        data_epoch: str,
    ) -> ArchiveAppendResult:
        data_epoch = _data_epoch(data_epoch)
        batch = require_contiguous_records(records)
        if len(batch) != self._segment_event_count:
            raise ParquetArchiveError(
                f"archive segments must contain exactly {self._segment_event_count} events"
            )
        first_offset = batch[0].offset
        last_offset = batch[-1].offset
        if first_offset % self._segment_event_count != 0:
            raise ParquetArchiveError(
                "archive segment start is not aligned to segment_event_count"
            )
        if len({record.envelope.partition_key for record in batch}) != 1:
            raise ParquetArchiveError("an archive segment cannot cross market streams")

        parent = await self._parent_snapshot(data_epoch, first_offset)
        object_key = segment_key(data_epoch, first_offset, last_offset)
        candidate_bytes = await asyncio.to_thread(self._codec.encode, batch)
        candidate_sha256 = hashlib.sha256(candidate_bytes).hexdigest()
        object_created = await self._store.put_if_absent(
            object_key,
            candidate_bytes,
            content_type="application/vnd.apache.parquet",
            metadata={
                "content-sha256": candidate_sha256,
                "schema-version": MARKET_EVENT_PARQUET_SCHEMA_VERSION,
                "schema-sha256": PARQUET_SCHEMA_SHA256,
            },
        )
        stored_object = await self._store.get(object_key)
        stored_sha256 = hashlib.sha256(stored_object.data).hexdigest()
        metadata_sha256 = stored_object.metadata.get("content-sha256")
        if metadata_sha256 is not None and metadata_sha256 != stored_sha256:
            raise ParquetArchiveIntegrityError(
                "Parquet object metadata SHA-256 does not match its bytes"
            )
        await asyncio.to_thread(self._codec.validate_records, stored_object.data, batch)

        sequence_values = [
            (record.envelope.sequence_start, record.envelope.sequence_end)
            for record in batch
        ]
        has_complete_sequence = all(
            start is not None and end is not None for start, end in sequence_values
        )
        sequence_start = (
            min(start for start, _ in sequence_values if start is not None)
            if has_complete_sequence
            else None
        )
        sequence_end = (
            max(end for _, end in sequence_values if end is not None)
            if has_complete_sequence
            else None
        )
        event_times = [record.envelope.event_time_ms for record in batch]
        segment = ParquetArchiveSegmentV1(
            object_uri=self._store.uri_for(object_key),
            content_sha256=stored_sha256,
            byte_size=len(stored_object.data),
            parquet_schema_sha256=PARQUET_SCHEMA_SHA256,
            row_count=len(batch),
            partition_key=batch[0].envelope.partition_key,
            kafka_topic=batch[0].topic,
            kafka_partition=batch[0].partition,
            first_kafka_offset=first_offset,
            last_kafka_offset=last_offset,
            start_event_time_ms=min(event_times),
            end_event_time_ms=max(event_times),
            sequence_start=sequence_start,
            sequence_end=sequence_end,
            envelope_sha256s=tuple(record.envelope_sha256 for record in batch),
        )
        manifest = MarketDataManifestV1(
            data_epoch=data_epoch,
            snapshot_version=last_offset + 1,
            parent_snapshot=parent,
            segment=segment,
        )
        manifest_bytes = manifest.canonical_bytes()
        manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
        manifest_object_key = manifest_key(data_epoch, manifest.snapshot_version)
        manifest_created = await self._store.put_if_absent(
            manifest_object_key,
            manifest_bytes,
            content_type="application/json",
            metadata={
                "content-sha256": manifest_sha256,
                "schema-version": manifest.schema_version,
            },
        )
        stored_manifest = await self._store.get(manifest_object_key)
        existing = _verified_manifest(
            stored_manifest.data,
            expected_sha256=hashlib.sha256(stored_manifest.data).hexdigest(),
        )
        if existing != manifest or stored_manifest.data != manifest_bytes:
            raise ParquetArchiveConflictError(
                "snapshot version already contains a different immutable manifest"
            )
        snapshot = MarketDataSnapshotRef(
            data_epoch=data_epoch,
            snapshot_version=manifest.snapshot_version,
            manifest_uri=self._store.uri_for(manifest_object_key),
            manifest_sha256=manifest_sha256,
        )
        covered_range = _range_for_rows(
            partition_key=segment.partition_key,
            events=tuple(record.envelope for record in batch),
            empty_bounds=None,
        )
        return ArchiveAppendResult(
            commit=ArchiveCommit(
                accepted_count=len(batch),
                snapshot=snapshot,
                object_uri=segment.object_uri,
                content_sha256=segment.content_sha256,
                covered_ranges=(covered_range,),
            ),
            first_offset=first_offset,
            last_offset=last_offset,
            object_created=object_created,
            manifest_created=manifest_created,
        )

    async def load_manifest(
        self,
        snapshot: MarketDataSnapshotRef,
    ) -> MarketDataManifestV1:
        if not isinstance(snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        expected_uri = self._store.uri_for(
            manifest_key(snapshot.data_epoch, snapshot.snapshot_version)
        )
        if snapshot.manifest_uri != expected_uri:
            raise ParquetArchiveIntegrityError(
                "snapshot manifest URI does not match its epoch and version"
            )
        stored = await self._store.get_uri(snapshot.manifest_uri)
        manifest = _verified_manifest(
            stored.data,
            expected_sha256=snapshot.manifest_sha256,
        )
        if (
            manifest.data_epoch != snapshot.data_epoch
            or manifest.snapshot_version != snapshot.snapshot_version
        ):
            raise ParquetArchiveIntegrityError(
                "manifest identity does not match the requested snapshot"
            )
        expected_object_uri = self._store.uri_for(
            segment_key(
                manifest.data_epoch,
                manifest.segment.first_kafka_offset,
                manifest.segment.last_kafka_offset,
            )
        )
        if manifest.segment.object_uri != expected_object_uri:
            raise ParquetArchiveIntegrityError(
                "manifest segment URI is not the deterministic archive key"
            )
        return manifest

    async def load_chain(
        self,
        snapshot: MarketDataSnapshotRef,
    ) -> tuple[MarketDataManifestV1, ...]:
        manifests: list[MarketDataManifestV1] = []
        current: MarketDataSnapshotRef | None = snapshot
        seen: set[str] = set()
        while current is not None:
            if current.manifest_sha256 in seen:
                raise ParquetArchiveIntegrityError("manifest chain contains a cycle")
            if len(manifests) >= self._max_manifest_depth:
                raise ParquetArchiveIntegrityError(
                    "manifest chain exceeds the configured depth"
                )
            seen.add(current.manifest_sha256)
            manifest = await self.load_manifest(current)
            manifests.append(manifest)
            current = manifest.parent_snapshot
        manifests.reverse()
        expected_first = 0
        for manifest in manifests:
            if manifest.segment.first_kafka_offset != expected_first:
                raise ParquetArchiveIntegrityError(
                    "manifest chain does not cover contiguous Kafka offsets"
                )
            expected_first = manifest.snapshot_version
        if expected_first != snapshot.snapshot_version:
            raise ParquetArchiveIntegrityError(
                "manifest chain does not terminate at the requested snapshot"
            )
        return tuple(manifests)

    async def load_segment_rows(
        self,
        manifest: MarketDataManifestV1,
    ) -> tuple[ArchivedMarketEventRow, ...]:
        if not isinstance(manifest, MarketDataManifestV1):
            raise TypeError("manifest must be a MarketDataManifestV1")
        segment = manifest.segment
        stored = await self._store.get_uri(segment.object_uri)
        if len(stored.data) != segment.byte_size:
            raise ParquetArchiveIntegrityError(
                "Parquet object byte size does not match its manifest"
            )
        if hashlib.sha256(stored.data).hexdigest() != segment.content_sha256:
            raise ParquetArchiveIntegrityError(
                "Parquet object SHA-256 does not match its manifest"
            )
        rows = await asyncio.to_thread(self._codec.decode, stored.data)
        _validate_segment_rows(segment, rows)
        return rows

    async def _parent_snapshot(
        self,
        data_epoch: str,
        first_offset: int,
    ) -> MarketDataSnapshotRef | None:
        if first_offset == 0:
            return None
        key = manifest_key(data_epoch, first_offset)
        uri = self._store.uri_for(key)
        try:
            stored = await self._store.get(key)
        except ObjectNotFoundError as exc:
            raise ParquetArchiveIntegrityError(
                "previous snapshot manifest is missing"
            ) from exc
        digest = hashlib.sha256(stored.data).hexdigest()
        parent = MarketDataSnapshotRef(
            data_epoch=data_epoch,
            snapshot_version=first_offset,
            manifest_uri=uri,
            manifest_sha256=digest,
        )
        await self.load_manifest(parent)
        return parent


class ParquetMarketEventQuery:
    """Read only through a verified immutable manifest chain."""

    def __init__(
        self,
        *,
        archive: ImmutableParquetMarketEventArchive,
        max_page_rows: int = 10_000,
    ) -> None:
        if not isinstance(archive, ImmutableParquetMarketEventArchive):
            raise TypeError("archive must be an ImmutableParquetMarketEventArchive")
        self._archive = archive
        self._max_page_rows = _positive_int(
            max_page_rows,
            field="max_page_rows",
        )

    async def start(self) -> None:
        await self._archive.check_ready()

    async def stop(self) -> None:
        return None

    async def query(
        self,
        *,
        snapshot: MarketDataSnapshotRef,
        stream: MarketStreamKey,
        start_event_time_ms: int,
        end_event_time_ms: int,
        limit: int,
        cursor: MarketEventCursor | None = None,
    ) -> MarketEventPage:
        manifests = await self._archive.load_chain(snapshot)
        rows: list[SnapshotQueryRow] = []
        for manifest in manifests:
            segment_rows = await self._archive.load_segment_rows(manifest)
            rows.extend(
                SnapshotQueryRow(
                    envelope=row.envelope,
                    envelope_sha256=row.envelope_sha256,
                    envelope_bytes=canonical_envelope_bytes(row.envelope),
                    kafka_partition=row.kafka_partition,
                    kafka_offset=row.kafka_offset,
                )
                for row in segment_rows
            )
        try:
            facts = canonical_fact_rows(rows)
            return paginate_snapshot_rows(
                facts.rows,
                snapshot=snapshot,
                stream=stream,
                start_event_time_ms=start_event_time_ms,
                end_event_time_ms=end_event_time_ms,
                limit=limit,
                max_page_rows=self._max_page_rows,
                cursor=cursor,
            )
        except SnapshotQueryCursorError as exc:
            raise ParquetArchiveCursorError(str(exc)) from exc
        except SnapshotQueryIntegrityError as exc:
            raise ParquetArchiveIntegrityError(str(exc)) from exc


def manifest_key(data_epoch: str, snapshot_version: int) -> str:
    data_epoch = _data_epoch(data_epoch)
    snapshot_version = _positive_int(snapshot_version, field="snapshot_version")
    return f"epochs/{data_epoch}/manifests/snapshot-{snapshot_version:020d}.json"


def segment_key(data_epoch: str, first_offset: int, last_offset: int) -> str:
    data_epoch = _data_epoch(data_epoch)
    first_offset = _non_negative_int(first_offset, field="first_offset")
    last_offset = _non_negative_int(last_offset, field="last_offset")
    if last_offset < first_offset:
        raise ValueError("last_offset must not precede first_offset")
    return (
        f"epochs/{data_epoch}/segments/partition-00000/"
        f"offset-{first_offset:020d}-{last_offset:020d}.parquet"
    )


def _verified_manifest(data: bytes, *, expected_sha256: str) -> MarketDataManifestV1:
    expected_sha256 = _sha256(expected_sha256, field="manifest_sha256")
    if hashlib.sha256(data).hexdigest() != expected_sha256:
        raise ParquetArchiveIntegrityError("manifest SHA-256 does not match its bytes")
    try:
        return parse_manifest_bytes(data)
    except (TypeError, ValueError) as exc:
        raise ParquetArchiveIntegrityError(
            "manifest violates the frozen canonical contract"
        ) from exc


def _validate_segment_rows(
    segment: ParquetArchiveSegmentV1,
    rows: Sequence[ArchivedMarketEventRow],
) -> None:
    if len(rows) != segment.row_count:
        raise ParquetArchiveIntegrityError("manifest row count does not match Parquet")
    if tuple(row.envelope_sha256 for row in rows) != segment.envelope_sha256s:
        raise ParquetArchiveIntegrityError(
            "manifest envelope hashes do not match Parquet"
        )
    if not rows:
        raise ParquetArchiveIntegrityError(
            "manifest references an empty Parquet object"
        )
    if (
        rows[0].kafka_offset != segment.first_kafka_offset
        or rows[-1].kafka_offset != segment.last_kafka_offset
    ):
        raise ParquetArchiveIntegrityError(
            "manifest Kafka range does not match Parquet"
        )
    for previous, current in pairwise(rows):
        if current.kafka_offset != previous.kafka_offset + 1:
            raise ParquetArchiveIntegrityError(
                "Parquet Kafka offsets are not contiguous"
            )
    if any(
        row.kafka_topic != segment.kafka_topic
        or row.kafka_partition != segment.kafka_partition
        or row.envelope.partition_key != segment.partition_key
        for row in rows
    ):
        raise ParquetArchiveIntegrityError(
            "manifest stream coordinates do not match Parquet"
        )
    event_times = [row.envelope.event_time_ms for row in rows]
    if (
        min(event_times) != segment.start_event_time_ms
        or max(event_times) != segment.end_event_time_ms
    ):
        raise ParquetArchiveIntegrityError(
            "manifest event-time range does not match Parquet"
        )
    sequence_values = [
        (row.envelope.sequence_start, row.envelope.sequence_end) for row in rows
    ]
    has_complete_sequence = all(
        start is not None and end is not None for start, end in sequence_values
    )
    expected_start = (
        min(start for start, _ in sequence_values if start is not None)
        if has_complete_sequence
        else None
    )
    expected_end = (
        max(end for _, end in sequence_values if end is not None)
        if has_complete_sequence
        else None
    )
    if segment.sequence_start != expected_start or segment.sequence_end != expected_end:
        raise ParquetArchiveIntegrityError(
            "manifest sequence range does not match Parquet"
        )


def _range_for_rows(
    *,
    partition_key: str,
    events: Sequence[MarketEventEnvelopeV1],
    empty_bounds: tuple[int, int] | None,
) -> MarketEventRange:
    if not events:
        if empty_bounds is None:
            raise ValueError("empty archive ranges are not allowed")
        return MarketEventRange(
            partition_key=partition_key,
            start_event_time_ms=empty_bounds[0],
            end_event_time_ms=empty_bounds[1],
            event_count=0,
        )
    times = [event.event_time_ms for event in events]
    sequence_values = [(event.sequence_start, event.sequence_end) for event in events]
    has_complete_sequence = all(
        start is not None and end is not None for start, end in sequence_values
    )
    return MarketEventRange(
        partition_key=partition_key,
        start_event_time_ms=min(times),
        end_event_time_ms=max(times),
        event_count=len(events),
        sequence_start=(
            min(start for start, _ in sequence_values if start is not None)
            if has_complete_sequence
            else None
        ),
        sequence_end=(
            max(end for _, end in sequence_values if end is not None)
            if has_complete_sequence
            else None
        ),
    )


def _data_epoch(value: object) -> str:
    value = _required_text(value, field="data_epoch")
    if not _DATA_EPOCH.fullmatch(value):
        raise ValueError(
            "data_epoch must be a path-safe identifier of at most 128 characters"
        )
    return value


def _sha256(value: object, *, field: str) -> str:
    value = _required_text(value, field=field).lower()
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value


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
