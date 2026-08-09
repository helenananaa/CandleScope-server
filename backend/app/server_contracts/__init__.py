"""Language-neutral contracts for the logical CandleScope server."""

from .archive_manifest import (
    MARKET_DATA_MANIFEST_SCHEMA_VERSION,
    MARKET_EVENT_PARQUET_SCHEMA_VERSION,
    MarketDataManifestV1,
    ParquetArchiveSegmentV1,
    parse_manifest_bytes,
)
from .market_event import (
    MARKET_EVENT_ENVELOPE_SCHEMA_VERSION,
    PAYLOAD_CANONICALIZATION,
    MarketEventEnvelopeV1,
    canonical_payload_bytes,
    canonical_payload_sha256,
)
from .ports import (
    ArchiveCommit,
    MarketDataSnapshotRef,
    MarketEventArchive,
    MarketEventCursor,
    MarketEventPage,
    MarketEventPublisher,
    MarketEventQuery,
    MarketEventRange,
    PublishReceipt,
)

__all__ = [
    "MARKET_DATA_MANIFEST_SCHEMA_VERSION",
    "MARKET_EVENT_ENVELOPE_SCHEMA_VERSION",
    "MARKET_EVENT_PARQUET_SCHEMA_VERSION",
    "PAYLOAD_CANONICALIZATION",
    "ArchiveCommit",
    "MarketDataManifestV1",
    "MarketDataSnapshotRef",
    "MarketEventArchive",
    "MarketEventCursor",
    "MarketEventEnvelopeV1",
    "MarketEventPage",
    "MarketEventPublisher",
    "MarketEventQuery",
    "MarketEventRange",
    "ParquetArchiveSegmentV1",
    "PublishReceipt",
    "canonical_payload_bytes",
    "canonical_payload_sha256",
    "parse_manifest_bytes",
]
