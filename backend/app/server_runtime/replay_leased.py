"""Load a Phase 1Q snapshot reader only while a replay session lease is active."""

from __future__ import annotations

from dataclasses import dataclass

from app.server_contracts import MarketEventQuery
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseStore,
    ReplaySessionSnapshotConflictError,
)
from app.server_runtime.replay_snapshot import (
    DEFAULT_MAX_QUERY_PAGES,
    DEFAULT_MAX_SCAN_ROWS,
    DEFAULT_QUERY_PAGE_LIMIT,
    DEFAULT_READER_PAGE_ROWS,
    ReplayServerSnapshotPin,
    ServerSnapshotTradeReader,
)

LEASED_SERVER_SNAPSHOT_REPLAY_SCHEMA_VERSION = (
    "candlescope.leased-server-snapshot-replay.v1"
)


@dataclass(frozen=True, slots=True)
class LeasedServerSnapshotReplay:
    """An active fenced session plus the frozen 1Q trade reader it may use."""

    lease: ReplaySessionLease
    reader: ServerSnapshotTradeReader
    schema_version: str = LEASED_SERVER_SNAPSHOT_REPLAY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != LEASED_SERVER_SNAPSHOT_REPLAY_SCHEMA_VERSION:
            raise ValueError("leased server snapshot replay schema_version has drifted")
        if not isinstance(self.lease, ReplaySessionLease):
            raise TypeError("lease must be a ReplaySessionLease")
        if not isinstance(self.reader, ServerSnapshotTradeReader):
            raise TypeError("reader must be a ServerSnapshotTradeReader")
        if self.lease.snapshot != self.reader.snapshot_pin.snapshot:
            raise ReplaySessionSnapshotConflictError(
                "replay session snapshot pin does not match the fenced lease"
            )

    def to_public_ref(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "lease": self.lease.to_public_ref(),
            "snapshot": self.reader.snapshot_pin.to_public_ref(),
        }


async def load_leased_server_snapshot(
    store: ReplaySessionLeaseStore,
    lease: ReplaySessionLease,
    query: MarketEventQuery,
    pin: ReplayServerSnapshotPin,
    *,
    query_page_limit: int = DEFAULT_QUERY_PAGE_LIMIT,
    page_rows: int = DEFAULT_READER_PAGE_ROWS,
    max_scan_rows: int = DEFAULT_MAX_SCAN_ROWS,
    max_query_pages: int = DEFAULT_MAX_QUERY_PAGES,
) -> LeasedServerSnapshotReplay:
    """Fence the session, then load the 1Q cold snapshot reader.

    Expired or stale leases never reach the query port. Unleased
    ``ServerSnapshotTradeReader.load`` remains available for Phase 1Q tests.
    """

    if not hasattr(store, "require_active"):
        raise TypeError("store must implement ReplaySessionLeaseStore.require_active")
    if not isinstance(lease, ReplaySessionLease):
        raise TypeError("lease must be a ReplaySessionLease")
    if not isinstance(pin, ReplayServerSnapshotPin):
        raise TypeError("pin must be a ReplayServerSnapshotPin")
    current = await store.require_active(lease)
    if current.snapshot != pin.snapshot:
        raise ReplaySessionSnapshotConflictError(
            "replay session snapshot pin does not match the fenced lease"
        )
    reader = await ServerSnapshotTradeReader.load(
        query,
        pin,
        query_page_limit=query_page_limit,
        page_rows=page_rows,
        max_scan_rows=max_scan_rows,
        max_query_pages=max_query_pages,
    )
    return LeasedServerSnapshotReplay(lease=current, reader=reader)


__all__ = [
    "LEASED_SERVER_SNAPSHOT_REPLAY_SCHEMA_VERSION",
    "LeasedServerSnapshotReplay",
    "load_leased_server_snapshot",
]
