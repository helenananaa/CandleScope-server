"""Bounded, contiguous PostgreSQL WAL coverage proofs for physical backups."""

from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import psycopg

from app.server_runtime.object_store import ObjectNotFoundError
from app.server_runtime.query_wal_archive import ArchivedWalObject

DEFAULT_MAX_WAL_COVERAGE_SEGMENTS = 4_096
_LSN = re.compile(r"^[0-9A-F]+/[0-9A-F]+$")


class WalCoverageError(RuntimeError):
    """The requested WAL interval cannot be proved from immutable objects."""


class _WalReader(Protocol):
    async def restore(self, filename: str) -> tuple[ArchivedWalObject, bytes]: ...


@dataclass(frozen=True, slots=True)
class PostgresRecoveryTarget:
    recovery_target_time: str
    recovery_target_lsn: str
    recovery_target_wal_filename: str
    wal_segment_size_bytes: int

    def __post_init__(self) -> None:
        if not isinstance(self.recovery_target_time, str):
            raise TypeError("recovery_target_time must be a string")
        try:
            parsed = datetime.strptime(
                self.recovery_target_time,
                "%Y-%m-%d %H:%M:%S.%f+00",
            )
        except ValueError as exc:
            raise ValueError("recovery_target_time must be canonical UTC") from exc
        if parsed.strftime("%Y-%m-%d %H:%M:%S.%f+00") != self.recovery_target_time:
            raise ValueError("recovery_target_time must be canonical UTC")
        if (
            not isinstance(self.recovery_target_wal_filename, str)
            or len(self.recovery_target_wal_filename) != 24
            or any(
                character not in "0123456789ABCDEF"
                for character in self.recovery_target_wal_filename
            )
        ):
            raise ValueError("recovery_target_wal_filename is invalid")
        expected = wal_segment_filenames(
            timeline=int(self.recovery_target_wal_filename[:8], 16),
            start_lsn=self.recovery_target_lsn,
            end_lsn=self.recovery_target_lsn,
            segment_size_bytes=self.wal_segment_size_bytes,
            max_segments=1,
        )[0]
        if expected != self.recovery_target_wal_filename:
            raise ValueError("recovery target WAL filename differs from its LSN")


async def capture_postgres_recovery_target(
    dsn: str,
    *,
    connect_timeout_ms: int = 5_000,
    request_timeout_ms: int = 30_000,
) -> PostgresRecoveryTarget:
    """Capture target time, LSN, filename, and segment size in one SQL statement."""

    connect_timeout_ms = _positive_int(
        connect_timeout_ms,
        field="connect_timeout_ms",
    )
    request_timeout_ms = _positive_int(
        request_timeout_ms,
        field="request_timeout_ms",
    )
    try:
        async with await psycopg.AsyncConnection.connect(
            dsn,
            connect_timeout=max(1, math.ceil(connect_timeout_ms / 1_000)),
            application_name="candlescope-query-backup-target",
        ) as connection:
            await connection.set_read_only(True)
            await connection.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (str(request_timeout_ms),),
            )
            result = await connection.execute(
                "WITH captured_time AS MATERIALIZED ("
                "SELECT clock_timestamp() AS target_time"
                "), target AS MATERIALIZED ("
                "SELECT target_time, pg_current_wal_lsn() AS target_lsn "
                "FROM captured_time"
                ") SELECT "
                "to_char(target_time AT TIME ZONE 'UTC', "
                "'YYYY-MM-DD HH24:MI:SS.US\"+00\"'), "
                "target_lsn::text, pg_walfile_name(target_lsn), "
                "pg_size_bytes(current_setting('wal_segment_size'))::bigint "
                "FROM target"
            )
            row = await result.fetchone()
    except psycopg.Error as exc:
        raise WalCoverageError("PostgreSQL recovery target is unavailable") from exc
    if (
        row is None
        or not isinstance(row[0], str)
        or not isinstance(row[1], str)
        or not isinstance(row[2], str)
        or isinstance(row[3], bool)
        or not isinstance(row[3], int)
    ):
        raise WalCoverageError("PostgreSQL recovery target is invalid")
    return PostgresRecoveryTarget(
        recovery_target_time=row[0],
        recovery_target_lsn=row[1],
        recovery_target_wal_filename=row[2],
        wal_segment_size_bytes=row[3],
    )


def wal_segment_filenames(
    *,
    timeline: int,
    start_lsn: str,
    end_lsn: str,
    segment_size_bytes: int,
    max_segments: int = DEFAULT_MAX_WAL_COVERAGE_SEGMENTS,
) -> tuple[str, ...]:
    """Return every WAL segment intersecting the inclusive LSN interval."""

    timeline = _timeline(timeline)
    segment_size_bytes = _segment_size(segment_size_bytes)
    max_segments = _positive_int(max_segments, field="max_segments")
    start_value = _lsn_value(start_lsn, field="start_lsn")
    end_value = _lsn_value(end_lsn, field="end_lsn")
    if end_value < start_value:
        raise WalCoverageError("WAL coverage interval ends before it starts")
    # PostgreSQL's pg_walfile_name() uses XLByteToPrevSeg: an LSN exactly on a
    # segment boundary names the preceding segment, which contains the record
    # ending at that LSN. Keep the proof range aligned with that server rule.
    first_segment = max(0, start_value - 1) // segment_size_bytes
    last_segment = max(0, end_value - 1) // segment_size_bytes
    count = last_segment - first_segment + 1
    if count > max_segments:
        raise WalCoverageError("WAL coverage interval exceeds the segment bound")
    segments_per_log = 0x1_0000_0000 // segment_size_bytes
    return tuple(
        f"{timeline:08X}{segment // segments_per_log:08X}"
        f"{segment % segments_per_log:08X}"
        for segment in range(first_segment, last_segment + 1)
    )


async def read_wal_coverage(
    archive: _WalReader,
    filenames: tuple[str, ...],
    *,
    segment_size_bytes: int,
) -> tuple[ArchivedWalObject, ...]:
    """Read and validate an already archived, ordered WAL segment interval."""

    segment_size_bytes = _segment_size(segment_size_bytes)
    if not filenames:
        raise WalCoverageError("WAL coverage must contain at least one segment")
    receipts: list[ArchivedWalObject] = []
    for filename in filenames:
        receipts.append(
            await _read_wal_segment(
                archive,
                filename,
                segment_size_bytes=segment_size_bytes,
            )
        )
    return tuple(receipts)


async def wait_for_wal_coverage(
    archive: _WalReader,
    filenames: tuple[str, ...],
    *,
    segment_size_bytes: int,
    timeout_ms: int,
    poll_interval_ms: int,
) -> tuple[ArchivedWalObject, ...]:
    """Wait only for missing immutable segments; integrity failures fail immediately."""

    timeout_ms = _positive_int(timeout_ms, field="timeout_ms")
    poll_interval_ms = _positive_int(poll_interval_ms, field="poll_interval_ms")
    if not filenames:
        raise WalCoverageError("WAL coverage must contain at least one segment")
    if poll_interval_ms >= timeout_ms:
        raise ValueError("poll_interval_ms must be shorter than timeout_ms")
    deadline = asyncio.get_running_loop().time() + timeout_ms / 1_000
    receipts: list[ArchivedWalObject] = []
    for filename in filenames:
        while True:
            try:
                receipt = await _read_wal_segment(
                    archive,
                    filename,
                    segment_size_bytes=segment_size_bytes,
                )
                receipts.append(receipt)
                break
            except ObjectNotFoundError as exc:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError(
                        "required WAL coverage was not archived before the timeout"
                    ) from exc
                await asyncio.sleep(min(poll_interval_ms / 1_000, remaining))
    return tuple(receipts)


async def _read_wal_segment(
    archive: _WalReader,
    filename: str,
    *,
    segment_size_bytes: int,
) -> ArchivedWalObject:
    receipt, data = await archive.restore(filename)
    if receipt.filename != filename or receipt.size_bytes != segment_size_bytes:
        raise WalCoverageError("WAL coverage receipt does not match its segment")
    if len(data) != segment_size_bytes:
        raise WalCoverageError("WAL coverage segment has an unexpected size")
    return receipt


def _lsn_value(value: object, *, field: str) -> int:
    if not isinstance(value, str) or not _LSN.fullmatch(value):
        raise ValueError(f"{field} must be an upper-case PostgreSQL LSN")
    high, low = (int(component, 16) for component in value.split("/", 1))
    if high > 0xFFFF_FFFF or low > 0xFFFF_FFFF:
        raise ValueError(f"{field} exceeds PostgreSQL's 64-bit LSN range")
    return high << 32 | low


def _timeline(value: object) -> int:
    value = _positive_int(value, field="timeline")
    if value > 0xFFFF_FFFF:
        raise ValueError("timeline exceeds PostgreSQL's filename range")
    return value


def _segment_size(value: object) -> int:
    value = _positive_int(value, field="segment_size_bytes")
    if not 1 << 20 <= value <= 1 << 30 or value & (value - 1):
        raise ValueError(
            "segment_size_bytes must be a PostgreSQL power of two from 1 MiB to 1 GiB"
        )
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value
