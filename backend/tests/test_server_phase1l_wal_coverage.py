from __future__ import annotations

import asyncio

import pytest
from app.server_runtime.query_wal_archive import ImmutablePostgresWalArchive
from app.server_runtime.query_wal_coverage import (
    PostgresRecoveryTarget,
    WalCoverageError,
    read_wal_coverage,
    wait_for_wal_coverage,
    wal_segment_filenames,
)
from app.server_runtime.testing import InMemoryImmutableObjectStore


def test_recovery_target_binds_canonical_time_lsn_filename_and_size() -> None:
    target = PostgresRecoveryTarget(
        recovery_target_time="2026-08-11 13:00:00.123456+00",
        recovery_target_lsn="0/3000200",
        recovery_target_wal_filename="000000010000000000000003",
        wal_segment_size_bytes=16 * 1024 * 1024,
    )
    assert target.recovery_target_lsn == "0/3000200"
    with pytest.raises(ValueError, match="differs from its LSN"):
        PostgresRecoveryTarget(
            recovery_target_time=target.recovery_target_time,
            recovery_target_lsn=target.recovery_target_lsn,
            recovery_target_wal_filename="000000010000000000000004",
            wal_segment_size_bytes=target.wal_segment_size_bytes,
        )


def test_wal_segment_range_crosses_postgres_log_boundary() -> None:
    assert wal_segment_filenames(
        timeline=1,
        start_lsn="0/FFFFFFF0",
        end_lsn="1/01000001",
        segment_size_bytes=16 * 1024 * 1024,
    ) == (
        "0000000100000000000000FF",
        "000000010000000100000000",
        "000000010000000100000001",
    )

    assert wal_segment_filenames(
        timeline=1,
        start_lsn="0/1000000",
        end_lsn="0/1000000",
        segment_size_bytes=16 * 1024 * 1024,
    ) == ("000000010000000000000000",)


def test_wal_segment_range_is_bounded_and_rejects_invalid_inputs() -> None:
    with pytest.raises(WalCoverageError, match="ends before"):
        wal_segment_filenames(
            timeline=1,
            start_lsn="1/0",
            end_lsn="0/FFFFFFFF",
            segment_size_bytes=16 * 1024 * 1024,
        )
    with pytest.raises(WalCoverageError, match="segment bound"):
        wal_segment_filenames(
            timeline=1,
            start_lsn="0/0",
            end_lsn="0/2000001",
            segment_size_bytes=16 * 1024 * 1024,
            max_segments=2,
        )
    with pytest.raises(ValueError, match="power of two"):
        wal_segment_filenames(
            timeline=1,
            start_lsn="0/0",
            end_lsn="0/1",
            segment_size_bytes=3 * 1024 * 1024,
        )
    with pytest.raises(ValueError, match="64-bit"):
        wal_segment_filenames(
            timeline=1,
            start_lsn="100000000/0",
            end_lsn="100000000/1",
            segment_size_bytes=16 * 1024 * 1024,
        )


def test_wal_coverage_waits_for_every_segment_and_returns_ordered_receipts() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        archive = ImmutablePostgresWalArchive(
            object_store=store,
            cluster_id="phase1l-primary",
        )
        filenames = (
            "000000010000000000000001",
            "000000010000000000000002",
        )
        data = b"w" * (1 << 20)
        await archive.archive(filenames[0], data)

        async def delayed_archive() -> None:
            await asyncio.sleep(0.01)
            await archive.archive(filenames[1], data)

        task = asyncio.create_task(delayed_archive())
        receipts = await wait_for_wal_coverage(
            archive,
            filenames,
            segment_size_bytes=1 << 20,
            timeout_ms=200,
            poll_interval_ms=5,
        )
        await task
        assert tuple(item.filename for item in receipts) == filenames

    asyncio.run(run())


def test_wal_coverage_timeout_and_wrong_size_fail_closed() -> None:
    async def run() -> None:
        store = InMemoryImmutableObjectStore()
        await store.ensure_bucket()
        archive = ImmutablePostgresWalArchive(
            object_store=store,
            cluster_id="phase1l-primary",
        )
        filename = "000000010000000000000001"
        with pytest.raises(TimeoutError, match="not archived"):
            await wait_for_wal_coverage(
                archive,
                (filename,),
                segment_size_bytes=1 << 20,
                timeout_ms=20,
                poll_interval_ms=5,
            )
        await archive.archive(filename, b"short")
        with pytest.raises(WalCoverageError, match="receipt"):
            await read_wal_coverage(
                archive,
                (filename,),
                segment_size_bytes=1 << 20,
            )

    asyncio.run(run())
