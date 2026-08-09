"""Production-equivalent identity model for focused Phase 1D tests."""

from __future__ import annotations

from collections.abc import Sequence

from app.server_runtime.projection import (
    KafkaMarketEventRecord,
    ProjectionBatchResult,
    require_contiguous_records,
)


class InMemoryMarketEventProjector:
    def __init__(self, *, order: list[str] | None = None) -> None:
        self.identities: dict[tuple[str, str], KafkaMarketEventRecord] = {}
        self.conflicts: dict[tuple[str, int, int], KafkaMarketEventRecord] = {}
        self.started = False
        self.order = order

    async def start(self) -> None:
        if self.started:
            raise RuntimeError("projector is already started")
        self.started = True

    async def stop(self) -> None:
        if self.order is not None:
            self.order.append("projector.stop")
        self.started = False

    async def apply_batch(
        self,
        records: Sequence[KafkaMarketEventRecord],
    ) -> ProjectionBatchResult:
        if not self.started:
            raise RuntimeError("projector is not started")
        batch = require_contiguous_records(records)
        inserted = 0
        duplicates = 0
        conflicts = 0
        for record in batch:
            identity = (record.envelope.partition_key, record.envelope.event_id)
            existing = self.identities.get(identity)
            if existing is None:
                self.identities[identity] = record
                inserted += 1
            elif (
                existing.envelope_sha256 == record.envelope_sha256
                and existing.envelope_bytes == record.envelope_bytes
            ):
                duplicates += 1
            else:
                self.conflicts[(record.topic, record.partition, record.offset)] = record
                conflicts += 1
        return ProjectionBatchResult(
            inserted_count=inserted,
            duplicate_count=duplicates,
            conflict_count=conflicts,
            first_offset=batch[0].offset,
            last_offset=batch[-1].offset,
        )
