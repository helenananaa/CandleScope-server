"""Fail-closed in-memory implementation of the durable publisher port."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

from app.server_contracts import MarketEventEnvelopeV1, PublishReceipt


class MarketEventIdentityConflictError(RuntimeError):
    """Raised when an existing durable identity has different event content."""


class InMemoryMarketEventLog:
    """Deterministic Publisher test double with idempotent retry receipts."""

    def __init__(self) -> None:
        self._events: list[MarketEventEnvelopeV1] = []
        self._by_event_id: dict[str, tuple[MarketEventEnvelopeV1, int]] = {}
        self._by_source_identity: dict[
            tuple[str, str],
            MarketEventEnvelopeV1,
        ] = {}
        self._next_offsets: dict[str, int] = {}
        self._lock = asyncio.Lock()

    @property
    def events(self) -> tuple[MarketEventEnvelopeV1, ...]:
        return tuple(self._events)

    async def publish(
        self,
        events: Sequence[MarketEventEnvelopeV1],
    ) -> PublishReceipt:
        batch = tuple(events)
        if not batch:
            raise ValueError("event batches cannot be empty")
        if any(not isinstance(event, MarketEventEnvelopeV1) for event in batch):
            raise TypeError("events must contain MarketEventEnvelopeV1 values")

        async with self._lock:
            planned: list[tuple[MarketEventEnvelopeV1, int, bool]] = []
            batch_ids: dict[str, MarketEventEnvelopeV1] = {}
            batch_offsets: dict[str, int] = {}
            batch_sources: dict[tuple[str, str], MarketEventEnvelopeV1] = {}
            tentative_offsets = dict(self._next_offsets)
            for event in batch:
                existing_batch = batch_ids.get(event.event_id)
                if existing_batch is not None:
                    _require_same(existing_batch, event)
                    planned.append((event, batch_offsets[event.event_id], False))
                    continue
                batch_ids[event.event_id] = event

                source_identity = _source_identity(event)
                if source_identity is not None:
                    existing_batch_source = batch_sources.get(source_identity)
                    if existing_batch_source is not None:
                        _require_same(existing_batch_source, event)
                    else:
                        batch_sources[source_identity] = event

                existing = self._by_event_id.get(event.event_id)
                if existing is not None:
                    _require_same(existing[0], event)
                    batch_offsets[event.event_id] = existing[1]
                    planned.append((event, existing[1], False))
                    continue
                if source_identity is not None:
                    existing_source = self._by_source_identity.get(source_identity)
                    if existing_source is not None:
                        _require_same(existing_source, event)

                offset = tentative_offsets.get(event.partition_key, 0)
                tentative_offsets[event.partition_key] = offset + 1
                batch_offsets[event.event_id] = offset
                planned.append((event, offset, True))

            acknowledged: dict[str, int] = {}
            for event, offset, is_new in planned:
                if is_new:
                    self._events.append(event)
                    self._by_event_id[event.event_id] = (event, offset)
                    source_identity = _source_identity(event)
                    if source_identity is not None:
                        self._by_source_identity[source_identity] = event
                acknowledged[event.partition_key] = offset
            self._next_offsets = tentative_offsets
            return PublishReceipt(
                accepted_count=len(batch),
                partition_offsets=tuple(sorted(acknowledged.items())),
            )


def _source_identity(event: MarketEventEnvelopeV1) -> tuple[str, str] | None:
    if event.source_event_id is None:
        return None
    return event.partition_key, event.source_event_id


def _require_same(
    existing: MarketEventEnvelopeV1,
    candidate: MarketEventEnvelopeV1,
) -> None:
    if existing.to_wire() != candidate.to_wire():
        raise MarketEventIdentityConflictError(
            f"market event identity {existing.event_id} has conflicting content"
        )
