"""Durable outbox reader for server WebSocket replay."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from typing import Protocol

from app.replay.models import ReplayEvent


class OutboxReader(Protocol):
    async def events_after(
        self, session_id: str, after_sequence: int
    ) -> tuple[Mapping[str, object], ...]: ...


class ReplayEventStream:
    def __init__(
        self,
        reader: OutboxReader,
        *,
        scope_check: Callable[[str, str, str], None] | None = None,
    ) -> None:
        self._reader = reader
        self._scope_check = scope_check

    async def subscribe(
        self,
        session_id: str,
        *,
        after_sequence: int | None,
        organization_id: str,
        workspace_id: str,
    ) -> AsyncIterator[ReplayEvent]:
        if self._scope_check is not None:
            self._scope_check(session_id, organization_id, workspace_id)
        start = 0 if after_sequence is None else after_sequence
        rows = await self._reader.events_after(session_id, start)
        for row in rows:
            yield ReplayEvent.from_dict(row)
