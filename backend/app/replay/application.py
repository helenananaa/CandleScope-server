"""Transport-neutral replay application port used by FastAPI routers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Protocol

from app.replay.commands import CommandResult
from app.replay.models import ReplayCommand, ReplayEvent


class ReplayApplication(Protocol):
    """Methods the HTTP/WS replay surface actually calls."""

    def capabilities(self) -> Mapping[str, object]: ...

    async def catalog(self) -> Mapping[str, object]: ...

    async def create_run(
        self, payload: Mapping[str, object], *, principal: object
    ) -> Mapping[str, object]: ...

    async def list_runs(self, *, principal: object) -> Mapping[str, object]: ...

    async def get_run(
        self, run_id: str, *, principal: object
    ) -> Mapping[str, object]: ...

    async def get_session(
        self, session_id: str, *, principal: object
    ) -> Mapping[str, object]: ...

    async def submit_command(
        self,
        session_id: str,
        command: ReplayCommand,
        *,
        principal: object,
    ) -> CommandResult: ...

    async def cancel_run(
        self, run_id: str, *, principal: object
    ) -> Mapping[str, object]: ...

    def endpoint_inventory(self) -> Mapping[str, str]: ...

    async def subscribe_events(
        self,
        session_id: str,
        *,
        after_sequence: int | None,
        principal: object,
    ) -> AsyncIterator[ReplayEvent]: ...
