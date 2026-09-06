"""Single-session Replay Worker with lease renew and deterministic takeover."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from app.replay.commands import CommandResult
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.replay.models import ReplayCommand
from app.server_contracts import MarketEventQuery
from app.server_runtime.replay_lease import (
    ReplaySessionLease,
    ReplaySessionLeaseFencedError,
    ReplaySessionLeaseStore,
)
from app.server_runtime.replay_session import (
    ServerReplaySession,
    ServerReplaySessionSpec,
    ServerReplaySessionStore,
)
from app.server_runtime.replay_worker_health import (
    ReplayWorkerHealth,
    ReplayWorkerState,
)
from app.server_runtime.replay_worker_settings import ReplayWorkerSettings

ClockMs = Callable[[], int]


class ReplayWorker:
    """Own at most one leased ServerReplaySession and renew until fenced."""

    def __init__(
        self,
        settings: ReplayWorkerSettings,
        *,
        lease_store: ReplaySessionLeaseStore,
        session_store: ServerReplaySessionStore,
        query: MarketEventQuery,
        clock_ms: ClockMs | None = None,
        verify_privileges: Callable[[], object] | None = None,
    ) -> None:
        if not isinstance(settings, ReplayWorkerSettings):
            raise TypeError("settings must be ReplayWorkerSettings")
        self._settings = settings
        self._lease_store = lease_store
        self._session_store = session_store
        self._query = query
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._verify_privileges = verify_privileges
        self._state = ReplayWorkerState.STARTING
        self._session: ServerReplaySession | None = None
        self._lease: ReplaySessionLease | None = None
        self._renew_task: asyncio.Task[None] | None = None
        self._last_renew_success_at_ms: int | None = None
        self._last_mutation_at_ms: int | None = None
        self._last_checkpoint_at_ms: int | None = None
        self._recoveries = 0
        self._recovery_failures = 0
        self._fencing_conflicts = 0
        self._last_error_code: str | None = None
        self._started_at_ms = self._clock_ms()

    @property
    def lease(self) -> ReplaySessionLease | None:
        return self._lease

    @property
    def session(self) -> ServerReplaySession | None:
        return self._session

    def health(self) -> ReplayWorkerHealth:
        now = self._clock_ms()
        checkpoint_age = None
        if self._last_checkpoint_at_ms is not None:
            checkpoint_age = max(0, now - self._last_checkpoint_at_ms)
        ready = (
            self._state is ReplayWorkerState.READY
            and self._session is not None
            and self._lease is not None
        )
        return ReplayWorkerHealth(
            worker_id=self._settings.worker_id,
            state=self._state,
            ready=ready,
            active_actors=1 if self._session is not None else 0,
            lease_expires_at_ms=(
                None if self._lease is None else self._lease.lease_expires_at_ms
            ),
            last_renew_success_at_ms=self._last_renew_success_at_ms,
            last_mutation_at_ms=self._last_mutation_at_ms,
            last_checkpoint_at_ms=self._last_checkpoint_at_ms,
            checkpoint_age_ms=checkpoint_age,
            recoveries=self._recoveries,
            recovery_failures=self._recovery_failures,
            fencing_conflicts=self._fencing_conflicts,
            last_error_code=self._last_error_code,
            updated_at_ms=now,
        )

    async def start_new(self, spec: ServerReplaySessionSpec) -> dict[str, object]:
        await self._prepare()
        session = ServerReplaySession(
            spec,
            lease_store=self._lease_store,
            session_store=self._session_store,
            query=self._query,
        )
        snapshot = await session.start()
        self._lease = spec.lease
        self._session = session
        self._state = ReplayWorkerState.READY
        self._last_checkpoint_at_ms = self._clock_ms()
        self._start_renew_loop()
        return snapshot

    async def recover(self, spec: ServerReplaySessionSpec) -> dict[str, object]:
        await self._prepare()
        self._state = ReplayWorkerState.RECOVERING
        try:
            session = ServerReplaySession(
                spec,
                lease_store=self._lease_store,
                session_store=self._session_store,
                query=self._query,
            )
            snapshot = await session.recover()
        except BaseException:
            self._recovery_failures += 1
            self._state = ReplayWorkerState.FENCED
            self._last_error_code = "RECOVERY_FAILED"
            raise
        self._recoveries += 1
        self._lease = spec.lease
        self._session = session
        self._state = ReplayWorkerState.READY
        self._last_checkpoint_at_ms = self._clock_ms()
        self._start_renew_loop()
        return snapshot

    async def submit(self, command: ReplayCommand) -> CommandResult:
        session = self._require_ready_session()
        try:
            result = await session.submit(command)
        except ReplaySessionLeaseFencedError:
            await self._enter_fenced("LEASE_FENCED")
            raise
        except ReplayDomainError as exc:
            if exc.code is ReplayErrorCode.PERSISTENCE_DEGRADED:
                await self._enter_fenced(exc.code.value)
            raise
        self._last_mutation_at_ms = self._clock_ms()
        self._last_checkpoint_at_ms = self._clock_ms()
        return result

    async def abandon(self) -> None:
        """Simulate SIGKILL: stop loops without releasing or closing durable state."""

        self._state = ReplayWorkerState.STOPPED
        if self._renew_task is not None:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None
        if self._session is not None and self._session._actor is not None:
            from app.server_runtime.replay_session import abort_unregistered_actor

            await abort_unregistered_actor(self._session._actor)
        self._session = None
        self._lease = None

    async def stop(self) -> None:
        self._state = ReplayWorkerState.STOPPING
        if self._renew_task is not None:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None
        session = self._session
        lease = self._lease
        try:
            if session is not None:
                await session.close()
        finally:
            if lease is not None:
                try:
                    await self._lease_store.release(lease)
                except ReplaySessionLeaseFencedError:
                    self._fencing_conflicts += 1
            self._session = None
            self._lease = None
            self._state = ReplayWorkerState.STOPPED

    async def _prepare(self) -> None:
        if self._session is not None:
            raise RuntimeError("replay worker already owns a session")
        self._state = ReplayWorkerState.STARTING
        if self._verify_privileges is not None:
            result = self._verify_privileges()
            if asyncio.iscoroutine(result):
                await result

    def _start_renew_loop(self) -> None:
        if self._renew_task is None:
            self._renew_task = asyncio.create_task(
                self._renew_loop(),
                name=f"replay-worker-renew-{self._settings.worker_id}",
            )

    async def _renew_loop(self) -> None:
        interval = self._settings.renew_interval_ms / 1_000
        while self._state is ReplayWorkerState.READY:
            try:
                await asyncio.sleep(interval)
                lease = self._lease
                if lease is None:
                    return
                renewed = await self._lease_store.renew(
                    lease,
                    lease_ttl_ms=self._settings.lease_ttl_ms,
                )
                self._lease = renewed
                if self._session is not None:
                    self._session._lease = renewed
                self._last_renew_success_at_ms = self._clock_ms()
            except asyncio.CancelledError:
                raise
            except ReplaySessionLeaseFencedError:
                await self._enter_fenced("LEASE_FENCED")
                return
            except Exception:  # noqa: BLE001
                await self._enter_fenced("RENEW_FAILED")
                return

    async def _enter_fenced(self, code: str) -> None:
        self._fencing_conflicts += 1
        self._last_error_code = code
        self._state = ReplayWorkerState.FENCED
        session = self._session
        if session is not None:
            session._accepting = False

    def _require_ready_session(self) -> ServerReplaySession:
        if self._state is ReplayWorkerState.FENCED:
            raise ReplaySessionLeaseFencedError("replay worker is fenced")
        if self._state is not ReplayWorkerState.READY or self._session is None:
            raise RuntimeError("replay worker is not ready")
        return self._session
