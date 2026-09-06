"""Scheduler-claimed Replay Worker loop. Owns at most one leased Actor."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Mapping

from app.replay.broker.models import BrokerConfig
from app.replay.errors import ReplayDomainError, ReplayErrorCode
from app.replay.models import ReplayCommand, ReplaySessionConfig
from app.server_contracts import MarketDataSnapshotRef, MarketEventQuery
from app.server_runtime.query_client import HttpSnapshotMarketEventQuery
from app.server_runtime.replay_lease import (
    ReplaySessionLeaseFencedError,
    ReplaySessionLeaseStore,
)
from app.server_runtime.replay_scheduler import (
    ReplayRequestState,
    ReplayScheduler,
    ReplaySchedulerAssignment,
)
from app.server_runtime.replay_session import (
    ServerReplaySessionSpec,
    ServerReplaySessionStore,
)
from app.server_runtime.replay_snapshot import ReplayServerSnapshotPin
from app.server_runtime.replay_worker import ReplayWorker
from app.server_runtime.replay_worker_settings import ReplayWorkerSettings

QueryFactory = Callable[[Mapping[str, object]], MarketEventQuery]


def server_query_from_payload(
    settings: ReplayWorkerSettings,
    payload: Mapping[str, object],
) -> MarketEventQuery:
    organization_id = str(payload.get("organization_id") or "")
    workspace_id = str(payload.get("workspace_id") or "")
    if (
        organization_id != settings.organization_id
        or workspace_id != settings.workspace_id
    ):
        raise ValueError("scheduler payload is outside the Worker query scope")
    return HttpSnapshotMarketEventQuery(
        base_url=settings.query_url,
        bearer_token=settings.query_credential,
        organization_id=settings.organization_id,
        workspace_id=settings.workspace_id,
        request_timeout_ms=settings.query_request_timeout_ms,
    )


def spec_from_assignment(
    *,
    lease,
    payload: Mapping[str, object],
    shutdown_timeout_seconds: float,
) -> ServerReplaySessionSpec:
    snapshot_payload = payload["snapshot"]
    pin_payload = payload["pin"]
    snapshot = MarketDataSnapshotRef(
        data_epoch=str(snapshot_payload["data_epoch"]),
        snapshot_version=int(snapshot_payload["snapshot_version"]),
        manifest_uri=str(snapshot_payload["manifest_uri"]),
        manifest_sha256=str(snapshot_payload["manifest_sha256"]),
    )
    pin = ReplayServerSnapshotPin(
        snapshot=snapshot,
        start_event_time_ms=int(pin_payload["start_event_time_ms"]),
        end_event_time_ms=int(pin_payload["end_event_time_ms"]),
        expected_first_agg_trade_id=int(pin_payload["expected_first_agg_trade_id"]),
        expected_last_agg_trade_id=int(pin_payload["expected_last_agg_trade_id"]),
        row_count=int(pin_payload["row_count"]),
    )
    return ServerReplaySessionSpec(
        lease=lease,
        pin=pin,
        config=ReplaySessionConfig.from_dict(payload["config"]),
        broker_config=BrokerConfig.from_dict(payload["broker_config"]),
        replay_start_ms=int(payload["replay_start_ms"]),
        replay_end_time_ms=int(payload["replay_end_time_ms"]),
        command_queue_size=32,
        event_buffer_size=64,
        max_emit_fps=30,
        controller_ttl_seconds=30.0,
        checkpoint_event_interval=1,
        checkpoint_virtual_ms=60_000,
        max_closed_bars=16,
        shutdown_timeout_seconds=shutdown_timeout_seconds,
    )


class ReplayWorkerPoolLoop:
    """Heartbeat, claim, start or recover, then drain the durable command journal."""

    def __init__(
        self,
        settings: ReplayWorkerSettings,
        *,
        scheduler: ReplayScheduler,
        lease_store: ReplaySessionLeaseStore,
        session_store: ServerReplaySessionStore,
        query_factory: QueryFactory | None = None,
    ) -> None:
        self._settings = settings
        self._scheduler = scheduler
        self._lease_store = lease_store
        self._session_store = session_store
        self._query_factory = query_factory or (
            lambda payload: server_query_from_payload(settings, payload)
        )
        self._worker: ReplayWorker | None = None
        self._assignment: ReplaySchedulerAssignment | None = None
        self._stop = asyncio.Event()
        self._processed: set[str] = set()

    async def run(self) -> None:
        poll = max(0.05, self._settings.poll_interval_ms / 1_000)
        while not self._stop.is_set():
            try:
                await self._scheduler.heartbeat(self._settings.worker_id, capacity=1)
                if self._assignment is None:
                    await self._try_claim()
                elif await self._assignment_released():
                    await self._abandon_claim()
                else:
                    await self._drain_commands()
            except ReplaySessionLeaseFencedError:
                await self._abandon_claim()
            except ReplayDomainError as exc:
                print(
                    f"replay-worker-pool: {exc.code}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                await self._abandon_claim()
            except Exception as exc:  # noqa: BLE001
                print(
                    f"replay-worker-pool: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                await self._abandon_claim()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=poll)
            except TimeoutError:
                continue

    def request_stop(self) -> None:
        self._stop.set()

    async def stop(self) -> None:
        self.request_stop()
        await self._abandon_claim()

    async def _abandon_claim(self) -> None:
        worker = self._worker
        assignment = self._assignment
        self._worker = None
        self._assignment = None
        if worker is not None:
            try:
                await worker.stop()
            except Exception as exc:  # noqa: BLE001
                _stop_error = type(exc).__name__
                del _stop_error
        drop = getattr(self._scheduler, "drop_claim", None)
        if drop is not None and assignment is not None:
            try:
                await drop(assignment)
            except Exception as exc:  # noqa: BLE001
                _drop_error = type(exc).__name__
                del _drop_error

    async def _assignment_released(self) -> bool:
        assignment = self._assignment
        if assignment is None:
            return False
        request = await self._scheduler.get_request(assignment.request_id)
        if request is None:
            return True
        return request.state in {
            ReplayRequestState.CANCELLING,
            ReplayRequestState.CANCELLED,
            ReplayRequestState.FAILED,
            ReplayRequestState.COMPLETED,
        }

    async def _try_claim(self) -> None:
        assignment = await self._scheduler.claim(
            self._settings.worker_id,
            organization_id=self._settings.organization_id,
            workspace_id=self._settings.workspace_id,
        )
        if assignment is None:
            return
        self._assignment = assignment
        request = await self._scheduler.get_request(assignment.request_id)
        if request is None:
            return
        payload = dict(request.payload)
        query = self._query_factory(payload)
        snapshot_payload = payload["snapshot"]
        snapshot = MarketDataSnapshotRef(
            data_epoch=str(snapshot_payload["data_epoch"]),
            snapshot_version=int(snapshot_payload["snapshot_version"]),
            manifest_uri=str(snapshot_payload["manifest_uri"]),
            manifest_sha256=str(snapshot_payload["manifest_sha256"]),
        )
        lease = await self._lease_store.acquire(
            session_id=assignment.session_id,
            worker_id=self._settings.worker_id,
            snapshot=snapshot,
            organization_id=str(
                payload.get("organization_id") or request.organization_id
            ),
            workspace_id=str(payload.get("workspace_id") or request.workspace_id),
            lease_ttl_ms=self._settings.lease_ttl_ms,
        )
        spec = spec_from_assignment(
            lease=lease,
            payload=payload,
            shutdown_timeout_seconds=max(
                0.2, self._settings.shutdown_timeout_ms / 1_000
            ),
        )
        worker = ReplayWorker(
            self._settings,
            lease_store=self._lease_store,
            session_store=self._session_store,
            query=query,
            verify_privileges=getattr(
                self._session_store, "verify_runtime_privileges", None
            ),
        )
        if assignment.attempt > 1:
            await worker.recover(spec)
        else:
            await worker.start_new(spec)
        await self._scheduler.mark_running(assignment.request_id)
        self._worker = worker

    async def _drain_commands(self) -> None:
        if self._worker is None or self._assignment is None:
            return
        session_id = self._assignment.session_id
        for payload in await self._scheduler.list_commands(session_id):
            command_id = str(payload["command_id"])
            if command_id in self._processed:
                continue
            existing = await self._session_store.get_command_result(
                session_id, command_id
            )
            if existing is not None:
                self._processed.add(command_id)
                continue
            command = ReplayCommand.from_dict(payload)
            try:
                await self._worker.submit(command)
            except ReplaySessionLeaseFencedError:
                raise
            except ReplayDomainError as exc:
                if exc.code is ReplayErrorCode.PERSISTENCE_DEGRADED:
                    raise
            self._processed.add(command_id)


__all__ = [
    "ReplayWorkerPoolLoop",
    "server_query_from_payload",
    "spec_from_assignment",
]
