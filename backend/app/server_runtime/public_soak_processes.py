"""Role subprocess orchestration for the Phase 1AI public soak.

Secrets may enter child environments only. They are never stored on the
manager, ``ManagedRole``, evidence events, or ``repr()``.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from app.server_runtime.public_soak_manifest import (
    LOOPBACK_HOSTS,
    PublicSoakManifestError,
    _health_url,
    _positive_int,
    _required_text,
    _safe_id,
)

ClockMs = Callable[[], int]
Sleeper = Callable[[float], Awaitable[None]]
HealthProbe = Callable[[str], Awaitable[tuple[int, bytes]]]
START_ORDER = (
    "collector",
    "writer",
    "archiver",
    "query",
    "scheduler",
    "worker_a",
    "worker_b",
    "api",
)
STOP_ORDER = tuple(reversed(START_ORDER))
INIT_ORDER = (
    "collector-init-schema",
    "writer-init-schema",
    "archiver-init-bucket",
    "postgres-query-control",
    "postgres-replay-runtime",
)


class RoleProcessError(RuntimeError):
    """A soak role failed to start, become ready, or stop cleanly."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class RoleSpec:
    name: str
    argv: tuple[str, ...]
    sanitized_environment_keys: tuple[str, ...]
    health_url: str
    startup_timeout_ms: int
    shutdown_timeout_ms: int
    stdout_log: str
    stderr_log: str
    max_log_bytes: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _safe_id(self.name, "name"))
        argv = tuple(self.argv)
        if not argv or any(
            not isinstance(item, str) or not item.strip() for item in argv
        ):
            raise RoleProcessError(
                "INVALID_ARGV",
                "argv must be a non-empty argument vector",
            )
        object.__setattr__(self, "argv", argv)
        keys = tuple(
            _env_key(item, "sanitized_environment_keys")
            for item in self.sanitized_environment_keys
        )
        if len(set(keys)) != len(keys):
            raise RoleProcessError(
                "INVALID_ENV_KEYS",
                "sanitized_environment_keys cannot contain duplicates",
            )
        object.__setattr__(self, "sanitized_environment_keys", keys)
        object.__setattr__(
            self,
            "health_url",
            _health_url(self.health_url, field="health_url"),
        )
        object.__setattr__(
            self,
            "startup_timeout_ms",
            _positive_int(self.startup_timeout_ms, "startup_timeout_ms"),
        )
        object.__setattr__(
            self,
            "shutdown_timeout_ms",
            _positive_int(self.shutdown_timeout_ms, "shutdown_timeout_ms"),
        )
        object.__setattr__(
            self,
            "max_log_bytes",
            _positive_int(self.max_log_bytes, "max_log_bytes"),
        )
        object.__setattr__(
            self,
            "stdout_log",
            _absolute_log_path(self.stdout_log, "stdout_log"),
        )
        object.__setattr__(
            self,
            "stderr_log",
            _absolute_log_path(self.stderr_log, "stderr_log"),
        )


@dataclass(frozen=True, slots=True)
class InitCommand:
    name: str
    argv: tuple[str, ...]
    sanitized_environment_keys: tuple[str, ...]
    timeout_ms: int
    stdout_log: str
    stderr_log: str
    max_log_bytes: int


@dataclass(frozen=True, slots=True)
class ManagedRole:
    name: str
    pid: int | None
    started_at_utc: str | None
    started_at_monotonic_ms: int | None
    exit_code: int | None
    stdout_log: str
    stderr_log: str
    present_environment_keys: tuple[str, ...]

    def to_evidence(self) -> dict[str, object]:
        return {
            "name": self.name,
            "pid": self.pid,
            "started_at_utc": self.started_at_utc,
            "started_at_monotonic_ms": self.started_at_monotonic_ms,
            "exit_code": self.exit_code,
            "stdout_log": Path(self.stdout_log).name,
            "stderr_log": Path(self.stderr_log).name,
            "present_environment_keys": list(self.present_environment_keys),
        }


class RoleProcessManager:
    """Start, probe, and stop soak roles without retaining secrets."""

    def __init__(
        self,
        *,
        clock_ms: ClockMs | None = None,
        sleep: Sleeper | None = None,
        health_probe: HealthProbe | None = None,
        now_utc: Callable[[], str] | None = None,
    ) -> None:
        self._clock_ms = clock_ms or _default_clock_ms
        self._sleep = sleep or asyncio.sleep
        self._health_probe = health_probe or _default_health_probe
        self._now_utc = now_utc or _default_utc
        self._live: dict[str, asyncio.subprocess.Process] = {}
        self._pumps: dict[str, tuple[asyncio.Task[None], asyncio.Task[None]]] = {}
        self.roles: dict[str, ManagedRole] = {}
        self.events: list[dict[str, object]] = []

    def __repr__(self) -> str:
        return f"RoleProcessManager(roles={sorted(self.roles)})"

    async def run_init(
        self,
        command: InitCommand,
        environment: Mapping[str, str],
    ) -> int:
        spec = RoleSpec(
            name=command.name,
            argv=command.argv,
            sanitized_environment_keys=command.sanitized_environment_keys,
            health_url="http://127.0.0.1:9/health",
            startup_timeout_ms=command.timeout_ms,
            shutdown_timeout_ms=command.timeout_ms,
            stdout_log=command.stdout_log,
            stderr_log=command.stderr_log,
            max_log_bytes=command.max_log_bytes,
        )
        role = await self._spawn(spec, environment, event="init_started")
        process = self._live[spec.name]
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=command.timeout_ms / 1000,
            )
        except TimeoutError as exc:
            await self._signal_stop(spec, role, escalate=True)
            raise RoleProcessError(
                "INIT_TIMEOUT",
                f"{command.name} exceeded init timeout",
                details={"name": command.name},
            ) from exc
        finally:
            await self._finalize(spec.name)
        if process.returncode != 0:
            raise RoleProcessError(
                "INIT_FAILED",
                f"{command.name} exited {process.returncode}",
                details={"name": command.name, "exit_code": process.returncode},
            )
        self._record("init_completed", spec.name, exit_code=process.returncode)
        return int(process.returncode)

    async def start_role(
        self,
        spec: RoleSpec,
        environment: Mapping[str, str],
    ) -> ManagedRole:
        role = await self._spawn(spec, environment, event="started")
        deadline = role.started_at_monotonic_ms + spec.startup_timeout_ms
        assert deadline is not None
        while self._clock_ms() < deadline:
            if await self._is_ready(spec):
                self._record("ready", spec.name)
                return role
            await self._sleep(0.05)
        self._record("ready_timeout", spec.name)
        await self.stop_role(spec)
        raise RoleProcessError(
            "READY_TIMEOUT",
            f"{spec.name} did not become ready before startup_timeout_ms",
            details={"name": spec.name, "health_url": spec.health_url},
        )

    async def start_in_order(
        self,
        specs: Sequence[RoleSpec],
        environments: Mapping[str, Mapping[str, str]],
    ) -> list[ManagedRole]:
        started: list[RoleSpec] = []
        roles: list[ManagedRole] = []
        try:
            for spec in specs:
                env = environments[spec.name]
                roles.append(await self.start_role(spec, env))
                started.append(spec)
        except RoleProcessError:
            for spec in reversed(started):
                await self.stop_role(spec)
            raise
        return roles

    async def stop_role(self, spec: RoleSpec) -> ManagedRole:
        role = self.roles.get(spec.name)
        if role is None:
            raise RoleProcessError(
                "ROLE_NOT_STARTED",
                f"{spec.name} is not a live soak role",
            )
        return await self._signal_stop(spec, role, escalate=True)

    async def stop_in_reverse(self, specs: Sequence[RoleSpec]) -> list[ManagedRole]:
        stopped: list[ManagedRole] = []
        ordered = sorted(
            specs,
            key=lambda spec: (
                STOP_ORDER.index(spec.name) if spec.name in STOP_ORDER else 99
            ),
        )
        for spec in ordered:
            if spec.name in self.roles:
                stopped.append(await self.stop_role(spec))
        return stopped

    async def _spawn(
        self,
        spec: RoleSpec,
        environment: Mapping[str, str],
        *,
        event: str,
    ) -> ManagedRole:
        if spec.name in self._live:
            raise RoleProcessError(
                "ROLE_ALREADY_STARTED",
                f"{spec.name} is already running",
            )
        child_env = {str(key): str(value) for key, value in environment.items()}
        present = tuple(
            key for key in spec.sanitized_environment_keys if key in child_env
        )
        Path(spec.stdout_log).parent.mkdir(parents=True, exist_ok=True)
        Path(spec.stderr_log).parent.mkdir(parents=True, exist_ok=True)
        self._record("start_requested", spec.name)
        process = await asyncio.create_subprocess_exec(
            *spec.argv,
            env=child_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_task = asyncio.create_task(
            _pump_bounded(process.stdout, spec.stdout_log, spec.max_log_bytes)
        )
        stderr_task = asyncio.create_task(
            _pump_bounded(process.stderr, spec.stderr_log, spec.max_log_bytes)
        )
        self._live[spec.name] = process
        self._pumps[spec.name] = (stdout_task, stderr_task)
        role = ManagedRole(
            name=spec.name,
            pid=process.pid,
            started_at_utc=self._now_utc(),
            started_at_monotonic_ms=self._clock_ms(),
            exit_code=None,
            stdout_log=spec.stdout_log,
            stderr_log=spec.stderr_log,
            present_environment_keys=present,
        )
        self.roles[spec.name] = role
        self._record(event, spec.name, pid=process.pid)
        return role

    async def _is_ready(self, spec: RoleSpec) -> bool:
        process = self._live.get(spec.name)
        if process is not None and process.returncode is not None:
            raise RoleProcessError(
                "ROLE_EXITED",
                f"{spec.name} exited before becoming ready",
                details={"exit_code": process.returncode},
            )
        try:
            status, body = await self._health_probe(spec.health_url)
        except Exception:  # noqa: BLE001
            return False
        if len(body) > spec.max_log_bytes:
            raise RoleProcessError(
                "HEALTH_PAYLOAD_TOO_LARGE",
                f"{spec.name} health payload exceeded the log bound",
            )
        return status == 200

    async def _signal_stop(
        self,
        spec: RoleSpec,
        role: ManagedRole,
        *,
        escalate: bool,
    ) -> ManagedRole:
        process = self._live.get(spec.name)
        if process is None or process.returncode is not None:
            return await self._finalize(spec.name)
        self._record("stop_sigterm", spec.name, pid=process.pid)
        process.terminate()
        try:
            await asyncio.wait_for(
                process.wait(),
                timeout=spec.shutdown_timeout_ms / 1000,
            )
        except TimeoutError:
            if not escalate:
                raise
            self._record("stop_sigkill", spec.name, pid=process.pid)
            process.kill()
            await process.wait()
        return await self._finalize(spec.name)

    async def _finalize(self, name: str) -> ManagedRole:
        process = self._live.pop(name, None)
        pumps = self._pumps.pop(name, None)
        if pumps is not None:
            await asyncio.gather(*pumps, return_exceptions=True)
        role = self.roles[name]
        exit_code = None if process is None else process.returncode
        updated = replace(role, exit_code=exit_code)
        self.roles[name] = updated
        self._record("stopped", name, exit_code=exit_code, pid=role.pid)
        return updated

    def _record(self, event: str, role: str, **fields: object) -> None:
        payload = {
            "event": event,
            "role": role,
            "at_ms": self._clock_ms(),
        }
        payload.update(fields)
        self.events.append(payload)


def role_environment(
    spec: RoleSpec,
    values: Mapping[str, str],
    *,
    inherited: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Copy inherited process env plus declared keys. Caller keeps secrets."""

    env = dict(os.environ if inherited is None else inherited)
    for key in spec.sanitized_environment_keys:
        if key in values:
            env[key] = values[key]
    return env


async def _pump_bounded(
    stream: asyncio.StreamReader | None,
    path: str,
    max_bytes: int,
) -> None:
    written = 0
    with Path(path).open("wb") as handle:
        if stream is None:
            return
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                return
            remaining = max_bytes - written
            if remaining <= 0:
                continue
            piece = chunk if len(chunk) <= remaining else chunk[:remaining]
            handle.write(piece)
            written += len(piece)
            handle.flush()


async def _default_health_probe(url: str) -> tuple[int, bytes]:
    import aiohttp

    parsed_host = url.split("://", 1)[-1].split("/", 1)[0].split(":")[0]
    if parsed_host not in LOOPBACK_HOSTS:
        raise RoleProcessError("HEALTH_URL_UNSAFE", "health probe host is not loopback")
    timeout = aiohttp.ClientTimeout(total=0.5)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
        async with session.get(url, allow_redirects=False) as response:
            return response.status, await response.read()


def _env_key(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RoleProcessError("INVALID_ENV_KEYS", f"{field} must be a non-empty string")
    text = value.strip()
    allowed = frozenset(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-"
    )
    if len(text) > 128 or any(char not in allowed for char in text):
        raise RoleProcessError(
            "INVALID_ENV_KEYS",
            f"{field} contains unsupported characters",
        )
    return text


def _absolute_log_path(value: object, field: str) -> str:
    text = _required_text(value, field)
    path = Path(text)
    if not path.is_absolute():
        raise PublicSoakManifestError(
            "RELATIVE_PATH",
            f"{field} must be an absolute path",
        )
    return str(path)


def _default_clock_ms() -> int:
    import time

    return time.time_ns() // 1_000_000


def _default_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def bootstrap_postgres_main() -> None:
    """Create login roles and apply existing query/replay migrations."""

    import asyncio

    asyncio.run(_bootstrap_postgres())


async def _bootstrap_postgres() -> None:
    import psycopg
    from psycopg import sql

    from app.server_runtime.query_migrations import PostgresQueryControlMigrator
    from app.server_runtime.replay_runtime_migrations import (
        DEFAULT_REPLAY_MIGRATION_PATH,
        PostgresReplayRuntimeMigrator,
    )
    from app.server_runtime.storage.postgres_lease import PostgresStreamLeaseStore
    from app.server_runtime.storage.postgres_replay_lease import (
        PostgresReplaySessionLeaseStore,
    )
    from app.server_runtime.storage.postgres_replay_scheduler import (
        DEFAULT_SCHEDULER_MIGRATION_PATH,
        apply_scheduler_migration,
    )

    admin_dsn = _required_env("CANDLESCOPE_PHASE1AI_POSTGRES_ADMIN_DSN")
    replay_role = _required_env("CANDLESCOPE_PHASE1AI_REPLAY_ROLE")
    replay_password = _required_env("CANDLESCOPE_PHASE1AI_REPLAY_ROLE_PASSWORD")
    query_role = _required_env("CANDLESCOPE_PHASE1AI_QUERY_ROLE")
    query_password = _required_env("CANDLESCOPE_PHASE1AI_QUERY_ROLE_PASSWORD")
    auditor_role = _required_env("CANDLESCOPE_PHASE1AI_QUERY_AUDITOR_ROLE")
    auditor_password = _required_env("CANDLESCOPE_PHASE1AI_QUERY_AUDITOR_PASSWORD")
    async with (
        await psycopg.AsyncConnection.connect(admin_dsn) as connection,
        connection.cursor() as cursor,
    ):
        for role, password in (
            (replay_role, replay_password),
            (query_role, query_password),
            (auditor_role, auditor_password),
        ):
            await cursor.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s",
                (role,),
            )
            if await cursor.fetchone() is None:
                await cursor.execute(
                    sql.SQL(
                        "CREATE ROLE {} LOGIN PASSWORD {} INHERIT "
                        "NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOREPLICATION NOBYPASSRLS"
                    ).format(sql.Identifier(role), sql.Literal(password))
                )
    await PostgresStreamLeaseStore(admin_dsn).initialize_schema()
    await PostgresReplaySessionLeaseStore(admin_dsn).initialize_schema()
    async with (
        await psycopg.AsyncConnection.connect(admin_dsn) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            "TRUNCATE TABLE candlescope_market_stream_lease"
        )
    await PostgresQueryControlMigrator(
        admin_dsn,
        migration_path=(
            Path(__file__).resolve().parents[3]
            / "deploy"
            / "server"
            / "postgres"
            / "migrations"
            / "001_query_control.sql"
        ),
        runtime_login_role=query_role,
        auditor_login_role=auditor_role,
        backend_ids=("clickhouse-market-events-v1",),
    ).apply()
    await PostgresReplayRuntimeMigrator(
        admin_dsn,
        migration_path=DEFAULT_REPLAY_MIGRATION_PATH,
        runtime_login_role=replay_role,
    ).apply()
    await apply_scheduler_migration(
        admin_dsn,
        migration_path=DEFAULT_SCHEDULER_MIGRATION_PATH,
        runtime_login_role=replay_role,
    )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value or not value.strip():
        raise RoleProcessError("INIT_FAILED", f"{name} is required")
    return value.strip()


__all__ = [
    "INIT_ORDER",
    "InitCommand",
    "ManagedRole",
    "RoleProcessError",
    "RoleProcessManager",
    "RoleSpec",
    "START_ORDER",
    "STOP_ORDER",
    "bootstrap_postgres_main",
    "role_environment",
]
