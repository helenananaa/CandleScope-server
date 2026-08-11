"""Run one trusted backup operation inside an exclusive query-write fence."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

from app.server_runtime.storage.postgres_query_control import (
    PostgresQueryBackupFence,
)

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_"


async def run(command: tuple[str, ...]) -> dict[str, Any]:
    if (
        not isinstance(command, tuple)
        or not command
        or any(not isinstance(value, str) or not value for value in command)
    ):
        raise ValueError("command must be a non-empty argument tuple")
    maximum_runtime_ms = _positive_env("MAXIMUM_RUNTIME_MS", 300_000)
    heartbeat_interval_ms = _positive_env("HEARTBEAT_INTERVAL_MS", 500)
    drain_timeout_ms = _positive_env("DRAIN_TIMEOUT_MS", 30_000)
    if heartbeat_interval_ms > maximum_runtime_ms:
        raise RuntimeError("heartbeat interval cannot exceed maximum runtime")
    if heartbeat_interval_ms >= drain_timeout_ms:
        raise RuntimeError("heartbeat interval must be shorter than drain timeout")
    fence = PostgresQueryBackupFence(
        _required_env("POSTGRES_AUDITOR_DSN"),
        operator_id=_required_env("OPERATOR_ID"),
        connect_timeout_ms=_positive_env("CONNECT_TIMEOUT_MS", 5_000),
        drain_timeout_ms=drain_timeout_ms,
    )
    receipt = await fence.acquire()
    process: asyncio.subprocess.Process | None = None
    started = asyncio.get_running_loop().time()
    try:
        environment = os.environ.copy()
        environment[f"{ENV_PREFIX}FENCE_ID"] = receipt.fence_id
        environment[f"{ENV_PREFIX}FENCE_ACQUIRED_AT_MS"] = str(receipt.acquired_at_ms)
        process = await asyncio.create_subprocess_exec(*command, env=environment)
        wait_task = asyncio.create_task(
            process.wait(),
            name="query-backup-window-command",
        )
        while not wait_task.done():
            elapsed_ms = int((asyncio.get_running_loop().time() - started) * 1_000)
            remaining_ms = maximum_runtime_ms - elapsed_ms
            if remaining_ms <= 0:
                await _terminate(process)
                raise TimeoutError("backup window command exceeded its runtime bound")
            try:
                await asyncio.wait_for(
                    asyncio.shield(wait_task),
                    timeout=min(heartbeat_interval_ms, remaining_ms) / 1_000,
                )
            except TimeoutError:
                await fence.heartbeat()
        return_code = await wait_task
        await fence.heartbeat()
    except BaseException:
        if process is not None and process.returncode is None:
            await _terminate(process)
        raise
    finally:
        await fence.release()
    return {
        "fence_id": receipt.fence_id,
        "operator_id": receipt.operator_id,
        "fence_acquired_at_ms": receipt.acquired_at_ms,
        "command_executable": Path(command[0]).name,
        "command_exit_code": return_code,
        "duration_ms": int((asyncio.get_running_loop().time() - started) * 1_000),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a command while all query-control writes are fenced."
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    command = tuple(arguments.command)
    if command[:1] == ("--",):
        command = command[1:]
    if not command:
        parser.error("a command is required after --")
    result = asyncio.run(run(command))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    if result["command_exit_code"] != 0:
        raise SystemExit(int(result["command_exit_code"]))


async def _terminate(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=5)
    except TimeoutError:
        process.kill()
        await process.wait()


def _required_env(suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _positive_env(suffix: str, default: int) -> int:
    raw = os.environ.get(f"{ENV_PREFIX}{suffix}", str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} must be an integer") from exc
    if value <= 0:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} must be positive")
    return value


if __name__ == "__main__":
    main()
