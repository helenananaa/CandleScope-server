"""PostgreSQL restore_command adapter for immutable object storage."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

from app.server_runtime.query_wal_archive import ImmutablePostgresWalArchive
from app.server_runtime.storage.s3 import S3ImmutableObjectStore

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_WAL_"


async def run(filename: str, destination: Path) -> dict[str, object]:
    if not isinstance(destination, Path):
        raise TypeError("destination must be a pathlib.Path")
    root = Path(_required_env("RECOVERY_ROOT")).resolve(strict=True)
    resolved = destination.resolve(strict=False)
    if resolved == root or root not in resolved.parents:
        raise RuntimeError("WAL restore destination escapes the recovery root")
    if not resolved.parent.is_dir():
        raise RuntimeError("WAL restore destination parent does not exist")
    result, data = await _archive().restore(filename)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=resolved.parent,
        prefix=f".{resolved.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.chmod(0o600)
        temporary.replace(resolved)
        directory_descriptor = os.open(resolved.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return {
        "filename": result.filename,
        "uri": result.uri,
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
        "destination": str(resolved),
    }


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: server_query_wal_restore.py WAL_FILENAME DESTINATION")
    result = asyncio.run(run(sys.argv[1], Path(sys.argv[2])))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _archive() -> ImmutablePostgresWalArchive:
    return ImmutablePostgresWalArchive(
        object_store=_store(),
        cluster_id=_required_env("CLUSTER_ID"),
        max_object_bytes=_positive_env("MAX_OBJECT_BYTES", 64 * 1024 * 1024),
    )


def _store() -> S3ImmutableObjectStore:
    return S3ImmutableObjectStore(
        endpoint_url=_required_env("S3_ENDPOINT_URL"),
        region=_optional_env("S3_REGION", "us-east-1"),
        bucket=_required_env("S3_BUCKET"),
        prefix=_required_env("S3_PREFIX"),
        access_key_id=_required_env("S3_ACCESS_KEY_ID"),
        secret_access_key=_required_env("S3_SECRET_ACCESS_KEY"),
        request_timeout_ms=_positive_env("REQUEST_TIMEOUT_MS", 10_000),
    )


def _required_env(suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise RuntimeError(f"required setting {name} is missing")
    return value.strip()


def _optional_env(suffix: str, default: str) -> str:
    value = os.environ.get(f"{ENV_PREFIX}{suffix}", default).strip()
    if not value:
        raise RuntimeError(f"setting {ENV_PREFIX}{suffix} cannot be blank")
    return value


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
