"""PostgreSQL archive_command adapter for immutable object storage."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from app.server_runtime.query_wal_archive import ImmutablePostgresWalArchive
from app.server_runtime.storage.s3 import S3ImmutableObjectStore

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_WAL_"


async def run(source_path: Path, filename: str) -> dict[str, object]:
    if not isinstance(source_path, Path):
        raise TypeError("source_path must be a pathlib.Path")
    if source_path.name != filename:
        raise RuntimeError("WAL source basename differs from archive filename")
    max_bytes = _positive_env("MAX_OBJECT_BYTES", 64 * 1024 * 1024)
    try:
        size = source_path.stat().st_size
    except OSError as exc:
        raise RuntimeError("WAL source cannot be inspected") from exc
    if not 0 < size <= max_bytes:
        raise RuntimeError("WAL source size is outside the configured bound")
    try:
        data = source_path.read_bytes()
    except OSError as exc:
        raise RuntimeError("WAL source cannot be read") from exc
    if len(data) != size:
        raise RuntimeError("WAL source size changed while it was read")
    result = await _archive(max_bytes).archive(filename, data)
    return {
        "filename": result.filename,
        "uri": result.uri,
        "sha256": result.sha256,
        "size_bytes": result.size_bytes,
        "created": result.created,
    }


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: server_query_wal_archive.py SOURCE_PATH WAL_FILENAME")
    result = asyncio.run(run(Path(sys.argv[1]), sys.argv[2]))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


def _archive(max_bytes: int) -> ImmutablePostgresWalArchive:
    return ImmutablePostgresWalArchive(
        object_store=_store(),
        cluster_id=_required_env("CLUSTER_ID"),
        max_object_bytes=max_bytes,
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
