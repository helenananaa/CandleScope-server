"""Immutable S3-compatible archive for PostgreSQL WAL recovery objects."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass

from app.server_runtime.object_store import ImmutableObjectStore

WAL_ARCHIVE_FORMAT_VERSION = "candlescope.postgres-wal.v1"
DEFAULT_MAX_WAL_OBJECT_BYTES = 64 * 1024 * 1024
_WAL_SEGMENT = re.compile(r"^[0-9A-F]{24}$")
_TIMELINE_HISTORY = re.compile(r"^[0-9A-F]{8}\.history$")
_BACKUP_HISTORY = re.compile(r"^[0-9A-F]{24}\.[0-9A-F]{8}\.backup$")


class WalArchiveError(RuntimeError):
    """A WAL archive object is invalid, conflicting, or unavailable."""


@dataclass(frozen=True, slots=True)
class ArchivedWalObject:
    filename: str
    uri: str
    sha256: str
    size_bytes: int
    created: bool


class ImmutablePostgresWalArchive:
    def __init__(
        self,
        *,
        object_store: ImmutableObjectStore,
        cluster_id: str,
        max_object_bytes: int = DEFAULT_MAX_WAL_OBJECT_BYTES,
    ) -> None:
        if not isinstance(object_store, ImmutableObjectStore):
            raise TypeError("object_store must implement ImmutableObjectStore")
        self._object_store = object_store
        self._cluster_id = _safe_token(cluster_id, field="cluster_id")
        if (
            isinstance(max_object_bytes, bool)
            or not isinstance(max_object_bytes, int)
            or max_object_bytes <= 0
        ):
            raise ValueError("max_object_bytes must be a positive integer")
        self._max_object_bytes = max_object_bytes

    async def archive(self, filename: str, data: bytes) -> ArchivedWalObject:
        filename = _wal_filename(filename)
        data = self._bounded_data(data)
        sha256 = hashlib.sha256(data).hexdigest()
        key = self.key_for(filename)
        await self._object_store.check_bucket()
        created = await self._object_store.put_if_absent(
            key,
            data,
            content_type="application/octet-stream",
            metadata={
                "format-version": WAL_ARCHIVE_FORMAT_VERSION,
                "cluster-id": self._cluster_id,
                "sha256": sha256,
                "size-bytes": str(len(data)),
            },
        )
        if not created:
            existing = await self._object_store.get(key)
            if existing.data != data:
                raise WalArchiveError(
                    "immutable WAL archive key contains different bytes"
                )
            self._require_metadata(
                existing.metadata,
                sha256=sha256,
                size_bytes=len(data),
            )
        return ArchivedWalObject(
            filename=filename,
            uri=self._object_store.uri_for(key),
            sha256=sha256,
            size_bytes=len(data),
            created=created,
        )

    async def restore(self, filename: str) -> tuple[ArchivedWalObject, bytes]:
        filename = _wal_filename(filename)
        key = self.key_for(filename)
        await self._object_store.check_bucket()
        stored = await self._object_store.get(key)
        data = self._bounded_data(stored.data)
        sha256 = hashlib.sha256(data).hexdigest()
        self._require_metadata(
            stored.metadata,
            sha256=sha256,
            size_bytes=len(data),
        )
        return (
            ArchivedWalObject(
                filename=filename,
                uri=self._object_store.uri_for(key),
                sha256=sha256,
                size_bytes=len(data),
                created=False,
            ),
            data,
        )

    def key_for(self, filename: str) -> str:
        return f"wal/v1/{self._cluster_id}/{_wal_filename(filename)}"

    def _bounded_data(self, data: bytes) -> bytes:
        if not isinstance(data, bytes) or not data:
            raise WalArchiveError("WAL archive object must contain non-empty bytes")
        if len(data) > self._max_object_bytes:
            raise WalArchiveError("WAL archive object exceeds the configured bound")
        return data

    def _require_metadata(
        self,
        metadata: object,
        *,
        sha256: str,
        size_bytes: int,
    ) -> None:
        if not isinstance(metadata, Mapping):
            raise WalArchiveError("WAL archive metadata is invalid")
        if (
            metadata.get("format-version") != WAL_ARCHIVE_FORMAT_VERSION
            or metadata.get("cluster-id") != self._cluster_id
            or metadata.get("sha256") != sha256
            or metadata.get("size-bytes") != str(size_bytes)
        ):
            raise WalArchiveError("WAL archive metadata has drifted")


def _wal_filename(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("WAL filename must be a non-empty string")
    if not (
        _WAL_SEGMENT.fullmatch(value)
        or _TIMELINE_HISTORY.fullmatch(value)
        or _BACKUP_HISTORY.fullmatch(value)
    ):
        raise ValueError("WAL filename is not a PostgreSQL archive filename")
    return value


def _safe_token(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        raise ValueError(f"{field} must contain 1-128 characters")
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789._:-"
    if value[0] not in "abcdefghijklmnopqrstuvwxyz0123456789" or any(
        character not in allowed for character in value
    ):
        raise ValueError(f"{field} must use lower-case safe ASCII")
    return value
