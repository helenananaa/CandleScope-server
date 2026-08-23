"""Host-private success inventory for scheduled backup cadence monitoring."""

from __future__ import annotations

import json
import os
import re
import stat
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import rfc8785

BACKUP_SUCCESS_REFERENCE_SCHEMA_VERSION = (
    "candlescope.query-backup-success-reference.v1"
)
DEFAULT_MAXIMUM_INVENTORY_FILES = 4_096
DEFAULT_MAXIMUM_REFERENCE_BYTES = 16 * 1_024
_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_REFERENCE_NAME = re.compile(
    r"^(?P<completed_at_ms>[0-9]{20})-"
    r"(?P<backup_id>[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12})\.json$"
)
_TEMPORARY_REFERENCE_NAME = re.compile(
    r"^\.[0-9]{20}-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\.json\."
    r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}\.tmp$"
)


class BackupSuccessInventoryError(RuntimeError):
    """The host-local success inventory cannot be trusted or persisted."""


@dataclass(frozen=True, slots=True)
class BackupSuccessReference:
    cluster_id: str
    backup_id: str
    completed_at_ms: int
    history_uri: str
    history_sha256: str
    schema_version: str = BACKUP_SUCCESS_REFERENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BACKUP_SUCCESS_REFERENCE_SCHEMA_VERSION:
            raise ValueError("backup success reference schema_version has drifted")
        object.__setattr__(
            self,
            "cluster_id",
            _token(self.cluster_id, field="cluster_id"),
        )
        object.__setattr__(
            self,
            "backup_id",
            _uuid(self.backup_id, field="backup_id"),
        )
        object.__setattr__(
            self,
            "completed_at_ms",
            _timestamp_ms(self.completed_at_ms, field="completed_at_ms"),
        )
        object.__setattr__(
            self,
            "history_uri",
            _s3_uri(self.history_uri, field="history_uri"),
        )
        object.__setattr__(
            self,
            "history_sha256",
            _sha256(self.history_sha256, field="history_sha256"),
        )

    def to_wire(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "cluster_id": self.cluster_id,
            "backup_id": self.backup_id,
            "completed_at_ms": self.completed_at_ms,
            "history_uri": self.history_uri,
            "history_sha256": self.history_sha256,
        }

    def canonical_bytes(self) -> bytes:
        return rfc8785.dumps(self.to_wire())

    @property
    def filename(self) -> str:
        return f"{self.completed_at_ms:020d}-{self.backup_id}.json"


@dataclass(frozen=True, slots=True)
class PersistedBackupSuccessReference:
    reference: BackupSuccessReference
    path: Path
    created: bool


@dataclass(frozen=True, slots=True)
class BackupSuccessInventorySnapshot:
    references: tuple[BackupSuccessReference, ...]
    total_file_count: int
    window_start_ms: int
    window_end_ms: int


class PrivateBackupSuccessInventory:
    def __init__(
        self,
        root: Path,
        *,
        maximum_files: int = DEFAULT_MAXIMUM_INVENTORY_FILES,
        maximum_reference_bytes: int = DEFAULT_MAXIMUM_REFERENCE_BYTES,
    ) -> None:
        if not isinstance(root, Path):
            raise TypeError("inventory root must be a Path")
        self._root_path = root
        self._maximum_files = _positive_int(
            maximum_files,
            field="maximum_files",
        )
        self._maximum_reference_bytes = _positive_int(
            maximum_reference_bytes,
            field="maximum_reference_bytes",
        )

    def persist(
        self,
        reference: BackupSuccessReference,
    ) -> PersistedBackupSuccessReference:
        if not isinstance(reference, BackupSuccessReference):
            raise TypeError("reference must be a BackupSuccessReference")
        root = self._private_root()
        path = root / reference.filename
        data = reference.canonical_bytes()
        if len(data) > self._maximum_reference_bytes:
            raise BackupSuccessInventoryError(
                "backup success reference exceeds its byte bound"
            )
        existing_names = self._entry_names(root)
        if (
            reference.filename not in existing_names
            and len(existing_names) >= self._maximum_files
        ):
            raise BackupSuccessInventoryError(
                "backup success inventory exceeds its file bound"
            )

        temporary = root / f".{reference.filename}.{uuid.uuid4()}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor: int | None = None
        created = False
        try:
            descriptor = os.open(temporary, flags, 0o600)
            os.fchmod(descriptor, 0o600)
            _write_all(descriptor, data)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            try:
                os.link(temporary, path, follow_symlinks=False)
                created = True
                _fsync_directory(root)
            except FileExistsError:
                existing = self._read(path)
                if existing != reference or existing.canonical_bytes() != data:
                    raise BackupSuccessInventoryError(
                        "immutable backup success reference contains different bytes"
                    )
        except (OSError, TypeError, ValueError) as exc:
            raise BackupSuccessInventoryError(
                "backup success reference could not be persisted"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                temporary.unlink(missing_ok=True)
                _fsync_directory(root)
            except OSError as exc:
                raise BackupSuccessInventoryError(
                    "temporary backup success reference could not be removed"
                ) from exc
        return PersistedBackupSuccessReference(
            reference=reference,
            path=path,
            created=created,
        )

    def snapshot(
        self,
        *,
        window_start_ms: int,
        window_end_ms: int,
    ) -> BackupSuccessInventorySnapshot:
        window_start_ms = _positive_int(
            window_start_ms,
            field="window_start_ms",
        )
        window_end_ms = _positive_int(window_end_ms, field="window_end_ms")
        if window_end_ms <= window_start_ms:
            raise BackupSuccessInventoryError(
                "inventory snapshot window must have positive duration"
            )
        root = self._private_root()
        references: list[BackupSuccessReference] = []
        count = 0
        try:
            entries = os.scandir(root)
            with entries:
                for entry in entries:
                    matched = _REFERENCE_NAME.fullmatch(entry.name)
                    if matched is None and _TEMPORARY_REFERENCE_NAME.fullmatch(
                        entry.name
                    ):
                        continue
                    if matched is None:
                        raise BackupSuccessInventoryError(
                            "backup success inventory contains an unexpected entry"
                        )
                    count += 1
                    if count > self._maximum_files:
                        raise BackupSuccessInventoryError(
                            "backup success inventory exceeds its file bound"
                        )
                    reference = self._read(root / entry.name)
                    if reference.filename != entry.name:
                        raise BackupSuccessInventoryError(
                            "backup success reference filename has drifted"
                        )
                    if window_start_ms <= reference.completed_at_ms <= window_end_ms:
                        references.append(reference)
        except OSError as exc:
            raise BackupSuccessInventoryError(
                "backup success inventory could not be scanned"
            ) from exc
        references.sort(key=lambda item: (item.completed_at_ms, item.backup_id))
        return BackupSuccessInventorySnapshot(
            references=tuple(references),
            total_file_count=count,
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
        )

    def _private_root(self) -> Path:
        raw = self._root_path
        try:
            status = raw.lstat()
            resolved = raw.resolve(strict=True)
        except OSError as exc:
            raise BackupSuccessInventoryError(
                "backup success inventory root cannot be inspected"
            ) from exc
        if (
            not raw.is_absolute()
            or stat.S_ISLNK(status.st_mode)
            or raw != resolved
            or not stat.S_ISDIR(status.st_mode)
            or status.st_uid != os.geteuid()
            or stat.S_IMODE(status.st_mode) != 0o700
        ):
            raise BackupSuccessInventoryError(
                "backup success inventory root must be an owned private "
                "absolute directory without symlinks"
            )
        return resolved

    def _read(self, path: Path) -> BackupSuccessReference:
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise BackupSuccessInventoryError(
                "backup success reference cannot be opened safely"
            ) from exc
        try:
            status = os.fstat(descriptor)
            if (
                not stat.S_ISREG(status.st_mode)
                or status.st_uid != os.geteuid()
                or stat.S_IMODE(status.st_mode) != 0o600
                or not 0 < status.st_size <= self._maximum_reference_bytes
            ):
                raise BackupSuccessInventoryError(
                    "backup success reference must be an owned private bounded file"
                )
            data = _read_exact(descriptor, status.st_size)
        finally:
            os.close(descriptor)
        return _reference_from_bytes(data)

    def _entry_names(self, root: Path) -> set[str]:
        names: set[str] = set()
        try:
            entries = os.scandir(root)
            with entries:
                for entry in entries:
                    if _TEMPORARY_REFERENCE_NAME.fullmatch(entry.name):
                        continue
                    if _REFERENCE_NAME.fullmatch(entry.name) is None:
                        raise BackupSuccessInventoryError(
                            "backup success inventory contains an unexpected entry"
                        )
                    names.add(entry.name)
                    if len(names) > self._maximum_files:
                        raise BackupSuccessInventoryError(
                            "backup success inventory exceeds its file bound"
                        )
        except OSError as exc:
            raise BackupSuccessInventoryError(
                "backup success inventory could not be scanned"
            ) from exc
        return names


def _reference_from_bytes(data: bytes) -> BackupSuccessReference:
    try:
        wire = json.loads(data, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BackupSuccessInventoryError(
            "backup success reference is not strict JSON"
        ) from exc
    if not isinstance(wire, dict) or set(wire) != {
        "schema_version",
        "cluster_id",
        "backup_id",
        "completed_at_ms",
        "history_uri",
        "history_sha256",
    }:
        raise BackupSuccessInventoryError("backup success reference shape is invalid")
    try:
        reference = BackupSuccessReference(**wire)
    except (TypeError, ValueError) as exc:
        raise BackupSuccessInventoryError(
            "backup success reference fields are invalid"
        ) from exc
    if reference.canonical_bytes() != data:
        raise BackupSuccessInventoryError(
            "backup success reference is not RFC 8785 canonical JSON"
        )
    return reference


def _write_all(descriptor: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(descriptor, data[offset:])
        if written <= 0:
            raise OSError("short write")
        offset += written


def _read_exact(descriptor: int, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = os.read(descriptor, min(65_536, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    data = b"".join(chunks)
    if len(data) != size or os.read(descriptor, 1):
        raise BackupSuccessInventoryError(
            "backup success reference changed while it was read"
        )
    return data


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _token(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ValueError(f"{field} must be a bounded ASCII token")
    return value


def _uuid(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a canonical UUID") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise ValueError(f"{field} must be a canonical UUIDv4")
    return value


def _s3_uri(value: object, *, field: str) -> str:
    if not isinstance(value, str) or len(value) > 2_048:
        raise ValueError(f"{field} must be a bounded S3 URI")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.strip("/")
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{field} must be an absolute S3 URI")
    return value


def _sha256(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    return value


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _timestamp_ms(value: object, *, field: str) -> int:
    value = _positive_int(value, field=field)
    if value >= 10**20:
        raise ValueError(f"{field} must fit the fixed 20-digit filename field")
    return value
