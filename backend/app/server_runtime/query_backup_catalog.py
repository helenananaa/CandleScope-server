"""Signed immutable catalog for PostgreSQL physical query-control backups."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlsplit

import rfc8785

from app.server_runtime.object_store import ImmutableObjectStore

PHYSICAL_BACKUP_SCHEMA_VERSION = "candlescope.query-physical-backup.v1"
PHYSICAL_BACKUP_ALGORITHM = "hmac-sha256"
REQUIRED_BACKUP_ARTIFACTS = (
    "backup_manifest",
    "base.tar.gz",
    "pg_wal.tar.gz",
)
DEFAULT_MAX_BACKUP_ARTIFACT_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_BACKUP_TOTAL_BYTES = 1024 * 1024 * 1024
_LSN = re.compile(r"^[0-9A-F]+/[0-9A-F]+$")


class PhysicalBackupError(RuntimeError):
    """A physical backup catalog or artifact failed integrity validation."""


@dataclass(frozen=True, slots=True)
class PostgresBackupMetadata:
    system_identifier: int
    timeline: int
    start_lsn: str
    end_lsn: str
    manifest_sha256: str
    file_count: int


@dataclass(frozen=True, slots=True)
class PhysicalBackupArtifact:
    name: str
    uri: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if self.name not in REQUIRED_BACKUP_ARTIFACTS:
            raise ValueError("physical backup artifact name is not supported")
        object.__setattr__(self, "uri", _s3_uri(self.uri, field="artifact uri"))
        object.__setattr__(self, "sha256", _sha256(self.sha256, field="sha256"))
        object.__setattr__(
            self,
            "size_bytes",
            _positive_int(self.size_bytes, field="size_bytes"),
        )

    def to_wire(self) -> dict[str, str]:
        return {
            "name": self.name,
            "uri": self.uri,
            "sha256": self.sha256,
            "size_bytes": str(self.size_bytes),
        }


@dataclass(frozen=True, slots=True)
class PhysicalBackupRequest:
    backup_id: str
    cluster_id: str
    created_at_ms: int
    recovery_target_time: str
    postgres_version: str
    system_identifier: int
    timeline: int
    start_lsn: str
    end_lsn: str
    wal_archive_prefix_uri: str
    audit_anchor_uri: str
    audit_anchor_sha256: str
    audit_head_sequence: int
    audit_head_event_hash: str
    migration_version: int
    migration_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "backup_id", _uuid(self.backup_id, field="backup_id"))
        object.__setattr__(
            self,
            "cluster_id",
            _safe_token(self.cluster_id, field="cluster_id"),
        )
        for field in (
            "created_at_ms",
            "system_identifier",
            "timeline",
            "migration_version",
        ):
            object.__setattr__(
                self,
                field,
                _positive_int(getattr(self, field), field=field),
            )
        object.__setattr__(
            self,
            "audit_head_sequence",
            _non_negative_int(
                self.audit_head_sequence,
                field="audit_head_sequence",
            ),
        )
        object.__setattr__(
            self,
            "postgres_version",
            _safe_version(self.postgres_version),
        )
        object.__setattr__(
            self,
            "recovery_target_time",
            _utc_timestamp(self.recovery_target_time),
        )
        object.__setattr__(self, "start_lsn", _lsn(self.start_lsn, field="start_lsn"))
        object.__setattr__(self, "end_lsn", _lsn(self.end_lsn, field="end_lsn"))
        object.__setattr__(
            self,
            "wal_archive_prefix_uri",
            _s3_uri(self.wal_archive_prefix_uri, field="wal_archive_prefix_uri"),
        )
        object.__setattr__(
            self,
            "audit_anchor_uri",
            _s3_uri(self.audit_anchor_uri, field="audit_anchor_uri"),
        )
        object.__setattr__(
            self,
            "audit_anchor_sha256",
            _sha256(self.audit_anchor_sha256, field="audit_anchor_sha256"),
        )
        object.__setattr__(
            self,
            "audit_head_event_hash",
            _sha256(self.audit_head_event_hash, field="audit_head_event_hash"),
        )
        object.__setattr__(
            self,
            "migration_sha256",
            _sha256(self.migration_sha256, field="migration_sha256"),
        )


@dataclass(frozen=True, slots=True)
class PhysicalBackupManifest:
    request: PhysicalBackupRequest
    key_id: str
    artifacts: tuple[PhysicalBackupArtifact, ...]
    signature: str
    schema_version: str = PHYSICAL_BACKUP_SCHEMA_VERSION
    algorithm: str = PHYSICAL_BACKUP_ALGORITHM

    def __post_init__(self) -> None:
        if not isinstance(self.request, PhysicalBackupRequest):
            raise TypeError("request must be a PhysicalBackupRequest")
        if self.schema_version != PHYSICAL_BACKUP_SCHEMA_VERSION:
            raise ValueError("physical backup schema_version has drifted")
        if self.algorithm != PHYSICAL_BACKUP_ALGORITHM:
            raise ValueError("physical backup algorithm has drifted")
        object.__setattr__(self, "key_id", _safe_token(self.key_id, field="key_id"))
        if not isinstance(self.artifacts, tuple) or any(
            not isinstance(item, PhysicalBackupArtifact) for item in self.artifacts
        ):
            raise TypeError("artifacts must be a tuple of PhysicalBackupArtifact")
        if tuple(item.name for item in self.artifacts) != REQUIRED_BACKUP_ARTIFACTS:
            raise ValueError("physical backup artifacts are incomplete or unordered")
        object.__setattr__(
            self,
            "signature",
            _sha256(self.signature, field="signature"),
        )

    def unsigned_wire(self) -> dict[str, object]:
        request = self.request
        return {
            "schema_version": self.schema_version,
            "algorithm": self.algorithm,
            "key_id": self.key_id,
            "backup": {
                "backup_id": request.backup_id,
                "cluster_id": request.cluster_id,
                "created_at_ms": str(request.created_at_ms),
                "recovery_target_time": request.recovery_target_time,
                "postgres_version": request.postgres_version,
                "system_identifier": str(request.system_identifier),
                "timeline": str(request.timeline),
                "start_lsn": request.start_lsn,
                "end_lsn": request.end_lsn,
                "wal_archive_prefix_uri": request.wal_archive_prefix_uri,
                "audit_anchor_uri": request.audit_anchor_uri,
                "audit_anchor_sha256": request.audit_anchor_sha256,
                "audit_head_sequence": str(request.audit_head_sequence),
                "audit_head_event_hash": request.audit_head_event_hash,
                "migration_version": str(request.migration_version),
                "migration_sha256": request.migration_sha256,
            },
            "artifacts": [artifact.to_wire() for artifact in self.artifacts],
        }

    def to_wire(self) -> dict[str, object]:
        return {**self.unsigned_wire(), "signature": self.signature}

    def canonical_bytes(self) -> bytes:
        return rfc8785.dumps(self.to_wire())


@dataclass(frozen=True, slots=True)
class PublishedPhysicalBackup:
    manifest_uri: str
    manifest_sha256: str
    manifest: PhysicalBackupManifest
    created_artifacts: int
    manifest_created: bool


class PhysicalBackupManifestSigner:
    def __init__(self, *, key_id: str, secret: bytes) -> None:
        self._key_id = _safe_token(key_id, field="key_id")
        if not isinstance(secret, bytes) or len(secret) < 32:
            raise ValueError("backup HMAC secret must contain at least 32 bytes")
        self._secret = secret

    def sign(
        self,
        request: PhysicalBackupRequest,
        artifacts: tuple[PhysicalBackupArtifact, ...],
    ) -> PhysicalBackupManifest:
        placeholder = PhysicalBackupManifest(
            request=request,
            key_id=self._key_id,
            artifacts=artifacts,
            signature="0" * 64,
        )
        signature = hmac.digest(
            self._secret,
            rfc8785.dumps(placeholder.unsigned_wire()),
            "sha256",
        ).hex()
        return PhysicalBackupManifest(
            request=request,
            key_id=self._key_id,
            artifacts=artifacts,
            signature=signature,
        )

    def verify(self, manifest: PhysicalBackupManifest) -> None:
        if not isinstance(manifest, PhysicalBackupManifest):
            raise TypeError("manifest must be a PhysicalBackupManifest")
        if manifest.key_id != self._key_id:
            raise PhysicalBackupError("backup manifest key_id is not configured")
        expected = hmac.digest(
            self._secret,
            rfc8785.dumps(manifest.unsigned_wire()),
            "sha256",
        ).hex()
        if not hmac.compare_digest(expected, manifest.signature):
            raise PhysicalBackupError("backup manifest HMAC verification failed")


class ImmutablePhysicalBackupCatalog:
    def __init__(
        self,
        *,
        object_store: ImmutableObjectStore,
        signer: PhysicalBackupManifestSigner,
        max_artifact_bytes: int = DEFAULT_MAX_BACKUP_ARTIFACT_BYTES,
        max_total_bytes: int = DEFAULT_MAX_BACKUP_TOTAL_BYTES,
    ) -> None:
        if not isinstance(object_store, ImmutableObjectStore):
            raise TypeError("object_store must implement ImmutableObjectStore")
        if not isinstance(signer, PhysicalBackupManifestSigner):
            raise TypeError("signer must be a PhysicalBackupManifestSigner")
        self._object_store = object_store
        self._signer = signer
        self._max_artifact_bytes = _positive_int(
            max_artifact_bytes,
            field="max_artifact_bytes",
        )
        self._max_total_bytes = _positive_int(
            max_total_bytes,
            field="max_total_bytes",
        )

    async def publish(
        self,
        request: PhysicalBackupRequest,
        artifact_data: Mapping[str, bytes],
    ) -> PublishedPhysicalBackup:
        artifacts = self._describe_artifacts(request, artifact_data)
        _require_postgres_metadata(request, artifact_data["backup_manifest"])
        manifest = self._signer.sign(request, artifacts)
        await self._object_store.check_bucket()
        created_artifacts = 0
        for artifact in artifacts:
            data = artifact_data[artifact.name]
            key = _artifact_key(request, artifact.name)
            created = await self._object_store.put_if_absent(
                key,
                data,
                content_type="application/octet-stream",
                metadata={
                    "backup-id": request.backup_id,
                    "cluster-id": request.cluster_id,
                    "sha256": artifact.sha256,
                },
            )
            if created:
                created_artifacts += 1
            else:
                existing = await self._object_store.get(key)
                if existing.data != data:
                    raise PhysicalBackupError(
                        "immutable physical backup artifact contains different bytes"
                    )
        manifest_bytes = manifest.canonical_bytes()
        manifest_key = _manifest_key(request)
        manifest_created = await self._object_store.put_if_absent(
            manifest_key,
            manifest_bytes,
            content_type="application/json",
            metadata={
                "backup-id": request.backup_id,
                "cluster-id": request.cluster_id,
                "key-id": manifest.key_id,
            },
        )
        if not manifest_created:
            existing = await self._object_store.get(manifest_key)
            if existing.data != manifest_bytes:
                raise PhysicalBackupError(
                    "immutable physical backup manifest contains different bytes"
                )
        return PublishedPhysicalBackup(
            manifest_uri=self._object_store.uri_for(manifest_key),
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            manifest=manifest,
            created_artifacts=created_artifacts,
            manifest_created=manifest_created,
        )

    async def verify(
        self,
        manifest_uri: str,
        *,
        expected_audit_anchor_uri: str | None = None,
    ) -> PublishedPhysicalBackup:
        stored_manifest = await self._object_store.get_uri(manifest_uri)
        manifest = _manifest_from_bytes(stored_manifest.data)
        self._signer.verify(manifest)
        request = manifest.request
        if (
            expected_audit_anchor_uri is not None
            and request.audit_anchor_uri != expected_audit_anchor_uri
        ):
            raise PhysicalBackupError("backup references an unexpected audit anchor")
        total = 0
        artifact_data: dict[str, bytes] = {}
        for artifact in manifest.artifacts:
            expected_uri = self._object_store.uri_for(
                _artifact_key(request, artifact.name)
            )
            if artifact.uri != expected_uri:
                raise PhysicalBackupError("backup artifact URI has drifted")
            stored = await self._object_store.get_uri(artifact.uri)
            data = stored.data
            total += len(data)
            if (
                not data
                or len(data) > self._max_artifact_bytes
                or total > self._max_total_bytes
                or len(data) != artifact.size_bytes
                or hashlib.sha256(data).hexdigest() != artifact.sha256
            ):
                raise PhysicalBackupError("backup artifact integrity check failed")
            artifact_data[artifact.name] = data
        _require_postgres_metadata(request, artifact_data["backup_manifest"])
        return PublishedPhysicalBackup(
            manifest_uri=manifest_uri,
            manifest_sha256=hashlib.sha256(stored_manifest.data).hexdigest(),
            manifest=manifest,
            created_artifacts=0,
            manifest_created=False,
        )

    def _describe_artifacts(
        self,
        request: PhysicalBackupRequest,
        artifact_data: Mapping[str, bytes],
    ) -> tuple[PhysicalBackupArtifact, ...]:
        if not isinstance(request, PhysicalBackupRequest):
            raise TypeError("request must be a PhysicalBackupRequest")
        if set(artifact_data) != set(REQUIRED_BACKUP_ARTIFACTS):
            raise PhysicalBackupError("physical backup artifact set is incomplete")
        total = 0
        result: list[PhysicalBackupArtifact] = []
        for name in REQUIRED_BACKUP_ARTIFACTS:
            data = artifact_data[name]
            if not isinstance(data, bytes) or not data:
                raise PhysicalBackupError("physical backup artifact is empty")
            total += len(data)
            if len(data) > self._max_artifact_bytes or total > self._max_total_bytes:
                raise PhysicalBackupError("physical backup artifacts exceed bounds")
            result.append(
                PhysicalBackupArtifact(
                    name=name,
                    uri=self._object_store.uri_for(_artifact_key(request, name)),
                    sha256=hashlib.sha256(data).hexdigest(),
                    size_bytes=len(data),
                )
            )
        return tuple(result)


def parse_postgres_backup_manifest(data: bytes) -> PostgresBackupMetadata:
    if not isinstance(data, bytes) or not data:
        raise PhysicalBackupError("PostgreSQL backup_manifest is empty")
    try:
        wire = json.loads(data, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise PhysicalBackupError(
            "PostgreSQL backup_manifest is not strict JSON"
        ) from exc
    if not isinstance(wire, dict):
        raise PhysicalBackupError("PostgreSQL backup_manifest shape is invalid")
    try:
        version = wire["PostgreSQL-Backup-Manifest-Version"]
        system_identifier = wire["System-Identifier"]
        files = wire["Files"]
        ranges = wire["WAL-Ranges"]
        manifest_checksum = wire["Manifest-Checksum"]
    except KeyError as exc:
        raise PhysicalBackupError(
            "PostgreSQL backup_manifest is missing required fields"
        ) from exc
    if version != 2 or not isinstance(files, list) or not files:
        raise PhysicalBackupError(
            "PostgreSQL backup_manifest version/files are invalid"
        )
    if (
        isinstance(system_identifier, bool)
        or not isinstance(system_identifier, int)
        or system_identifier <= 0
    ):
        raise PhysicalBackupError("PostgreSQL system identifier is invalid")
    if (
        not isinstance(ranges, list)
        or len(ranges) != 1
        or not isinstance(ranges[0], dict)
    ):
        raise PhysicalBackupError("PostgreSQL WAL range is not singular")
    wal_range = ranges[0]
    try:
        timeline = _positive_int(wal_range["Timeline"], field="timeline")
        start_lsn = _lsn(wal_range["Start-LSN"], field="start_lsn")
        end_lsn = _lsn(wal_range["End-LSN"], field="end_lsn")
    except (KeyError, TypeError, ValueError) as exc:
        raise PhysicalBackupError("PostgreSQL WAL range is invalid") from exc
    return PostgresBackupMetadata(
        system_identifier=system_identifier,
        timeline=timeline,
        start_lsn=start_lsn,
        end_lsn=end_lsn,
        manifest_sha256=_sha256(manifest_checksum, field="Manifest-Checksum"),
        file_count=len(files),
    )


def _require_postgres_metadata(
    request: PhysicalBackupRequest,
    manifest_data: bytes,
) -> None:
    metadata = parse_postgres_backup_manifest(manifest_data)
    if (
        request.system_identifier,
        request.timeline,
        request.start_lsn,
        request.end_lsn,
    ) != (
        metadata.system_identifier,
        metadata.timeline,
        metadata.start_lsn,
        metadata.end_lsn,
    ):
        raise PhysicalBackupError(
            "backup request differs from PostgreSQL backup_manifest"
        )


def _manifest_from_bytes(data: bytes) -> PhysicalBackupManifest:
    if not isinstance(data, bytes) or not data:
        raise PhysicalBackupError("physical backup manifest is empty")
    try:
        wire = json.loads(data, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise PhysicalBackupError(
            "physical backup manifest is not strict JSON"
        ) from exc
    if not isinstance(wire, dict) or set(wire) != {
        "schema_version",
        "algorithm",
        "key_id",
        "backup",
        "artifacts",
        "signature",
    }:
        raise PhysicalBackupError("physical backup manifest shape is invalid")
    backup = wire["backup"]
    if not isinstance(backup, dict) or set(backup) != {
        "backup_id",
        "cluster_id",
        "created_at_ms",
        "recovery_target_time",
        "postgres_version",
        "system_identifier",
        "timeline",
        "start_lsn",
        "end_lsn",
        "wal_archive_prefix_uri",
        "audit_anchor_uri",
        "audit_anchor_sha256",
        "audit_head_sequence",
        "audit_head_event_hash",
        "migration_version",
        "migration_sha256",
    }:
        raise PhysicalBackupError("physical backup fields are invalid")
    artifact_wire = wire["artifacts"]
    if not isinstance(artifact_wire, list):
        raise PhysicalBackupError("physical backup artifacts are invalid")
    try:
        request = PhysicalBackupRequest(
            backup_id=backup["backup_id"],
            cluster_id=backup["cluster_id"],
            created_at_ms=_decimal(backup["created_at_ms"], field="created_at_ms"),
            recovery_target_time=backup["recovery_target_time"],
            postgres_version=backup["postgres_version"],
            system_identifier=_decimal(
                backup["system_identifier"],
                field="system_identifier",
            ),
            timeline=_decimal(backup["timeline"], field="timeline"),
            start_lsn=backup["start_lsn"],
            end_lsn=backup["end_lsn"],
            wal_archive_prefix_uri=backup["wal_archive_prefix_uri"],
            audit_anchor_uri=backup["audit_anchor_uri"],
            audit_anchor_sha256=backup["audit_anchor_sha256"],
            audit_head_sequence=_decimal(
                backup["audit_head_sequence"],
                field="audit_head_sequence",
            ),
            audit_head_event_hash=backup["audit_head_event_hash"],
            migration_version=_decimal(
                backup["migration_version"],
                field="migration_version",
            ),
            migration_sha256=backup["migration_sha256"],
        )
        artifacts = tuple(_artifact_from_wire(item) for item in artifact_wire)
        manifest = PhysicalBackupManifest(
            request=request,
            key_id=wire["key_id"],
            artifacts=artifacts,
            signature=wire["signature"],
            schema_version=wire["schema_version"],
            algorithm=wire["algorithm"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PhysicalBackupError("physical backup values are invalid") from exc
    if manifest.canonical_bytes() != data:
        raise PhysicalBackupError(
            "physical backup manifest is not RFC 8785 canonical JSON"
        )
    return manifest


def _artifact_from_wire(value: object) -> PhysicalBackupArtifact:
    if not isinstance(value, dict) or set(value) != {
        "name",
        "uri",
        "sha256",
        "size_bytes",
    }:
        raise ValueError("physical backup artifact shape is invalid")
    return PhysicalBackupArtifact(
        name=value["name"],
        uri=value["uri"],
        sha256=value["sha256"],
        size_bytes=_decimal(value["size_bytes"], field="size_bytes"),
    )


def _artifact_key(request: PhysicalBackupRequest, name: str) -> str:
    return f"backups/v1/{request.cluster_id}/{request.backup_id}/artifacts/{name}"


def _manifest_key(request: PhysicalBackupRequest) -> str:
    return f"backups/v1/{request.cluster_id}/{request.backup_id}/manifest.json"


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _uuid(value: object, *, field: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a UUID") from exc
    rendered = str(parsed)
    if value != rendered:
        raise ValueError(f"{field} must be a canonical UUID")
    return rendered


def _utc_timestamp(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("recovery_target_time must be a string")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f+00")
    except ValueError as exc:
        raise ValueError(
            "recovery_target_time must be PostgreSQL UTC time with microseconds"
        ) from exc
    rendered = parsed.strftime("%Y-%m-%d %H:%M:%S.%f+00")
    if value != rendered:
        raise ValueError("recovery_target_time must use canonical UTC form")
    return rendered


def _decimal(value: object, *, field: str) -> int:
    if (
        not isinstance(value, str)
        or not value
        or not value.isascii()
        or not value.isdecimal()
        or (len(value) > 1 and value.startswith("0"))
    ):
        raise ValueError(f"{field} must be a canonical decimal string")
    return int(value)


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _lsn(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _LSN.fullmatch(value):
        raise ValueError(f"{field} must be an upper-case PostgreSQL LSN")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    value = value.lower()
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{field} must be a SHA-256 hex digest")
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


def _safe_version(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise ValueError("postgres_version must contain 1-64 characters")
    if not value.isascii() or any(
        character not in "0123456789." for character in value
    ):
        raise ValueError("postgres_version must contain digits and dots")
    return value


def _s3_uri(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{field} must be an absolute credential-free S3 URI")
    return value
