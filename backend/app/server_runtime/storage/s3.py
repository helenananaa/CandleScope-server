"""S3-compatible conditional object store for immutable archive publication."""

from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from app.server_runtime.object_store import (
    ObjectNotFoundError,
    ObjectStoreError,
    StoredObject,
)


class S3ImmutableObjectStore:
    """Write new keys with If-None-Match and never overwrite or delete them."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        region: str,
        bucket: str,
        prefix: str,
        access_key_id: str,
        secret_access_key: str,
        request_timeout_ms: int = 10_000,
        client_factory: Any = boto3.client,
    ) -> None:
        parsed = urlsplit(endpoint_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("endpoint_url must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError(
                "endpoint_url cannot contain credentials, query, or fragment"
            )
        self._endpoint_url = endpoint_url.rstrip("/")
        self._region = _required_text(region, field="region")
        self._bucket = _bucket(bucket)
        self._prefix = _prefix(prefix)
        access_key_id = _required_text(access_key_id, field="access_key_id")
        secret_access_key = _required_text(
            secret_access_key,
            field="secret_access_key",
        )
        timeout_seconds = (
            _positive_int(
                request_timeout_ms,
                field="request_timeout_ms",
            )
            / 1_000
        )
        self._client = client_factory(
            "s3",
            endpoint_url=self._endpoint_url,
            region_name=self._region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            config=Config(
                signature_version="s3v4",
                connect_timeout=timeout_seconds,
                read_timeout=timeout_seconds,
                retries={"max_attempts": 3, "mode": "standard"},
                s3={"addressing_style": "path"},
            ),
        )

    async def ensure_bucket(self) -> None:
        try:
            await asyncio.to_thread(self._client.head_bucket, Bucket=self._bucket)
            return
        except ClientError as exc:
            status = _status(exc)
            if status not in {404}:
                raise ObjectStoreError("S3 bucket access check failed") from exc
        arguments: dict[str, Any] = {"Bucket": self._bucket}
        if self._region != "us-east-1":
            arguments["CreateBucketConfiguration"] = {
                "LocationConstraint": self._region
            }
        try:
            await asyncio.to_thread(self._client.create_bucket, **arguments)
        except (BotoCoreError, ClientError) as exc:
            raise ObjectStoreError("S3 bucket creation failed") from exc

    async def check_bucket(self) -> None:
        """Verify read access without creating or mutating the bucket."""

        try:
            await asyncio.to_thread(self._client.head_bucket, Bucket=self._bucket)
        except (BotoCoreError, ClientError) as exc:
            raise ObjectStoreError("S3 bucket readiness check failed") from exc

    async def put_if_absent(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        metadata: Mapping[str, str],
    ) -> bool:
        if not isinstance(data, bytes) or not data:
            raise ValueError("immutable objects must contain non-empty bytes")
        full_key = self._full_key(key)
        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
        try:
            await asyncio.to_thread(
                self._client.put_object,
                Bucket=self._bucket,
                Key=full_key,
                Body=data,
                ContentLength=len(data),
                ContentType=_required_text(content_type, field="content_type"),
                Metadata={
                    _metadata_name(name): _required_text(value, field="metadata value")
                    for name, value in metadata.items()
                },
                ChecksumSHA256=checksum,
                IfNoneMatch="*",
            )
            return True
        except ClientError as exc:
            if _status(exc) == 412 or _code(exc) in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
            }:
                return False
            raise ObjectStoreError("S3 conditional object write failed") from exc
        except BotoCoreError as exc:
            raise ObjectStoreError("S3 conditional object write failed") from exc

    async def get(self, key: str) -> StoredObject:
        return await self._get_full_key(self._full_key(key))

    async def get_uri(self, uri: str) -> StoredObject:
        return await self._get_full_key(self._full_key_from_uri(uri))

    def uri_for(self, key: str) -> str:
        return f"s3://{self._bucket}/{quote(self._full_key(key), safe='/')}"

    async def _get_full_key(self, full_key: str) -> StoredObject:
        try:
            response = await asyncio.to_thread(
                self._client.get_object,
                Bucket=self._bucket,
                Key=full_key,
            )
            body = response["Body"]
            try:
                data = await asyncio.to_thread(body.read)
            finally:
                body.close()
            if not isinstance(data, bytes) or not data:
                raise ObjectStoreError("S3 returned an empty immutable object")
            return StoredObject(
                data=data,
                metadata={
                    str(name).lower(): str(value)
                    for name, value in response.get("Metadata", {}).items()
                },
                content_type=response.get("ContentType"),
            )
        except ClientError as exc:
            if _status(exc) == 404 or _code(exc) in {"NoSuchKey", "NotFound"}:
                raise ObjectNotFoundError(
                    f"immutable object does not exist: {full_key}"
                ) from exc
            raise ObjectStoreError("S3 immutable object read failed") from exc
        except BotoCoreError as exc:
            raise ObjectStoreError("S3 immutable object read failed") from exc

    def _full_key(self, key: str) -> str:
        key = _object_key(key)
        return f"{self._prefix}/{key}" if self._prefix else key

    def _full_key_from_uri(self, uri: str) -> str:
        parsed = urlsplit(_required_text(uri, field="uri"))
        if parsed.scheme != "s3" or parsed.netloc != self._bucket:
            raise ObjectStoreError("object URI belongs to another S3 bucket")
        if parsed.query or parsed.fragment:
            raise ObjectStoreError("object URI cannot contain query or fragment")
        full_key = _object_key(unquote(parsed.path.lstrip("/")))
        if self._prefix and not full_key.startswith(f"{self._prefix}/"):
            raise ObjectStoreError("object URI is outside the configured prefix")
        return full_key


def _status(exc: ClientError) -> int | None:
    value = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return value if isinstance(value, int) else None


def _code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))


def _bucket(value: object) -> str:
    value = _required_text(value, field="bucket")
    if not 3 <= len(value) <= 63:
        raise ValueError("bucket must contain between 3 and 63 characters")
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789.-")
    if any(character not in allowed for character in value):
        raise ValueError("bucket must use lowercase S3-compatible characters")
    if value[0] in ".-" or value[-1] in ".-" or ".." in value:
        raise ValueError("bucket has an invalid S3-compatible shape")
    return value


def _prefix(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("prefix must be a string")
    normalized = value.strip("/")
    if not normalized:
        return ""
    _object_key(normalized)
    return normalized


def _object_key(value: object) -> str:
    value = _required_text(value, field="object key")
    if value.startswith("/") or value.endswith("/"):
        raise ValueError("object key cannot start or end with a slash")
    components = value.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise ValueError("object key contains an unsafe path component")
    return value


def _metadata_name(value: object) -> str:
    value = _required_text(value, field="metadata name").lower()
    if any(
        character not in "abcdefghijklmnopqrstuvwxyz0123456789-" for character in value
    ):
        raise ValueError("metadata names must be lowercase ASCII tokens")
    return value


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value
