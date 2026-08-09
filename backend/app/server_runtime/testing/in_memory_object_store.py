"""Deterministic conditional object store for archive contract tests."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import quote, unquote, urlsplit

from app.server_runtime.object_store import ObjectNotFoundError, StoredObject


class InMemoryImmutableObjectStore:
    def __init__(self, *, bucket: str = "candlescope-test") -> None:
        self.bucket = bucket
        self.objects: dict[str, StoredObject] = {}
        self.bucket_ready = False

    async def ensure_bucket(self) -> None:
        self.bucket_ready = True

    async def put_if_absent(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        metadata: Mapping[str, str],
    ) -> bool:
        if key in self.objects:
            return False
        self.objects[key] = StoredObject(
            data=data,
            metadata=dict(metadata),
            content_type=content_type,
        )
        return True

    async def get(self, key: str) -> StoredObject:
        try:
            return self.objects[key]
        except KeyError as exc:
            raise ObjectNotFoundError(key) from exc

    async def get_uri(self, uri: str) -> StoredObject:
        parsed = urlsplit(uri)
        if parsed.scheme != "s3" or parsed.netloc != self.bucket:
            raise ObjectNotFoundError(uri)
        return await self.get(unquote(parsed.path.lstrip("/")))

    def uri_for(self, key: str) -> str:
        return f"s3://{self.bucket}/{quote(key, safe='/')}"
