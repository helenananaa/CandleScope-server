"""Object-store port used by the immutable server archive."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


class ObjectStoreError(RuntimeError):
    """The configured object store could not prove an operation succeeded."""


class ObjectNotFoundError(ObjectStoreError):
    """A required immutable object does not exist."""


@dataclass(frozen=True, slots=True)
class StoredObject:
    data: bytes
    metadata: Mapping[str, str]
    content_type: str | None


@runtime_checkable
class ImmutableObjectStore(Protocol):
    async def ensure_bucket(self) -> None: ...

    async def put_if_absent(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str,
        metadata: Mapping[str, str],
    ) -> bool: ...

    async def get(self, key: str) -> StoredObject: ...

    async def get_uri(self, uri: str) -> StoredObject: ...

    def uri_for(self, key: str) -> str: ...
