"""Read the ClickHouse writer's committed Kafka projection boundary."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol, runtime_checkable

from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.structs import TopicPartition

from app.server_runtime.publishers import MARKET_EVENTS_TOPIC


class ProjectionCursorError(RuntimeError):
    """The projection cursor cannot be read safely."""


@runtime_checkable
class ProjectionCursorReader(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def committed_next_offset(self) -> int | None: ...


class KafkaProjectionCursorReader:
    """Inspect one consumer group's committed next offset without consuming."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | Sequence[str],
        group_id: str,
        client_id: str,
        connection_options: Mapping[str, Any] | None = None,
        admin_factory: Callable[..., Any] = AIOKafkaAdminClient,
    ) -> None:
        self._bootstrap_servers = _servers(bootstrap_servers)
        self._group_id = _required_text(group_id, field="group_id")
        self._client_id = _required_text(client_id, field="client_id")
        options = dict(connection_options or {})
        forbidden = {
            "bootstrap_servers",
            "client_id",
        }
        conflict = forbidden.intersection(options)
        if conflict:
            raise ValueError(f"connection_options cannot override {sorted(conflict)}")
        self._connection_options = options
        self._admin_factory = admin_factory
        self._admin: Any | None = None
        self._topic_partition = TopicPartition(MARKET_EVENTS_TOPIC, 0)

    @property
    def started(self) -> bool:
        return self._admin is not None

    async def start(self) -> None:
        if self._admin is not None:
            raise ProjectionCursorError("projection cursor reader is already started")
        admin = self._admin_factory(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            **self._connection_options,
        )
        try:
            await admin.start()
            descriptions = await admin.describe_topics([MARKET_EVENTS_TOPIC])
            if len(descriptions) != 1:
                raise ProjectionCursorError(
                    f"{MARKET_EVENTS_TOPIC!r} must have exactly partition 0"
                )
            description = descriptions[0]
            partitions = {
                int(item["partition"])
                for item in description.get("partitions", [])
                if isinstance(item, dict) and "partition" in item
            }
            if description.get("topic") != MARKET_EVENTS_TOPIC or partitions != {0}:
                raise ProjectionCursorError(
                    f"{MARKET_EVENTS_TOPIC!r} must have exactly partition 0"
                )
        except BaseException:
            await admin.close()
            raise
        self._admin = admin

    async def stop(self) -> None:
        admin = self._admin
        self._admin = None
        if admin is not None:
            await admin.close()

    async def committed_next_offset(self) -> int | None:
        if self._admin is None:
            raise ProjectionCursorError("projection cursor reader is not started")
        offsets = await self._admin.list_consumer_group_offsets(
            self._group_id,
            partitions=[self._topic_partition],
        )
        offset_and_metadata = offsets.get(self._topic_partition)
        if offset_and_metadata is None or offset_and_metadata.offset == -1:
            return None
        value = offset_and_metadata.offset
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ProjectionCursorError("Kafka returned an invalid committed offset")
        return value


def _servers(value: str | Sequence[str]) -> str | tuple[str, ...]:
    if isinstance(value, str):
        return _required_text(value, field="bootstrap_servers")
    servers = tuple(value)
    if not servers:
        raise ValueError("bootstrap_servers cannot be empty")
    return tuple(
        _required_text(server, field="bootstrap_servers") for server in servers
    )


def _required_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value.strip()
