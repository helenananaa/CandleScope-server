"""Strict HTTP MarketEventQuery client for Replay Worker processes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import aiohttp

from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import (
    MarketDataSnapshotRef,
    MarketEventCursor,
    MarketEventEnvelopeV1,
    MarketEventPage,
    MarketEventRange,
)
from app.server_runtime.query_identity import (
    normalize_organization_id,
    normalize_workspace_id,
)

QUERY_PATH = "/api/v1/server/market-events/query"


class SnapshotQueryClientError(RuntimeError):
    """The standalone snapshot-query service failed or broke its wire contract."""

    def __init__(self, code: str, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class HttpSnapshotMarketEventQuery:
    """Read immutable snapshot pages through the authenticated Query Service."""

    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str,
        organization_id: str,
        workspace_id: str,
        request_timeout_ms: int = 10_000,
        session_factory: Any = aiohttp.ClientSession,
    ) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("base_url cannot contain credentials, query, or fragment")
        if not isinstance(bearer_token, str) or not bearer_token.strip():
            raise ValueError("bearer_token must be a non-blank string")
        if (
            isinstance(request_timeout_ms, bool)
            or not isinstance(request_timeout_ms, int)
            or request_timeout_ms <= 0
        ):
            raise ValueError("request_timeout_ms must be a positive integer")
        self._endpoint = f"{base_url.rstrip('/')}{QUERY_PATH}"
        self._bearer_token = bearer_token.strip()
        self._organization_id = normalize_organization_id(organization_id)
        self._workspace_id = normalize_workspace_id(workspace_id)
        self._request_timeout_ms = request_timeout_ms
        self._session_factory = session_factory

    def __repr__(self) -> str:
        parsed = urlsplit(self._endpoint)
        return (
            "HttpSnapshotMarketEventQuery("
            f"endpoint={parsed.scheme}://{parsed.netloc}{parsed.path}, "
            f"organization_id={self._organization_id!r}, "
            f"workspace_id={self._workspace_id!r}, bearer_token=<redacted>)"
        )

    async def query(
        self,
        *,
        snapshot: MarketDataSnapshotRef,
        stream: MarketStreamKey,
        start_event_time_ms: int,
        end_event_time_ms: int,
        limit: int,
        cursor: MarketEventCursor | None = None,
    ) -> MarketEventPage:
        if not isinstance(snapshot, MarketDataSnapshotRef):
            raise TypeError("snapshot must be a MarketDataSnapshotRef")
        if not isinstance(stream, MarketStreamKey):
            raise TypeError("stream must be a MarketStreamKey")
        if cursor is not None and not isinstance(cursor, MarketEventCursor):
            raise TypeError("cursor must be a MarketEventCursor or None")
        body = {
            "snapshot": {
                "data_epoch": snapshot.data_epoch,
                "snapshot_version": snapshot.snapshot_version,
                "manifest_uri": snapshot.manifest_uri,
                "manifest_sha256": snapshot.manifest_sha256,
            },
            "stream": stream.to_dict(),
            "start_event_time_ms": start_event_time_ms,
            "end_event_time_ms": end_event_time_ms,
            "limit": limit,
            "cursor": (
                None
                if cursor is None
                else {
                    "value": cursor.value,
                    "manifest_sha256": cursor.manifest_sha256,
                }
            ),
            # Replay is pinned to the immutable archive. It must never silently
            # switch to a mutable hot projection.
            "preference": "cold",
            "organization_id": self._organization_id,
            "workspace_id": self._workspace_id,
        }
        timeout = aiohttp.ClientTimeout(total=self._request_timeout_ms / 1_000)
        session_kwargs: dict[str, Any] = {"timeout": timeout}
        if self._session_factory is aiohttp.ClientSession:
            session_kwargs["trust_env"] = False
        try:
            async with (
                self._session_factory(**session_kwargs) as session,
                session.post(
                    self._endpoint,
                    json=body,
                    headers={"Authorization": f"Bearer {self._bearer_token}"},
                ) as response,
            ):
                try:
                    payload = await response.json(content_type=None)
                except Exception as exc:
                    raise SnapshotQueryClientError(
                        "QUERY_RESPONSE_INVALID",
                        "snapshot-query service returned non-JSON content",
                        status=response.status,
                    ) from exc
                if response.status != 200:
                    raise SnapshotQueryClientError(
                        _response_error_code(payload),
                        "snapshot-query service rejected the request",
                        status=response.status,
                    )
        except SnapshotQueryClientError:
            raise
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise SnapshotQueryClientError(
                "QUERY_SERVICE_UNAVAILABLE",
                "snapshot-query service is unavailable",
            ) from exc
        return _parse_query_response(payload, expected_snapshot=snapshot, stream=stream)


def _parse_query_response(
    payload: object,
    *,
    expected_snapshot: MarketDataSnapshotRef,
    stream: MarketStreamKey,
) -> MarketEventPage:
    root = _strict_mapping(
        payload,
        fields={
            "backend",
            "hot_committed_next_offset",
            "parity_verified",
            "hot_quarantined",
            "page",
        },
        label="query response",
    )
    if root["backend"] != "cold":
        raise SnapshotQueryClientError(
            "QUERY_BACKEND_DRIFT",
            "replay snapshot query did not use the immutable cold backend",
        )
    if not isinstance(root["parity_verified"], bool) or not isinstance(
        root["hot_quarantined"], bool
    ):
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID",
            "query routing flags must be booleans",
        )
    hot_offset = root["hot_committed_next_offset"]
    if hot_offset is not None and (
        isinstance(hot_offset, bool)
        or not isinstance(hot_offset, int)
        or hot_offset < 0
    ):
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID",
            "hot_committed_next_offset must be a non-negative integer or null",
        )
    page_wire = _strict_mapping(
        root["page"],
        fields={"snapshot", "events", "covered_range", "next_cursor"},
        label="query page",
    )
    snapshot_wire = _strict_mapping(
        page_wire["snapshot"],
        fields={
            "data_epoch",
            "snapshot_version",
            "manifest_uri",
            "manifest_sha256",
        },
        label="query page snapshot",
    )
    try:
        snapshot = MarketDataSnapshotRef(
            data_epoch=snapshot_wire["data_epoch"],
            snapshot_version=snapshot_wire["snapshot_version"],
            manifest_uri=snapshot_wire["manifest_uri"],
            manifest_sha256=snapshot_wire["manifest_sha256"],
        )
    except (TypeError, ValueError) as exc:
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID",
            "query page snapshot violates the contract",
        ) from exc
    if snapshot != expected_snapshot:
        raise SnapshotQueryClientError(
            "QUERY_SNAPSHOT_DRIFT",
            "snapshot-query response escaped the requested immutable snapshot",
        )
    events_wire = page_wire["events"]
    if not isinstance(events_wire, list):
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID", "query page events must be an array"
        )
    try:
        events = tuple(MarketEventEnvelopeV1.from_wire(item) for item in events_wire)
    except (TypeError, ValueError) as exc:
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID",
            "query page events violate the market-event contract",
        ) from exc
    range_wire = _strict_mapping(
        page_wire["covered_range"],
        fields={
            "partition_key",
            "start_event_time_ms",
            "end_event_time_ms",
            "event_count",
            "sequence_start",
            "sequence_end",
        },
        label="query covered range",
    )
    try:
        covered_range = MarketEventRange(
            partition_key=range_wire["partition_key"],
            start_event_time_ms=range_wire["start_event_time_ms"],
            end_event_time_ms=range_wire["end_event_time_ms"],
            event_count=range_wire["event_count"],
            sequence_start=range_wire["sequence_start"],
            sequence_end=range_wire["sequence_end"],
        )
    except (TypeError, ValueError) as exc:
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID",
            "query covered range violates the contract",
        ) from exc
    if covered_range.partition_key != stream.topic:
        raise SnapshotQueryClientError(
            "QUERY_STREAM_DRIFT",
            "snapshot-query response escaped the requested market stream",
        )
    cursor_wire = page_wire["next_cursor"]
    next_cursor = None
    if cursor_wire is not None:
        parsed_cursor = _strict_mapping(
            cursor_wire,
            fields={"value", "manifest_sha256"},
            label="query cursor",
        )
        try:
            next_cursor = MarketEventCursor(
                value=parsed_cursor["value"],
                manifest_sha256=parsed_cursor["manifest_sha256"],
            )
        except (TypeError, ValueError) as exc:
            raise SnapshotQueryClientError(
                "QUERY_RESPONSE_INVALID",
                "query cursor violates the contract",
            ) from exc
    try:
        return MarketEventPage(
            snapshot=snapshot,
            events=events,
            covered_range=covered_range,
            next_cursor=next_cursor,
        )
    except (TypeError, ValueError) as exc:
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID",
            "snapshot-query page violates the market-event contract",
        ) from exc


def _strict_mapping(
    value: object, *, fields: set[str], label: str
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID", f"{label} must be an object"
        )
    if set(value) != fields:
        raise SnapshotQueryClientError(
            "QUERY_RESPONSE_INVALID", f"{label} fields do not match the contract"
        )
    return value


def _response_error_code(payload: object) -> str:
    if not isinstance(payload, Mapping):
        return "QUERY_REQUEST_REJECTED"
    detail = payload.get("detail")
    if not isinstance(detail, Mapping):
        return "QUERY_REQUEST_REJECTED"
    code = detail.get("code")
    if not isinstance(code, str) or not code.strip():
        return "QUERY_REQUEST_REJECTED"
    return code.strip()[:128]


__all__ = [
    "QUERY_PATH",
    "HttpSnapshotMarketEventQuery",
    "SnapshotQueryClientError",
]
