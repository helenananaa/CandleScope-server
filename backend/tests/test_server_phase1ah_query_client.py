from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from aiohttp import web
from app.data_engine.market_data import MarketStreamKey
from app.server_contracts import MarketDataSnapshotRef, MarketEventCursor
from app.server_runtime.query_client import HttpSnapshotMarketEventQuery
from app.server_runtime.testing import FrozenAggTradeQuery

START_MS = 1_710_000_000_000
TOKEN = "query-token-that-is-never-serialized"


def _write_query_fixture(path: Path) -> MarketDataSnapshotRef:
    snapshot = MarketDataSnapshotRef(
        data_epoch="sha256:" + ("c" * 64),
        snapshot_version=4,
        manifest_uri="s3://archive/snapshot-4.json",
        manifest_sha256="a" * 64,
    )
    path.write_text(
        json.dumps(
            {
                "start_ms": START_MS,
                "sequences": [42, 43, 44],
                "snapshot": {
                    "data_epoch": snapshot.data_epoch,
                    "snapshot_version": snapshot.snapshot_version,
                    "manifest_uri": snapshot.manifest_uri,
                    "manifest_sha256": snapshot.manifest_sha256,
                },
            }
        ),
        encoding="utf-8",
    )
    return snapshot


def _page_wire(page: Any) -> dict[str, object]:
    covered = page.covered_range
    return {
        "backend": "cold",
        "hot_committed_next_offset": None,
        "parity_verified": False,
        "hot_quarantined": False,
        "page": {
            "snapshot": {
                "data_epoch": page.snapshot.data_epoch,
                "snapshot_version": page.snapshot.snapshot_version,
                "manifest_uri": page.snapshot.manifest_uri,
                "manifest_sha256": page.snapshot.manifest_sha256,
            },
            "events": [event.to_wire() for event in page.events],
            "covered_range": {
                "partition_key": covered.partition_key,
                "start_event_time_ms": covered.start_event_time_ms,
                "end_event_time_ms": covered.end_event_time_ms,
                "event_count": covered.event_count,
                "sequence_start": covered.sequence_start,
                "sequence_end": covered.sequence_end,
            },
            "next_cursor": (
                None
                if page.next_cursor is None
                else {
                    "value": page.next_cursor.value,
                    "manifest_sha256": page.next_cursor.manifest_sha256,
                }
            ),
        },
    }


def test_http_snapshot_query_uses_authenticated_scoped_cold_api(
    tmp_path: Path,
) -> None:
    async def run() -> None:
        query_path = tmp_path / "frozen-query.json"
        snapshot = _write_query_fixture(query_path)
        frozen = FrozenAggTradeQuery(query_path)
        observed: list[dict[str, object]] = []
        app = web.Application()

        async def query(request: web.Request) -> web.Response:
            assert request.headers["Authorization"] == f"Bearer {TOKEN}"
            body = await request.json()
            observed.append(body)
            requested_snapshot = MarketDataSnapshotRef(**body["snapshot"])
            stream = MarketStreamKey.build(**body["stream"])
            cursor = (
                None if body["cursor"] is None else MarketEventCursor(**body["cursor"])
            )
            page = await frozen.query(
                snapshot=requested_snapshot,
                stream=stream,
                start_event_time_ms=body["start_event_time_ms"],
                end_event_time_ms=body["end_event_time_ms"],
                limit=body["limit"],
                cursor=cursor,
            )
            return web.json_response(_page_wire(page))

        app.router.add_post("/api/v1/server/market-events/query", query)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None
        port = server.sockets[0].getsockname()[1]
        stream = MarketStreamKey.build("binance", "futures", "BTCUSDT", "agg_trade")
        client = HttpSnapshotMarketEventQuery(
            base_url=f"http://127.0.0.1:{port}",
            bearer_token=TOKEN,
            organization_id="org-alpha",
            workspace_id="ws-research",
        )
        try:
            page = await client.query(
                snapshot=snapshot,
                stream=stream,
                start_event_time_ms=START_MS + 42,
                end_event_time_ms=START_MS + 44,
                limit=2,
            )
        finally:
            await runner.cleanup()

        assert [event.sequence_end for event in page.events] == [42, 43]
        assert page.next_cursor is not None
        assert observed == [
            {
                "snapshot": {
                    "data_epoch": snapshot.data_epoch,
                    "snapshot_version": snapshot.snapshot_version,
                    "manifest_uri": snapshot.manifest_uri,
                    "manifest_sha256": snapshot.manifest_sha256,
                },
                "stream": stream.to_dict(),
                "start_event_time_ms": START_MS + 42,
                "end_event_time_ms": START_MS + 44,
                "limit": 2,
                "cursor": None,
                "preference": "cold",
                "organization_id": "org-alpha",
                "workspace_id": "ws-research",
            }
        ]
        assert TOKEN not in repr(client)

    asyncio.run(run())
