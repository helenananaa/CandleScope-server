"""Phase 1U bounded soak supervisor over loopback health scrapes."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

from app.server_runtime.soak_supervisor import (
    PUBLIC_SOAK_ENV,
    SoakSupervisorError,
    SoakSupervisorSettings,
    public_24h_refusal,
    run_soak_supervisor,
)
from scripts.server_phase1t_collector_worker import parse_sequences


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


def _clock_ms() -> int:
    return int(time.time() * 1000)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scrape loopback role health and reconcile a bounded soak",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    scrape = sub.add_parser("scrape")
    scrape.add_argument("--collector-url", required=True)
    scrape.add_argument("--writer-url", required=True)
    scrape.add_argument("--archive-url", required=True)
    scrape.add_argument("--sequences", required=True)
    scrape.add_argument("--duration-ms", type=int, default=5_000)
    scrape.add_argument("--scrape-interval-ms", type=int, default=250)
    scrape.add_argument("--stale-after-ms", type=int, default=5_000)
    scrape.add_argument("--source", default="scripted")
    sub.add_parser("public-24h")
    args = parser.parse_args(argv)
    allow_public = os.environ.get(PUBLIC_SOAK_ENV) == "1"
    if args.command == "public-24h":
        payload = public_24h_refusal(allow_public_soak=allow_public)
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 1
    try:
        settings = SoakSupervisorSettings(
            collector_health_url=args.collector_url,
            writer_health_url=args.writer_url,
            archive_health_url=args.archive_url,
            query_sequences=parse_sequences(args.sequences),
            duration_ms=args.duration_ms,
            scrape_interval_ms=args.scrape_interval_ms,
            stale_after_ms=args.stale_after_ms,
            source=args.source,
            allow_public_soak=allow_public,
        )
        payload = asyncio.run(
            run_soak_supervisor(settings, clock_ms=_clock_ms, sleep=_sleep)
        )
    except SoakSupervisorError as exc:
        payload = exc.to_wire()
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 1
    print(json.dumps(payload, sort_keys=True), flush=True)
    return 0 if payload.get("phase1u_passed") is True else 1


if __name__ == "__main__":
    sys.exit(main())
