"""Phase 1R soak contract: in-process rehearsal or explicit public-24h refusal."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from app.server_runtime.soak_rehearsal import (
    PUBLIC_SOAK_ENV,
    public_soak_refusal,
    run_phase1r_rehearsal,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the Phase 1R vertical-chain soak contract",
    )
    parser.add_argument(
        "mode",
        choices=("rehearsal", "public-24h"),
        help="rehearsal is the in-process gate; public-24h remains fail closed",
    )
    args = parser.parse_args(argv)
    if args.mode == "public-24h":
        payload = public_soak_refusal(
            allow_public_soak=os.environ.get(PUBLIC_SOAK_ENV) == "1",
        )
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 1
    payload = asyncio.run(run_phase1r_rehearsal())
    print(json.dumps(payload, sort_keys=True), flush=True)
    return 0 if payload.get("phase1r_passed") is True else 1


if __name__ == "__main__":
    sys.exit(main())
