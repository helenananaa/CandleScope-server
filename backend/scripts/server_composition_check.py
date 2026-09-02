"""Validate independent server process configuration without unlocking FastAPI."""

from __future__ import annotations

import argparse
import json
import sys

from app.server_runtime.composition import (
    ServerCompositionError,
    fastapi_unlock_refusal,
    load_server_data_plane_composition,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check CandleScope server data-plane composition",
    )
    parser.add_argument(
        "command",
        choices=("config", "fastapi-unlock"),
        help="validate process env, or print FastAPI unlock blockers",
    )
    args = parser.parse_args(argv)
    if args.command == "fastapi-unlock":
        print(json.dumps(fastapi_unlock_refusal(), sort_keys=True), flush=True)
        return 1
    try:
        composition = load_server_data_plane_composition()
    except ServerCompositionError as exc:
        print(json.dumps(exc.to_wire(), sort_keys=True), flush=True)
        return 1
    print(json.dumps(composition.to_public_wire(), sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
