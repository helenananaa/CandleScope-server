"""Emit a bounded journal signal for a failed scheduled backup unit."""

from __future__ import annotations

import argparse
import json
import re
import time

SCHEMA_VERSION = "candlescope.query-backup-failure-signal.v1"
_UNIT = re.compile(r"^[A-Za-z0-9_.@:-]{1,128}$")


def run(unit: str) -> dict[str, object]:
    if not isinstance(unit, str) or not _UNIT.fullmatch(unit):
        raise ValueError("unit must be a bounded systemd identifier")
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "backup-unit-failed",
        "unit": unit,
        "observed_at_ms": time.time_ns() // 1_000_000,
        "operator_action_required": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Emit a structured query backup failure signal."
    )
    parser.add_argument("--unit", required=True)
    arguments = parser.parse_args()
    print(json.dumps(run(arguments.unit), sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
