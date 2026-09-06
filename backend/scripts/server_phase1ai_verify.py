"""Independent Phase 1AI evidence verifier CLI. Frozen files only."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.server_runtime.public_soak_verify import SoakVerifyError, verify_evidence


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Recompute Phase 1AI hashes from frozen evidence files",
    )
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--result", required=True)
    parser.add_argument("--samples")
    args = parser.parse_args(argv)
    try:
        report = verify_evidence(
            manifest_path=args.manifest,
            result_path=args.result,
            sample_path=args.samples,
        )
    except SoakVerifyError as exc:
        payload = {
            "verified": False,
            "code": exc.code,
            "message": exc.message,
            "details": exc.details,
            "twenty_four_hour_public_continuity": False,
            "production_ready": False,
        }
        print(json.dumps(payload, sort_keys=True), flush=True)
        return 1
    print(json.dumps(report, sort_keys=True), flush=True)
    return 0 if report.get("verified") is True else 1


if __name__ == "__main__":
    sys.exit(main())
