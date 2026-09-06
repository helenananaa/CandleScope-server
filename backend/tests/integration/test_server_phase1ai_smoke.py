from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("CANDLESCOPE_PHASE1AI_SMOKE") != "1",
    reason="requires the explicit Phase 1AI Compose + Binance development-smoke",
)


def test_development_smoke_cli_writes_non_24h_result(tmp_path: Path) -> None:
    from scripts.server_phase1ai_public_soak import main as soak_main

    manifest = os.environ.get("CANDLESCOPE_PHASE1AI_SMOKE_MANIFEST")
    output = os.environ.get("CANDLESCOPE_PHASE1AI_SMOKE_OUTPUT")
    if not manifest or not output:
        pytest.skip("smoke manifest/output paths are required")
    del tmp_path
    code = soak_main(["development-smoke", "--manifest", manifest, "--output", output])
    result = json.loads(Path(output).read_text(encoding="utf-8"))
    assert result["twenty_four_hour_public_continuity"] is False
    assert result["production_ready"] is False
    assert code == 0
    assert result["phase_passed"] is True
