from __future__ import annotations

import asyncio
import copy
import json
import socket
import sys
from pathlib import Path
from typing import Any

import pytest
from app.server_runtime.public_soak_manifest import (
    MODE_DEVELOPMENT_SMOKE,
    MODE_RUN,
    PUBLIC_SOAK_DURATION_MS,
    SCHEMA_VERSION,
    PublicSoakManifestError,
    load_manifest,
    parse_manifest,
)

GIT_COMMIT = "1b3abb54481d565dc5cb79d378c57e50a87d4f10"
STARTED = "2026-09-06T00:00:00Z"


def _output_paths(tmp_path: Path) -> dict[str, Any]:
    return {
        "result_path": str(tmp_path / "phase1ai-result.json"),
        "sample_dir": str(tmp_path / "samples"),
        "log_dir": str(tmp_path / "logs"),
        "max_log_bytes": 1_048_576,
        "max_health_bytes": 16_384,
        "max_sample_payload_bytes": 16_384,
        "exclusive_create": True,
    }


def _health() -> dict[str, str]:
    return {
        "collector": "http://127.0.0.1:18121/health",
        "writer": "http://127.0.0.1:18122/health",
        "archiver": "http://127.0.0.1:18123/health",
        "query": "http://127.0.0.1:18124/health/ready",
        "scheduler": "http://127.0.0.1:18125/health",
        "worker_a": "http://127.0.0.1:18126/health",
        "worker_b": "http://127.0.0.1:18127/health",
        "api": "http://127.0.0.1:18128/health/ready",
    }


def _infrastructure(tmp_path: Path) -> dict[str, Any]:
    return {
        "postgres_bind": "127.0.0.1:29432",
        "redpanda_bind": "127.0.0.1:60092",
        "clickhouse_bind": "127.0.0.1:59123",
        "minio_bind": "127.0.0.1:60000",
        "compose_project": "candlescope-phase1ai",
        "compose_file": str(tmp_path / "compose.phase1ai.yml"),
    }


def _replay_limits() -> dict[str, int]:
    return {
        "max_active_replays": 2,
        "max_queued_replays": 1,
        "max_actors_per_worker": 1,
        "command_timeout_ms": 8_000,
        "step_interval_ms": 1_000,
        "max_commands_per_replay": 1_000,
    }


def _acceptance() -> dict[str, Any]:
    return {
        "max_unresolved_gaps": 0,
        "max_hash_conflicts": 0,
        "max_producer_epoch_rollbacks": 0,
        "require_caught_up": True,
    }


def _run_faults() -> list[dict[str, Any]]:
    plan = []
    methods = (
        ("collector-sigkill", "collector", "collector_sigkill", 7_200_000),
        ("writer-pre-commit", "writer", "writer_pre_commit_exit", 14_400_000),
        ("archiver-pre-commit", "archiver", "archiver_pre_commit_exit", 21_600_000),
        ("worker-sigkill", "worker_a", "worker_sigkill", 28_800_000),
        ("scheduler-restart", "scheduler", "scheduler_restart", 36_000_000),
        ("api-restart", "api", "api_restart", 43_200_000),
    )
    for fault_id, target, method, elapsed in methods:
        plan.append(
            {
                "fault_id": fault_id,
                "target_role": target,
                "method": method,
                "scheduled_elapsed_ms": elapsed,
                "observation_timeout_ms": 60_000,
                "recovery_timeout_ms": 180_000,
            }
        )
    return plan


def _smoke_faults() -> list[dict[str, Any]]:
    return [
        {
            "fault_id": "worker-sigkill",
            "target_role": "worker_a",
            "method": "worker_sigkill",
            "scheduled_elapsed_ms": 60_000,
            "observation_timeout_ms": 30_000,
            "recovery_timeout_ms": 60_000,
        }
    ]


def _base_payload(tmp_path: Path, *, mode: str) -> dict[str, Any]:
    smoke = mode == MODE_DEVELOPMENT_SMOKE
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": "phase1ai-smoke-1" if smoke else "phase1ai-run-1",
        "source": "binance",
        "exchange": "binance",
        "market": "futures",
        "symbol": "BTCUSDT",
        "event_kind": "agg_trade",
        "organization_id": "org-phase1ai",
        "workspace_id": "ws-phase1ai",
        "duration_ms": 300_000 if smoke else PUBLIC_SOAK_DURATION_MS,
        "scrape_interval_ms": 1_000,
        "stale_after_ms": 5_000,
        "quiet_checkpoint_timeout_ms": 30_000,
        "started_not_before_utc": STARTED,
        "git_commit": GIT_COMMIT,
        "require_clean_worktree": True,
        "infrastructure": _infrastructure(tmp_path),
        "health_endpoints": _health(),
        "replay_limits": _replay_limits(),
        "fault_plan": _smoke_faults() if smoke else _run_faults(),
        "acceptance": _acceptance(),
        "output": _output_paths(tmp_path),
    }


def test_valid_run_manifest_serializes_stably(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path, mode=MODE_RUN)
    first = parse_manifest(payload, mode=MODE_RUN)
    second = parse_manifest(json.loads(first.dumps()), mode=MODE_RUN)
    assert first.sha256() == second.sha256()
    assert first.source == "binance"
    assert first.duration_ms == PUBLIC_SOAK_DURATION_MS
    assert [item.method for item in first.fault_plan] == [
        "collector_sigkill",
        "writer_pre_commit_exit",
        "archiver_pre_commit_exit",
        "worker_sigkill",
        "scheduler_restart",
        "api_restart",
    ]
    dumped = json.loads(first.dumps())
    assert dumped["schema_version"] == SCHEMA_VERSION
    assert "token" not in first.dumps().lower()
    assert "password" not in first.dumps().lower()


def test_manifest_same_content_same_sha256(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path, mode=MODE_RUN)
    left = parse_manifest(copy.deepcopy(payload), mode=MODE_RUN)
    right = parse_manifest(copy.deepcopy(payload), mode=MODE_RUN)
    assert left.sha256() == right.sha256()
    assert len(left.sha256()) == 64


def test_development_smoke_manifest_cannot_claim_24h(tmp_path: Path) -> None:
    manifest = parse_manifest(
        _base_payload(tmp_path, mode=MODE_DEVELOPMENT_SMOKE),
        mode=MODE_DEVELOPMENT_SMOKE,
    )
    assert manifest.duration_ms == 300_000
    assert [item.method for item in manifest.fault_plan] == ["worker_sigkill"]


def test_load_manifest_requires_absolute_path(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path, mode=MODE_DEVELOPMENT_SMOKE)
    path = tmp_path / "phase1ai-smoke-manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    loaded = load_manifest(path, mode=MODE_DEVELOPMENT_SMOKE)
    assert loaded.run_id == "phase1ai-smoke-1"
    with pytest.raises(PublicSoakManifestError) as rejected:
        load_manifest("phase1ai-smoke-manifest.json", mode=MODE_DEVELOPMENT_SMOKE)
    assert rejected.value.code == "RELATIVE_PATH"


@pytest.mark.parametrize(
    ("mutator", "mode", "code"),
    [
        (
            lambda body: body.__setitem__("schema_version", "wrong.v0"),
            MODE_RUN,
            "SCHEMA_VERSION_MISMATCH",
        ),
        (
            lambda body: body.__setitem__("source", "okx"),
            MODE_RUN,
            "SOURCE_NOT_BINANCE",
        ),
        (
            lambda body: body.__setitem__("duration_ms", 300_000),
            MODE_RUN,
            "DURATION_BELOW_PUBLIC_SOAK",
        ),
        (
            lambda body: body.__setitem__("duration_ms", 1_000),
            MODE_DEVELOPMENT_SMOKE,
            "DURATION_BELOW_SMOKE",
        ),
        (
            lambda body: body["output"].__setitem__("result_path", "result.json"),
            MODE_RUN,
            "RELATIVE_PATH",
        ),
        (
            lambda body: body["fault_plan"].__setitem__(
                1,
                {**body["fault_plan"][1], "fault_id": "collector-sigkill"},
            ),
            MODE_RUN,
            "FAULT_ID_DUPLICATE",
        ),
        (
            lambda body: body["fault_plan"].__setitem__(
                1,
                {**body["fault_plan"][1], "scheduled_elapsed_ms": 1_000},
            ),
            MODE_RUN,
            "FAULT_TIME_NOT_INCREASING",
        ),
        (
            lambda body: body["fault_plan"].__setitem__(
                5,
                {
                    **body["fault_plan"][5],
                    "scheduled_elapsed_ms": PUBLIC_SOAK_DURATION_MS,
                },
            ),
            MODE_RUN,
            "FAULT_TIME_OUTSIDE_WINDOW",
        ),
        (
            lambda body: body.__setitem__("fault_plan", body["fault_plan"][:5]),
            MODE_RUN,
            "REQUIRED_FAULTS_MISSING",
        ),
        (
            lambda body: body["health_endpoints"].__setitem__(
                "collector",
                "http://user:pass@127.0.0.1:18121/health",
            ),
            MODE_RUN,
            "HEALTH_URL_UNSAFE",
        ),
        (
            lambda body: body["health_endpoints"].__setitem__(
                "writer",
                "http://127.0.0.1:18122/health?x=1",
            ),
            MODE_RUN,
            "HEALTH_URL_UNSAFE",
        ),
        (
            lambda body: body["health_endpoints"].__setitem__(
                "archiver",
                "http://10.0.0.8:18123/health",
            ),
            MODE_RUN,
            "HEALTH_URL_UNSAFE",
        ),
        (
            lambda body: body["infrastructure"].__setitem__(
                "postgres_bind",
                "127.0.0.1:29432?password=hidden",
            ),
            MODE_RUN,
            "ENDPOINT_HAS_CREDENTIALS",
        ),
        (
            lambda body: body["replay_limits"].__setitem__("max_active_replays", 8),
            MODE_RUN,
            "LIMIT_OUT_OF_BOUNDS",
        ),
        (
            lambda body: body["output"].__setitem__("max_log_bytes", 12),
            MODE_RUN,
            "LIMIT_OUT_OF_BOUNDS",
        ),
        (
            lambda body: body.__setitem__(
                "duration_ms",
                PUBLIC_SOAK_DURATION_MS,
            ),
            MODE_DEVELOPMENT_SMOKE,
            "SMOKE_CANNOT_CLAIM_PUBLIC_SOAK",
        ),
        (
            lambda body: body.__setitem__(
                "fault_plan",
                [
                    {
                        "fault_id": "collector-sigkill",
                        "target_role": "collector",
                        "method": "collector_sigkill",
                        "scheduled_elapsed_ms": 60_000,
                        "observation_timeout_ms": 30_000,
                        "recovery_timeout_ms": 60_000,
                    }
                ],
            ),
            MODE_DEVELOPMENT_SMOKE,
            "SMOKE_FAULT_NOT_WORKER_SIGKILL",
        ),
    ],
)
def test_manifest_reject_branches(
    tmp_path: Path,
    mutator: Any,
    mode: str,
    code: str,
) -> None:
    payload = _base_payload(tmp_path, mode=mode)
    mutator(payload)
    with pytest.raises(PublicSoakManifestError) as rejected:
        parse_manifest(payload, mode=mode)
    assert rejected.value.code == code


def test_manifest_rejects_existing_output(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path, mode=MODE_RUN)
    existing = Path(payload["output"]["result_path"])
    existing.write_text("{}", encoding="utf-8")
    with pytest.raises(PublicSoakManifestError) as rejected:
        parse_manifest(payload, mode=MODE_RUN)
    assert rejected.value.code == "OUTPUT_EXISTS"


def test_manifest_rejects_secret_values(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path, mode=MODE_RUN)
    payload["output"]["sample_dir"] = str(
        tmp_path / "postgresql://candlescope:hunter2@127.0.0.1/candlescope"
    )
    with pytest.raises(PublicSoakManifestError) as rejected:
        parse_manifest(payload, mode=MODE_RUN)
    assert rejected.value.code == "SECRET_IN_MANIFEST"


def test_manifest_rejects_secret_keys(tmp_path: Path) -> None:
    payload = _base_payload(tmp_path, mode=MODE_RUN)
    payload["api_token"] = "not-a-real-secret"
    with pytest.raises(PublicSoakManifestError) as rejected:
        parse_manifest(payload, mode=MODE_RUN)
    assert rejected.value.code == "UNKNOWN_MANIFEST_FIELD"


_PROCESS_CHILD = r"""
import signal
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[1])
mode = sys.argv[2]
if mode == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return
        status = 503 if mode == "never" else 200
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ready":true}')
        if mode == "noisy":
            sys.stdout.write("n" * 8192)
            sys.stdout.flush()

    def log_message(self, format: str, *args: object) -> None:
        del format, args


HTTPServer(("127.0.0.1", port), Handler).serve_forever()
"""


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


def _role_spec(tmp_path: Path, name: str, port: int, mode: str, **overrides: Any):
    from app.server_runtime.public_soak_processes import RoleSpec

    values = {
        "name": name,
        "argv": (sys.executable, "-c", _PROCESS_CHILD, str(port), mode),
        "sanitized_environment_keys": ("PATH", "CANDLESCOPE_PHASE1AI_API_TOKEN"),
        "health_url": f"http://127.0.0.1:{port}/health",
        "startup_timeout_ms": 2_000,
        "shutdown_timeout_ms": 500,
        "stdout_log": str(tmp_path / f"{name}.stdout.log"),
        "stderr_log": str(tmp_path / f"{name}.stderr.log"),
        "max_log_bytes": 2_048,
    }
    values.update(overrides)
    return RoleSpec(**values)


def _child_env() -> dict[str, str]:
    return {
        "PATH": "/usr/bin",
        "CANDLESCOPE_PHASE1AI_API_TOKEN": "super-secret-token-value",
        "CANDLESCOPE_PHASE1AI_POSTGRES_DSN": "postgresql://user:secret@127.0.0.1/db",
    }


def test_process_start_ready_and_sigterm(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_processes import RoleProcessManager

    async def run() -> None:
        port = _free_port()
        spec = _role_spec(tmp_path, "collector", port, "ready")
        manager = RoleProcessManager()
        role = await manager.start_role(spec, _child_env())
        assert role.pid is not None
        assert role.exit_code is None
        stopped = await manager.stop_role(spec)
        assert stopped.exit_code is not None
        events = [item["event"] for item in manager.events]
        assert events.count("stop_sigterm") == 1
        assert "stop_sigkill" not in events

    asyncio.run(run())


def test_process_ready_timeout(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_processes import (
        RoleProcessError,
        RoleProcessManager,
    )

    async def run() -> None:
        port = _free_port()
        spec = _role_spec(
            tmp_path,
            "writer",
            port,
            "never",
            startup_timeout_ms=400,
        )
        manager = RoleProcessManager()
        with pytest.raises(RoleProcessError) as rejected:
            await manager.start_role(spec, _child_env())
        assert rejected.value.code == "READY_TIMEOUT"
        assert manager.roles["writer"].exit_code is not None

    asyncio.run(run())


def test_process_sigkill_escalate(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_processes import RoleProcessManager

    async def run() -> None:
        port = _free_port()
        spec = _role_spec(
            tmp_path,
            "api",
            port,
            "ignore-term",
            shutdown_timeout_ms=200,
        )
        manager = RoleProcessManager()
        await manager.start_role(spec, _child_env())
        stopped = await manager.stop_role(spec)
        assert stopped.exit_code == -9
        events = [item["event"] for item in manager.events]
        assert "stop_sigterm" in events
        assert "stop_sigkill" in events

    asyncio.run(run())


def test_process_log_bounds(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_processes import RoleProcessManager

    async def run() -> None:
        port = _free_port()
        spec = _role_spec(
            tmp_path,
            "archiver",
            port,
            "noisy",
            max_log_bytes=1_024,
        )
        manager = RoleProcessManager()
        await manager.start_role(spec, _child_env())
        await asyncio.sleep(0.2)
        await manager.stop_role(spec)
        size = Path(spec.stdout_log).stat().st_size
        assert size <= 1_024

    asyncio.run(run())


def test_process_reverse_stop_order(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_processes import RoleProcessManager

    async def run() -> None:
        collector_port = _free_port()
        api_port = _free_port()
        collector = _role_spec(tmp_path, "collector", collector_port, "ready")
        api = _role_spec(tmp_path, "api", api_port, "ready")
        manager = RoleProcessManager()
        await manager.start_in_order(
            [collector, api],
            {"collector": _child_env(), "api": _child_env()},
        )
        await manager.stop_in_reverse([collector, api])
        stops = [
            item["role"] for item in manager.events if item["event"] == "stop_sigterm"
        ]
        assert stops == ["api", "collector"]

    asyncio.run(run())


def test_process_does_not_keep_secrets(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_processes import RoleProcessManager

    async def run() -> None:
        port = _free_port()
        spec = _role_spec(tmp_path, "scheduler", port, "ready")
        manager = RoleProcessManager()
        role = await manager.start_role(spec, _child_env())
        try:
            dumped = json.dumps(role.to_evidence()) + repr(manager) + repr(role)
            assert "super-secret-token-value" not in dumped
            assert "postgresql://user:secret" not in dumped
            assert "CANDLESCOPE_PHASE1AI_API_TOKEN" in role.present_environment_keys
        finally:
            await manager.stop_role(spec)

    asyncio.run(run())


class _Clock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now


def _parsed_manifest(tmp_path: Path, *, mode: str = MODE_DEVELOPMENT_SMOKE):
    return parse_manifest(_base_payload(tmp_path, mode=mode), mode=mode)


def _role_bodies(now_ms: int) -> dict[str, dict[str, object]]:
    snapshot = {
        "snapshot_version": 4,
        "manifest_sha256": "a" * 64,
        "manifest_uri": "s3://candlescope-archive/snapshot-4.json",
        "data_epoch": "epoch-1",
    }
    return {
        "collector": {
            "ready": True,
            "state": "leader",
            "owner_id": "collector-a",
            "producer_epoch": 1,
            "last_partition_offset": 9,
            "updated_at_ms": now_ms,
        },
        "writer": {
            "ready": True,
            "state": "running",
            "owner_id": "writer-a",
            "committed_next_offset": 10,
            "duplicate_events": 0,
            "conflict_events": 0,
            "updated_at_ms": now_ms,
        },
        "archiver": {
            "ready": True,
            "state": "running",
            "owner_id": "archiver-a",
            "committed_next_offset": 10,
            "current_snapshot": snapshot,
            "updated_at_ms": now_ms,
        },
        "query": {
            "ready": True,
            "status": "ready",
            "snapshot": snapshot,
            "updated_at_ms": now_ms,
        },
        "scheduler": {
            "ready": True,
            "pending": 1,
            "running": 2,
            "live_workers": 2,
            "updated_at_ms": now_ms,
        },
        "worker_a": {
            "ready": True,
            "state": "ready",
            "owner_id": "worker-a",
            "fencing_epoch": 1,
            "active_actors": 1,
            "updated_at_ms": now_ms,
        },
        "worker_b": {
            "ready": True,
            "state": "ready",
            "owner_id": "worker-b",
            "fencing_epoch": 1,
            "active_actors": 1,
            "updated_at_ms": now_ms,
        },
        "api": {"ready": True, "status": "ready", "updated_at_ms": now_ms},
    }


def test_sample_hash_chain_links_previous(tmp_path: Path) -> None:
    from app.server_runtime.public_soak import SoakSampler

    clock = _Clock(1_700_000_000_000)
    sampler = SoakSampler(_parsed_manifest(tmp_path), clock_ms=clock)
    bodies = _role_bodies(clock.now)
    first = sampler.observe(bodies)
    clock.now += 1_000
    second = sampler.observe(bodies)
    assert first.sequence == 1
    assert second.sequence == 2
    assert second.previous_sample_sha256 == first.sample_sha256
    assert first.payload_sha256 == second.payload_sha256
    rebuilt = SoakSampler(
        _parsed_manifest(tmp_path / "other"), clock_ms=_Clock(1_700_000_000_000)
    )
    assert (
        rebuilt.observe(_role_bodies(1_700_000_000_000)).sample_sha256
        == first.sample_sha256
    )


def test_sample_rejects_clock_rollback(tmp_path: Path) -> None:
    from app.server_runtime.public_soak import SoakObservationError, SoakSampler

    clock = _Clock(1_700_000_000_000)
    sampler = SoakSampler(_parsed_manifest(tmp_path), clock_ms=clock)
    sampler.observe(_role_bodies(clock.now))
    clock.now -= 5
    with pytest.raises(SoakObservationError) as rejected:
        sampler.observe(_role_bodies(clock.now))
    assert rejected.value.code == "CLOCK_ROLLBACK"


def test_sample_rejects_stale_health(tmp_path: Path) -> None:
    from app.server_runtime.public_soak import SoakObservationError, SoakSampler

    clock = _Clock(1_700_000_000_000)
    sampler = SoakSampler(_parsed_manifest(tmp_path), clock_ms=clock)
    bodies = _role_bodies(clock.now - 30_000)
    with pytest.raises(SoakObservationError) as rejected:
        sampler.observe(bodies)
    assert rejected.value.code == "HEALTH_STALE"


def test_sample_rejects_oversize_and_invalid_json(tmp_path: Path) -> None:
    from app.server_runtime.public_soak import (
        SoakObservationError,
        SoakSampler,
        parse_health_bytes,
    )

    manifest = _parsed_manifest(tmp_path)
    with pytest.raises(SoakObservationError) as too_large:
        parse_health_bytes(
            b"x" * (manifest.output.max_health_bytes + 1),
            max_bytes=manifest.output.max_health_bytes,
            role="collector",
        )
    assert too_large.value.code == "HEALTH_PAYLOAD_TOO_LARGE"
    with pytest.raises(SoakObservationError) as invalid:
        parse_health_bytes(
            b"not-json",
            max_bytes=manifest.output.max_health_bytes,
            role="writer",
        )
    assert invalid.value.code == "HEALTH_JSON_INVALID"
    clock = _Clock(1_700_000_000_000)
    sampler = SoakSampler(manifest, clock_ms=clock)
    with pytest.raises(SoakObservationError) as sampled:
        sampler.observe(
            _role_bodies(clock.now),
            raw_bytes_by_role={"collector": manifest.output.max_health_bytes + 8},
        )
    assert sampled.value.code == "HEALTH_PAYLOAD_TOO_LARGE"


def test_redact_secrets_dsn_paths_and_payloads() -> None:
    from app.server_runtime.public_soak import redact_for_evidence

    redacted = redact_for_evidence(
        {
            "api_token": "abcd1234",
            "lease_token": "lease-secret",
            "dsn": "postgresql://candlescope:hunter2@127.0.0.1/db",
            "log_path": "/home/helenanana/projects/CandleScope-server/logs/api.log",
            "events": [
                {"price": "1", "qty": "2", "agg_trade_id": 1},
                {"price": "1", "qty": "2", "agg_trade_id": 2},
            ],
        },
        max_bytes=16_384,
    )
    assert redacted["api_token"] == "<redacted>"
    assert redacted["lease_token"] == "<redacted>"
    assert redacted["dsn"] == "<redacted>"
    assert redacted["log_path"] == "api.log"
    assert redacted["events"] == {"dropped": 2, "kind": "market_payload"}


def test_evidence_exclusive_create_and_hash_reread(tmp_path: Path) -> None:
    from app.server_runtime.public_soak import EvidenceWriter, SoakSampler

    manifest = _parsed_manifest(tmp_path)
    clock = _Clock(1_700_000_000_000)
    sampler = SoakSampler(manifest, clock_ms=clock)
    writer = EvidenceWriter(
        manifest,
        mode=MODE_DEVELOPMENT_SMOKE,
        sample_path=tmp_path / "samples" / "run.samples.jsonl",
    )
    record = sampler.observe(_role_bodies(clock.now))
    writer.append_sample(record, fsync=True)
    result = writer.finalize(phase_passed=True, elapsed_ms=300_000)
    assert result["phase_passed"] is True
    assert result["twenty_four_hour_public_continuity"] is False
    assert result["production_ready"] is False
    assert result["final_sample_sha256"] == record.sample_sha256
    assert Path(manifest.output.result_path).is_file()
    with pytest.raises(FileExistsError):
        EvidenceWriter(
            manifest,
            mode=MODE_DEVELOPMENT_SMOKE,
            sample_path=tmp_path / "samples" / "run.samples.jsonl",
        )


def test_evidence_failure_writes_phase_passed_false(tmp_path: Path) -> None:
    from app.server_runtime.public_soak import EvidenceWriter

    manifest = _parsed_manifest(tmp_path)
    writer = EvidenceWriter(
        manifest,
        mode=MODE_DEVELOPMENT_SMOKE,
        sample_path=tmp_path / "samples" / "fail.samples.jsonl",
    )
    result = writer.finalize(
        phase_passed=False,
        elapsed_ms=12_000,
        error_code="READY_TIMEOUT",
        error_message="writer did not become ready",
    )
    assert result["phase_passed"] is False
    assert result["phase1ai_passed"] is False
    assert result["twenty_four_hour_public_continuity"] is False
    assert result["production_ready"] is False
    assert result["error"]["code"] == "READY_TIMEOUT"
    stored = json.loads(Path(manifest.output.result_path).read_text(encoding="utf-8"))
    assert stored["phase_passed"] is False


def _pin_payload() -> dict[str, object]:
    return {
        "snapshot": {
            "data_epoch": "epoch-1",
            "snapshot_version": 4,
            "manifest_uri": "s3://candlescope-archive/snapshot-4.json",
            "manifest_sha256": "a" * 64,
        },
        "pin": {
            "start_event_time_ms": 1_700_000_000_042,
            "end_event_time_ms": 1_700_000_000_044,
            "expected_first_agg_trade_id": 42,
            "expected_last_agg_trade_id": 44,
            "row_count": 3,
        },
    }


class _FakeReplayTransport:
    def __init__(self) -> None:
        self.snapshot = _pin_payload()["snapshot"]
        self.live_workers = 2
        self.max_active = 2
        self.runs: dict[str, dict[str, object]] = {}
        self.commands: dict[str, dict[str, object]] = {}
        self.durable_counts: dict[str, int] = {}
        self._active = 0
        self.created_payloads: list[dict[str, object]] = []

    async def cold_query_snapshot(
        self, snapshot: dict[str, object]
    ) -> dict[str, object]:
        del snapshot
        return {**self.snapshot, "preference": "cold"}

    async def live_worker_count(self) -> int:
        return self.live_workers

    async def create_run(self, payload: dict[str, object]) -> dict[str, object]:
        self.created_payloads.append(dict(payload))
        run_id = str(payload["idempotency_key"])
        if self._active < self.max_active:
            state = "RUNNING"
            session_id: str | None = f"sess-{run_id}"
            self._active += 1
        else:
            state = "PENDING"
            session_id = None
        body = {
            "run_id": run_id,
            "state": state,
            "session_id": session_id,
            "organization_id": payload.get("organization_id"),
            "workspace_id": payload.get("workspace_id"),
        }
        self.runs[run_id] = body
        return body

    async def get_run(self, run_id: str) -> dict[str, object]:
        return self.runs[run_id]

    async def get_session(self, session_id: str) -> dict[str, object]:
        for body in self.runs.values():
            if body.get("session_id") == session_id:
                return {
                    "session_id": session_id,
                    "run_id": body["run_id"],
                    "state": body["state"],
                    "attempt": 1,
                }
        return {"session_id": session_id, "state": "PENDING", "attempt": 0}

    async def cancel_run(self, run_id: str) -> dict[str, object]:
        body = self.runs[run_id]
        body["state"] = "CANCELLED"
        if self._active > 0 and body.get("session_id"):
            self._active -= 1
        return body

    async def submit_command(
        self, session_id: str, command: dict[str, object]
    ) -> dict[str, object]:
        if command.get("simulate_timeout"):
            raise TimeoutError("client timed out before the command response")
        command_id = str(command["command_id"])
        existing = self.commands.get(command_id)
        if existing is not None:
            return existing
        result = {
            "command_id": command_id,
            "session_id": session_id,
            "revision": 1,
            "state_hash": f"hash-{command_id}",
            "component_hash": f"comp-{command_id}",
            "cursor": {"source_sequence": 1, "last_agg_trade_id": 42},
        }
        self.commands[command_id] = result
        self.durable_counts[command_id] = 1
        return result

    async def get_command_result(
        self, session_id: str, command_id: str
    ) -> dict[str, object]:
        del session_id
        return self.commands[command_id]

    def durable_command_count(self, command_id: str) -> int:
        return int(self.durable_counts.get(command_id, 0))


def test_replay_worker_role_from_id() -> None:
    from app.server_runtime.public_soak_replay import worker_role_from_id

    assert worker_role_from_id("worker-a") == "worker_a"
    assert worker_role_from_id("worker-b") == "worker_b"
    assert worker_role_from_id("worker_b") == "worker_b"


def test_replay_health_origin() -> None:
    from app.server_runtime.public_soak_replay import health_origin

    assert (
        health_origin("http://127.0.0.1:18128/health/ready") == "http://127.0.0.1:18128"
    )


def test_api_bearer_token_falls_back_to_static_identity() -> None:
    from app.server_runtime.public_soak import _api_bearer_token

    assert _api_bearer_token({"CANDLESCOPE_PHASE1AI_API_TOKEN": "from-env"}) == (
        "from-env"
    )
    assert (
        _api_bearer_token(
            {
                "CANDLESCOPE_SERVER_API_STATIC_TOKENS_JSON": (
                    '{"phase1ai-local-api-token-00000000000000":{"role":"trader"}}'
                )
            }
        )
        == "phase1ai-local-api-token-00000000000000"
    )


def test_replay_http_transport_repr_hides_tokens() -> None:
    from app.server_runtime.public_soak_replay import HttpSoakReplayTransport

    transport = HttpSoakReplayTransport(
        api_origin="http://127.0.0.1:18128",
        query_origin="http://127.0.0.1:18124",
        api_token="super-secret-api-token",
        query_token="super-secret-query-token",
        organization_id="org-phase1ai",
        workspace_id="ws-phase1ai",
        session=object(),
    )
    text = repr(transport)
    assert "super-secret" not in text
    assert "18128" in text


def test_replay_pin_from_query_events() -> None:
    from app.server_runtime.public_soak_replay import pin_from_query_events

    pin = pin_from_query_events(
        _pin_payload()["snapshot"],
        [
            {
                "event_time_ms": 1_700_000_000_042,
                "payload": {"agg_trade_id": 42},
            },
            {
                "event_time_ms": 1_700_000_000_043,
                "payload": {"agg_trade_id": 43},
            },
            {
                "event_time_ms": 1_700_000_000_044,
                "payload": {"agg_trade_id": 44},
            },
        ],
    )
    assert pin.expected_first_agg_trade_id == 42
    assert pin.expected_last_agg_trade_id == 44
    assert pin.row_count == 3


def test_replay_payload_aligns_start_to_one_minute_bar(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_replay import (
        build_run_payload,
        parse_snapshot_pin,
    )

    pin = parse_snapshot_pin(_pin_payload())
    payload = build_run_payload(
        _parsed_manifest(tmp_path),
        pin,
        idempotency_key="replay-a",
        priority=10,
    )
    assert payload["replay_start_ms"] % 60_000 == 0
    assert payload["replay_end_time_ms"] == payload["replay_start_ms"] + 60_000 - 1
    config = payload["config"]
    assert isinstance(config, dict)
    assert config["requested_start_ms"] == payload["replay_start_ms"]
    assert config["horizon_ms"] == 60_000


def test_replay_pin_uses_consecutive_prefix_only() -> None:
    from app.server_runtime.public_soak_replay import pin_from_query_events

    pin = pin_from_query_events(
        _pin_payload()["snapshot"],
        [
            {
                "event_time_ms": 1_700_000_000_042,
                "payload": {"agg_trade_id": 42},
            },
            {
                "event_time_ms": 1_700_000_000_044,
                "payload": {"agg_trade_id": 44},
            },
        ],
    )
    assert pin.expected_first_agg_trade_id == 42
    assert pin.expected_last_agg_trade_id == 42
    assert pin.row_count == 1
    assert pin.start_event_time_ms == 1_700_000_000_042
    assert pin.end_event_time_ms == 1_700_000_000_042


def test_replay_rejects_latest_and_query_path() -> None:
    from app.server_runtime.public_soak_replay import (
        ReplaySoakError,
        parse_snapshot_pin,
    )

    latest = _pin_payload()
    latest["snapshot"]["data_epoch"] = "latest"
    with pytest.raises(ReplaySoakError) as rejected:
        parse_snapshot_pin(latest)
    assert rejected.value.code == "LATEST_SNAPSHOT_FORBIDDEN"
    local = _pin_payload()
    local["query_path"] = "/tmp/frozen-query.json"
    with pytest.raises(ReplaySoakError) as path_rejected:
        parse_snapshot_pin(local)
    assert path_rejected.value.code == "QUERY_PATH_FORBIDDEN"
    zero = _pin_payload()
    zero["snapshot"]["snapshot_version"] = 0
    with pytest.raises(ReplaySoakError) as version_rejected:
        parse_snapshot_pin(zero)
    assert version_rejected.value.code == "INVALID_SNAPSHOT"


def test_replay_create_preconditions_and_three_tasks(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_replay import (
        PublicSoakReplayDriver,
        ReplaySoakError,
        parse_snapshot_pin,
    )

    async def run() -> None:
        manifest = _parsed_manifest(tmp_path)
        transport = _FakeReplayTransport()
        driver = PublicSoakReplayDriver(manifest, transport)
        pin = parse_snapshot_pin(_pin_payload())
        transport.live_workers = 1
        with pytest.raises(ReplaySoakError) as rejected:
            await driver.create_three_tasks(pin)
        assert rejected.value.code == "WORKERS_NOT_LIVE"
        transport.live_workers = 2
        workload = await driver.create_three_tasks(pin)
        assert workload.replay_a.state == "RUNNING"
        assert workload.replay_b.state == "RUNNING"
        assert workload.replay_queued.observed_queued is True
        assert workload.replay_queued.state == "PENDING"
        for payload in transport.created_payloads:
            assert "query_path" not in payload
            assert payload["organization_id"] == manifest.organization_id
            assert payload["workspace_id"] == manifest.workspace_id
            assert payload["snapshot"]["snapshot_version"] == 4
            assert "config" in payload
            assert "broker_config" in payload

    asyncio.run(run())


def test_replay_commands_and_command_id_idempotency(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_replay import (
        FROZEN_IDEMPOTENT_COMMAND_ID,
        PublicSoakReplayDriver,
        parse_snapshot_pin,
    )

    async def run() -> None:
        manifest = _parsed_manifest(tmp_path)
        transport = _FakeReplayTransport()
        driver = PublicSoakReplayDriver(manifest, transport)
        workload = await driver.create_three_tasks(parse_snapshot_pin(_pin_payload()))
        observed = await driver.drive_commands(workload)
        assert observed["replay-a-acquire"].command_id.endswith("-acquire")
        assert observed["replay-a-step"].revision == 1
        assert observed["replay-b-resume"].command_id.endswith("-resume")
        assert transport.runs["replay-queued"]["state"] == "PENDING"
        first = await driver.probe_idempotency(workload)
        second = await driver.probe_idempotency(workload)
        assert first.revision == second.revision == 1
        assert first.cursor == second.cursor
        assert first.state_hash == second.state_hash
        assert transport.durable_command_count(FROZEN_IDEMPOTENT_COMMAND_ID) == 1

    asyncio.run(run())


def test_replay_queued_must_be_observed(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_replay import (
        PublicSoakReplayDriver,
        ReplaySoakError,
        parse_snapshot_pin,
    )

    async def run() -> None:
        transport = _FakeReplayTransport()
        transport.max_active = 8
        driver = PublicSoakReplayDriver(_parsed_manifest(tmp_path), transport)
        with pytest.raises(ReplaySoakError) as rejected:
            await driver.create_three_tasks(parse_snapshot_pin(_pin_payload()))
        assert rejected.value.code == "QUEUED_NOT_OBSERVED"

    asyncio.run(run())


class _FaultClock:
    def __init__(self, now: int) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    async def sleep(self, seconds: float) -> None:
        advanced = int(seconds * 1000)
        self.now += advanced if advanced > 0 else 50


class _FakeActuator:
    def __init__(self, *, trigger_ok: bool = True, recover_ok: bool = True) -> None:
        self.trigger_ok = trigger_ok
        self.recover_ok = recover_ok
        self.triggered: list[str] = []
        self.snapshot = {
            "data_epoch": "epoch-1",
            "snapshot_version": 4,
            "manifest_sha256": "a" * 64,
            "manifest_uri": "s3://candlescope-archive/snapshot-4.json",
        }
        self.offset = 10

    async def trigger(self, spec: object) -> None:
        self.triggered.append(spec.method)

    async def trigger_observed(self, spec: object) -> bool:
        del spec
        return self.trigger_ok

    async def recovery_observed(self, spec: object) -> bool:
        del spec
        return self.recover_ok

    async def quiet_observation(self):
        from app.server_runtime.soak_faults import QuietObservation

        return QuietObservation(
            collector_durable_next_offset=self.offset,
            writer_committed_next_offset=self.offset,
            archiver_covered_next_offset=self.offset,
            query_snapshot=self.snapshot,
            replay_pinned_snapshot=dict(self.snapshot),
            unresolved_gaps=0,
            hash_conflicts=0,
            producer_epoch_rollback=0,
        )


def test_fault_one_way_transitions_and_quiet_checkpoint(tmp_path: Path) -> None:
    from app.server_runtime.soak_faults import (
        FaultStatus,
        SoakFaultMachine,
        verify_quiet_checkpoint,
    )

    async def run() -> None:
        clock = _FaultClock(0)
        actuator = _FakeActuator()
        machine = SoakFaultMachine(
            _parsed_manifest(tmp_path),
            actuator,
            clock_ms=clock,
            sleep=clock.sleep,
        )
        completed = await machine.run_all(started_at_ms=0)
        assert len(completed) == 1
        assert completed[0].status is FaultStatus.QUIET_CHECKPOINT_VERIFIED
        assert actuator.triggered == ["worker_sigkill"]
        assert machine.remaining() == ()
        verify_quiet_checkpoint(await actuator.quiet_observation())

    asyncio.run(run())


def test_fault_timeout_does_not_continue(tmp_path: Path) -> None:
    from app.server_runtime.soak_faults import (
        FaultMachineError,
        FaultStatus,
        SoakFaultMachine,
    )

    async def run() -> None:
        clock = _FaultClock(0)
        actuator = _FakeActuator(trigger_ok=False)
        machine = SoakFaultMachine(
            _parsed_manifest(tmp_path),
            actuator,
            clock_ms=clock,
            sleep=clock.sleep,
        )
        with pytest.raises(FaultMachineError) as rejected:
            await machine.run_all(started_at_ms=0)
        assert rejected.value.code == "TRIGGER_TIMEOUT"
        assert machine.records[0].status is FaultStatus.FAILED
        assert machine._index == 0

    asyncio.run(run())


def test_fault_quiet_checkpoint_rejects_mismatch() -> None:
    from app.server_runtime.soak_faults import (
        FaultMachineError,
        QuietObservation,
        verify_quiet_checkpoint,
    )

    snapshot = {
        "data_epoch": "epoch-1",
        "snapshot_version": 4,
        "manifest_sha256": "a" * 64,
        "manifest_uri": "s3://candlescope-archive/snapshot-4.json",
    }
    with pytest.raises(FaultMachineError) as rejected:
        verify_quiet_checkpoint(
            QuietObservation(
                collector_durable_next_offset=10,
                writer_committed_next_offset=9,
                archiver_covered_next_offset=10,
                query_snapshot=snapshot,
                replay_pinned_snapshot=snapshot,
                unresolved_gaps=0,
                hash_conflicts=0,
                producer_epoch_rollback=0,
            )
        )
    assert rejected.value.code == "QUIET_CHECKPOINT_MISMATCH"


def test_fault_writer_archiver_reuse_precommit_hooks(tmp_path: Path) -> None:
    from app.server_runtime.public_soak_manifest import FaultSpec
    from app.server_runtime.soak_faults import (
        ARCHIVER_PRECOMMIT_HOOK,
        WRITER_PRECOMMIT_HOOK,
        arm_precommit_hook,
        precommit_hook_name,
        required_methods_for_plan,
    )

    writer = FaultSpec(
        fault_id="writer-pre-commit",
        target_role="writer",
        method="writer_pre_commit_exit",
        scheduled_elapsed_ms=1_000,
        observation_timeout_ms=1_000,
        recovery_timeout_ms=1_000,
    )
    archiver = FaultSpec(
        fault_id="archiver-pre-commit",
        target_role="archiver",
        method="archiver_pre_commit_exit",
        scheduled_elapsed_ms=2_000,
        observation_timeout_ms=1_000,
        recovery_timeout_ms=1_000,
    )
    assert precommit_hook_name(writer.method) == WRITER_PRECOMMIT_HOOK
    assert precommit_hook_name(archiver.method) == ARCHIVER_PRECOMMIT_HOOK
    writer_path = arm_precommit_hook(tmp_path, writer)
    archiver_path = arm_precommit_hook(tmp_path, archiver)
    assert writer_path.name == f"writer.{WRITER_PRECOMMIT_HOOK}.arm"
    assert archiver_path.name == f"archiver.{ARCHIVER_PRECOMMIT_HOOK}.arm"
    assert writer_path.read_text(encoding="utf-8") == "writer-pre-commit"
    plan = _parsed_manifest(tmp_path, mode=MODE_RUN).fault_plan
    assert required_methods_for_plan(plan) == (
        "collector_sigkill",
        "writer_pre_commit_exit",
        "archiver_pre_commit_exit",
        "worker_sigkill",
        "scheduler_restart",
        "api_restart",
    )


def test_fault_run_plan_executes_six_in_order(tmp_path: Path) -> None:
    from app.server_runtime.soak_faults import FaultStatus, SoakFaultMachine

    async def run() -> None:
        clock = _FaultClock(0)
        actuator = _FakeActuator()
        machine = SoakFaultMachine(
            _parsed_manifest(tmp_path, mode=MODE_RUN),
            actuator,
            clock_ms=clock,
            sleep=clock.sleep,
        )
        completed = await machine.run_all(started_at_ms=0)
        assert [item.spec.method for item in completed] == [
            "collector_sigkill",
            "writer_pre_commit_exit",
            "archiver_pre_commit_exit",
            "worker_sigkill",
            "scheduler_restart",
            "api_restart",
        ]
        assert all(
            item.status is FaultStatus.QUIET_CHECKPOINT_VERIFIED for item in completed
        )

    asyncio.run(run())


def test_fault_evidence_after_plan_index_advances() -> None:
    from app.server_runtime.public_soak import _fault_evidence
    from app.server_runtime.public_soak_manifest import FaultSpec
    from app.server_runtime.soak_faults import FaultRecord

    spec = FaultSpec(
        fault_id="worker-sigkill",
        target_role="worker_a",
        method="worker_sigkill",
        scheduled_elapsed_ms=1_000,
        observation_timeout_ms=1_000,
        recovery_timeout_ms=1_000,
    )

    class _Machine:
        def __init__(self) -> None:
            self.records = [FaultRecord(spec)]
            self._index = 1

    evidence = _fault_evidence(_Machine())
    assert evidence["fault_id"] == "worker-sigkill"


def test_cli_refuses_relative_path_and_existing_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts.server_phase1ai_public_soak import main as soak_main

    output = tmp_path / "out.json"
    assert (
        soak_main(
            [
                "development-smoke",
                "--manifest",
                "phase1ai-smoke-manifest.json",
                "--output",
                str(output),
            ]
        )
        == 1
    )
    relative = json.loads(capsys.readouterr().out)
    assert relative["code"] == "RELATIVE_PATH"
    assert relative["twenty_four_hour_public_continuity"] is False
    assert relative["production_ready"] is False
    existing = tmp_path / "exists.json"
    existing.write_text("{}", encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    assert (
        soak_main(
            [
                "development-smoke",
                "--manifest",
                str(manifest),
                "--output",
                str(existing),
            ]
        )
        == 1
    )
    exists = json.loads(capsys.readouterr().out)
    assert exists["code"] == "OUTPUT_EXISTS"


def test_cli_run_refuses_missing_dual_switch_and_short_duration(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts.server_phase1ai_public_soak import (
        FAULT_INJECTION_ENV,
        PUBLIC_SOAK_ENV,
    )
    from scripts.server_phase1ai_public_soak import (
        main as soak_main,
    )

    payload = _base_payload(tmp_path, mode=MODE_RUN)
    payload["duration_ms"] = 300_000
    payload["fault_plan"] = _smoke_faults()
    manifest = tmp_path / "run-manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    output = Path(payload["output"]["result_path"])
    monkeypatch.delenv(PUBLIC_SOAK_ENV, raising=False)
    monkeypatch.delenv(FAULT_INJECTION_ENV, raising=False)
    assert soak_main(["run", "--manifest", str(manifest), "--output", str(output)]) == 1
    missing = json.loads(capsys.readouterr().out)
    assert missing["code"] == "DUAL_SWITCH_MISSING"
    monkeypatch.setenv(PUBLIC_SOAK_ENV, "1")
    monkeypatch.setenv(FAULT_INJECTION_ENV, "1")
    assert soak_main(["run", "--manifest", str(manifest), "--output", str(output)]) == 1
    duration = json.loads(capsys.readouterr().out)
    assert duration["code"] == "DURATION_BELOW_PUBLIC_SOAK"
    assert duration["twenty_four_hour_public_continuity"] is False


def test_cli_help_does_not_claim_24h(capsys: pytest.CaptureFixture[str]) -> None:
    from scripts.server_phase1ai_public_soak import main as soak_main

    with pytest.raises(SystemExit) as exited:
        soak_main(["development-smoke", "--help"])
    assert exited.value.code == 0
    help_text = capsys.readouterr().out
    assert "twenty_four_hour_public_continuity=true" not in help_text
    assert "production_ready=true" not in help_text


def test_cli_verifier_recomputes_hashes_from_files(tmp_path: Path) -> None:
    from app.server_runtime.public_soak import EvidenceWriter, SoakSampler
    from app.server_runtime.public_soak_verify import verify_evidence
    from scripts.server_phase1ai_verify import main as verify_main

    manifest = _parsed_manifest(tmp_path)
    manifest_path = tmp_path / "phase1ai-smoke-manifest.json"
    manifest_path.write_text(manifest.dumps(), encoding="utf-8")
    clock = _Clock(1_700_000_000_000)
    sampler = SoakSampler(manifest, clock_ms=clock)
    writer = EvidenceWriter(
        manifest,
        mode=MODE_DEVELOPMENT_SMOKE,
        sample_path=tmp_path / "samples" / "run.samples.jsonl",
    )
    writer.append_sample(sampler.observe(_role_bodies(clock.now)), fsync=True)
    result = writer.finalize(phase_passed=True, elapsed_ms=300_000)
    report = verify_evidence(
        manifest_path=manifest_path,
        result_path=manifest.output.result_path,
        sample_path=tmp_path / "samples" / "run.samples.jsonl",
    )
    assert report["verified"] is True
    assert report["final_sample_sha256"] == result["final_sample_sha256"]
    assert report["twenty_four_hour_public_continuity"] is False
    assert report["production_ready"] is False
    assert (
        verify_main(
            [
                "--manifest",
                str(manifest_path),
                "--result",
                manifest.output.result_path,
                "--samples",
                str(tmp_path / "samples" / "run.samples.jsonl"),
            ]
        )
        == 0
    )
