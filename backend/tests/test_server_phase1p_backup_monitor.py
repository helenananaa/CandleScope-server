from __future__ import annotations

import asyncio
import base64
import hmac
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import rfc8785

from app.server_runtime.query_backup_history import (
    BackupRunHistorySigner,
    ImmutableBackupRunHistoryRepository,
)
from app.server_runtime.query_backup_monitor import (
    BackupSuccessInventoryError,
    BackupSuccessReference,
    PrivateBackupSuccessInventory,
)
from app.server_runtime.query_backup_selection import BackupJobReceipt
from app.server_runtime.testing import InMemoryImmutableObjectStore
from scripts import server_query_backup_monitor

ROOT = Path(__file__).parents[2]
CLUSTER_ID = "phase1p-primary"
BACKUP_ID = "11111111-2222-4333-8444-555555555555"
COMPLETED_AT_MS = 1_700_000_100_000
HISTORY_SECRET = b"phase1p-history-secret-is-at-least-32-bytes"
ALERT_SECRET = b"phase1p-alert-secret-is-at-least-32-bytes"


def test_private_success_inventory_is_deterministic_and_rejects_drift(
    tmp_path,
) -> None:
    root = _private_directory(tmp_path / "inventory")
    inventory = PrivateBackupSuccessInventory(root)
    reference = _reference()

    first = inventory.persist(reference)
    replay = inventory.persist(reference)
    snapshot = inventory.snapshot(
        window_start_ms=COMPLETED_AT_MS - 1,
        window_end_ms=COMPLETED_AT_MS + 1,
    )

    assert first.created is True
    assert replay.created is False
    assert first.path == replay.path
    assert first.path.stat().st_mode & 0o777 == 0o600
    assert snapshot.references == (reference,)
    assert snapshot.total_file_count == 1

    temporary = root / (
        f".{reference.filename}.33333333-4444-4555-8666-777777777777.tmp"
    )
    temporary.write_bytes(reference.canonical_bytes())
    temporary.chmod(0o600)
    concurrent_snapshot = inventory.snapshot(
        window_start_ms=COMPLETED_AT_MS - 1,
        window_end_ms=COMPLETED_AT_MS + 1,
    )
    assert concurrent_snapshot.references == (reference,)
    assert concurrent_snapshot.total_file_count == 1
    temporary.unlink()

    changed = BackupSuccessReference(
        cluster_id=reference.cluster_id,
        backup_id=reference.backup_id,
        completed_at_ms=reference.completed_at_ms,
        history_uri=reference.history_uri,
        history_sha256="e" * 64,
    )
    with pytest.raises(BackupSuccessInventoryError, match="different bytes"):
        inventory.persist(changed)
    with pytest.raises(ValueError, match="20-digit"):
        BackupSuccessReference(
            cluster_id=reference.cluster_id,
            backup_id=reference.backup_id,
            completed_at_ms=10**20,
            history_uri=reference.history_uri,
            history_sha256=reference.history_sha256,
        )
    bounded = PrivateBackupSuccessInventory(root, maximum_files=1)
    with pytest.raises(BackupSuccessInventoryError, match="file bound"):
        bounded.persist(
            BackupSuccessReference(
                cluster_id=CLUSTER_ID,
                backup_id="22222222-3333-4444-8555-666666666666",
                completed_at_ms=COMPLETED_AT_MS + 1,
                history_uri=reference.history_uri,
                history_sha256=reference.history_sha256,
            )
        )


def test_private_success_inventory_rejects_shared_and_unexpected_entries(
    tmp_path,
) -> None:
    root = _private_directory(tmp_path / "inventory")
    inventory = PrivateBackupSuccessInventory(root)
    inventory.persist(_reference())

    (root / "unexpected").write_text("not an inventory entry", encoding="utf-8")
    (root / "unexpected").chmod(0o600)
    with pytest.raises(BackupSuccessInventoryError, match="unexpected entry"):
        inventory.snapshot(
            window_start_ms=COMPLETED_AT_MS - 1,
            window_end_ms=COMPLETED_AT_MS + 1,
        )
    (root / "unexpected").unlink()

    root.chmod(0o750)
    with pytest.raises(BackupSuccessInventoryError, match="owned private"):
        inventory.persist(_reference())


def test_monitor_persists_and_verifies_host_observed_signed_cadence(
    tmp_path,
    monkeypatch,
) -> None:
    async def exercise() -> None:
        root = _configure_monitor(tmp_path, monkeypatch)
        repository, published, receipt = await _published_history()
        monkeypatch.setattr(
            server_query_backup_monitor.server_query_backup_history,
            "history_repository",
            lambda: repository,
        )
        publication = _publication(published)
        persisted = server_query_backup_monitor.persist_success_reference(
            receipt.to_wire(),
            publication,
        )
        replay = server_query_backup_monitor.persist_success_reference(
            receipt.to_wire(),
            publication,
        )
        result = await server_query_backup_monitor.run(
            evaluated_at_ms=COMPLETED_AT_MS + 150_000,
        )

        assert persisted["created"] is True
        assert replay["created"] is False
        assert len(list(root.iterdir())) == 1
        assert result["status"] == "healthy"
        assert result["source"] == "host-private-success-inventory"
        assert result["window_reference_count"] == 1
        assert result["host_inventory_completeness_proven"] is False
        assert result["global_history_completeness_proven"] is False
        assert result["cadence"]["maximum_observed_gap_ms"] == 150_000

    asyncio.run(exercise())


def test_monitor_rejects_empty_gap_and_reference_hash_drift(
    tmp_path,
    monkeypatch,
) -> None:
    async def exercise() -> None:
        root = _configure_monitor(tmp_path, monkeypatch)
        repository, published, receipt = await _published_history()
        monkeypatch.setattr(
            server_query_backup_monitor.server_query_backup_history,
            "history_repository",
            lambda: repository,
        )
        with pytest.raises(
            server_query_backup_monitor.BackupCadenceMonitorError,
            match="no reference",
        ):
            await server_query_backup_monitor.run(
                evaluated_at_ms=COMPLETED_AT_MS + 150_000,
            )

        wrong = _publication(published)
        wrong["history_sha256"] = "e" * 64
        server_query_backup_monitor.persist_success_reference(
            receipt.to_wire(),
            wrong,
        )
        with pytest.raises(
            server_query_backup_monitor.BackupCadenceMonitorError,
            match="differs",
        ):
            await server_query_backup_monitor.run(
                evaluated_at_ms=COMPLETED_AT_MS + 150_000,
            )

        for path in root.iterdir():
            path.unlink()
        server_query_backup_monitor.persist_success_reference(
            receipt.to_wire(),
            _publication(published),
        )
        monkeypatch.setenv(
            "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_MAXIMUM_GAP_MS",
            "149999",
        )
        with pytest.raises(RuntimeError, match="maximum gap"):
            await server_query_backup_monitor.run(
                evaluated_at_ms=COMPLETED_AT_MS + 150_000,
            )

    asyncio.run(exercise())


def test_alert_webhook_is_hmac_signed_bounded_and_does_not_redirect(
    monkeypatch,
) -> None:
    received: list[tuple[str, bytes, dict[str, str]]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers["Content-Length"])
            body = self.rfile.read(length)
            received.append((self.path, body, dict(self.headers.items())))
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/alerts")
                self.end_headers()
                return
            response = b"x" * (4_097 if self.path == "/large" else 2)
            self.send_response(202)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, _format: str, *_arguments) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _configure_alert(monkeypatch, server.server_port, "/alerts")
        alert = server_query_backup_monitor.build_alert(
            evaluated_at_ms=COMPLETED_AT_MS,
            code="BACKUP_CADENCE_MONITOR_FAILED",
        )
        delivery = server_query_backup_monitor.deliver_alert(alert)
        assert delivery["status"] == "delivered"
        assert delivery["http_status"] == 202
        assert delivery["response_bytes"] == 2
        assert delivery["retry_attempted"] is False
        assert delivery["redirect_followed"] is False
        path, body, headers = received[0]
        headers = {name.lower(): value for name, value in headers.items()}
        assert path == "/alerts"
        assert body == rfc8785.dumps(alert)
        assert headers["x-candlescope-key-id"] == "phase1p-alert-key"
        assert (
            headers["x-candlescope-signature"]
            == hmac.digest(
                ALERT_SECRET,
                body,
                "sha256",
            ).hex()
        )

        with pytest.raises(
            server_query_backup_monitor.BackupCadenceMonitorError,
            match="fields are invalid",
        ):
            server_query_backup_monitor.deliver_alert(
                {**alert, "operator_action_required": False}
            )

        _configure_alert(monkeypatch, server.server_port, "/redirect")
        with pytest.raises(
            server_query_backup_monitor.BackupCadenceMonitorError,
            match="delivery failed",
        ):
            server_query_backup_monitor.deliver_alert(alert)
        assert [item[0] for item in received].count("/alerts") == 1

        _configure_alert(monkeypatch, server.server_port, "/large")
        with pytest.raises(
            server_query_backup_monitor.BackupCadenceMonitorError,
            match="byte bound",
        ):
            server_query_backup_monitor.deliver_alert(alert)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_monitor_cli_failure_is_sanitized_and_reports_alert_delivery(
    monkeypatch,
    capsys,
) -> None:
    async def fail(*, evaluated_at_ms):
        raise RuntimeError(f"secret diagnostic at {evaluated_at_ms}")

    delivered: list[dict[str, object]] = []

    def deliver(alert):
        delivered.append(alert)
        return {
            "schema_version": ("candlescope.query-backup-cadence-alert-delivery.v1"),
            "status": "delivered",
        }

    monkeypatch.setattr(server_query_backup_monitor, "run", fail)
    monkeypatch.setattr(server_query_backup_monitor, "deliver_alert", deliver)
    monkeypatch.setattr(sys, "argv", ["server_query_backup_monitor.py"])
    with pytest.raises(SystemExit) as captured:
        server_query_backup_monitor.main()
    assert captured.value.code == 1
    output_text = capsys.readouterr().err
    assert "secret diagnostic" not in output_text
    output = json.loads(output_text)
    assert output["status"] == "unhealthy-alert-delivered"
    assert output["code"] == "BACKUP_CADENCE_MONITOR_FAILED"
    assert delivered[0]["operator_action_required"] is True


def test_phase1p_systemd_templates_are_hardened_and_not_enabled() -> None:
    systemd = ROOT / "deploy/server/systemd"
    backup = (systemd / "candlescope-query-backup.service").read_text()
    monitor = (systemd / "candlescope-query-backup-monitor.service").read_text()
    timer = (systemd / "candlescope-query-backup-monitor.timer").read_text()
    environment = (systemd / "query-backup-monitor.env.example").read_text()

    assert "StateDirectory=candlescope-query-backup-history" in backup
    assert "ProtectSystem=strict" in monitor
    assert "NoNewPrivileges=yes" in monitor
    assert "MemoryDenyWriteExecute=yes" in monitor
    assert "EnvironmentFile=/etc/candlescope/query-backup-monitor.env" in monitor
    assert "OnCalendar=hourly" in timer
    assert "Persistent=yes" in timer
    assert "RandomizedDelaySec=5min" in timer
    assert "WantedBy=timers.target" in timer
    assert "ALLOW_INSECURE_LOOPBACK_FOR_TESTS" not in environment
    assert "ALERT_WEBHOOK_URL=https://replace-me" in environment
    assert "ALERT_HMAC_SECRET_BASE64=replace-me" in environment


async def _published_history():
    store = InMemoryImmutableObjectStore()
    await store.ensure_bucket()
    repository = ImmutableBackupRunHistoryRepository(
        object_store=store,
        signer=BackupRunHistorySigner(
            key_id="phase1p-history-key",
            secret=HISTORY_SECRET,
        ),
    )
    receipt = _receipt()
    published = await repository.publish(receipt, cluster_id=CLUSTER_ID)
    return repository, published, receipt


def _publication(published) -> dict[str, object]:
    receipt = published.history.receipt
    return {
        "schema_version": published.history.schema_version,
        "cluster_id": published.history.cluster_id,
        "backup_id": receipt.backup_id,
        "completed_at_ms": receipt.completed_at_ms,
        "history_uri": published.uri,
        "history_sha256": published.content_sha256,
        "created": published.created,
    }


def _reference() -> BackupSuccessReference:
    return BackupSuccessReference(
        cluster_id=CLUSTER_ID,
        backup_id=BACKUP_ID,
        completed_at_ms=COMPLETED_AT_MS,
        history_uri=(
            "s3://candlescope-test/query-operations/backup-runs/v1/"
            f"{CLUSTER_ID}/{COMPLETED_AT_MS:020d}-{BACKUP_ID}.json"
        ),
        history_sha256="d" * 64,
    )


def _receipt() -> BackupJobReceipt:
    return BackupJobReceipt(
        run_id=BACKUP_ID,
        backup_id=BACKUP_ID,
        started_at_ms=COMPLETED_AT_MS - 60_000,
        completed_at_ms=COMPLETED_AT_MS,
        duration_ms=60_000,
        manifest_uri=(
            f"s3://candlescope-test/backups/v3/{CLUSTER_ID}/{BACKUP_ID}/manifest.json"
        ),
        manifest_sha256="a" * 64,
        audit_anchor_uri=(f"s3://candlescope-test/anchors/v1/{BACKUP_ID}/anchor.json"),
        audit_anchor_sha256="b" * 64,
        recovery_target_time="2026-08-23 12:00:00.000000+00",
        recovery_target_lsn="0/3000200",
        recovery_target_wal_filename="000000010000000000000030",
        wal_coverage_segment_count=1,
        write_fence_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        operator_id="scheduled-backup",
    )


def _configure_monitor(tmp_path, monkeypatch) -> Path:
    root = _private_directory(tmp_path / "inventory")
    settings = {
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_INVENTORY_ROOT": str(root),
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_CLUSTER_ID": CLUSTER_ID,
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_LOOKBACK_MS": "200000",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_MAXIMUM_GAP_MS": "150000",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_MAXIMUM_HISTORIES": "64",
    }
    for name, value in settings.items():
        monkeypatch.setenv(name, value)
    return root


def _configure_alert(monkeypatch, port: int, path: str) -> None:
    settings = {
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_CLUSTER_ID": CLUSTER_ID,
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_ALERT_WEBHOOK_URL": (
            f"http://127.0.0.1:{port}{path}"
        ),
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_ALERT_HMAC_KEY_ID": (
            "phase1p-alert-key"
        ),
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_ALERT_HMAC_SECRET_BASE64": (
            base64.b64encode(ALERT_SECRET).decode()
        ),
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_ALERT_TIMEOUT_MS": "5000",
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_MAXIMUM_ALERT_RESPONSE_BYTES": (
            "4096"
        ),
        "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_ALLOW_INSECURE_LOOPBACK_FOR_TESTS": (
            "1"
        ),
    }
    for name, value in settings.items():
        monkeypatch.setenv(name, value)


def _private_directory(path: Path) -> Path:
    path.mkdir()
    path.chmod(0o700)
    return path.resolve()
