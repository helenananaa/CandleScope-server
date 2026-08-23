"""Monitor host-observed signed backup cadence and deliver bounded alerts."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import rfc8785

from app.server_runtime.query_backup_history import (
    BACKUP_RUN_HISTORY_SCHEMA_VERSION,
    DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
    DEFAULT_MAXIMUM_GAP_MS,
    DEFAULT_MAXIMUM_HISTORIES,
    verify_success_cadence,
)
from app.server_runtime.query_backup_monitor import (
    DEFAULT_MAXIMUM_INVENTORY_FILES,
    DEFAULT_MAXIMUM_REFERENCE_BYTES,
    BackupSuccessReference,
    PrivateBackupSuccessInventory,
)
from app.server_runtime.query_backup_selection import BackupJobReceipt
from scripts import server_query_backup_history

ENV_PREFIX = "CANDLESCOPE_SERVER_QUERY_BACKUP_MONITOR_"
MONITOR_SCHEMA_VERSION = "candlescope.query-backup-cadence-monitor.v1"
ALERT_SCHEMA_VERSION = "candlescope.query-backup-cadence-alert.v1"
DELIVERY_SCHEMA_VERSION = "candlescope.query-backup-cadence-alert-delivery.v1"
FAILURE_SCHEMA_VERSION = "candlescope.query-backup-cadence-monitor-failure.v1"
DEFAULT_LOOKBACK_MS = 72 * 60 * 60 * 1_000
DEFAULT_ALERT_TIMEOUT_MS = 10_000
DEFAULT_MAXIMUM_ALERT_RESPONSE_BYTES = 4 * 1_024
_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class BackupCadenceMonitorError(RuntimeError):
    """The monitor or its bounded alert delivery cannot prove success."""


def persist_success_reference(
    receipt_wire: dict[str, object],
    history_publication: dict[str, object],
) -> dict[str, object]:
    receipt = BackupJobReceipt.from_wire(receipt_wire)
    expected_fields = {
        "schema_version",
        "cluster_id",
        "backup_id",
        "completed_at_ms",
        "history_uri",
        "history_sha256",
        "created",
    }
    if not isinstance(history_publication, dict) or set(history_publication) != (
        expected_fields
    ):
        raise BackupCadenceMonitorError("backup history publication shape is invalid")
    configured_cluster = _required_env("CLUSTER_ID")
    if (
        history_publication["schema_version"] != BACKUP_RUN_HISTORY_SCHEMA_VERSION
        or history_publication["cluster_id"] != configured_cluster
        or history_publication["backup_id"] != receipt.backup_id
        or history_publication["completed_at_ms"] != receipt.completed_at_ms
        or not isinstance(history_publication["created"], bool)
    ):
        raise BackupCadenceMonitorError(
            "backup history publication differs from its job receipt"
        )
    reference = BackupSuccessReference(
        cluster_id=configured_cluster,
        backup_id=receipt.backup_id,
        completed_at_ms=receipt.completed_at_ms,
        history_uri=history_publication["history_uri"],
        history_sha256=history_publication["history_sha256"],
    )
    persisted = inventory().persist(reference)
    return {
        "schema_version": reference.schema_version,
        "backup_id": reference.backup_id,
        "completed_at_ms": reference.completed_at_ms,
        "created": persisted.created,
    }


async def run(*, evaluated_at_ms: int | None = None) -> dict[str, object]:
    evaluated_at_ms = _positive_int(
        evaluated_at_ms if evaluated_at_ms is not None else time.time_ns() // 1_000_000,
        field="evaluated_at_ms",
    )
    lookback_ms = _positive_env("LOOKBACK_MS", DEFAULT_LOOKBACK_MS)
    if evaluated_at_ms <= lookback_ms:
        raise BackupCadenceMonitorError(
            "monitor evaluation time is outside its supported range"
        )
    window_start_ms = evaluated_at_ms - lookback_ms
    snapshot = inventory().snapshot(
        window_start_ms=window_start_ms,
        window_end_ms=evaluated_at_ms,
    )
    if not snapshot.references:
        raise BackupCadenceMonitorError(
            "host success inventory has no reference in the monitor window"
        )
    maximum_histories = _positive_env(
        "MAXIMUM_HISTORIES",
        DEFAULT_MAXIMUM_HISTORIES,
    )
    if len(snapshot.references) > maximum_histories:
        raise BackupCadenceMonitorError(
            "monitor window contains too many success references"
        )
    expected_cluster_id = _required_env("CLUSTER_ID")
    repository = server_query_backup_history.history_repository()
    histories = []
    for reference in snapshot.references:
        if reference.cluster_id != expected_cluster_id:
            raise BackupCadenceMonitorError(
                "host success reference belongs to another cluster"
            )
        published = await repository.verify(reference.history_uri)
        receipt = published.history.receipt
        if (
            published.content_sha256 != reference.history_sha256
            or published.history.cluster_id != reference.cluster_id
            or receipt.backup_id != reference.backup_id
            or receipt.completed_at_ms != reference.completed_at_ms
        ):
            raise BackupCadenceMonitorError(
                "signed backup history differs from its host success reference"
            )
        histories.append(published)
    cadence = verify_success_cadence(
        tuple(histories),
        expected_cluster_id=expected_cluster_id,
        window_start_ms=window_start_ms,
        window_end_ms=evaluated_at_ms,
        evaluated_at_ms=evaluated_at_ms,
        maximum_gap_ms=_positive_env("MAXIMUM_GAP_MS", DEFAULT_MAXIMUM_GAP_MS),
        maximum_future_skew_ms=_non_negative_env(
            "MAXIMUM_FUTURE_SKEW_MS",
            DEFAULT_MAXIMUM_FUTURE_SKEW_MS,
        ),
        maximum_histories=maximum_histories,
    )
    return {
        "schema_version": MONITOR_SCHEMA_VERSION,
        "status": "healthy",
        "source": "host-private-success-inventory",
        "evaluated_at_ms": evaluated_at_ms,
        "lookback_ms": lookback_ms,
        "inventory_total_file_count": snapshot.total_file_count,
        "window_reference_count": len(snapshot.references),
        "host_inventory_completeness_proven": False,
        "global_history_completeness_proven": False,
        "cadence": cadence.to_wire(),
    }


def build_alert(*, evaluated_at_ms: int, code: str) -> dict[str, object]:
    return {
        "schema_version": ALERT_SCHEMA_VERSION,
        "status": "backup-cadence-unhealthy",
        "code": _token(code, field="code"),
        "cluster_id": _safe_cluster_id(),
        "observed_at_ms": _positive_int(evaluated_at_ms, field="evaluated_at_ms"),
        "operator_action_required": True,
        "host_inventory_completeness_proven": False,
        "global_history_completeness_proven": False,
    }


def deliver_alert(
    alert: dict[str, object],
    *,
    opener: Any | None = None,
) -> dict[str, object]:
    if not isinstance(alert, dict) or set(alert) != {
        "schema_version",
        "status",
        "code",
        "cluster_id",
        "observed_at_ms",
        "operator_action_required",
        "host_inventory_completeness_proven",
        "global_history_completeness_proven",
    }:
        raise BackupCadenceMonitorError("alert shape is invalid")
    if (
        alert["schema_version"] != ALERT_SCHEMA_VERSION
        or alert["status"] != "backup-cadence-unhealthy"
        or alert["operator_action_required"] is not True
        or alert["host_inventory_completeness_proven"] is not False
        or alert["global_history_completeness_proven"] is not False
    ):
        raise BackupCadenceMonitorError("alert fields are invalid")
    try:
        _token(alert["code"], field="alert code")
        _token(alert["cluster_id"], field="alert cluster_id")
        _positive_int(alert["observed_at_ms"], field="alert observed_at_ms")
    except (TypeError, ValueError) as exc:
        raise BackupCadenceMonitorError("alert fields are invalid") from exc
    body = rfc8785.dumps(alert)
    endpoint = _alert_endpoint()
    key_id = _token(_required_env("ALERT_HMAC_KEY_ID"), field="alert key_id")
    signature = hmac.digest(_alert_secret(), body, "sha256").hex()
    request = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "X-CandleScope-Schema": ALERT_SCHEMA_VERSION,
            "X-CandleScope-Key-Id": key_id,
            "X-CandleScope-Signature": signature,
        },
    )
    client = opener if opener is not None else _webhook_opener()
    timeout_seconds = (
        _positive_env("ALERT_TIMEOUT_MS", DEFAULT_ALERT_TIMEOUT_MS) / 1_000
    )
    maximum_response_bytes = _positive_env(
        "MAXIMUM_ALERT_RESPONSE_BYTES",
        DEFAULT_MAXIMUM_ALERT_RESPONSE_BYTES,
    )
    try:
        with client.open(request, timeout=timeout_seconds) as response:
            status = response.getcode()
            if not isinstance(status, int) or not 200 <= status < 300:
                raise BackupCadenceMonitorError(
                    "alert webhook returned an unsuccessful status"
                )
            content_length = response.headers.get("Content-Length")
            if content_length is not None:
                try:
                    declared_length = int(content_length)
                except ValueError as exc:
                    raise BackupCadenceMonitorError(
                        "alert webhook response length is invalid"
                    ) from exc
                if declared_length < 0 or declared_length > maximum_response_bytes:
                    raise BackupCadenceMonitorError(
                        "alert webhook response exceeds its byte bound"
                    )
            response_body = response.read(maximum_response_bytes + 1)
            if len(response_body) > maximum_response_bytes:
                raise BackupCadenceMonitorError(
                    "alert webhook response exceeds its byte bound"
                )
    except BackupCadenceMonitorError:
        raise
    except (OSError, urllib.error.URLError) as exc:
        raise BackupCadenceMonitorError("alert webhook delivery failed") from exc
    return {
        "schema_version": DELIVERY_SCHEMA_VERSION,
        "status": "delivered",
        "alert_schema_version": ALERT_SCHEMA_VERSION,
        "alert_sha256": hashlib.sha256(body).hexdigest(),
        "algorithm": "hmac-sha256",
        "key_id": key_id,
        "http_status": status,
        "response_bytes": len(response_body),
        "retry_attempted": False,
        "redirect_followed": False,
    }


def inventory() -> PrivateBackupSuccessInventory:
    return PrivateBackupSuccessInventory(
        Path(_required_env("INVENTORY_ROOT")),
        maximum_files=_positive_env(
            "MAXIMUM_INVENTORY_FILES",
            DEFAULT_MAXIMUM_INVENTORY_FILES,
        ),
        maximum_reference_bytes=_positive_env(
            "MAXIMUM_REFERENCE_BYTES",
            DEFAULT_MAXIMUM_REFERENCE_BYTES,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Verify host-observed signed backup cadence and deliver an alert on "
            "failure."
        )
    )
    parser.parse_args()
    evaluated_at_ms = time.time_ns() // 1_000_000
    try:
        result = asyncio.run(run(evaluated_at_ms=evaluated_at_ms))
    except (OSError, RuntimeError, TypeError, ValueError) as monitor_exc:
        alert = build_alert(
            evaluated_at_ms=evaluated_at_ms,
            code="BACKUP_CADENCE_MONITOR_FAILED",
        )
        try:
            delivery = deliver_alert(alert)
        except (OSError, RuntimeError, TypeError, ValueError) as delivery_exc:
            print(
                _canonical_json(
                    {
                        "schema_version": FAILURE_SCHEMA_VERSION,
                        "status": "failed",
                        "code": "BACKUP_CADENCE_ALERT_DELIVERY_FAILED",
                        "observed_at_ms": evaluated_at_ms,
                    }
                ),
                file=sys.stderr,
            )
            raise SystemExit(1) from delivery_exc
        print(
            _canonical_json(
                {
                    "schema_version": FAILURE_SCHEMA_VERSION,
                    "status": "unhealthy-alert-delivered",
                    "code": "BACKUP_CADENCE_MONITOR_FAILED",
                    "observed_at_ms": evaluated_at_ms,
                    "delivery": delivery,
                }
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from monitor_exc
    print(_canonical_json(result))


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, url):
        return None


def _webhook_opener():
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirectHandler(),
    )


def _alert_endpoint() -> str:
    value = _required_env("ALERT_WEBHOOK_URL")
    if (
        len(value) > 2_048
        or not value.isascii()
        or any(character.isspace() for character in value)
    ):
        raise BackupCadenceMonitorError("alert webhook URL is not a bounded ASCII URL")
    parsed = urlsplit(value)
    try:
        _ = parsed.port
    except ValueError as exc:
        raise BackupCadenceMonitorError("alert webhook port is invalid") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise BackupCadenceMonitorError(
            "alert webhook must be an absolute URL without credentials, query, "
            "or fragment"
        )
    if parsed.scheme == "http" and not (
        os.environ.get(f"{ENV_PREFIX}ALLOW_INSECURE_LOOPBACK_FOR_TESTS") == "1"
        and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    ):
        raise BackupCadenceMonitorError("alert webhook must use HTTPS")
    return value


def _alert_secret() -> bytes:
    try:
        secret = base64.b64decode(
            _required_env("ALERT_HMAC_SECRET_BASE64"),
            validate=True,
        )
    except (binascii.Error, ValueError) as exc:
        raise BackupCadenceMonitorError(
            f"setting {ENV_PREFIX}ALERT_HMAC_SECRET_BASE64 is not strict base64"
        ) from exc
    if len(secret) < 32:
        raise BackupCadenceMonitorError(
            f"setting {ENV_PREFIX}ALERT_HMAC_SECRET_BASE64 is shorter than 32 bytes"
        )
    return secret


def _safe_cluster_id() -> str:
    value = os.environ.get(f"{ENV_PREFIX}CLUSTER_ID", "")
    try:
        return _token(value.strip(), field="cluster_id")
    except (TypeError, ValueError):
        return "unconfigured"


def _required_env(suffix: str) -> str:
    name = f"{ENV_PREFIX}{suffix}"
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise BackupCadenceMonitorError(f"required setting {name} is missing")
    return value.strip()


def _positive_env(suffix: str, default: int) -> int:
    value = _integer_env(suffix, default)
    if value <= 0:
        raise BackupCadenceMonitorError(
            f"setting {ENV_PREFIX}{suffix} must be positive"
        )
    return value


def _non_negative_env(suffix: str, default: int) -> int:
    value = _integer_env(suffix, default)
    if value < 0:
        raise BackupCadenceMonitorError(
            f"setting {ENV_PREFIX}{suffix} must be non-negative"
        )
    return value


def _integer_env(suffix: str, default: int) -> int:
    raw = os.environ.get(f"{ENV_PREFIX}{suffix}", str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise BackupCadenceMonitorError(
            f"setting {ENV_PREFIX}{suffix} must be an integer"
        ) from exc


def _positive_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _token(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ValueError(f"{field} must be a bounded ASCII token")
    return value


def _canonical_json(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


if __name__ == "__main__":
    main()
