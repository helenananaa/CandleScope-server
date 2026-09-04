from __future__ import annotations

import json

import pytest
from app.deployment import (
    FASTAPI_UNLOCK_BLOCKERS,
    DeploymentProfile,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)
from app.server_runtime.composition import (
    ServerCompositionError,
    fastapi_unlock_refusal,
    load_server_data_plane_composition,
)
from scripts import server_composition_check

TOKEN_A = "a" * 32
TOKEN_B = "b" * 32


def _complete_env(**overrides: str) -> dict[str, str]:
    values = {
        "CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN": "postgresql://collector@localhost:15432/candlescope",
        "CANDLESCOPE_SERVER_COLLECTOR_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_COLLECTOR_OWNER_ID": "collector-a",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_OWNER_ID": "writer-a",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_URL": "http://localhost:18123",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_USER": "candlescope",
        "CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_PASSWORD": "writer-secret",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_OWNER_ID": "archiver-a",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_DATA_EPOCH": "epoch-1",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ENDPOINT_URL": "http://localhost:19000",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ACCESS_KEY_ID": "access",
        "CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_SECRET_ACCESS_KEY": "secret",
        "CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_URL": "http://localhost:18123",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_USER": "query",
        "CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_PASSWORD": "query-secret",
        "CANDLESCOPE_SERVER_QUERY_S3_ENDPOINT_URL": "http://localhost:19000",
        "CANDLESCOPE_SERVER_QUERY_S3_BUCKET": "candlescope-archive",
        "CANDLESCOPE_SERVER_QUERY_S3_ACCESS_KEY_ID": "access",
        "CANDLESCOPE_SERVER_QUERY_S3_SECRET_ACCESS_KEY": "secret",
        "CANDLESCOPE_SERVER_QUERY_AUTH_BEARER_TOKEN": TOKEN_A,
        "CANDLESCOPE_SERVER_QUERY_CONTROL_BEARER_TOKEN": TOKEN_B,
        "CANDLESCOPE_SERVER_QUERY_POSTGRES_DSN": "postgresql://query@localhost:15432/candlescope",
        "CANDLESCOPE_SERVER_QUERY_INSTANCE_ID": "query-a",
    }
    values.update(overrides)
    return values


def test_complete_env_is_configured_but_does_not_unlock_fastapi() -> None:
    composition = load_server_data_plane_composition(_complete_env())
    wire = composition.to_public_wire()
    assert wire["status"] == "configured"
    assert wire["fastapi_runtime_supported"] is True
    assert wire["fastapi_unlock_blockers"] == list(FASTAPI_UNLOCK_BLOCKERS)
    assert wire["production_ready"] is False
    assert "writer-secret" not in json.dumps(wire)
    assert "query-secret" not in json.dumps(wire)
    assert "secret" not in json.dumps(wire["s3_endpoint_url"])
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": "server"})
    assert settings.runtime_supported is True
    settings.require_runtime_support()


def test_missing_role_and_cross_role_drift_fail_closed() -> None:
    missing = _complete_env()
    missing.pop("CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN")
    with pytest.raises(ServerCompositionError) as missing_exc:
        load_server_data_plane_composition(missing)
    assert missing_exc.value.code == "ROLE_CONFIG_INCOMPLETE"
    assert missing_exc.value.details["roles"][0]["role"] == "collector"

    drifted = _complete_env(
        CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS="localhost:19093"
    )
    with pytest.raises(ServerCompositionError) as kafka_exc:
        load_server_data_plane_composition(drifted)
    assert kafka_exc.value.code == "KAFKA_BOOTSTRAP_DRIFT"

    store = _complete_env(CANDLESCOPE_SERVER_QUERY_S3_BUCKET="other-bucket")
    with pytest.raises(ServerCompositionError) as store_exc:
        load_server_data_plane_composition(store)
    assert store_exc.value.code == "OBJECT_STORE_DRIFT"


def test_optional_health_binds_must_be_loopback() -> None:
    good = load_server_data_plane_composition(
        _complete_env(
            CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND="127.0.0.1:18121",
        )
    )
    assert good.health_binds["collector"] == "127.0.0.1:18121"
    with pytest.raises(ServerCompositionError) as bind_exc:
        load_server_data_plane_composition(
            _complete_env(
                CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND="0.0.0.0:18121",
            )
        )
    assert bind_exc.value.code == "HEALTH_BIND_INVALID"


def test_cli_config_and_fastapi_unlock(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for key, value in _complete_env().items():
        monkeypatch.setenv(key, value)
    assert server_composition_check.main(["config"]) == 0
    configured = json.loads(capsys.readouterr().out)
    assert configured["status"] == "configured"
    assert configured["fastapi_runtime_supported"] is True
    assert server_composition_check.main(["fastapi-unlock"]) == 0
    unlocked = json.loads(capsys.readouterr().out)
    assert unlocked["fastapi_runtime_supported"] is True
    assert unlocked["production_ready"] is False
    assert fastapi_unlock_refusal()["fastapi_runtime_supported"] is False
    assert (
        load_deployment_settings({"CANDLESCOPE_PROFILE": "server"}).profile
        is DeploymentProfile.SERVER
    )
