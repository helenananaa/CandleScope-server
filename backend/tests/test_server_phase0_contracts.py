from __future__ import annotations

import asyncio
import inspect
import json
import math
from pathlib import Path

import pytest
from app.data_engine.market_data import DeliveryClass, MarketChannel, MarketStreamKey
from app.deployment import (
    DeploymentProfile,
    ServerRuntimeUnavailableError,
    load_deployment_settings,
)
from app.server_contracts import (
    MARKET_EVENT_ENVELOPE_SCHEMA_VERSION,
    PAYLOAD_CANONICALIZATION,
    ArchiveCommit,
    MarketDataSnapshotRef,
    MarketEventArchive,
    MarketEventCursor,
    MarketEventEnvelopeV1,
    MarketEventPage,
    MarketEventQuery,
    MarketEventRange,
    canonical_payload_bytes,
    canonical_payload_sha256,
)
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

BACKEND_ROOT = Path(__file__).parents[1]
REPOSITORY_ROOT = BACKEND_ROOT.parent
SCHEMA_PATH = (
    REPOSITORY_ROOT
    / "docs"
    / "server"
    / "contracts"
    / "market-event-envelope-v1.schema.json"
)
CAPACITY_PATH = (
    REPOSITORY_ROOT
    / "docs"
    / "server"
    / "contracts"
    / "phase0-capacity-envelope-v1.json"
)
RFC8785_VECTORS_PATH = (
    REPOSITORY_ROOT
    / "docs"
    / "server"
    / "contracts"
    / "rfc8785-payload-golden-vectors-v1.json"
)


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        value[key] = item
    return value


def _load_strict_json(path: Path) -> dict[str, object]:
    value = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_strict_json_object,
    )
    assert isinstance(value, dict)
    return value


def _agg_trade_event(**overrides: object) -> MarketEventEnvelopeV1:
    values: dict[str, object] = {
        "stream": MarketStreamKey.build(
            "binance",
            "futures",
            "BTCUSDT",
            MarketChannel.AGG_TRADE,
        ),
        "delivery_class": DeliveryClass.APPEND,
        "source": "websocket",
        "producer_id": "collector-binance-futures-01",
        "producer_epoch": 7,
        "event_time_ms": 1_754_000_000_000,
        "received_at_ms": 1_754_000_000_012,
        "published_at_ms": 1_754_000_000_020,
        "payload_schema": "binance.agg-trade.normalized.v1",
        "payload": {
            "agg_trade_id": 991,
            "price": "100000.10",
            "quantity": "0.025",
            "buyer_is_maker": False,
        },
        "source_event_id": "991",
        "sequence_start": 991,
        "sequence_end": 991,
    }
    values.update(overrides)
    return MarketEventEnvelopeV1.build(**values)  # type: ignore[arg-type]


def test_deployment_profile_defaults_to_personal_without_behavior_change() -> None:
    settings = load_deployment_settings({})

    assert settings.profile is DeploymentProfile.PERSONAL
    assert settings.runtime_supported is True
    assert settings.bindings.control_store == "sqlite"
    assert settings.bindings.market_event_log == "in_process"
    settings.require_runtime_support()


def test_server_profile_freezes_roles_but_fails_closed_in_phase_zero() -> None:
    settings = load_deployment_settings({"CANDLESCOPE_PROFILE": " SERVER "})

    assert settings.profile is DeploymentProfile.SERVER
    assert settings.runtime_supported is False
    assert settings.bindings.control_store == "postgresql"
    assert settings.bindings.market_event_log == "kafka_compatible"
    assert settings.bindings.analytical_store == "clickhouse"
    assert settings.bindings.immutable_archive == "object_storage_parquet"
    with pytest.raises(ServerRuntimeUnavailableError, match="contract-only"):
        settings.require_runtime_support()


def test_application_startup_rejects_server_before_local_storage(monkeypatch) -> None:
    from app import main as main_module

    storage_calls: list[str] = []
    monkeypatch.setenv("CANDLESCOPE_PROFILE", "server")
    monkeypatch.setattr(
        main_module,
        "init_klines_storage",
        lambda: storage_calls.append("sqlite"),
    )

    with pytest.raises(ServerRuntimeUnavailableError, match="contract-only"):
        asyncio.run(main_module.startup_event())

    assert storage_calls == []


@pytest.mark.parametrize("value", ["", "cluster", "sqlite", "personal,server"])
def test_deployment_profile_rejects_unknown_or_ambiguous_values(value: str) -> None:
    with pytest.raises(ValueError, match="CANDLESCOPE_PROFILE must be one of"):
        load_deployment_settings({"CANDLESCOPE_PROFILE": value})


def test_payload_hash_is_canonical_across_mapping_order() -> None:
    left = {"price": "1.25", "nested": {"b": 2, "a": 1}}
    right = {"nested": {"a": 1, "b": 2}, "price": "1.25"}

    assert canonical_payload_sha256(left) == canonical_payload_sha256(right)


def test_payload_canonicalization_uses_ecmascript_number_encoding() -> None:
    payload = {"negative_zero": -0.0, "x": 1.0}

    assert canonical_payload_bytes(payload) == b'{"negative_zero":0,"x":1}'
    assert canonical_payload_sha256(payload) == (
        "5317cf5d6cb5034704fcb8756bf5451e8e5c3ae00d65f16603c9c868b1b4a842"
    )


def test_payload_canonicalization_matches_shared_rfc8785_golden_vectors() -> None:
    contract = _load_strict_json(RFC8785_VECTORS_PATH)

    assert contract["canonicalization"] == PAYLOAD_CANONICALIZATION
    vectors = contract["vectors"]
    assert isinstance(vectors, list)
    assert len(vectors) >= 3
    for vector in vectors:
        assert isinstance(vector, dict)
        payload = vector["input"]
        assert isinstance(payload, dict)
        canonical = canonical_payload_bytes(payload)
        assert canonical.decode("utf-8") == vector["canonical_utf8"], vector["name"]
        assert canonical_payload_sha256(payload) == vector["sha256"], vector["name"]


def test_event_envelope_canonicalizes_stream_and_copies_payload() -> None:
    payload = {"levels": [["100.0", "2.0"]], "last_update_id": 50}
    event = MarketEventEnvelopeV1.build(
        stream=MarketStreamKey.build(
            " BINANCE ",
            " SPOT ",
            " btcusdt ",
            MarketChannel.FULL_DEPTH,
            speed="100ms",
        ),
        delivery_class=DeliveryClass.ORDERED_DELTA,
        source=" WEBSOCKET ",
        producer_id="collector-1",
        producer_epoch=2,
        event_time_ms=1000,
        received_at_ms=1010,
        published_at_ms=1020,
        payload_schema="binance.full-depth.v1",
        payload=payload,
        sequence_start=50,
        sequence_end=51,
        previous_sequence=49,
    )
    payload["last_update_id"] = 999

    wire = event.to_wire()
    assert event.partition_key == "binance:spot:BTCUSDT@full_depth?speed=100ms"
    assert event.source == "websocket"
    assert wire["payload"]["last_update_id"] == 50
    assert wire["partition_key"] == event.partition_key
    assert wire["payload_canonicalization"] == PAYLOAD_CANONICALIZATION
    assert wire["payload_sha256"] == canonical_payload_sha256(wire["payload"])


def test_event_envelope_wire_round_trip_is_lossless() -> None:
    event = _agg_trade_event(event_id="56E6C33F-7498-4C3F-8B27-2EE89C3DFDAE")
    wire = json.loads(json.dumps(event.to_wire()))

    restored = MarketEventEnvelopeV1.from_wire(wire)

    assert restored.to_wire() == event.to_wire()
    assert restored.event_id == "56e6c33f-7498-4c3f-8b27-2ee89c3dfdae"
    assert restored.payload_sha256 == event.payload_sha256


def test_event_envelope_rejects_payload_tampering() -> None:
    wire = _agg_trade_event().to_wire()
    wire["payload"]["price"] = "1.00"

    with pytest.raises(ValueError, match="payload_sha256 does not match"):
        MarketEventEnvelopeV1.from_wire(wire)


def test_event_envelope_rejects_unknown_payload_canonicalization() -> None:
    wire = _agg_trade_event().to_wire()
    wire["payload_canonicalization"] = "python-json-sort-keys"

    with pytest.raises(ValueError, match="payload_canonicalization"):
        MarketEventEnvelopeV1.from_wire(wire)


def test_event_envelope_rejects_partition_key_tampering() -> None:
    wire = _agg_trade_event().to_wire()
    wire["partition_key"] = "binance:futures:ETHUSDT@agg_trade"

    with pytest.raises(ValueError, match="partition_key does not match"):
        MarketEventEnvelopeV1.from_wire(wire)


@pytest.mark.parametrize("mutation", ["missing", "unknown"])
def test_event_envelope_rejects_wire_shape_drift(mutation: str) -> None:
    wire = _agg_trade_event().to_wire()
    if mutation == "missing":
        del wire["producer_epoch"]
        message = "missing fields"
    else:
        wire["broker_offset"] = 42
        message = "unknown fields"

    with pytest.raises(ValueError, match=message):
        MarketEventEnvelopeV1.from_wire(wire)


@pytest.mark.parametrize("mutation", ["missing", "unknown"])
def test_event_envelope_rejects_nested_stream_shape_drift(mutation: str) -> None:
    wire = _agg_trade_event().to_wire()
    if mutation == "missing":
        del wire["stream"]["params"]
        message = "stream is missing fields"
    else:
        wire["stream"]["broker_partition"] = "3"
        message = "stream has unknown fields"

    with pytest.raises(ValueError, match=message):
        MarketEventEnvelopeV1.from_wire(wire)


def test_ordered_delta_requires_a_complete_sequence_range() -> None:
    with pytest.raises(ValueError, match="require a sequence range"):
        _agg_trade_event(
            delivery_class=DeliveryClass.ORDERED_DELTA,
            source_event_id="book-1",
            sequence_start=None,
            sequence_end=None,
        )


def test_append_event_requires_a_durable_source_identity() -> None:
    with pytest.raises(ValueError, match="durable source identity"):
        _agg_trade_event(
            source_event_id=None,
            sequence_start=None,
            sequence_end=None,
        )


def test_event_envelope_rejects_reversed_sequence_ranges() -> None:
    with pytest.raises(ValueError, match="greater than or equal"):
        _agg_trade_event(sequence_start=992, sequence_end=991)


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_event_envelope_rejects_non_finite_payload_values(value: float) -> None:
    with pytest.raises(ValueError, match="RFC 8785/I-JSON"):
        _agg_trade_event(payload={"price": value}, source_event_id="bad-json")


def test_event_envelope_rejects_non_interoperable_integer_values() -> None:
    with pytest.raises(ValueError, match="RFC 8785/I-JSON"):
        _agg_trade_event(payload={"event_id": 2**53}, source_event_id="unsafe-int")


def test_event_envelope_rejects_non_string_json_object_keys() -> None:
    with pytest.raises(TypeError, match="object keys must be strings"):
        _agg_trade_event(
            payload={"levels": [{1: "invalid"}]},
            source_event_id="bad-key",
        )


def test_json_schema_tracks_the_python_wire_shape() -> None:
    schema = _load_strict_json(SCHEMA_PATH)
    wire = _agg_trade_event().to_wire()

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["$id"].endswith("/market-event-envelope-v1.schema.json")
    assert schema["properties"]["schema_version"]["const"] == (
        MARKET_EVENT_ENVELOPE_SCHEMA_VERSION
    )
    assert set(schema["required"]) == set(wire)
    assert set(schema["properties"]) == set(wire)
    assert schema["additionalProperties"] is False
    assert schema["properties"]["delivery_class"]["enum"] == [
        item.value for item in DeliveryClass
    ]
    assert schema["properties"]["stream"]["properties"]["channel"]["enum"] == [
        item.value for item in MarketChannel
    ]
    assert schema["properties"]["payload_canonicalization"]["const"] == (
        PAYLOAD_CANONICALIZATION
    )
    assert len(schema["allOf"]) >= 3


def test_json_schema_validates_wire_and_delivery_semantics() -> None:
    schema = _load_strict_json(SCHEMA_PATH)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    validator.validate(_agg_trade_event().to_wire())

    ordered_delta_without_sequence = _agg_trade_event().to_wire()
    ordered_delta_without_sequence["delivery_class"] = "ordered_delta"
    ordered_delta_without_sequence["sequence_start"] = None
    ordered_delta_without_sequence["sequence_end"] = None
    with pytest.raises(ValidationError):
        validator.validate(ordered_delta_without_sequence)

    append_without_identity = _agg_trade_event().to_wire()
    append_without_identity["source_event_id"] = None
    append_without_identity["sequence_start"] = None
    append_without_identity["sequence_end"] = None
    with pytest.raises(ValidationError):
        validator.validate(append_without_identity)


def test_capacity_envelope_is_explicitly_provisional_and_promotable() -> None:
    capacity = _load_strict_json(CAPACITY_PATH)

    assert capacity["status"] == "provisional_validation_target_not_capacity_claim"
    assert capacity["logical_deployment"] == "one_company_one_candlescope_cluster"
    assert capacity["ingestion"]["accepted_event_data_loss"] == 0
    assert capacity["interactive_frontends"]["concurrent_users"] >= 100
    assert capacity["replay"]["concurrent_sessions"] > 8
    assert capacity["analysis"]["resource_pool_isolated_from_ingestion"] is True
    assert len(capacity["promotion_requires"]) >= 6


def _snapshot() -> MarketDataSnapshotRef:
    return MarketDataSnapshotRef(
        data_epoch="binance-futures-2026-08-07",
        snapshot_version=3,
        manifest_uri="s3://candlescope/epochs/2026-08-07/manifest-v3.json",
        manifest_sha256="a" * 64,
    )


def _event_range(*, event_count: int = 1) -> MarketEventRange:
    return MarketEventRange(
        partition_key="binance:futures:BTCUSDT@agg_trade",
        start_event_time_ms=1_754_000_000_000,
        end_event_time_ms=1_754_000_000_000,
        event_count=event_count,
        sequence_start=991 if event_count else None,
        sequence_end=991 if event_count else None,
    )


def test_snapshot_ref_pins_versioned_immutable_manifest() -> None:
    snapshot = MarketDataSnapshotRef(
        data_epoch=" epoch-1 ",
        snapshot_version=1,
        manifest_uri=" s3://bucket/manifest-v1.json ",
        manifest_sha256="A" * 64,
    )

    assert snapshot.data_epoch == "epoch-1"
    assert snapshot.manifest_uri == "s3://bucket/manifest-v1.json"
    assert snapshot.manifest_sha256 == "a" * 64
    with pytest.raises(ValueError, match="greater than zero"):
        MarketDataSnapshotRef("epoch-1", 0, "s3://bucket/manifest.json", "a" * 64)


def test_archive_commit_binds_manifest_and_auditable_ranges() -> None:
    commit = ArchiveCommit(
        accepted_count=1,
        snapshot=_snapshot(),
        object_uri="s3://candlescope/epochs/2026-08-07/part-0001.parquet",
        content_sha256="b" * 64,
        covered_ranges=(_event_range(),),
    )

    assert commit.snapshot.data_epoch == "binance-futures-2026-08-07"
    assert commit.covered_ranges[0].sequence_start == 991
    with pytest.raises(ValueError, match="accepted_count"):
        ArchiveCommit(
            accepted_count=2,
            snapshot=_snapshot(),
            object_uri="s3://candlescope/part-0001.parquet",
            content_sha256="b" * 64,
            covered_ranges=(_event_range(),),
        )


def test_market_event_page_is_pinned_to_snapshot_and_covered_range() -> None:
    event = _agg_trade_event()
    page = MarketEventPage(
        snapshot=_snapshot(),
        events=(event,),
        covered_range=_event_range(),
        next_cursor=MarketEventCursor("partition-0:991", "a" * 64),
    )

    assert page.snapshot.snapshot_version == 3
    assert page.covered_range.event_count == len(page.events)
    with pytest.raises(ValueError, match="page size"):
        MarketEventPage(
            snapshot=_snapshot(),
            events=(event,),
            covered_range=_event_range(event_count=0),
            next_cursor=None,
        )
    with pytest.raises(ValueError, match="page snapshot"):
        MarketEventPage(
            snapshot=_snapshot(),
            events=(event,),
            covered_range=_event_range(),
            next_cursor=MarketEventCursor("partition-0:991", "c" * 64),
        )


def test_archive_and_query_ports_require_epoch_and_snapshot_bindings() -> None:
    archive_parameters = inspect.signature(MarketEventArchive.append).parameters
    query_parameters = inspect.signature(MarketEventQuery.query).parameters

    assert {"data_epoch", "snapshot_version"} <= set(archive_parameters)
    assert "snapshot" in query_parameters
