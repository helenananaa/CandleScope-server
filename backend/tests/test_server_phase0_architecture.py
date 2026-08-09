from __future__ import annotations

import ast
import json
from pathlib import Path

BACKEND_ROOT = Path(__file__).parents[1]
REPOSITORY_ROOT = BACKEND_ROOT.parent
SERVER_CONTRACT_ROOT = BACKEND_ROOT / "app" / "server_contracts"
DEPLOYMENT_ROOT = BACKEND_ROOT / "app" / "deployment"
SERVER_DOC_ROOT = REPOSITORY_ROOT / "docs" / "server"

FORBIDDEN_IMPORT_ROOTS = {
    "aiokafka",
    "boto3",
    "clickhouse_connect",
    "clickhouse_driver",
    "fastapi",
    "kafka",
    "minio",
    "psycopg",
    "psycopg2",
    "sqlite3",
    "starlette",
}
FORBIDDEN_APP_MODULES = {
    "app.core.config",
    "app.main",
}


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        value[key] = item
    return value


def _imports(path: Path) -> list[tuple[str, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append((node.module or "", node.lineno))
    return imports


def _forbidden_imports(root: Path) -> list[str]:
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        for module, line in _imports(path):
            module_root = module.split(".", 1)[0]
            if module_root in FORBIDDEN_IMPORT_ROOTS or module in FORBIDDEN_APP_MODULES:
                violations.append(f"{path.relative_to(root)}:{line}:{module}")
    return violations


def test_server_contracts_are_transport_and_storage_neutral() -> None:
    assert SERVER_CONTRACT_ROOT.is_dir()
    assert _forbidden_imports(SERVER_CONTRACT_ROOT) == []


def test_deployment_profile_has_no_runtime_adapter_dependency() -> None:
    assert DEPLOYMENT_ROOT.is_dir()
    assert _forbidden_imports(DEPLOYMENT_ROOT) == []


def test_server_phase_zero_has_all_frozen_contract_artifacts() -> None:
    required_paths = (
        SERVER_DOC_ROOT / "CANDLESCOPE_SERVER_PRODUCT_CONTRACT_zh.md",
        SERVER_DOC_ROOT / "CANDLESCOPE_SERVER_ARCHITECTURE_zh.md",
        SERVER_DOC_ROOT / "CANDLESCOPE_SERVER_PHASE0_EXECUTION_zh.md",
        SERVER_DOC_ROOT / "contracts" / "market-event-envelope-v1.schema.json",
        SERVER_DOC_ROOT / "contracts" / "market-data-manifest-v1.schema.json",
        SERVER_DOC_ROOT / "contracts" / "phase0-capacity-envelope-v1.json",
        SERVER_DOC_ROOT / "contracts" / "rfc8785-payload-golden-vectors-v1.json",
        BACKEND_ROOT / "scripts" / "verify_server_rfc8785_vectors.mjs",
    )
    assert all(path.is_file() for path in required_paths)

    product = required_paths[0].read_text(encoding="utf-8")
    architecture = required_paths[1].read_text(encoding="utf-8")
    assert "FROZEN_FOR_PHASE_1" in product
    assert "FROZEN_FOR_PHASE_1" in architecture
    assert "server 必须拒绝启动" in product
    assert "单会话单写者" in architecture


def test_server_json_contracts_parse_without_runtime_services() -> None:
    contract_paths = sorted((SERVER_DOC_ROOT / "contracts").glob("*.json"))
    assert len(contract_paths) == 4
    for path in contract_paths:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_json_object,
        )
        assert isinstance(value, dict), path
