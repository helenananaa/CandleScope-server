# CandleScope Server Phase 1W：数据平面组合检查，FastAPI 仍锁定

状态：DATA_PLANE_COMPOSITION_CHECK_COMPLETE_FASTAPI_LOCKED

后续状态：Phase 1X/1Y 已在独立查询进程上增加组织与工作区范围身份；Phase 1Z 已把 `fastapi_must_not_boot_sqlite_control_or_market_paths` 写成启动清单。FastAPI server Profile 与其余 unlock blockers 仍在。当前 FastAPI SQLite 启动边界以 `CANDLESCOPE_SERVER_PHASE1Z_EXECUTION_zh.md` 为准。

Phase 1W 补上打开 `CANDLESCOPE_PROFILE=server` 之前的组合根：独立进程配置必须能作为同一套数据平面被读出来，并且缺依赖、跨角色漂移时 fail closed。它 **不** 让 FastAPI 以 server Profile 启动。

```text
collector + writer + archiver + snapshot query from_env
  -> kafka bootstrap / ClickHouse URL / S3 endpoint+bucket 必须一致
  -> 可选 HEALTH_BIND 必须是 loopback
  -> 公开 JSON 不含秘密
  -> FastAPI unlock 仍返回固定 blockers
```

主应用 `startup_event` 继续调用 `require_runtime_support()`；`runtime_supported` 仍然只对 `personal` 为真。完整数据平面 env 不能使 FastAPI server 启动。

## 1. 组合合同

`load_server_data_plane_composition` 只解析环境，不连接 Kafka、PostgreSQL、ClickHouse 或对象存储。

| 失败码 | 含义 |
| --- | --- |
| `ROLE_CONFIG_INCOMPLETE` | 某一角色 `from_env` 失败 |
| `KAFKA_BOOTSTRAP_DRIFT` | 四角色 bootstrap 不一致 |
| `CLICKHOUSE_URL_DRIFT` | writer 与 query URL 不一致 |
| `OBJECT_STORE_DRIFT` | archiver 与 query 的 endpoint/bucket 不一致 |
| `HEALTH_BIND_INVALID` | 已设置的 `*_HEALTH_BIND` 不是 loopback HOST:PORT |

FastAPI 仍锁定，blockers 为：

- `twenty_four_hour_public_continuity`
- `api_gateway_identity_and_tenancy`
- `replay_worker_pool`
- `fastapi_must_not_boot_sqlite_control_or_market_paths`

`fastapi-unlock` 子命令始终非零退出。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| blockers | `backend/app/deployment/profile.py` 中 `FASTAPI_UNLOCK_BLOCKERS` |
| 组合加载 | `backend/app/server_runtime/composition.py` |
| CLI | `backend/scripts/server_composition_check.py` |
| 单元门禁 | `backend/tests/test_server_phase1w_composition.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1w_composition.py \
  backend/tests/test_server_phase0_contracts.py
```

## 4. 明确未完成

- FastAPI `CANDLESCOPE_PROFILE=server` 仍不可启动；
- 本检查不探测真实网络连通性；
- 没有 24 小时公网 soak、接入网关租户模型或 Replay Worker 池。

机器可读结果位于 `docs/server/evidence/phase1w-composition-verification.json`。
