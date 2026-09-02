# CandleScope Server Phase 1Z：FastAPI SQLite 控制/行情启动清单

状态：FASTAPI_SQLITE_BOOT_INVENTORY_COMPLETE_SERVER_LOCKED

后续状态：Phase 1AA 已把 `replay_worker_pool` 写成会话级单写者租约合同，仍不是 Worker 进程池。1Z 的 SQLite 启动清单不变；当前回放所有权边界以 `CANDLESCOPE_SERVER_PHASE1AA_EXECUTION_zh.md` 为准。

Phase 1Z 把 FastAPI unlock blocker `fastapi_must_not_boot_sqlite_control_or_market_paths` 写成 **可复验的启动清单**，而不是只留在错误字符串里。主应用 `startup_event` 在 `require_runtime_support()` 之后仍会初始化 SQLite 控制/行情存储，并默认用 `ReplaySQLiteStore` 打开回放；这些调用 **没有** 按 `CANDLESCOPE_PROFILE` 分岔。因此 server Profile 不得启动 FastAPI，即使有人误把 `runtime_supported` 设为 true。

```text
CANDLESCOPE_PROFILE=server
  -> require_runtime_support() still fail-closed
  -> refuse_server_sqlite_boot() still fail-closed
  -> init_klines_storage / market metrics / trade_flow / liquidation
     and ReplaySQLiteStore must not run
```

这不是把 SQLite 从 personal 路径移除，也不是完整的 `sqlite3.connect` 普查。独立数据平面进程、查询身份和组合检查保持不变。主 FastAPI `server` Profile 仍锁定。

## 1. 启动清单

`candlescope.fastapi-sqlite-boot-inventory.v1`：

| role | initializer | module |
| --- | --- | --- |
| control | `init_klines_storage` | `app.main` |
| market | `init_market_metrics_storage` | `app.main` |
| market | `init_trade_flow_storage` | `app.main` |
| market | `init_liquidation_storage` | `app.main` |
| replay | `ReplaySQLiteStore` | `app.replay.runtime` |

`profile_gated=false`：trade_flow / liquidation 只看 `TRADE_FLOW_ROLLUP_BACKEND` / `LIQUIDATION_ROLLUP_BACKEND`，默认仍是 `sqlite`，不是 Profile。

`refuse_server_sqlite_boot`：

| Profile | 结果 |
| --- | --- |
| personal | no-op，既有 SQLite 启动继续 |
| server | 抛 `FASTAPI_SQLITE_CONTROL_OR_MARKET_PATH`，公开 JSON 不含秘密 |

`fastapi-unlock` 的 details 携带同一份 inventory；该子命令仍非零退出。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 清单与拒绝 | `backend/app/deployment/fastapi_sqlite_boot.py` |
| FastAPI 第二道门 | `backend/app/main.py` 中 `startup_event` |
| unlock 细节 | `backend/app/server_runtime/composition.py` |
| 单元门禁 | `backend/tests/test_server_phase1z_fastapi_sqlite_boot.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1z_fastapi_sqlite_boot.py \
  backend/tests/test_server_phase1w_composition.py \
  backend/tests/test_server_phase0_contracts.py
```

## 4. 明确未完成

- FastAPI `CANDLESCOPE_PROFILE=server` 仍不可启动；
- 没有把 klines/回放改到 PostgreSQL/对象存储；
- 没有 Replay Worker 池、接入网关租户或 24 小时公网连续性。

机器可读结果位于 `docs/server/evidence/phase1z-fastapi-sqlite-boot-verification.json`。
