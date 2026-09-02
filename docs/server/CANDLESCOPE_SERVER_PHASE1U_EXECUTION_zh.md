# CandleScope Server Phase 1U：有界 Soak 监督器与 loopback 健康刮取

状态：BOUNDED_SOAK_SUPERVISOR_COMPLETE_PUBLIC_24H_NOT_RUN

后续状态：Phase 1V 已把可选 `--health-bind` 接到 collector/writer/archiver 常驻入口，默认仍关闭。1U 的刮取合同与 `public-24h` fail-closed 保持不变；当前进程绑定边界以 `CANDLESCOPE_SERVER_PHASE1V_EXECUTION_zh.md` 为准。

Phase 1U 交付 Phase 1R 所缺的监督器合同，但默认仍然不跑 24 小时、不启动 Binance：

```text
loopback RoleHealthHttpServer /health
  -> bounded scrape loop (duration, interval, staleness)
  -> Phase 1R reconcile_chain (lagging allowed mid-run)
  -> quiet checkpoint require caught_up
  -> soak result JSON
```

主 FastAPI `server` Profile 仍 fail closed。监督器不拉起 collector/writer/archiver 进程，也不替代 Phase 1T 的跨进程门禁；它只刮取已经存在的 loopback 健康快照并对账。

## 1. 健康 HTTP

`RoleHealthHttpServer` 只能绑定 `127.0.0.1` / `localhost` / `::1`。

| 路径 | 行为 |
| --- | --- |
| `GET /health/live` | 进程在服务即 200 |
| `GET /health/ready` | 已 publish 且 `ready=true` 才 200，否则 503 |
| `GET /health` | 角色 `to_wire()` 精确 JSON，供 `chain_health` 解码 |

这不是公网 API：禁止非 loopback bind，刮取 URL 必须是 `http://127.0.0.1:<port>/health`，不得带 userinfo、query 或 fragment。生产 collector/writer/archiver 入口尚未默认开启该 HTTP；本阶段冻结的是服务与刮取合同。

## 2. 监督器门禁

`SoakSupervisorSettings`：

- 默认 `source=scripted`，`duration_ms=5000`
- 脚本源最长 300 秒，除非 `CANDLESCOPE_PHASE1U_ALLOW_PUBLIC_SOAK=1`
- `source=binance` 必须有该开关，且 duration 至少 24 小时
- 健康 `updated_at_ms` 超过 `stale_after_ms` 立即失败
- 刮取禁用环境代理、禁止 redirect、限制响应 16KiB
- 结束时必须 caught-up；`twenty_four_hour_public_continuity` 只有在 binance 源且实际 elapsed ≥ 24h 时才为 true

`public-24h` 子命令即使打开开关也返回 `PUBLIC_SOAK_NOT_RUN`：监督器没有 24 小时墙钟循环，也不会启动 Binance collector。

## 3. 交付物

| 交付物 | 路径 |
| --- | --- |
| loopback health HTTP | `backend/app/server_runtime/health_http.py` |
| 监督器 | `backend/app/server_runtime/soak_supervisor.py` |
| CLI | `backend/scripts/server_phase1u_soak_supervisor.py` |
| 单元门禁 | `backend/tests/test_server_phase1u_soak_supervisor.py` |

## 4. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1u_soak_supervisor.py
PYTHONPATH=. backend/.venv/bin/python \
  scripts/server_phase1u_soak_supervisor.py public-24h
```

## 5. 明确未完成

- 没有把 health HTTP 接到生产 collector/writer/archiver 常驻入口；
- 没有 24 小时墙钟或 Binance 公网连续性证据；
- 监督器不编排进程启动/崩溃注入（仍由 Phase 1T 门禁证明）；
- 没有打开主 FastAPI `server` Profile，也没有接入 ReplayService。

机器可读结果位于 `docs/server/evidence/phase1u-soak-supervisor-verification.json`。
