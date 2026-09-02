# CandleScope Server Phase 1V：常驻进程可选 loopback health HTTP

状态：ROLE_HEALTH_BIND_COMPLETE_NOT_DEFAULT_ON

后续状态：Phase 1W 已增加独立进程环境的组合检查，并明确 FastAPI `server` Profile 仍锁定。1V 的可选 health bind 合同不变；当前组合/解锁边界以 `CANDLESCOPE_SERVER_PHASE1W_EXECUTION_zh.md` 为准。

Phase 1V 把 Phase 1U 的 `RoleHealthHttpServer` 接到三个常驻入口，且默认保持关闭：

```text
optional --health-bind 127.0.0.1:PORT
  or CANDLESCOPE_SERVER_*_HEALTH_BIND
  -> parse_health_bind (loopback HOST:PORT only)
  -> HealthHttpObserver publishes each on_health snapshot
  -> existing log observer still runs
  -> GET /health serves exact to_wire JSON
```

未设置 bind 时行为与 Phase 1C/1D/1E 完全相同：只写日志，不监听端口。主 FastAPI `server` Profile 仍 fail closed。本阶段不启动 Binance soak，也不把 health HTTP 暴露到非 loopback。

## 1. 绑定合同

`parse_health_bind` 只接受 `127.0.0.1:<port>`、`localhost:<port>` 或 `::1:<port>`。路径、query、userinfo、`0.0.0.0` 和越界端口一律拒绝。port `0` 允许操作系统分配，便于测试。

环境变量：

| 进程 | 变量 |
| --- | --- |
| collector | `CANDLESCOPE_SERVER_COLLECTOR_HEALTH_BIND` |
| ClickHouse writer | `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_HEALTH_BIND` |
| Parquet archiver | `CANDLESCOPE_SERVER_ARCHIVE_WRITER_HEALTH_BIND` |

CLI `--health-bind` 覆盖对应环境变量。`init-schema` / `init-bucket` 不启动 health HTTP。

`HealthHttpObserver` 先 `publish` 再调用原有日志 observer；进程退出时关闭 HTTP。Phase 1U 监督器仍只刮取 loopback `/health`。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| bind 解析与 observer | `backend/app/server_runtime/health_http.py` |
| collector 入口 | `backend/scripts/server_agg_trade_collector.py` |
| writer 入口 | `backend/scripts/server_clickhouse_writer.py` |
| archiver 入口 | `backend/scripts/server_parquet_archiver.py` |
| 单元门禁 | `backend/tests/test_server_phase1v_health_bind.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1v_health_bind.py \
  backend/tests/test_server_phase1u_soak_supervisor.py
```

## 4. 明确未完成

- 默认部署仍不绑定 health HTTP，也没有 systemd 打开这些端口；
- 测试用 process worker（1C/1D/1E/1T）尚未接 `--health-bind`；
- 没有 24 小时公网 soak，也没有打开 server Profile 或 ReplayService。

机器可读结果位于 `docs/server/evidence/phase1v-health-bind-verification.json`。
