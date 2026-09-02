# CandleScope Server Phase 1S：真实栈跨进程重启与纵向对账

状态：PROCESS_RESTART_CHAIN_GATE_COMPLETE_NOT_DEPLOYABLE

后续状态：Phase 1T 已把 Phase 1C collector 接管接入同一条 Redpanda/PostgreSQL/ClickHouse/MinIO 门禁，并用接管后的 collector 健康线替换 1S 的脚本化 publisher 合成健康。1S 关于 writer/archiver commit-前崩溃的证明仍然成立；当前组合边界以 `CANDLESCOPE_SERVER_PHASE1T_EXECUTION_zh.md` 为准。

Phase 1S 把 Phase 1R 的 caught-up 对账接到一条真实、可故障注入的数据平面：

```text
scripted Kafka publisher (4 contiguous aggTrade envelopes)
  -> Phase 1D writer process: ClickHouse success, crash before Kafka commit
  -> replacement writer: idempotent replay, commit offset 4, SIGTERM
  -> Phase 1E archiver process: Parquet+manifest success, crash before Kafka commit
  -> replacement archiver: reuse immutable objects, commit offset 4, SIGTERM
  -> cold Parquet query
  -> Phase 1Q TradeReplaySource
  -> Phase 1R caught-up reconciliation
```

Collector 常驻接管仍以 Phase 1C 为准；本阶段用脚本化 Kafka publisher 作为该冻结流的发布端，并在健康快照里标记 `scripted_kafka_publisher`。主 FastAPI `server` Profile 仍 fail closed。这不是 24 小时公网 soak。

## 1. 基础设施

`deploy/server/compose.phase1s.yml` 固定：

- Redpanda `v26.1.14`，宿主机 `localhost:19092`
- ClickHouse `26.3.17.56`，宿主机 `http://localhost:18123`
- MinIO `RELEASE.2025-09-07T16-13-09Z`，宿主机 `http://localhost:19000`

重置只接受上述本机端口。必须同时设置：

```text
CANDLESCOPE_PHASE1S_INTEGRATION=1
CANDLESCOPE_PHASE1S_ALLOW_TEST_RESET=1
```

后者会删除测试 topic、ClickHouse database 和 bucket 对象。

## 2. 健康线与对账

进程健康文件必须是角色 `to_wire()` 的精确 JSON。`chain_health` 拒绝缺键、多键和类型漂移，再交给 Phase 1R `reconcile_chain(require_caught_up=True)`。

安静检查点期望：

- writer 与 archive `committed_next_offset=4`
- archive snapshot_version=4，并与 1Q pin 逐项相同
- ClickHouse FINAL sequences 与 replay ids 均为 `42–45`
- 无 integrity conflict

## 3. 交付物

| 交付物 | 路径 |
| --- | --- |
| compose 栈 | `deploy/server/compose.phase1s.yml` |
| 健康线编解码 | `backend/app/server_runtime/chain_health.py` |
| 单元门禁 | `backend/tests/test_server_phase1s_chain_health.py` |
| 跨进程门禁 | `backend/tests/integration/test_server_phase1s_process_restart.py` |

## 4. 本地重跑

```bash
docker compose -f deploy/server/compose.phase1s.yml up -d --wait
CANDLESCOPE_PHASE1S_INTEGRATION=1 \
  CANDLESCOPE_PHASE1S_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1s_process_restart.py
docker compose -f deploy/server/compose.phase1s.yml down -v
```

单元门禁不需要 compose：

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1s_chain_health.py
```

## 5. 明确未完成

- 没有真实 Binance 公网 24 小时连续性；
- 没有把 1C collector 进程纳入本门禁（仍用脚本化 publisher）；
- 没有常驻 soak supervisor、HTTP health 刮取或 systemd；
- 没有打开主 FastAPI `server` Profile，也没有接入 ReplayService。

机器可读结果位于 `docs/server/evidence/phase1s-process-restart-verification.json`。
