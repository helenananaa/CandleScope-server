# CandleScope Server Phase 1T：Collector 接管接入纵向重启对账

状态：COLLECTOR_TAKEOVER_CHAIN_GATE_COMPLETE_NOT_DEPLOYABLE

Phase 1T 把 Phase 1C 的 PostgreSQL 单主 collector 接进 Phase 1S 的真实数据平面：

```text
collector A scripted span 42,43
  -> SIGKILL while lease still held
  -> collector B takeover, epoch 1, span 44,45
  -> Phase 1D writer crash before Kafka commit, replacement commits offset 4
  -> Phase 1E archiver crash before Kafka commit, replacement commits offset 4
  -> cold Parquet query
  -> Phase 1Q TradeReplaySource
  -> Phase 1R caught-up reconciliation using collector B health wire
```

发布端不再是 1S 的进程内 Kafka publisher。本阶段使用生产 `AggTradeCollectorService`、lease store 和 publisher；交易所 source 仍替换成有界脚本源，避免公网。主 FastAPI `server` Profile 仍 fail closed。这不是 24 小时公网 soak。

## 1. 基础设施

`deploy/server/compose.phase1t.yml` 固定：

- Redpanda `v26.1.14`，`localhost:19092`
- PostgreSQL `18.4-bookworm`，`localhost:15432`
- ClickHouse `26.3.17.56`，`http://localhost:18123`
- MinIO `RELEASE.2025-09-07T16-13-09Z`，`http://localhost:19000`

必须同时设置：

```text
CANDLESCOPE_PHASE1T_INTEGRATION=1
CANDLESCOPE_PHASE1T_ALLOW_TEST_RESET=1
```

重置只接受上述本机端口，并会删除 lease 表、topic、ClickHouse database 和 bucket 对象。

## 2. 脚本化跨度

`server_phase1t_collector_worker.py` 接受 `--sequences 42,43` 这样的连续正整数列表，最多 8 个。source 只在本进程真正成为 leader 后发射；standby 不启动 pipeline。缺口、空项和非正整数均拒绝。

安静检查点使用 **collector B 的健康线**，不再合成 publisher health：`producer_epoch=1`、`last_sequence=45`、`last_partition_offset=3`、`pending_event_id=null`。

## 3. 交付物

| 交付物 | 路径 |
| --- | --- |
| compose 栈 | `deploy/server/compose.phase1t.yml` |
| collector span worker | `backend/scripts/server_phase1t_collector_worker.py` |
| 单元门禁 | `backend/tests/test_server_phase1t_collector_span.py` |
| 跨进程门禁 | `backend/tests/integration/test_server_phase1t_collector_takeover.py` |

## 4. 本地重跑

```bash
docker compose -f deploy/server/compose.phase1t.yml up -d --wait
CANDLESCOPE_PHASE1T_INTEGRATION=1 \
  CANDLESCOPE_PHASE1T_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1t_collector_takeover.py
docker compose -f deploy/server/compose.phase1t.yml down -v
```

单元门禁不需要 compose：

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1t_collector_span.py
```

## 5. 明确未完成

- 没有真实 Binance 公网 24 小时连续性；
- 没有常驻 soak supervisor 或外部 HTTP health 刮取；
- 没有打开主 FastAPI `server` Profile，也没有接入 ReplayService。

机器可读结果位于 `docs/server/evidence/phase1t-collector-takeover-verification.json`。
