# CandleScope Server Phase 1R：纵向链路对账与 24 小时 Soak 合同

状态：VERTICAL_CHAIN_REHEARSAL_COMPLETE_PUBLIC_SOAK_NOT_RUN

后续状态：Phase 1S/1T 已把对账接到真实栈跨进程重启与 collector 接管；Phase 1U 交付有界 loopback 健康刮取监督器。1R 的进程内 rehearsal 仍可用；`public-24h` 在 1R CLI 上继续拒绝启动 Binance。当前监督器边界以 `CANDLESCOPE_SERVER_PHASE1U_EXECUTION_zh.md` 为准。

Phase 1R 把 Phase 0 的硬验收写成可 fail-closed 的对账合同，并提供一条可重复的进程内 rehearsal：

```text
contiguous Phase 1A envelopes
  -> in-memory Kafka records
  -> ClickHouse identity projector
  -> immutable Parquet segment + manifest
  -> cold Parquet query
  -> Phase 1Q ServerSnapshotTradeReader
  -> existing TradeReplaySource
  -> caught-up chain reconciliation
```

本阶段明确拒绝把 PID 存活、单个 health.ready 或未跑满 24 小时的公网采集写成成功。主 FastAPI `server` Profile 仍 fail closed。`public-24h` 入口即使设置了显式开关也不会启动 Binance Collector 或声称连续性。

## 1. 对账合同

观测 schema 由 `CollectorHealth`、`ClickHouseWriterHealth`、`ArchiveWriterHealth`、冷查询首事实 sequence 和 Phase 1Q pin 组成。成功安静检查点必须同时满足：

| 检查 | 规则 |
| --- | --- |
| 进度证据 | collector 必须有 `last_sequence` 与 `last_partition_offset`，且没有 pending envelope |
| 物理水位 | `physical_next_offset = last_partition_offset + 1` |
| 消费者不得超前 | writer/archive 的 `committed_next_offset` 不得大于 physical next |
| 安静对齐 | writer、archive、snapshot_version 三者等于 physical next，查询最后 sequence 等于 collector last_sequence |
| 快照绑定 | archive `current_snapshot`、冷查询 snapshot、replay pin 必须逐项相等，且 `snapshot_version == archive.committed_next_offset` |
| 首事实连续 | 查询 sequence 严格连续；replay first/last/row_count 必须与该跨度一致 |
| 故障 | 任一角色 `terminal_error`、DEGRADED/FENCED 立即失败 |

允许的中间态是 `lagging`：消费者落后于 collector，但查询/replay/archive snapshot 仍自洽。它不是 soak 成功。重复 Kafka 记录可以增加 writer `duplicate_events`；冲突必须保持隔离，不能改变首事实跨度。

## 2. Rehearsal

`run_phase1r_rehearsal` 使用内存对象存储和内存 projector，发布 offsets 0–3、sequences 42–45 的完整 segment，再原样重放一次以证明归档条件写与 projector 幂等。然后冷查询该 snapshot，加载 Phase 1Q reader，驱动既有 `TradeReplaySource`，并在合成 collector 健康（明确标记为 `synthetic_from_published_records`）上做 caught-up 对账。

这证明纵向语义闭合，不代替真实进程、真实 broker 或公网交易所。

入口：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_phase1r_soak.py rehearsal
PYTHONPATH=. .venv/bin/python scripts/server_phase1r_soak.py public-24h
```

`public-24h` 未设置 `CANDLESCOPE_PHASE1R_ALLOW_PUBLIC_SOAK=1` 时返回 `PUBLIC_SOAK_NOT_AUTHORIZED`。设置后仍返回 `PUBLIC_SOAK_SUPERVISOR_NOT_DELIVERED`：1R 没有交付采集/写入/归档的常驻监督器，也没有 24 小时计时循环。

## 3. 交付物

| 交付物 | 路径 |
| --- | --- |
| 链路对账 | `backend/app/server_runtime/chain_reconciliation.py` |
| 进程内 rehearsal | `backend/app/server_runtime/soak_rehearsal.py` |
| CLI | `backend/scripts/server_phase1r_soak.py` |
| 单元门禁 | `backend/tests/test_server_phase1r_soak.py` |

## 4. 验证门禁

- 安静检查点要求 offset/snapshot/replay 对齐，并声明 PID 存活不够；
- writer 落后可为 lagging，要求 caught-up 时失败；
- 缺 sequence/offset、pending、查询缺口、snapshot 漂移、消费者超前、replay 跨度漂移、terminal error 均 fail closed；
- rehearsal 产出 sequences 42–45、幂等再归档、caught-up JSON，且 `twenty_four_hour_public_continuity=false`；
- `public-24h` 在有无显式开关时都不声称连续性；
- `CANDLESCOPE_PROFILE=server` 仍 fail closed。

重跑：

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1r_soak.py \
  backend/tests/test_server_phase1q_replay_snapshot.py
```

## 5. 明确未完成

- 没有真实 Binance 公网 24 小时采集，也没有 24 小时证据文件；
- 没有把 collector/writer/archiver 组成可部署的 soak supervisor；
- 没有对真实 Redpanda/ClickHouse/MinIO 做跨进程重启注入（各阶段门禁仍独立存在）；
- 没有外部 HTTP health/metrics 供 supervisor 刮取；合成 collector 健康不能冒充常驻 Phase 1C 进程；
- 没有打开主 FastAPI `server` Profile，也没有接入 ReplayService。

机器可读结果位于 `docs/server/evidence/phase1r-soak-rehearsal-verification.json`。
