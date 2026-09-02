# CandleScope Server Phase 1Q：快照绑定的回放成交读取入口

状态：REPLAY_SNAPSHOT_READ_BOUNDARY_COMPLETE_NOT_DEPLOYABLE

后续状态：Phase 1R 已把冷查询 + Phase 1Q replay pin 纳入纵向链路 caught-up 对账，并提供进程内 rehearsal。1R 仍未跑公网 24 小时 soak，也不打开 server Profile；当前 soak/对账边界以 `CANDLESCOPE_SERVER_PHASE1R_EXECUTION_zh.md` 为准。

Phase 1Q 把 Phase 0 纵向链路的最后一跳接到现有回放领域，而不是继续加深查询控制面备份：

```text
caller-pinned MarketDataSnapshotRef
  + frozen binance:futures:BTCUSDT@agg_trade
  + event-time window
  + exact agg_trade_id span
  -> cold MarketEventQuery pages only
  -> bounded first-fact materialization
  -> ServerSnapshotTradeReader
  -> existing TradeReplaySource
```

本阶段不解除主 FastAPI `server` Profile，不修改 ReplayService/Actor 组合根，不启动 Replay Worker，不读热 ClickHouse，也不把个人版 `RawAggTradeArchive` 伪装成服务器快照。

## 1. 冻结读取合同

| 项目 | Phase 1Q 决定 |
| --- | --- |
| 逻辑流 | 仅 `binance:futures:BTCUSDT@agg_trade` |
| 快照 | 调用方显式提供完整 `MarketDataSnapshotRef`；禁止 latest / version 0 |
| 查询端口 | `MarketEventQuery`；只走冷端，不传 `preference` |
| 权威 | Phase 1F/1E 已证明的不可变 Parquet 首事实页 |
| 物化 | 加载时有界扫完整针定区间，然后冻结为同步页 |
| 回放入口 | 现有 `TradeReplaySource` 通过 `ReplayTradePageReader` |
| 成交来源标记 | `source=server_snapshot`，`source_quality=server_snapshot_manifest` |
| 完整性 | `completeness=exact`；ID 必须连续且等于 pinned row_count |

针定 schema 为 `candlescope.replay-server-snapshot.v1`，必须同时包含：

- `data_epoch` / `snapshot_version` / `manifest_uri` / `manifest_sha256`
- 冻结 stream
- `start_event_time_ms` / `end_event_time_ms`
- `expected_first_agg_trade_id` / `expected_last_agg_trade_id` / `row_count`

`row_count` 必须等于闭区间 ID 跨度，且至少为 1。缺快照、错流、热冷混读、个人归档回退或实时行情回退都没有代码路径。

## 2. 加载与 fail-closed

`ServerSnapshotTradeReader.load` 顺序固定为：

1. 拒绝 pin 扫描预算不足或非冻结流；
2. 按 snapshot-bound cursor 向冷查询分页，默认每页 500、最多 256 页、最多 100000 行；
3. 每页回显的 snapshot 必须与 pin 逐项相等，cursor 必须绑定同一 manifest SHA-256；
4. 每条 envelope 必须是 `binance.agg-trade.normalized.v1`，stream、事件时间窗、`source_event_id` 与 sequence 必须等于 `agg_trade_id`；
5. payload 精确十进制 `price`/`quantity` 计算 `quote_quantity`，`buyer_is_maker` 映射为 Replay `is_buyer_maker`；
6. 物化结果必须从 first ID 连续到 last ID，数量等于 pin；缺口、倒退、身份漂移或查询异常一律 fail closed。

加载完成后，`read_page` / `read_sequence_page` 只扫描这块冻结内存，不再访问查询端口。现有 `TradeReplaySource` 可以 peek/next/fork_at_sequence；`snapshot_ref()` 在原 v1 字段之外附加 `server_snapshot`，个人版归档路径不会出现该字段。

回放包不得导入 `app.server_runtime`。依赖方向只能是服务器适配器读取回放领域模型。

## 3. 交付物

| 交付物 | 路径 |
| --- | --- |
| snapshot pin 与冷读取适配器 | `backend/app/server_runtime/replay_snapshot.py` |
| 回放页协议 | `backend/app/replay/sources/trade_reader.py` 中 `ReplayTradePageReader` |
| 既有回放源接入协议 | `backend/app/replay/sources/trade_source.py` |
| 单元门禁 | `backend/tests/test_server_phase1q_replay_snapshot.py` |

## 4. 验证门禁

单元门禁覆盖：

- 非冻结 stream、snapshot version 0、row_count 0 拒绝；
- 冷查询分页物化后，`TradeReplaySource` 得到连续 42–44、精确十进制与 quote quantity；
- `snapshot_ref` 绑定 manifest SHA-256，且查询调用不含 hot/preference；
- snapshot 漂移、ID 缺口、行数不足、查询失败、扫描/页数预算越界均 fail closed；
- payload schema、stream、事件时间窗逃逸拒绝；
- 适配器没有 latest/live/hot/personal fallback 属性；
- replay 包不导入 `server_runtime`；
- `CANDLESCOPE_PROFILE=server` 仍 fail closed。

本阶段不重跑 Phase 1F 的 Redpanda/ClickHouse/MinIO HTTP 门禁。冷查询正确性仍以 Phase 1F 证据为准；1Q 只证明在该端口上的回放绑定语义。

重跑：

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1q_replay_snapshot.py \
  backend/tests/test_replay_trade_source.py \
  backend/tests/test_server_phase0_architecture.py
```

## 5. 明确未完成

- 没有把该适配器接入 `ReplayService`、HTTP replay API 或 Actor 组合根（租约保护的加载入口见 Phase 1AC，仍不是 Worker 池）；
- 没有 checkpoint 目录或多会话调度（会话租约合同见 Phase 1AA，仍不是 Worker 池）；
- 没有 WebSocket fan-out，也没有 `/api/v1` K 线兼容读取；
- 没有真实 Binance 公网 24 小时纵向 soak，也没有进程/存储重启对账证据；
- 没有打开主 FastAPI `server` Profile；
- 冷查询仍可能对长 snapshot chain 做完整重建，1Q 不声称交互容量；
- 备份 systemd 模板、真实值班渠道和跨主机 cadence 证明仍未部署。

机器可读结果位于 `docs/server/evidence/phase1q-replay-snapshot-verification.json`。
