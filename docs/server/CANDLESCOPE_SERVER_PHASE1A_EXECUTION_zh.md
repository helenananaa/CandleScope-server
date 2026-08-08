# CandleScope Server Phase 1A：aggTrade 发布边界

状态：IN_MEMORY_GATE_COMPLETE

Phase 1A 只实现第一条服务器数据链路的确定性发布边界：

    Binance Futures BTCUSDT aggTrade
      -> 现有 Binance/CCXT normalizer
      -> AggTradeEnvelopeAdapter
      -> AggTradeCollector
      -> MarketEventPublisher
      -> InMemoryMarketEventLog test double

本阶段不接入 Kafka/Redpanda、ClickHouse、MinIO、PostgreSQL，也不解除 FastAPI `server` Profile 的 fail-closed 状态。

## 1. 交付物

| 交付物 | 路径 | 作用 |
| --- | --- | --- |
| 精确十进制字段 | `backend/app/exchanges/plugins/binance/normalizer.py` | 保留既有 float 字段，同时增加原始 `price_text` 与 `quantity_text` |
| Capability 更新 | `backend/app/exchanges/plugins/binance/adapter.py` | 对外声明新增的精确字段，保持能力矩阵与实际输出一致 |
| Producer identity | `backend/app/server_runtime/producer_identity.py` | 冻结 `producer_id` 与非负 fencing epoch |
| Envelope adapter | `backend/app/server_runtime/adapters/market_event.py` | 将唯一支持的 aggTrade 形状转换为 `MarketEventEnvelopeV1` |
| 单流 Collector | `backend/app/server_runtime/collector/agg_trade.py` | 串行化发布、连续性检查、pending 重试和回执验证 |
| 内存事件日志 | `backend/app/server_runtime/testing/in_memory_event_log.py` | 实现幂等回执和身份冲突检测，不冒充持久化系统 |
| 定向测试 | `backend/tests/test_server_phase1a_agg_trade.py` | 锁定精度、身份、重试、gap、回退、冲突和回执语义 |

## 2. 冻结映射

Phase 1A 仅接受 `binance + futures + BTCUSDT + aggTrade`。任何其他交易所、市场类型、symbol、channel 或 mock source 都明确拒绝。

| Envelope 字段 | 来源或规则 |
| --- | --- |
| `stream` | `binance:futures:BTCUSDT@agg_trade` |
| `delivery_class` | `append` |
| `source_event_id` | 十进制字符串形式的 `agg_trade_id` |
| `sequence_start/end` | `agg_trade_id` |
| `previous_sequence` | Collector 已确认的实际前序 ID；不得根据当前 ID 伪造 |
| `event_id` | 固定 namespace 下对 `partition_key + "\n" + source_event_id` 计算 UUIDv5 |
| `payload.price/quantity` | Binance 原始精确十进制字符串 |
| `payload_sha256` | 继续由 `MarketEventEnvelopeV1` 按 RFC 8785 计算 |

`event_id` 不包含 producer epoch。Collector 重启后同一交易所事实仍得到同一身份；如果该身份对应不同 wire 内容，Publisher 必须隔离并拒绝。

## 3. Collector 状态机

- 每个 Collector 实例只处理一个逻辑市场流，并使用异步锁串行化状态变化。
- 只有 Publisher 返回 `accepted_count=1`、正确 partition key 和非负 offset 后，`last_sequence` 才能推进。
- Publisher 已持久化但回执丢失时，pending Envelope 保留原 `event_id`、`published_at_ms`、hash 和全部 wire 字段；重试不得重建为新事件。
- 最后一个已确认事件的相同重投直接复用原回执；内容变化则为完整性冲突。
- sequence gap、回退、pending 期间收到其他事件、错误回执均 fail closed。
- 当前 sequence checkpoint 仅在内存中。持久化租约、producer epoch 和 checkpoint 属于后续 Phase 1B，不得把本阶段标记为可部署 Collector。

## 4. Phase 1B 入口

以下入口已经由 `CANDLESCOPE_SERVER_PHASE1B_EXECUTION_zh.md` 落地并通过真实 Redpanda/PostgreSQL 进程故障门禁：

Phase 1B 的验收目标如下：

1. 使用固定物理 topic，逻辑 `partition_key` 作为消息 key；
2. 第一条验收流保持单 partition，避免扩容改变 key 到 partition 的映射；
3. 开启强确认和幂等 producer；
4. 持久化 producer lease、epoch 与最后确认 sequence；
5. 使用进程级故障测试证明“持久化成功但回执丢失”的重试仍为同一事件；
6. 完成后再增加 ClickHouse writer、Parquet archive 与 snapshot-pinned query。

机器可读验证结果位于 `docs/server/evidence/phase1a-agg-trade-verification.json`。
