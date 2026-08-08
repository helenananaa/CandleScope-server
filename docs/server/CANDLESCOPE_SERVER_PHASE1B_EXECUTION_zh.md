# CandleScope Server Phase 1B：持久化发布与进程接管边界

状态：DURABLE_PUBLISH_BOUNDARY_COMPLETE_NOT_DEPLOYABLE

Phase 1B 把 Phase 1A 的内存 seam 推进到一条真实、可故障验证的持久化链路：

    Binance Futures BTCUSDT aggTrade
      -> AggTradeEnvelopeAdapter
      -> PostgreSQL lease + pending intent
      -> Kafka-compatible single-partition topic
      -> PostgreSQL acknowledged checkpoint

本阶段仍不解除 FastAPI `server` Profile 的 fail-closed 状态。它证明第一条 Collector 的发布、fencing 和进程接管语义，不声称 ClickHouse 查询、Parquet 归档、常驻采集进程或完整服务器部署已经完成。

## 1. 冻结范围

| 项目 | Phase 1B 决定 |
| --- | --- |
| 逻辑流 | `binance:futures:BTCUSDT@agg_trade` |
| 物理 topic | `candlescope.market-events.v1` |
| topic 分区 | 必须恰好为 `{0}`，启动时检查，漂移即拒绝 |
| Kafka message key | 完整逻辑 `partition_key` UTF-8 字节 |
| Kafka value | 完整 Envelope 的 RFC 8785 JSON 字节 |
| 每次发布 | 恰好一个 Envelope；不伪装批次原子性 |
| broker 确认 | `acks=all`、`enable_idempotence=true` |
| 控制平面 | PostgreSQL；共享所有权路径不使用 SQLite |
| 写者模型 | 每个逻辑流同一时刻只有一个未过期 lease owner |

实现使用 `aiokafka==0.14.0` 和 `psycopg[binary]==3.3.4`。开发集成栈固定为单 broker Redpanda `v26.1.14` 与 PostgreSQL `18.4-bookworm`。

## 2. PostgreSQL 状态机

`candlescope_market_stream_lease` 每个 `partition_key` 一行，保存：

- 当前 `owner_id`、随机 `lease_token`、数据库时钟计算的到期时间；
- 单调递增 `producer_epoch`，首次 owner 为 0，每次过期接管加 1；
- `last_sequence`、`last_event_id`、`last_partition_offset`；
- Kafka 发布前已提交的完整 `pending_envelope` JSONB。

所有 acquire、renew、stage、checkpoint 和 release 都是短 PostgreSQL 事务。写操作先取得按 `partition_key` 派生的事务级 advisory lock，再锁定行并校验 owner、token、epoch 和数据库时间。过期或旧 token 的写者被 fencing，不能推进 checkpoint。

发布顺序固定为：

1. acquire 或 renew lease；
2. 校验 sequence 与 durable checkpoint 连续；
3. 在 PostgreSQL 原子保存完整 pending Envelope；
4. 把相同 Envelope 字节发到 Kafka partition 0；
5. 验证 topic、partition、offset 回执；
6. 在 PostgreSQL 推进 sequence/event/offset，并清除 pending。

若第 4 步成功但进程在第 6 步前退出，新 owner 会看到旧 pending。它使用 pending 自带的旧 producer identity、epoch、`published_at_ms` 和 payload 重发，而不是按新 epoch 重建事件。因此进程接管前后的重试具有相同 `event_id` 和完全相同的 Kafka value 字节；下一条新事件才使用递增后的 epoch。

## 3. 交付物

| 交付物 | 路径 |
| --- | --- |
| lease/checkpoint contract | `backend/app/server_runtime/leases.py` |
| PostgreSQL adapter | `backend/app/server_runtime/storage/postgres_lease.py` |
| Kafka-compatible publisher | `backend/app/server_runtime/publishers/kafka.py` |
| leased collector coordinator | `backend/app/server_runtime/collector/leased_agg_trade.py` |
| deterministic test double | `backend/app/server_runtime/testing/in_memory_lease_store.py` |
| process-fault worker | `backend/scripts/server_phase1b_fault_worker.py` |
| unit gate | `backend/tests/test_server_phase1b_runtime.py` |
| Redpanda/PostgreSQL gate | `backend/tests/integration/test_server_phase1b_redpanda.py` |
| local integration stack | `deploy/server/compose.phase1b.yml` |

## 4. 真实故障门禁证明了什么

集成测试会启动一个独立 Python 子进程。子进程依次完成 PostgreSQL pending stage 和 Kafka offset 0 写入，然后在 checkpoint 前正常退出。父测试等待 lease 过期，由新 owner 接管并验证：

- producer epoch 从 0 递增到 1；
- 旧 lease 的 renew 被 fencing；
- sequence 42 被重发到 offset 1，offset 0 与 1 的 key 和 value 字节完全相同；
- sequence 43 使用 epoch 1 写入 offset 2；
- PostgreSQL 最终 checkpoint 为 sequence 43 / offset 2，pending 已清空。

这里的保证是应用级 at-least-once 加稳定身份，不是跨 PostgreSQL/Kafka 的分布式 exactly-once。Kafka idempotent producer 会消除同一 producer session 内的协议级重复；跨进程主动重发仍可能留下两条物理记录。下游必须按 `event_id` 幂等消费，并在相同 identity 出现不同内容时 fail closed。

## 5. 本地重跑

安装新增依赖后：

```bash
docker compose -f deploy/server/compose.phase1b.yml up -d --wait
CANDLESCOPE_PHASE1B_INTEGRATION=1 \
  CANDLESCOPE_PHASE1B_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest \
  backend/tests/integration/test_server_phase1b_redpanda.py -q
docker compose -f deploy/server/compose.phase1b.yml down -v
```

默认连接为 `localhost:19092` 和本地开发 PostgreSQL `localhost:15432`。普通运行可以分别用 `CANDLESCOPE_PHASE1B_KAFKA_BOOTSTRAP_SERVERS` 与 `CANDLESCOPE_PHASE1B_POSTGRES_DSN` 覆盖。集成门禁只有同时显式设置 `CANDLESCOPE_PHASE1B_INTEGRATION=1` 和 `CANDLESCOPE_PHASE1B_ALLOW_TEST_RESET=1` 才会重建测试 topic/table，并且即使覆盖连接，也只接受本机 `19092/15432` 的 Compose 测试目标。

## 6. 尚未完成

- 常驻采集 runner、lease heartbeat 生命周期、健康状态和优雅停止；
- Kafka TLS/SASL 生产配置与秘密管理；
- ClickHouse writer、不可变 Parquet archive、manifest 和 snapshot-pinned query；
- 下游 `event_id` 幂等物化与冲突隔离；
- 24 小时连续采集、断网/重连和资源容量证据；
- FastAPI server Profile 启动门禁解除。

机器可读验证结果位于 `docs/server/evidence/phase1b-durable-publish-verification.json`。
