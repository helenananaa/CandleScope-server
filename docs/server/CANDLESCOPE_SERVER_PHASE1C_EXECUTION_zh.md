# CandleScope Server Phase 1C：常驻 Collector 生命周期与单主接管

状态：ALWAYS_ON_COLLECTOR_LIFECYCLE_COMPLETE_NOT_DEPLOYABLE

Phase 1C 把 Phase 1B 的持久化发布 seam 接成一个可独立运行的常驻进程：

    CandleScope six-layer ExchangeIngestionFactory
      -> Binance Futures BTCUSDT aggTrade
      -> fenced LeasedAggTradeCollector
      -> PostgreSQL lease / pending / checkpoint
      -> Redpanda/Kafka partition 0

本阶段只完成第一条 Collector 的生命周期，不解除 FastAPI `server` Profile 的 fail-closed 门禁。ClickHouse、Parquet、查询与 WebSocket 服务仍未交付。

## 1. 冻结运行范围

| 项目 | Phase 1C 决定 |
| --- | --- |
| 逻辑流 | `binance:futures:BTCUSDT@agg_trade` |
| 交易所源 | 复用 `ExchangeIngestionFactory.start_market()` 的 L1-L6 pipeline |
| descriptor | Binance / futures / BTCUSDT / `aggTrade` |
| 所有权 | PostgreSQL 单 stream lease + token + producer epoch |
| 心跳 | 独立于事件发布锁；Kafka publish 阻塞时仍可 renew |
| 待命实例 | 只重试 acquire，不启动交易所 pipeline |
| 健康状态 | starting / standby / recovering / leader / degraded / fenced / stopping / stopped |
| ready | 仅 active lease、source connected 且状态为 leader 时为 true |
| 停止顺序 | source stop -> heartbeat cancel -> healthy lease release -> publisher stop |

`LeasedAggTradeCollector` 的心跳只把数据库返回的 `lease_expires_at_ms` 合并到当前不可变 lease。并发完成的 pending stage 或 checkpoint 字段不会被旧 renewal 回执覆盖。单次 renew 最多等待一个 heartbeat interval；配合 3:1 TTL 门禁，即使数据库调用挂起也会在旧 lease 到期前 fail closed。若 heartbeat、stage 或 checkpoint 使所有权不再可证明，Collector 进入 terminal/fenced 状态；Kafka 发布即使随后返回，也不能再推进 checkpoint，durable pending 留给新 owner 恢复。

## 2. 配置与启动

生产入口是独立进程脚本 `backend/scripts/server_agg_trade_collector.py`，不经过当前仍 fail closed 的 FastAPI server Profile。

必填环境变量：

| 变量 | 含义 |
| --- | --- |
| `CANDLESCOPE_SERVER_COLLECTOR_POSTGRES_DSN` | PostgreSQL 控制平面 DSN；不会进入 settings repr 或健康快照 |
| `CANDLESCOPE_SERVER_COLLECTOR_KAFKA_BOOTSTRAP_SERVERS` | 逗号分隔的 Kafka-compatible broker 地址 |
| `CANDLESCOPE_SERVER_COLLECTOR_OWNER_ID` | 此进程稳定且非空的 owner identity |

可选正整数变量与默认值：

- `CANDLESCOPE_SERVER_COLLECTOR_LEASE_TTL_MS=15000`
- `CANDLESCOPE_SERVER_COLLECTOR_HEARTBEAT_INTERVAL_MS=5000`
- `CANDLESCOPE_SERVER_COLLECTOR_LEADERSHIP_RETRY_MS=1000`
- `CANDLESCOPE_SERVER_COLLECTOR_SHUTDOWN_TIMEOUT_MS=10000`

配置在连接任何外部服务前严格校验。heartbeat interval 必须不大于 lease TTL 的三分之一；空值、带空白的数字、负数和隐式 fallback 都被拒绝。

首次初始化控制平面表：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_agg_trade_collector.py init-schema
```

启动常驻 Collector：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_agg_trade_collector.py run
```

`SIGINT` 和 `SIGTERM` 会触发上述有序停止。每次状态变化和 heartbeat 更新以不含秘密的 JSON health snapshot 写入日志。

## 3. 状态与故障语义

- `standby` 实例没有 producer epoch，也不会建立重复的交易所连接。
- 获取 lease 后先进入 `recovering`；若 PostgreSQL 有 durable pending，必须从相同 source event 重建并重发完全相同的 Envelope。
- source connected 且 pending 已清除后进入 `leader`；只有此状态可报告 ready。
- ingestion 到达无法修复的 gap 时，回调即使被 DeliveryLayer 捕获，service 内部 fatal signal 仍会终止进程。
- heartbeat 失败视为所有权不再可证明，进入 fenced 路径；该进程不 release 不确定的 lease，也不继续 checkpoint。
- source 的 reconnecting/unhealthy/disconnected 会公开为 recovering/degraded，但由已有 ingestion recovery 管理是否恢复连接。

## 4. 双进程真实基础设施门禁

Phase 1C 集成门禁使用真实 PostgreSQL 与单分区 Redpanda，并启动两个独立 Python 进程；为了确定性和不依赖公网，只把交易所 source 替换成受控的单事件 source，service、lease、Kafka publisher 和信号停止路径均使用生产实现。

测试过程：

1. process A 获取 epoch 0，提交 sequence 42 / Kafka offset 0；
2. process B 同时运行但保持 standby，未启动 source、未发布事件；
3. 向 process A 发送 `SIGKILL`，因此没有 graceful release；
4. lease TTL 到期后 process B 获取 epoch 1，启动 source 并提交 sequence 43 / offset 1；
5. 向 process B 发送 `SIGTERM`，验证退出码 0 和最终 stopped health；
6. PostgreSQL 最终为 sequence 43 / offset 1，pending 为空。

本地重跑：

```bash
docker compose -f deploy/server/compose.phase1b.yml up -d --wait
CANDLESCOPE_PHASE1C_INTEGRATION=1 \
  CANDLESCOPE_PHASE1C_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest \
  backend/tests/integration/test_server_phase1c_takeover.py -q
```

门禁会重建测试 topic/table，因此必须显式设置 reset 开关，并且只接受本机 `19092/15432` 的开发 Compose 目标。

## 5. 交付物

| 交付物 | 路径 |
| --- | --- |
| 严格配置 | `backend/app/server_runtime/settings.py` |
| 生命周期健康模型 | `backend/app/server_runtime/health.py` |
| 常驻 service | `backend/app/server_runtime/service.py` |
| production ingestion adapter | `backend/app/server_runtime/sources/agg_trade.py` |
| 独立心跳 leased collector | `backend/app/server_runtime/collector/leased_agg_trade.py` |
| production CLI | `backend/scripts/server_agg_trade_collector.py` |
| deterministic process worker | `backend/scripts/server_phase1c_process_worker.py` |
| unit lifecycle gate | `backend/tests/test_server_phase1c_collector_service.py` |
| two-process infrastructure gate | `backend/tests/integration/test_server_phase1c_takeover.py` |

## 6. 尚未完成

- Kafka TLS/SASL、生产 secret provider 与部署编排；
- ClickHouse 幂等 writer、offset checkpoint 和冲突隔离；
- 不可变 Parquet archive、manifest、snapshot-pinned query；
- 健康 HTTP/metrics endpoint 与外部 supervisor 配置；
- 真实 Binance 公网 24 小时连续采集、断网重连和资源容量证据；
- FastAPI server Profile 启动门禁解除。

机器可读验证结果位于 `docs/server/evidence/phase1c-collector-lifecycle-verification.json`。
