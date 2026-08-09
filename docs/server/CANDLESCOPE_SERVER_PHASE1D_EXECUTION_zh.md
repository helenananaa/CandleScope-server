# CandleScope Server Phase 1D：Kafka 到 ClickHouse 的可恢复事实投影

状态：CLICKHOUSE_PROJECTION_BOUNDARY_COMPLETE_NOT_DEPLOYABLE

Phase 1D 在 Phase 1C 常驻 Collector 的下游增加一个独立 writer 进程：

    Redpanda/Kafka partition 0
      -> strict canonical MarketEventEnvelopeV1 decoder
      -> replay-safe identity classification
      -> ClickHouse fact / integrity-conflict tables
      -> Kafka consumer-group committed offset

本阶段交付的是单个冻结市场流的 ClickHouse 投影与恢复边界。它不解除 FastAPI `server` Profile 的 fail-closed 门禁，也不把 ClickHouse 声明为长期归档权威源。

## 1. 冻结一致性合同

| 项目 | Phase 1D 决定 |
| --- | --- |
| topic / partition | `candlescope.market-events.v1` / partition 0 |
| durable recovery cursor | Kafka consumer group committed offset |
| consumer mode | `read_committed`、关闭 auto commit、`auto_offset_reset=earliest` |
| batch order | validate -> ClickHouse facts/conflicts -> Kafka commit `last_offset + 1` |
| identity | `(partition_key, event_id)` |
| exact replay | 完整 canonical envelope bytes 与 SHA-256 均相同，跳过事实插入 |
| identity conflict | 同 identity、不同完整 envelope，写 conflict quarantine 后推进 offset |
| history gap | 新 group 必须从 offset 0 开始；已有 group 必须精确接上 committed offset，否则 fail closed |
| schema drift | 表、列、engine、partition key 或 sorting key 不一致时拒绝启动 |

进入一个批次的每条 Kafka record 都必须满足冻结 topic、partition、key、headers、schema 和 RFC 8785 canonical JSON bytes。批内 offset 与跨批 offset 都必须连续。任何 wire drift、历史缺口、ClickHouse 请求失败或 Kafka commit 失败都会终止 writer；未提交的批次由同一 consumer group 在重启后重放。

事实表 `market_event_fact_v1` 保存完整 envelope、payload、hash、事件时间、sequence、producer identity 和首次 Kafka 坐标。冲突表 `market_event_conflict_v1` 保存既有与新到 envelope 的 bytes/hash 以及冲突 Kafka 坐标。两表使用 `ReplacingMergeTree`，插入携带由批次 record 坐标与 envelope hash 推导的确定性 deduplication token；验收查询使用 `FINAL` 验证逻辑结果。

## 2. 配置与启动

生产入口是独立进程脚本 `backend/scripts/server_clickhouse_writer.py`。必填环境变量：

| 变量 | 含义 |
| --- | --- |
| `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_BOOTSTRAP_SERVERS` | 逗号分隔的 Kafka-compatible broker 地址 |
| `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_OWNER_ID` | 此 writer 的稳定 owner/client identity |
| `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_URL` | 不含凭据、query 或 fragment 的 HTTP(S) endpoint |
| `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_USER` | ClickHouse writer 用户 |
| `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_PASSWORD` | ClickHouse 密码；不会进入 settings repr 或 health |

可选变量与默认值：

- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_DATABASE=candlescope`
- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_GROUP_ID=candlescope-clickhouse-writer-v1`
- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_BATCH_SIZE=500`
- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_POLL_TIMEOUT_MS=1000`
- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_CLICKHOUSE_REQUEST_TIMEOUT_MS=10000`
- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_SESSION_TIMEOUT_MS=10000`
- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_KAFKA_HEARTBEAT_INTERVAL_MS=3000`
- `CANDLESCOPE_SERVER_CLICKHOUSE_WRITER_SHUTDOWN_TIMEOUT_MS=10000`

所有整数必须是无空白的正十进制数，heartbeat interval 必须不大于 session timeout 的三分之一。

首次初始化表：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_clickhouse_writer.py init-schema
```

启动 writer：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_clickhouse_writer.py run
```

`SIGINT` 与 `SIGTERM` 会先停止 consumer，再停止 ClickHouse projector。健康状态为 starting / running / degraded / stopping / stopped；只有没有观察到完整性冲突的 running 状态为 ready。

## 3. 崩溃与重放语义

ClickHouse facts 与 conflicts 是两次独立批量插入，Kafka offset 只在二者都成功后提交。因此任意中间失败都保留 Kafka 重放能力：

- fact 成功、conflict 失败：重启将既有 fact 识别为 replay，再补写 conflict；
- ClickHouse 全部成功、Kafka commit 前崩溃：重启重放整批，事实保持单一逻辑 identity，冲突保持单一 Kafka 坐标；
- Kafka commit 回执不确定：同一批次可安全重放；
- 同 event_id 不同 envelope：不覆盖事实，隔离冲突并把 writer 保持在 degraded/not-ready。

冲突不是暂时性传输错误。推进 offset 是为了避免一个确定性坏 record 永久阻塞分区；`degraded` health 和 quarantine row 保留人工处置证据。

## 4. 真实基础设施门禁

集成门禁使用 Redpanda 26.1.14、ClickHouse 26.3.17.56 和两个独立 Python writer 进程：

1. 发布 offset 0 原始事件、offset 1 精确重复、offset 2 同 identity 冲突、offset 3 后续事件；
2. process A 写完 ClickHouse 后、Kafka commit 前以退出码 97 强制退出；
3. 重启前验证事实为 2、冲突为 1，consumer group 尚未提交 offset 4；
4. process B 使用相同 group 重放，并提交 next offset 4；
5. `SIGTERM` 后退出码为 0，最终 `FINAL` 查询仍为 sequence 42/43 两条事实、offset 2 一条冲突，首次事实 offsets 保持 0/3。

本地重跑：

```bash
docker compose -f deploy/server/compose.phase1d.yml up -d --wait
CANDLESCOPE_PHASE1D_INTEGRATION=1 \
  CANDLESCOPE_PHASE1D_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1d_clickhouse.py
docker compose -f deploy/server/compose.phase1d.yml down -v
```

门禁会删除并重建本地测试 topic 与 ClickHouse database，必须显式设置 reset 开关；代码只允许重置本机 `19092/18123` 的开发目标。

## 5. 交付物

| 交付物 | 路径 |
| --- | --- |
| 严格 writer 配置 | `backend/app/server_runtime/writer_settings.py` |
| 手动提交 consumer | `backend/app/server_runtime/consumers/kafka.py` |
| canonical record 边界 | `backend/app/server_runtime/projection.py` |
| ClickHouse schema/projector | `backend/app/server_runtime/storage/clickhouse.py` |
| writer lifecycle/health | `backend/app/server_runtime/projector_service.py`、`writer_health.py` |
| production CLI | `backend/scripts/server_clickhouse_writer.py` |
| fault process worker | `backend/scripts/server_phase1d_process_worker.py` |
| unit contract gate | `backend/tests/test_server_phase1d_clickhouse_writer.py` |
| real infrastructure gate | `backend/tests/integration/test_server_phase1d_clickhouse.py` |
| local stack | `deploy/server/compose.phase1d.yml` |

## 6. 尚未完成

- 不可变 Parquet archive、versioned manifest 与 `MarketDataSnapshotRef` 发布；
- snapshot-pinned query、分页 cursor、HTTP API 与 WebSocket fan-out；
- Kafka/ClickHouse TLS、SASL、生产 secret provider、备份与部署编排；
- 多 stream/partition 容量、ClickHouse 集群拓扑和保留策略；
- 外部 metrics/health endpoint、积压报警与冲突处置工作流；
- 真实 Binance 公网 24 小时连续链路、断网/节点重启和容量证据；
- FastAPI server Profile 启动门禁解除。

机器可读验证结果位于 `docs/server/evidence/phase1d-clickhouse-projection-verification.json`。
