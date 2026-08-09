# CandleScope Server Phase 1F：快照查询服务与热冷一致性路由

状态：SNAPSHOT_QUERY_PROOF_BOUNDARY_COMPLETE_NOT_DEPLOYABLE

Phase 1F 在 Phase 1D ClickHouse 事实投影和 Phase 1E 不可变 Parquet 快照之上增加一个独立 HTTP 查询进程：

    MarketDataSnapshotRef + stream + time range + cursor
      -> verify complete Parquet manifest/object chain
      -> canonical first-fact cold page
      -> inspect ClickHouse writer committed Kafka offset
         -> cursor behind: auto returns cold; forced hot rejects
         -> cursor covered: query ClickHouse at snapshot offset boundary
            -> exact hot/cold page equality: return hot
            -> any difference: fail closed

本阶段没有解除主 FastAPI 应用的 `server` Profile 门禁。查询服务是独立的 correctness proof process；它没有认证、生产 TLS、缓存或容量背书。

## 1. 冻结路由合同

不可变 Parquet 归档始终是查询正确性的权威。`SnapshotQueryRouter` 先读取并验证冷页，再决定能否使用热页：

| preference | ClickHouse writer committed next offset | 行为 |
| --- | --- | --- |
| `cold` | 任意 | 返回已验证 Parquet 页，不读取热端游标 |
| `auto` | 小于 `snapshot_version` 或不存在 | 返回冷页，并回显当前热端游标 |
| `hot` | 小于 `snapshot_version` 或不存在 | HTTP 409 `HOT_PROJECTION_BEHIND` |
| `auto` / `hot` | 大于等于 `snapshot_version` | 查询 ClickHouse，并与冷页逐项比较 |
| `auto` / `hot` | 覆盖但两页不同 | HTTP 503 `HOT_COLD_PARITY_FAILED`，不降级掩盖 |

这里比较完整 `MarketEventPage`：snapshot、events、covered range 和 next cursor 都必须相等。ClickHouse 查询额外限制 `first_kafka_offset < snapshot_version`；Kafka 游标通过 admin offset API 只读检查 ClickHouse writer 消费组，不加入该消费组，也不消费事件。

## 2. 统一事实与分页语义

Phase 1E 保存的是完整 Kafka 事件日志，因此可能同时含有原事件、精确重放和同 identity 的冲突事件；Phase 1D ClickHouse 表只保存首个可信事实，并把冲突隔离。Phase 1F 冷查询在整个固定 manifest chain 上按 Kafka partition/offset 重建相同语义：

1. `(partition_key, event_id)` 第一次出现的 canonical envelope 成为事实；
2. 后续 bytes/hash 完全相同的记录计为精确重复，不进入查询结果；
3. 后续同 identity 不同 bytes/hash 的记录计为完整性冲突，首事实保持不变，冲突记录不进入查询结果；
4. canonical facts 再按 event time、Kafka partition、Kafka offset 排序；
5. 热冷共用同一个 snapshot-bound cursor 编解码和分页函数。

冷端不能先按查询时间裁剪原始日志再做 identity 去重，否则一个较早首事实和较晚冲突可能被错误倒置。当前 correctness implementation 因此验证并重建完整 manifest chain，不声称有大快照交互容量。

## 3. HTTP 合同

入口：

```text
POST /api/v1/server/market-events/query
GET  /health/live
GET  /health/ready
GET  /metrics
```

请求示例：

```json
{
  "snapshot": {
    "data_epoch": "binance-futures-2026-08",
    "snapshot_version": 80000,
    "manifest_uri": "s3://archive/market-data/epochs/binance-futures-2026-08/manifests/snapshot-00000000000000080000.json",
    "manifest_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "stream": {
    "exchange": "binance",
    "market_type": "futures",
    "symbol": "BTCUSDT",
    "channel": "agg_trade",
    "params": {}
  },
  "start_event_time_ms": 1786204800000,
  "end_event_time_ms": 1786204860000,
  "limit": 500,
  "cursor": null,
  "preference": "auto"
}
```

请求模型启用 strict type 和 unknown-field rejection。成功响应包含 `backend`、`hot_committed_next_offset`、`parity_verified` 和完整 page。cursor 继续绑定 manifest SHA-256、partition key、时间范围和 next index；跨 snapshot 或跨条件使用返回 409。

服务以 semaphore 限制同时查询数，以队列等待超时返回 429。`/metrics` 暴露请求、in-flight、热/冷响应、失败、parity failure 和 overload rejection 计数。readiness 必须同时通过对象存储 bucket 只读检查、ClickHouse fact table 检查、Kafka topic/游标 admin client 启动；它不会为查询进程隐式创建 bucket。

## 4. 配置与启动

入口是 `backend/scripts/server_snapshot_query.py`。需要安装 `requirements.txt` 和 `requirements-parquet.txt`。必填环境变量：

| 变量 | 含义 |
| --- | --- |
| `CANDLESCOPE_SERVER_QUERY_KAFKA_BOOTSTRAP_SERVERS` | Kafka-compatible broker |
| `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_URL` | ClickHouse HTTP endpoint |
| `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_USER` | 独立查询用户 |
| `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_PASSWORD` | ClickHouse 密码；settings repr 不显示 |
| `CANDLESCOPE_SERVER_QUERY_S3_ENDPOINT_URL` | S3-compatible endpoint |
| `CANDLESCOPE_SERVER_QUERY_S3_BUCKET` | 已存在归档 bucket |
| `CANDLESCOPE_SERVER_QUERY_S3_ACCESS_KEY_ID` | S3 access key；settings repr 不显示 |
| `CANDLESCOPE_SERVER_QUERY_S3_SECRET_ACCESS_KEY` | S3 secret；settings repr 不显示 |

主要可选值：

- `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_DATABASE=candlescope`
- `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_WRITER_GROUP_ID=candlescope-clickhouse-writer-v1`
- `CANDLESCOPE_SERVER_QUERY_S3_REGION=us-east-1`
- `CANDLESCOPE_SERVER_QUERY_S3_PREFIX=market-data`
- `CANDLESCOPE_SERVER_QUERY_BIND_HOST=127.0.0.1`
- `CANDLESCOPE_SERVER_QUERY_BIND_PORT=8110`
- `CANDLESCOPE_SERVER_QUERY_MAX_PAGE_ROWS=1000`
- `CANDLESCOPE_SERVER_QUERY_MAX_SCAN_ROWS=100000`
- `CANDLESCOPE_SERVER_QUERY_MAX_MANIFEST_DEPTH=10000`
- `CANDLESCOPE_SERVER_QUERY_MAX_CONCURRENT_QUERIES=16`
- `CANDLESCOPE_SERVER_QUERY_QUERY_QUEUE_TIMEOUT_MS=1000`

启动：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_snapshot_query.py
```

## 5. 真实基础设施门禁

门禁组合 Redpanda 26.1.14、ClickHouse 26.3.17.56、MinIO `RELEASE.2025-09-07T16-13-09Z` 和独立 Uvicorn 查询进程：

1. Kafka offsets 0–3 写入 original、exact duplicate、same-identity conflict、following event；
2. Phase 1D production projector 得到 2 facts、1 duplicate、1 conflict，提交 ClickHouse group next offset 4；
3. Phase 1E production archive adapter 发布 snapshot 4；
4. 真实 HTTP `auto` 查询 snapshot 4，返回 `hot`、游标 4、`parity_verified=true`，事实 sequences 为 42、43；
5. 再发布 offsets 4–7，只推进 archive 到 snapshot 8，不推进 ClickHouse；
6. `auto` 查询 snapshot 8 返回 `cold`，分页得到 sequences 42–47；
7. 强制 `hot` 查询 snapshot 8 返回 409 `HOT_PROJECTION_BEHIND`；
8. SIGINT 后独立查询进程完成 lifecycle shutdown，退出码为 0。

本地重跑：

```bash
docker compose -f deploy/server/compose.phase1f.yml up -d --wait
CANDLESCOPE_PHASE1F_INTEGRATION=1 \
  CANDLESCOPE_PHASE1F_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1f_snapshot_query.py
docker compose -f deploy/server/compose.phase1f.yml down -v
```

门禁会重建本机测试 topic、ClickHouse database 和 MinIO bucket 内容，因此必须显式设置 reset 开关。

## 6. 尚未完成

- 身份、组织/工作区授权、审计、生产 TLS 和 secret manager；
- 冷端 segment pruning、manifest checkpoint/compaction、缓存和请求取消；
- 去掉 correctness proof 双读前所需的持续后台 parity sampling 和可审计水位证明；
- ClickHouse 独立 query quota、resource group、只读用户和容量/慢查询门禁；
- 多实例查询服务、限流共享状态、负载均衡和故障转移；
- 24 小时公网采集、节点重启、网络分区和生产容量证据；
- WebSocket fan-out、Replay Worker snapshot adapter；
- 主 FastAPI `server` Profile 启动门禁解除。

机器可读结果位于 `docs/server/evidence/phase1f-snapshot-query-verification.json`。
