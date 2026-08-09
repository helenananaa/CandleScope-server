# CandleScope Server Phase 1E：不可变 Parquet 归档与快照绑定查询

状态：IMMUTABLE_PARQUET_SNAPSHOT_BOUNDARY_COMPLETE_NOT_DEPLOYABLE

Phase 1E 在 Kafka-compatible 事件日志上增加独立的冷归档和验证查询边界：

    Redpanda/Kafka partition 0
      -> exact offset-aligned segment consumer
      -> immutable Parquet object
      -> canonical versioned manifest
      -> parent manifest chain
      -> Kafka committed offset

    MarketDataSnapshotRef
      -> verify manifest URI/hash and complete parent chain
      -> verify every referenced Parquet size/hash/schema/range
      -> bounded page + manifest-bound cursor

本阶段没有解除 FastAPI `server` Profile 的 fail-closed 门禁。S3-compatible adapter 是产品边界，MinIO 只是真实兼容性门禁目标，不是生产对象存储选型结论。

## 1. 冻结归档合同

| 项目 | Phase 1E 决定 |
| --- | --- |
| Kafka 输入 | `candlescope.market-events.v1` / partition 0 / `read_committed` |
| segment 边界 | 固定事件数，first offset 必须按 `segment_event_count` 对齐 |
| 默认 segment 大小 | 10000 events |
| incomplete tail | 不发布、不提交；保留给同一 group 下次补成完整段 |
| Parquet key | `epochs/{data_epoch}/segments/partition-00000/offset-{first}-{last}.parquet` |
| manifest key | `epochs/{data_epoch}/manifests/snapshot-{last_offset+1}.json` |
| snapshot version | 精确等于 `last_kafka_offset + 1` |
| parent version | 精确等于当前 segment 的 `first_kafka_offset` |
| 对象发布 | S3 `If-None-Match: *` 条件写；代码没有 overwrite/delete 路径 |
| manifest 编码 | RFC 8785 canonical UTF-8 JSON + SHA-256 |
| Kafka commit | Parquet 和 manifest 均发布并回读验证后提交 `last_offset + 1` |

固定大小而不是 poll 时可见数量决定 segment。这样相同 Kafka offsets 在进程重启、积压变化或 batch 到达节奏变化后仍映射到相同 object/manifest key。若一个 segment 只到达部分记录，consumer 在内存保留它但不暴露给 archive service；进程停止后因为 offset 未提交，记录会从 Kafka 重放。

Parquet v1 保存 Kafka topic/partition/offset、event identity、partition key、event time、sequence、完整 canonical envelope JSON 和 envelope SHA-256。schema 的规范化定义另有固定 SHA-256；读取时不只依赖文件可解析，还会逐列比对 envelope 与索引字段。

manifest v1 只增加一个 segment，并通过完整 `MarketDataSnapshotRef` 指向父 manifest。链首必须从 Kafka offset 0 开始，链中每个 parent/version/offset 必须精确连续。合同的 Draft 2020-12 schema 位于 `docs/server/contracts/market-data-manifest-v1.schema.json`。

## 2. 崩溃和冲突语义

- Parquet 成功、manifest 前崩溃：重启从确定性 key 回读并逐事件验证已有 Parquet，再发布 manifest；
- manifest 成功、Kafka commit 前崩溃：重启回读同一 Parquet 和 canonical manifest，确认完全相同后提交 offset；
- 相同 offset segment 对应不同事件：已有 Parquet 内容验证失败，归档停止；
- 相同 snapshot version 对应不同 manifest：条件写不覆盖，随后比较失败并停止；
- 父 manifest 缺失、链断裂、对象 size/hash/schema/range 漂移：archive/query 均 fail closed；
- object store 回执不确定：重试只允许接受可回读且完全一致的已有对象。

`data_epoch` 必须是路径安全、最长 128 字符的显式标识。新 epoch 与旧 epoch 使用不同不可变命名空间；当前实现不提供隐式 latest，也不删除历史对象。

## 3. Snapshot-pinned query

`ParquetMarketEventQuery` 只接受完整 `MarketDataSnapshotRef`：

1. manifest URI 必须由 data epoch 与 snapshot version 唯一推导；
2. 下载 bytes 的 SHA-256 必须等于调用方固定的 `manifest_sha256`；
3. 递归验证父 snapshot，形成从 offset 0 连续到当前 version 的不可变链；
4. 每个 Parquet 的 byte size、content hash、schema、Kafka range、event range、sequence range 和 envelope hashes 必须匹配 manifest；
5. 分页 cursor 同时绑定 manifest hash、partition key、查询时间范围和 next index；跨 snapshot 或跨查询条件复用会失败。

当前查询实现以正确性门禁为目标，会在单进程内读取匹配 manifest 链并排序，不声称已达到交互式查询容量。后续查询服务需要 segment pruning、受限并发、缓存/ClickHouse 路由和 HTTP 鉴权，但不能放松 snapshot/cursor 绑定。

## 4. 配置与启动

安装归档依赖：

```bash
cd backend
.venv/bin/pip install -r requirements-parquet.txt
```

生产入口为 `backend/scripts/server_parquet_archiver.py`。必填环境变量：

| 变量 | 含义 |
| --- | --- |
| `CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_BOOTSTRAP_SERVERS` | Kafka-compatible broker 地址 |
| `CANDLESCOPE_SERVER_ARCHIVE_WRITER_OWNER_ID` | 稳定 writer identity |
| `CANDLESCOPE_SERVER_ARCHIVE_WRITER_DATA_EPOCH` | 显式不可变数据 epoch |
| `CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ENDPOINT_URL` | S3-compatible HTTP(S) endpoint |
| `CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_BUCKET` | 归档 bucket |
| `CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_ACCESS_KEY_ID` | S3 access key；不会进入 settings repr/health |
| `CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_SECRET_ACCESS_KEY` | S3 secret；不会进入 settings repr/health |

主要可选变量：

- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_REGION=us-east-1`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_PREFIX=market-data`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_GROUP_ID=candlescope-parquet-archiver-v1`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_SEGMENT_EVENT_COUNT=10000`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_POLL_TIMEOUT_MS=1000`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_S3_REQUEST_TIMEOUT_MS=10000`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_SESSION_TIMEOUT_MS=10000`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_KAFKA_HEARTBEAT_INTERVAL_MS=3000`
- `CANDLESCOPE_SERVER_ARCHIVE_WRITER_SHUTDOWN_TIMEOUT_MS=10000`

初始化 bucket 或启动 writer：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_parquet_archiver.py init-bucket
PYTHONPATH=. .venv/bin/python scripts/server_parquet_archiver.py run
```

`SIGINT` 与 `SIGTERM` 会停止 consumer；未完成 segment 不会提交。生产 bucket 仍需独立配置 TLS、最小权限 IAM、拒绝 overwrite/delete、版本控制或 Object Lock、复制和备份，本阶段本地门禁未替这些策略背书。

## 5. 真实基础设施门禁

门禁使用 Redpanda 26.1.14、S3-compatible MinIO `RELEASE.2025-09-07T16-13-09Z`、Boto3 1.43.53 和两个独立 Python 进程：

1. 发布 Kafka offsets 0–7，对应 sequences 42–49；
2. process A 发布 offsets 0–3 的 Parquet 与 snapshot 4 manifest 后，以退出码 98 在 Kafka commit 前退出；
3. 重启前对象存储恰有 1 Parquet + 1 manifest，consumer group 尚未提交 offset 4；
4. process B 使用相同 group 重放 offsets 0–3，复用已有两个对象，再归档 offsets 4–7；
5. 最终提交 next offset 8，对象存储恰有 2 Parquet + 2 manifest，快照链为 4 -> 8；
6. 用 snapshot 8 分三页查询，严格得到 sequences 42–49，无重复、无缺口；
7. `SIGTERM` 后 process B 退出码为 0。

本地重跑：

```bash
docker compose -f deploy/server/compose.phase1e.yml up -d --wait
CANDLESCOPE_PHASE1E_INTEGRATION=1 \
  CANDLESCOPE_PHASE1E_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1e_parquet_archive.py
docker compose -f deploy/server/compose.phase1e.yml down -v
```

门禁会删除本机 `19092/19000` 测试目标中的 topic 与 bucket objects，必须显式设置 reset 开关。

## 6. 尚未完成

- 生产对象存储选型、TLS、IAM/Object Lock、跨节点复制、备份和灾难恢复演练；
- 大 segment 的内存/CPU/压缩吞吐基准、multipart upload 与 backpressure；
- manifest checkpoint/compaction，避免极长 parent chain；
- ClickHouse 与 Parquet 查询路由、HTTP query service、权限和缓存；
- 实时 WebSocket fan-out、Replay Worker snapshot adapter；
- 真实 Binance 公网 24 小时端到端连续性与容量证据；
- FastAPI server Profile 启动门禁解除。

机器可读验证结果位于 `docs/server/evidence/phase1e-parquet-archive-verification.json`。
