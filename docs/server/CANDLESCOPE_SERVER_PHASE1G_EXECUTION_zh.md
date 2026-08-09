# CandleScope Server Phase 1G：查询认证、审计、资源限额与热端隔离

状态：INTERNAL_QUERY_HARDENING_COMPLETE_NOT_DEPLOYABLE

Phase 1G 加固 Phase 1F 的独立快照查询进程，但仍不解除主 FastAPI `server` Profile：

    internal API gateway
      -> constant-time bearer authentication
      -> strict request-id and request schema
      -> bounded query concurrency
      -> immutable Parquet authority
      -> ClickHouse read-only query limits
      -> foreground exact parity proof
      -> structured redacted audit

    successful immutable hot probes
      -> bounded background parity sampler
      -> mismatch
         -> latch ClickHouse hot quarantine
         -> auto uses cold
         -> forced hot returns 503

这里的 bearer credential 只是内部服务身份，不是最终用户、组织或工作区授权，也不替代 API gateway、OIDC、mTLS 或控制平面权限。

## 1. 内部认证与 request identity

`POST /api/v1/server/market-events/query` 和 `GET /metrics` 必须携带：

```text
Authorization: Bearer <internal token>
```

- token 配置至少 32 字符，在 settings repr、审计和响应中均不显示；
- 使用 constant-time comparison，认证失败返回 401 和 `WWW-Authenticate: Bearer`；
- token 映射到一个显式内部 principal，当前不接受客户端自报 principal；
- `/health/live` 与 `/health/ready` 保持无认证，供本机/编排器探针使用；
- `X-Request-ID` 可由可信入口提供，字符集限制为安全 ASCII、最长 128；缺失时服务生成 UUID；非法值在读取 body 前返回 400；
- 所有响应回显 `X-Request-ID`。

HTTP request model 还限制 data epoch、manifest URI、cursor、stream identity 和 params 的长度/数量，并把 snapshot/event-time 整数限制在 UInt64 范围；这些边界在进入对象存储或 ClickHouse 前执行。

单 token 不提供轮换重叠窗口、撤销列表、用户授权或多租户策略。生产入口仍需 mTLS/OIDC、secret manager 和独立控制平面授权。

## 2. Fail-closed 结构化审计

审计 schema 固定为 `candlescope.snapshot-query-audit.v1`，覆盖：

- 非法 request ID；
- 认证拒绝；
- Pydantic request validation 拒绝；
- 成功、落后、quarantine、cursor 冲突、后端失败和容量拒绝。

事件只记录 request ID、内部 principal、action/outcome、HTTP status、时间/耗时、snapshot version、manifest SHA-256、partition key、preference 和最终 backend；禁止记录 bearer token、ClickHouse/S3 密码、完整 envelope 或 payload。

默认 sink 输出单行结构化 JSON 日志，必须由部署侧收集到耐久审计系统。本阶段没有把审计日志写入 PostgreSQL 或对象存储。如果 sink 无法接受事件，已认证查询返回 503 `QUERY_AUDIT_UNAVAILABLE`，不会无审计地返回数据。

## 3. ClickHouse 固定查询限额

所有 query adapter HTTP 请求固定带 `readonly=2`。事实页查询额外固定：

| setting | 默认值 |
| --- | ---: |
| `max_execution_time` | 5 秒 |
| `max_result_rows` | `max_scan_rows + 1` |
| `result_overflow_mode` | `throw` |
| `max_memory_usage` | 256 MiB |
| `max_bytes_to_read` | 512 MiB |
| `max_threads` | 4 |

这些是客户端侧不可放宽的请求上限，不代表数据库侧 resource group 已完成。生产部署仍必须使用只能读取目标 fact/system metadata 的独立 ClickHouse 用户，并在服务端配置 quota、并发和慢查询审计。

相关环境变量：

- `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_MAX_EXECUTION_TIME_MS=5000`
- `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_MAX_MEMORY_USAGE_BYTES=268435456`
- `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_MAX_BYTES_TO_READ=536870912`
- `CANDLESCOPE_SERVER_QUERY_CLICKHOUSE_MAX_THREADS=4`

## 4. 安全冷端 pruning

Parquet 查询在完整验证 manifest chain 后，可以跳过 `partition_key` 与请求 stream 不同的 segment，因为事实 identity 明确包含 partition key，另一逻辑流不可能决定本流的首事实。

同一 stream 的 segment 即使 event-time range 完全落在请求范围外，当前也不能跳过：更早 segment 可能包含某 identity 的首事实，查询时间内的记录可能只是重复或冲突。只有未来 manifest 增加可验证的首事实索引/checkpoint 后，才能安全做同流时间 pruning。

因此 Phase 1G 的 pruning 是保守且语义安全的，不宣称解决长单流快照的容量问题。

## 5. 后台 parity probe 与 quarantine

前台查询只有在 ClickHouse writer cursor 覆盖 snapshot 且热冷完整 `MarketEventPage` 相等后才登记 probe。probe 固定 snapshot、stream、时间范围、limit 和 cursor，不使用隐式 latest。

后台 sampler：

1. 使用有界 registry，默认最多保存 128 个最近成功 probe；
2. 默认每 30 秒轮转抽样一个 probe；
3. writer cursor 落后时记录 skipped，不读取不完整热页；
4. 临时后端异常记录 failed，但不会把未知网络故障误判成数据差异；
5. 热冷页明确不相等时锁存 quarantine；前台发现不相等也立即锁存；
6. quarantine 后 `auto` 返回已验证 cold page，并标记 `hot_quarantined=true`；强制 `hot` 返回 503 `HOT_PROJECTION_QUARANTINED`；
7. quarantine 不会因为后续一次成功自动解除，需进程重启或未来人工控制面处置。

`/metrics` 增加 registered probes、sample total/pass/skip/fail、quarantine reason 和最后抽样时间。`/health/ready` 仍表示冷端正确查询服务可用，同时通过 `hot_status=quarantined` 暴露降级；不会因为所有实例共同发现热数据错误而把正确冷端全部摘除。

配置：

- `CANDLESCOPE_SERVER_QUERY_PARITY_SAMPLE_INTERVAL_MS=30000`
- `CANDLESCOPE_SERVER_QUERY_PARITY_PROBE_CAPACITY=128`

## 6. 配置变化

新增必填：

```text
CANDLESCOPE_SERVER_QUERY_AUTH_BEARER_TOKEN=<at least 32 characters>
```

新增可选：

```text
CANDLESCOPE_SERVER_QUERY_AUTH_PRINCIPAL=candlescope-api-gateway
```

其余 Kafka、ClickHouse、S3、分页、并发和 bind 配置沿用 Phase 1F。启动入口仍为：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_snapshot_query.py
```

## 7. 真实基础设施门禁

门禁使用 Redpanda 26.1.14、ClickHouse 26.3.17.56、MinIO `RELEASE.2025-09-07T16-13-09Z` 和独立 Uvicorn 进程：

1. 写入 original、exact duplicate、same-identity conflict 和 following event；
2. Phase 1D projector 提交 ClickHouse cursor 4，Phase 1E archive 发布 snapshot 4；
3. 无 bearer 请求返回 401；有效 bearer 请求通过真实 ClickHouse 固定 limits 并返回 verified hot page；
4. 等待后台 sampler 对同一 immutable probe 至少成功一次；
5. 用 ClickHouse mutation 将首事实 envelope 替换为另一个内部一致、但与 Parquet 不同的 canonical envelope；
6. 后台 sampler 检出页差异并锁存 quarantine；
7. 后续 `auto` 返回 cold 且 `hot_quarantined=true`；强制 `hot` 返回 503；
8. SIGINT 后查询进程完成 lifecycle shutdown，退出码为 0。

重跑：

```bash
docker compose -f deploy/server/compose.phase1g.yml up -d --wait
CANDLESCOPE_PHASE1G_INTEGRATION=1 \
  CANDLESCOPE_PHASE1G_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1f_snapshot_query.py
docker compose -f deploy/server/compose.phase1g.yml down -v
```

## 8. 尚未完成

- API gateway、mTLS/OIDC、多 token 轮换、撤销以及用户/组织/工作区授权；
- 耐久审计存储、审计链 hash、保留策略和查询接口；
- ClickHouse 服务端只读用户、resource group、quota 和生产慢查询门禁；
- manifest 首事实索引/checkpoint、同流时间 pruning、缓存和长链 compaction；
- 多实例共享 quarantine 状态、人工确认/解除流程和告警路由；
- 24 小时公网连续性、节点重启、网络分区和生产容量证据；
- WebSocket fan-out、Replay Worker snapshot adapter；
- 主 FastAPI `server` Profile 启动门禁解除。

机器可读结果位于 `docs/server/evidence/phase1g-query-hardening-verification.json`。
