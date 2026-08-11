# CandleScope Server Phase 1H：共享查询控制面与耐久审计链

状态：SHARED_QUERY_CONTROL_COMPLETE_NOT_DEPLOYABLE

Phase 1H 延续独立快照查询进程，不解除主 FastAPI `server` Profile。它把 Phase 1G 的进程内热端隔离和日志审计升级为 PostgreSQL 权威控制状态：

```text
query instance A/B/C
  -> PostgreSQL query control
       -> one generation-fenced ClickHouse quarantine row
       -> append-only redacted query/control audit events
       -> serialized RFC 8785 SHA-256 chain head

explicit hot/cold mismatch
  -> atomic latch + audit
  -> every instance auto routes cold
  -> process restart preserves quarantine

operator repair
  -> separate control bearer
  -> expected_generation compare-and-set
  -> atomic clear + audit
  -> no automatic clear
```

这里仍是内部服务控制面，不是最终用户、组织或工作区授权，也不替代 API gateway、mTLS、OIDC、secret manager 或生产数据库权限。

## 1. 配置与启动边界

默认 `control_backend=postgres`，缺少 PostgreSQL、实例身份或独立控制凭据时拒绝启动：

```text
CANDLESCOPE_SERVER_QUERY_INSTANCE_ID=query-a
CANDLESCOPE_SERVER_QUERY_POSTGRES_DSN=postgresql://...
CANDLESCOPE_SERVER_QUERY_CONTROL_BEARER_TOKEN=<at least 32 characters>
```

可选配置：

```text
CANDLESCOPE_SERVER_QUERY_CONTROL_BACKEND=postgres
CANDLESCOPE_SERVER_QUERY_CONTROL_PRINCIPAL=candlescope-query-operator
CANDLESCOPE_SERVER_QUERY_HOT_BACKEND_ID=clickhouse-market-events-v1
CANDLESCOPE_SERVER_QUERY_POSTGRES_CONNECT_TIMEOUT_MS=5000
CANDLESCOPE_SERVER_QUERY_POSTGRES_REQUEST_TIMEOUT_MS=5000
```

- 查询 token 和控制 token 必须不同，均不会出现在 settings repr、响应或审计事件中；
- 每个实例必须配置稳定且可辨认的 `INSTANCE_ID`，quarantine 记录会保存锁存实例；
- PostgreSQL 连接固定 application name，并设置 statement、lock 和 idle-in-transaction timeout；
- schema 初始化使用事务级 advisory lock，避免多个实例同时启动时发生 PostgreSQL catalog DDL 竞态；
- `process` backend 只保留给 Phase 1F/1G 历史回归门禁，不接受 PostgreSQL 或控制凭据，也不暴露人工解除接口。

入口不变：

```bash
cd backend
PYTHONPATH=. .venv/bin/python scripts/server_snapshot_query.py
```

## 2. 共享 quarantine 状态

`candlescope_query_hot_quarantine` 以逻辑 hot backend ID 为主键，保存：

- 单调 `generation`；
- `active`；
- mismatch reason、`latched_by`、`latched_at`；
- `cleared_by`、受限 `clear_reason_code`、`cleared_at`。

初始 generation 为 0。inactive 状态被新 mismatch 锁存时 generation 加一；已经 active 时后续实例不能覆盖首次检测者和原因。查询路由在以下位置读取共享状态：

1. 实例启动；
2. `/health/ready`；
3. 每次 `auto` 或 `hot` 查询；
4. 每轮后台 parity sampler。

因此未执行 parity 的 B 实例也会在 A 锁存后立即走 cold；A 退出后新启动的 C 仍看到相同 generation。显式 `cold` 不依赖热端状态做路由，但 HTTP 查询最后仍必须成功写入耐久审计，否则返回 503。

## 3. 人工读取与解除

控制接口只在 PostgreSQL backend 下注册：

```text
GET  /api/v1/server/control/hot-projection
POST /api/v1/server/control/hot-projection/clear
```

它们要求独立的 control bearer。查询 bearer 不能调用控制接口。

解除请求：

```json
{
  "expected_generation": 1,
  "reason_code": "projection_repaired"
}
```

- generation 不匹配返回 409 `HOT_PROJECTION_GENERATION_CONFLICT`；
- 当前未隔离返回 409 `HOT_PROJECTION_NOT_QUARANTINED`；
- reason code 只能使用小写安全 ASCII，最长 128；
- 成功的状态转换和 `quarantine_cleared` 审计事件在同一 PostgreSQL 事务提交；
- 失败的解除尝试也写入审计链；审计不可用时不继续操作；
- 操作员必须先在控制面之外修复并验证 ClickHouse 投影。本接口不会修数据，也不会把一次采样成功解释为修复证明。

## 4. 耐久审计哈希链

审计 schema 升级为 `candlescope.query-control-audit.v2`，每条事件增加 UUID `event_id`，并可携带 `control_generation` 和受限 `reason_code`。可能超过 IEEE-754 安全整数范围的 snapshot version 和 generation 在 wire JSON 中使用十进制字符串，保持 I-JSON/RFC 8785 可规范化。继续禁止记录：

- query/control bearer；
- PostgreSQL、ClickHouse 或 S3 凭据；
- 完整行情 envelope 或 payload。

PostgreSQL 表：

- `candlescope_query_audit_event`：事件 JSON、previous hash、event hash；
- `candlescope_query_audit_head`：当前 tail sequence/hash；
- `candlescope_query_hot_quarantine`：共享状态。

每次 append 在锁定 head 的短事务中执行：

```text
event_hash = SHA256(bytes(previous_hash) || RFC8785(event_json))
```

链 verifier 有显式最大记录数，逐项重算 body hash、previous hash 和 head。真实门禁故意改写第一条 `event_json`，验证器检测到 `event hash mismatch`。

这是一条数据库内 tamper-evident chain，不是外部不可抵赖账本。拥有完整 PostgreSQL 写权限的攻击者可以重算事件和 head。生产方案仍需独立只追加角色、备份/WAL、外部签名或周期锚定、保留策略及受权审计查询接口。

## 5. 故障关闭语义

- PostgreSQL schema 或初始状态不可读：实例启动失败；
- readiness 无法刷新控制状态：503 `QUERY_CONTROL_UNAVAILABLE`；
- 查询过程中控制状态不可读：不使用热端；最终审计也必须成功，否则 503；
- 查询或认证审计写入失败：503 `QUERY_AUDIT_UNAVAILABLE`；
- quarantine 成功锁存后，`auto` 返回 cold，强制 `hot` 返回 503；
- 只有匹配 generation 的显式控制命令才能解除；进程重启和成功采样均不能解除。

## 6. 真实多实例门禁

Compose 使用 PostgreSQL 18.4、Redpanda 26.1.14、ClickHouse 26.3.17.56、MinIO `RELEASE.2025-09-07T16-13-09Z`，查询服务仍作为独立 Uvicorn 进程启动。

门禁步骤：

1. 发布 original、exact duplicate、same-identity conflict 和 following event；
2. ClickHouse 与 Parquet 固定同一 snapshot 4；
3. 启动 A、B 两个查询进程，B 的 parity interval 固定为 60 秒；
4. A、B 均先返回 verified hot；
5. 有意把 ClickHouse 首事实替换为另一个内部 canonical envelope；
6. A 后台 sampler 锁存 generation 1；B 未自行采样，但下一次查询读取共享状态并走 cold；
7. 查询 bearer 调控制接口返回 401；
8. 退出 A 并启动 C，C 在 startup/readiness 读取到仍 active 的 generation 1；
9. 修复 ClickHouse；generation 2 的过时/错误解除返回 409；generation 1 解除成功；
10. B 刷新共享状态并恢复 verified hot；
11. 精确验证 11 条耐久审计记录、链 tail、状态和凭据脱敏；
12. 故意改写首条审计 JSON，verifier 明确检测 hash mismatch；所有进程 SIGINT 为 0。

重跑：

```bash
docker compose -f deploy/server/compose.phase1h.yml up -d --wait
CANDLESCOPE_PHASE1H_INTEGRATION=1 \
  CANDLESCOPE_PHASE1H_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1f_snapshot_query.py
docker compose -f deploy/server/compose.phase1h.yml down -v
```

## 7. 尚未完成

- API gateway、mTLS/OIDC、多 token 轮换/撤销以及用户/组织/工作区授权；
- PostgreSQL 最小权限/只追加角色、WAL/备份、保留/分区、外部链锚点和审计查询 API；
- quorum、跨区域 PostgreSQL 高可用、网络分区和灾难恢复证据；
- ClickHouse 服务端只读用户、resource group、quota 和生产慢查询门禁；
- manifest 首事实索引/checkpoint、同流时间 pruning、缓存和长链 compaction；
- quarantine 告警路由、修复任务编排和双人批准策略；
- 24 小时公网连续性、节点重启、网络分区和生产容量证据；
- WebSocket fan-out、Replay Worker snapshot adapter；
- 主 FastAPI `server` Profile 启动门禁解除。

机器可读结果位于 `docs/server/evidence/phase1h-shared-query-control-verification.json`。
