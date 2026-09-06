# CandleScope Server Phase 1AH：Server Profile 组合与启动门禁

状态：SERVER_PROFILE_RUNTIME_COMPLETE_NOT_PRODUCTION_READY

Phase 1AH 把 personal/server FastAPI lifespan 拆开，并把 `CANDLESCOPE_PROFILE=server` 的 `runtime_supported` 打开。Server 启动必须先加载 settings、通过数据平面组合检查，再把 `refuse_server_sqlite_boot` 当作负面清单：证明组合不是 SQLite / 本地文件 / 进程内总线。公网 24 小时连续性仍不存在，因此不得声称生产就绪。

```text
load_deployment_settings
  -> require_runtime_support
  -> personal: refuse noop + personal SQLite lifespan
  -> server: load composition (no db) + refuse sqlite unless composition proves otherwise
  -> start_server_runtime: scheduler, identity, replay API, query readiness
  -> scoped Replay Worker -> authenticated cold Query Service HTTP
```

## 1. 交付物

| 交付物 | 路径 |
| --- | --- |
| personal lifespan | `backend/app/deployment/personal_runtime.py` |
| server lifespan | `backend/app/deployment/server_runtime.py` |
| Server 应用组合 | `backend/app/server_runtime/application.py` |
| Worker 池循环 | `backend/app/server_runtime/replay_worker_pool.py` |
| Worker 冷查询 HTTP 客户端 | `backend/app/server_runtime/query_client.py` |
| Compose | `deploy/server/compose.phase1ah.yml` |
| 门禁 | `test_server_phase1ah_profile.py`、`integration/test_server_phase1ah_profile.py` |

Scheduler pool 模式的 Worker 必须显式配置以下边界；不允许从用户 payload 读取本地路径：

```text
CANDLESCOPE_SERVER_REPLAY_WORKER_WORKER_ID
CANDLESCOPE_SERVER_REPLAY_WORKER_POSTGRES_DSN
CANDLESCOPE_SERVER_REPLAY_WORKER_QUERY_URL
CANDLESCOPE_SERVER_REPLAY_WORKER_QUERY_CREDENTIAL
CANDLESCOPE_SERVER_REPLAY_WORKER_ORGANIZATION_ID
CANDLESCOPE_SERVER_REPLAY_WORKER_WORKSPACE_ID
CANDLESCOPE_SERVER_REPLAY_WORKER_CONTROL_TOKEN
```

`QUERY_CREDENTIAL` 对应的 Query Service 必须用相同的
`CANDLESCOPE_SERVER_QUERY_AUTH_ORGANIZATION_ID` 与
`CANDLESCOPE_SERVER_QUERY_AUTH_WORKSPACE_ID` 绑定。一个 Worker 只领取这个 scope 的任务；
部署多个租户时应使用分别受限的 Worker/Query 凭据，不能把一个全局 token 当作所有租户的
共享身份。

## 2. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ah_query_client.py \
  backend/tests/test_server_phase1ah_profile.py \
  backend/tests/test_server_phase1af_replay_scheduler.py \
  backend/tests/test_server_phase1w_composition.py \
  backend/tests/test_server_phase1z_fastapi_sqlite_boot.py \
  backend/tests/test_server_phase1ag_replay_api.py

docker compose -p candlescope-phase1ah \
  -f deploy/server/compose.phase1ah.yml up -d --wait
CANDLESCOPE_PHASE1AH_INTEGRATION=1 \
  CANDLESCOPE_PHASE1AH_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1ah_profile.py
docker compose -p candlescope-phase1ah \
  -f deploy/server/compose.phase1ah.yml down -v
```

## 3. 2026-09-04 接入面复核与修复

复核发现并关闭以下缺口：

1. `cancel_run` 原来先调用 scheduler cancel，再做 scope 检查；现在先读取、授权，再改变状态，
   并有跨租户状态不变测试。
2. `GET /api/v1/replay/runs` 原来固定返回空数组；现在 PostgreSQL 与内存 store 都实现按
   `organization_id + workspace_id` 过滤、按创建时间倒序、最多 100 条的列表。
3. Worker 池原来从用户 payload 中读取本地 `query_path`；生产池现在只调用独立 Query
   Service，使用专用 bearer、权威 scope、严格响应合同与固定冷端路由。本地 frozen query
   只保留在显式 Phase 1AE 确定性 assignment 门禁中。
4. Worker 启动必须声明组织和工作区；scheduler 在 claim 阶段只返回匹配 scope 的任务，
   payload scope 不匹配时 Worker 在查询前 fail closed。
5. Server Profile 原来保留 personal routes，只把 Server routes 放在前面；现在完整替换路由表，
   并验证 settings/debug 等 personal 入口不可达。
6. readiness 原来不必观察 Query Service；现在 `snapshot_query` 的 loopback bind 是必选健康
   探针，非 loopback 配置在启动组合阶段被拒绝。

本轮实际门禁结果：聚焦测试通过、全部 Server Phase + personal replay 回归 `329 passed`、
Ruff check/format 通过、Phase 1AH Compose 集成 `1 passed`。该门禁会启动真实 PostgreSQL、
Redpanda、ClickHouse 和 MinIO 容器，但本轮 Replay/scheduler 路径只实际读写 PostgreSQL；
独立 Worker 进程通过 HTTP 服务边界读取冷页并完成接管，而 collector/writer/archiver 健康
响应和 Query Service 数据响应仍是测试替身。真实 Query Service 到 ClickHouse/MinIO 的合同由
早期 Query 阶段门禁独立覆盖，但尚未在同一个 Phase 1AH 测试中与 Replay Worker 串成一条
完整数据链。

## 4. 明确未完成

- 尚未新增一条同时启动真实 collector、writer、archiver、Query Service 和 Replay Worker 的
  全链 Phase 1AH 集成门禁；
- 没有公网 Binance 24 小时连续性证据；
- `production_ready` 必须保持 false；
- Phase 1AI 的双重开关 soak 尚未运行。

机器可读结果位于 `docs/server/evidence/phase1ah-server-profile-verification.json`。
本轮复核结果另见
`docs/server/evidence/phase1ah-server-profile-remediation-verification.json`；原 evidence 保留为
历史快照，没有改写。
