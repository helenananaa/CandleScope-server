# CandleScope Server Phase 1AB：回放会话 PostgreSQL 目录与租户 pin

状态：REPLAY_SESSION_LEASE_DIRECTORY_COMPLETE_NOT_WORKER_POOL

后续状态：Phase 1AC 已要求 1Q 快照物化必须经过未过期会话租约，且 pin 与租约 snapshot 一致。1AB 的 PostgreSQL 目录合同不变；当前读取绑定以 `CANDLESCOPE_SERVER_PHASE1AC_EXECUTION_zh.md` 为准。

Phase 1AB 把 Phase 1AA 的内存租约推进到 **PostgreSQL 会话目录**，并冻结组织/工作区 pin。会话首次 acquire 写入 snapshot 与 `organization_id`/`workspace_id`；接管只能换 `worker_id` 和递增 `fencing_epoch`，不能改 pin。这仍不是 Worker 进程池、调度器或 FastAPI 解锁。

```text
session_id
  -> PostgreSQL row FOR UPDATE + advisory lock replay-session:{id}
  -> snapshot pin + organization/workspace pin
  -> busy while unexpired
  -> expire/release -> epoch+1, same pins
```

内存 store 继续作为同一合同的测试双。公开 `to_public_ref()` 含组织/工作区，不含 `lease_token`。DSN 不出现在 repr。主 FastAPI `server` Profile 仍 fail closed。本阶段单元门禁不连接真实 PostgreSQL。

## 1. 合同升级

`candlescope.replay-session-lease.v2` 在 v1 之上增加：

- 必填 `organization_id` / `workspace_id`，规范化规则与查询身份相同，拒绝通配/保留名；
- 接管时组织或工作区不一致 → `ReplaySessionScopeConflictError`；
- 表 `candlescope_replay_session_lease`：snapshot 与租户列为普通列，不是 JSON 通配；
- 每次所有权转换使用短事务、`FOR UPDATE` 与 `pg_advisory_xact_lock(hashtext('replay-session:' || session_id))`，避免与行情 stream lease 锁空间碰撞。

`PostgresReplaySessionLeaseStore` 实现与内存 store 相同的 acquire/renew/release/inspect 端口。没有接到 `ReplayService` / Actor。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 租约合同 v2 | `backend/app/server_runtime/replay_lease.py` |
| 内存 fencing | `backend/app/server_runtime/testing/in_memory_replay_lease_store.py` |
| PostgreSQL 目录 | `backend/app/server_runtime/storage/postgres_replay_lease.py` |
| 单元门禁 | `backend/tests/test_server_phase1ab_replay_lease_postgres.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1aa_replay_lease.py \
  backend/tests/test_server_phase1ab_replay_lease_postgres.py \
  backend/tests/test_server_phase1y_query_workspace.py
```

## 4. 明确未完成

- 没有 Replay Worker 进程池或调度器；
- 没有把该目录接入 `ReplayService`、HTTP replay API 或 Actor；
- 本阶段未跑真实 PostgreSQL 集成门禁；
- 没有打开 FastAPI `server` Profile。

机器可读结果位于 `docs/server/evidence/phase1ab-replay-lease-postgres-verification.json`。
