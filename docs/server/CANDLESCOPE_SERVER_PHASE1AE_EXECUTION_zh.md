# CandleScope Server Phase 1AE：PostgreSQL 回放状态与单 Worker 故障接管

状态：REPLAY_WORKER_TAKEOVER_COMPLETE_NOT_SCHEDULER

Phase 1AE 把 Phase 1AD 的内存 fenced mutation 端口写成 PostgreSQL 事务，并增加一个显式 assignment 的单会话 Replay Worker。Worker 被 SIGKILL 后，租约到期、下一 Worker 以递增 fencing epoch 接管，并从 checkpoint + contiguous tail 恢复。旧 token 的迟到写入被同一事务拒绝且 0 行提交。

```text
verify migration and privileges
  -> acquire ReplaySessionLease
  -> load checkpoint + contiguous mutation tail
  -> load leased cold snapshot
  -> reconstruct Actor
  -> renew loop
  -> SIGKILL -> lease expire -> epoch+1 takeover
```

本阶段没有调度器自动分配、没有多 Worker 池，也没有外部用户 API 或 Server Profile 解锁。

## 1. 持久化合同

迁移 `002_replay_runtime.sql` 由部署身份执行；Worker 登录角色只有 DML。每次 mutation 在同一事务内：

1. `pg_advisory_xact_lock(hashtext('replay-session:' || session_id))`；
2. `SELECT ... FOR UPDATE` 当前 lease，并用 `clock_timestamp()` 校验到期；
3. 比对 owner / token / epoch / snapshot / scope；
4. 插入 mutation、command result、event outbox，更新 current state/head。

`lease_token` 不进入 session spec、mutation payload 或 outbox。DSN 不出现在 repr。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 回放运行时迁移 | `deploy/server/postgres/migrations/002_replay_runtime.sql` |
| PostgreSQL session store | `backend/app/server_runtime/storage/postgres_replay_session.py` |
| Worker 配置 / 健康 / 生命周期 | `replay_worker_settings.py`、`replay_worker_health.py`、`replay_worker.py` |
| Worker CLI | `backend/scripts/server_replay_worker.py` |
| Compose | `deploy/server/compose.phase1ae.yml` |
| 单元 / 集成门禁 | `test_server_phase1ae_replay_worker.py`、`integration/test_server_phase1ae_replay_worker_takeover.py` |

## 3. 本地重跑

```bash
docker compose -p candlescope-phase1ae \
  -f deploy/server/compose.phase1ae.yml up -d --wait
CANDLESCOPE_PHASE1AE_INTEGRATION=1 \
  CANDLESCOPE_PHASE1AE_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1ae_replay_worker_takeover.py
docker compose -p candlescope-phase1ae \
  -f deploy/server/compose.phase1ae.yml down -v

PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ae_replay_worker.py \
  backend/tests/test_server_phase1ad_replay_actor.py \
  backend/tests/test_server_phase1ab_replay_lease_postgres.py \
  backend/tests/test_replay_recovery.py \
  backend/tests/test_replay_shutdown.py
```

## 4. 明确未完成

- 没有调度器、Worker 池或队列选择；
- 没有 Server HTTP replay API；
- 没有打开 FastAPI `server` Profile，也没有公网 24 小时连续性证据。

机器可读结果位于 `docs/server/evidence/phase1ae-replay-worker-verification.json`。
