# CandleScope Server Phase 1AF：持久化调度器与 Worker 池

状态：REPLAY_SCHEDULER_POOL_COMPLETE_NOT_EXTERNAL_API

Phase 1AF 增加 PostgreSQL 调度器：只管理 request / assignment / worker heartbeat，不在调度进程内创建 Actor 或读取行情。assignment 使用 `FOR UPDATE SKIP LOCKED`；真正写权仍须通过既有 Replay lease。组织配额、幂等 key、cancel/timeout 与 Worker 心跳过期都由数据库时间驱动。

```text
PENDING -> ASSIGNED -> STARTING -> RUNNING
        -> CANCELLING -> CANCELLED
        -> FAILED
RUNNING -> COMPLETED
```

外部用户仍无 Server replay API，主 Profile 仍关闭。

## 1. 交付物

| 交付物 | 路径 |
| --- | --- |
| 调度迁移 | `deploy/server/postgres/migrations/003_replay_scheduler.sql` |
| 调度合同 | `backend/app/server_runtime/replay_scheduler.py` |
| PostgreSQL store | `backend/app/server_runtime/storage/postgres_replay_scheduler.py` |
| 双 Worker Compose | `deploy/server/compose.phase1af.yml` |
| 单元 / 集成 | `test_server_phase1af_replay_scheduler.py`、`integration/test_server_phase1af_replay_scheduler.py` |

## 2. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1af_replay_scheduler.py \
  backend/tests/test_server_phase1ae_replay_worker.py \
  backend/tests/test_server_phase1aa_replay_lease.py

docker compose -p candlescope-phase1af \
  -f deploy/server/compose.phase1af.yml up -d --wait
CANDLESCOPE_PHASE1AF_INTEGRATION=1 \
  CANDLESCOPE_PHASE1AF_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1af_replay_scheduler.py
docker compose -p candlescope-phase1af \
  -f deploy/server/compose.phase1af.yml down -v
```

## 3. 明确未完成

- 没有外部用户 Server replay API；
- 没有打开 FastAPI `server` Profile；
- 没有公网 24 小时连续性证据。

机器可读结果位于 `docs/server/evidence/phase1af-replay-scheduler-verification.json`。
