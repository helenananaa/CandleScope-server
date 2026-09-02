# CandleScope Server Phase 1AA：回放会话单写者租约

状态：REPLAY_SESSION_LEASE_CONTRACT_COMPLETE_NOT_WORKER_POOL

后续状态：Phase 1AB 已把同一租约写成 PostgreSQL 会话目录，并冻结组织/工作区 pin。1AA 的 fencing 语义仍由内存 store 覆盖；当前目录边界以 `CANDLESCOPE_SERVER_PHASE1AB_EXECUTION_zh.md` 为准。

Phase 1AA 把 unlock blocker `replay_worker_pool` 写成 **会话级单写者租约合同**，而不是 Worker 进程池。一个 `session_id` 同时只能被一个 `worker_id` 持有；过期或主动释放后，新 owner 必须以递增 `fencing_epoch` 接管，并继承首次 acquire 冻结的 `MarketDataSnapshotRef`。不允许在接管时改 pin。

```text
session_id + worker_id + MarketDataSnapshotRef
  -> exclusive lease + UUID token + fencing_epoch
  -> busy while unexpired
  -> expire/release -> epoch+1, same snapshot
  -> stale token cannot renew or write
```

这不是 PostgreSQL 控制面、checkpoint 目录、网关按 session 路由，也不是把租约接到 `ReplayService` / Actor。主 FastAPI `server` Profile 仍 fail closed。内存 store 只证明 fencing 语义，不声称耐久。

## 1. 租约合同

`candlescope.replay-session-lease.v1`：

- `session_id` / `worker_id` 为有界小写标识，拒绝 `*` / `all` / `any` / `default` / `global` / `public` / `shared` / `wildcard`；
- 首次 acquire 的 `fencing_epoch` 为 0，此后每次接管加一；
- `lease_token` 为 UUID，不出现在 `to_public_ref()`；
- `snapshot` 在会话生命周期内不可变；接管若携带不同 manifest 则 `ReplaySessionSnapshotConflictError`；
- 未过期时二次 acquire（含同一 worker）为 `ReplaySessionLeaseBusyError`，必须 `renew`；
- 过期 token / 旧 epoch 的 `renew` / `require_write_fence` 为 `ReplaySessionLeaseFencedError`。

`InMemoryReplaySessionLeaseStore` 用可注入时钟复现上述状态机。PostgreSQL 会话目录见 Phase 1AB；Worker 进程和调度器仍属后续阶段。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 租约合同 | `backend/app/server_runtime/replay_lease.py` |
| 内存 fencing | `backend/app/server_runtime/testing/in_memory_replay_lease_store.py` |
| 单元门禁 | `backend/tests/test_server_phase1aa_replay_lease.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1aa_replay_lease.py \
  backend/tests/test_server_phase1q_replay_snapshot.py \
  backend/tests/test_server_phase1z_fastapi_sqlite_boot.py
```

## 4. 明确未完成

- 没有 Replay Worker 进程池、调度器或 PostgreSQL 会话目录；
- 没有把该租约接入 `ReplayService`、HTTP replay API 或 Actor；
- 没有打开 FastAPI `server` Profile，也没有 24 小时公网连续性或接入网关。

机器可读结果位于 `docs/server/evidence/phase1aa-replay-lease-verification.json`。
