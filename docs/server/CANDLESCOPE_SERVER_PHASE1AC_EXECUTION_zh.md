# CandleScope Server Phase 1AC：租约保护的快照回放读取

状态：LEASED_SNAPSHOT_REPLAY_BIND_COMPLETE_NOT_WORKER_POOL

Phase 1AC 把 Phase 1AB 的会话租约接到 Phase 1Q 冷快照读取器：只有 **未过期且 fencing 有效** 的 lease 才能物化成交，并且 pin 的 `MarketDataSnapshotRef` 必须等于租约冻结的 snapshot。过期、旧 token 或改 pin 都不得调用查询端口。

```text
ReplaySessionLease.require_active
  -> pin.snapshot == lease.snapshot
  -> ServerSnapshotTradeReader.load (Phase 1Q, cold only)
  -> LeasedServerSnapshotReplay { lease public ref, reader }
```

未租约的 `ServerSnapshotTradeReader.load` 仍供 1Q 测试与 rehearsal 使用，避免把个人/门禁路径伪装成 Worker。本阶段不修改 `ReplayService` / Actor，不启动 Worker 池，也不打开 FastAPI `server` Profile。

## 1. 绑定合同

`candlescope.leased-server-snapshot-replay.v1`：

| 条件 | 结果 |
| --- | --- |
| lease 过期或 token/epoch 陈旧 | `ReplaySessionLeaseFencedError`，查询 0 次 |
| pin.snapshot ≠ lease.snapshot | `ReplaySessionSnapshotConflictError`，查询 0 次 |
| 租约有效且 pin 一致 | 进入既有 1Q 冷分页物化 |

`to_public_ref()` 含 lease 与 snapshot pin，不含 `lease_token`。查询调用仍不得带 `preference` / 热端。

`ReplaySessionLeaseStore` 增加 `require_active`；内存与 PostgreSQL store 均实现。加载函数在调用 1Q 之前必须经过该端口。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 租约保护加载 | `backend/app/server_runtime/replay_leased.py` |
| require_active | `backend/app/server_runtime/replay_lease.py` 与两个 store |
| 单元门禁 | `backend/tests/test_server_phase1ac_leased_snapshot.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ac_leased_snapshot.py \
  backend/tests/test_server_phase1aa_replay_lease.py \
  backend/tests/test_server_phase1q_replay_snapshot.py
```

## 4. 明确未完成

- 没有 Replay Worker 进程池、调度器或 HTTP replay API；
- 没有把该绑定接入 `ReplayService` / Actor 组合根；
- 没有打开 FastAPI `server` Profile。

机器可读结果位于 `docs/server/evidence/phase1ac-leased-snapshot-verification.json`。
