# CandleScope Server Phase 1AD：租约保护的回放 Actor 组合根

状态：LEASED_REPLAY_ACTOR_COMPOSITION_COMPLETE_NOT_WORKER

Phase 1AD 把 Phase 1AC 的租约保护冷快照读取接到现有 `ReplaySessionActor`：进程内、可注入依赖的 Server 会话组件在每次可见状态改变时都经过同一原子 mutation 端口的租约校验。personal `ReplayService` 的 aggTrade 路径改为调用共享、无存储依赖的 `AggTradeReplaySessionFactory`。

```text
validate spec and active lease
  -> load_leased_server_snapshot
  -> shared aggTrade Actor factory
  -> start unregistered actor
  -> atomically create durable session under the same lease fence
  -> register actor and publish ready snapshot
```

本阶段不启动独立 Worker、不连接生产 PostgreSQL、不新增外部 API，也不解锁 Server Profile。

## 1. 组合合同

`candlescope.server-replay-session.v1`：

| 条件 | 结果 |
| --- | --- |
| 有效 lease + matching pin | 启动 Actor 并完成 STEP |
| 过期、旧 epoch/token、snapshot 漂移 | 查询前失败，query 次数为 0 |
| 快照加载完成后接管 | 旧 mutation 在 durable commit 时 fenced，Actor 回滚 |
| mutation 失败 | revision / cursor / state hash / component hash / 事件序列回滚 |
| 相同 command_id 重试 | 返回同一 durable 结果 |
| 相同 command revision/sequence 的不同 hash | 完整性冲突 |
| public ref / repr / 日志 | 不含 `lease_token` |

`commit_mutation` 在一次原子操作中验证当前租约并提交；持久化 spec 只保存 lease public ref。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 共享 aggTrade Actor 工厂 | `backend/app/replay/session_factory.py` |
| Server 会话与 mutation 端口 | `backend/app/server_runtime/replay_session.py` |
| 内存 fenced mutation store | `backend/app/server_runtime/testing/in_memory_replay_session_store.py` |
| 单元门禁 | `backend/tests/test_server_phase1ad_replay_actor.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ad_replay_actor.py \
  backend/tests/test_server_phase1ac_leased_snapshot.py \
  backend/tests/test_server_phase1q_replay_snapshot.py \
  backend/tests/test_replay_trade_service.py \
  backend/tests/test_replay_recovery.py \
  backend/tests/test_replay_shutdown.py \
  backend/tests/test_server_phase0_architecture.py \
  backend/tests/test_server_phase1z_fastapi_sqlite_boot.py
```

## 4. 明确未完成

- 没有 Replay Worker 进程、调度器或外部 replay API；
- 没有 PostgreSQL 会话/mutation 目录；
- 没有打开 FastAPI `server` Profile，也没有公网 24 小时连续性证据。

机器可读结果位于 `docs/server/evidence/phase1ad-replay-actor-verification.json`。
