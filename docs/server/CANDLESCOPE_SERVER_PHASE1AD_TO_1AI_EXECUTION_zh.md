# CandleScope Server Phase 1AD–1AI：回放服务落地执行计划

状态：IMPLEMENTED_THROUGH_PHASE_1AH_REMEDIATED_NOT_PRODUCTION_READY

原规划基线：Phase 1AC 已完成租约保护的冷快照成交读取，但尚未把该读取接入
`ReplayService` / `ReplaySessionActor` 组合根，也没有 Replay Worker、调度器、
Server HTTP replay API、可启动的 FastAPI `server` Profile 或公网 Binance 24 小时
连续性证据。

2026-09-04 核验更新：Phase 1AD–1AH 的实现、阶段文档和历史 evidence 已存在；本轮又完成
Phase 1AH 接入面复核与修复。当前工作树已经：

- 把 Replay Worker 的默认行情读取从客户端可控本地 `query_path` 改成带 bearer、组织和
  工作区范围的独立 Query Service HTTP 客户端，并强制使用 `preference=cold`；
- 将 Worker 的组织/工作区范围变成启动必填项，并在 PostgreSQL `SKIP LOCKED` 领取时过滤，
  不允许带某租户 Query 凭据的 Worker 领取另一租户任务；
- 在 API 入队时写入经过身份验证的权威 scope，并拒绝客户端提交 `query_path`；
- 在取消前完成 scope 授权，避免跨租户请求先改变状态再返回 403；
- 实现按组织/工作区隔离的 PostgreSQL run list；
- 让 Server Profile 替换 personal routes，而不是把 Server routes 插到 personal routes 前面；
- 把 Query Service 纳入 Server Profile readiness 必选探针。

本轮聚焦/全量单元回归、Ruff 和 Phase 1AH Compose 进程接管门禁均通过。补丁仍未提交；
Phase 1AI 公网 Binance 24 小时连续性与故障注入没有开始，因此仍不得声称生产就绪。

本文把上述缺口拆成六个必须顺序通过的阶段：

```text
Phase 1AD 回放 Actor 组合根
  -> Phase 1AE PostgreSQL 持久化与单 Worker 接管
  -> Phase 1AF 多 Worker 调度器
  -> Phase 1AG Server API、认证与 WS
  -> Phase 1AH Server Profile 组合与启动
  -> Phase 1AI 公网 Binance 24 小时验收
```

阶段编号和边界从本文开始作为实施计划使用；在对应代码、测试和机器可读证据全部
落地前，不得把任一阶段标记为 COMPLETE。Phase 1AI 通过前仍不得声称生产就绪。

## 1. 最终结果与首个可交付切片

最终结果是一个保留现有确定性回放领域模型、但把所有权、持久化、调度和接入面迁到
服务器边界的纵向闭环：

```text
authenticated user
  -> FastAPI /api/v1 replay DTO
  -> PostgreSQL replay scheduler and command journal
  -> fenced Replay Worker
  -> one ReplaySessionActor per session
  -> leased immutable MarketDataSnapshotRef
  -> cold Parquet snapshot query
  -> durable checkpoint / mutation / event outbox
  -> HTTP response and WebSocket projection
```

首个可交付切片保持有意收窄：

- 只支持 `binance:futures:BTCUSDT@agg_trade`；
- 只支持调用方已经明确固定的 `MarketDataSnapshotRef`，禁止 `latest` 或 version 0；
- 只读 Phase 1Q/1AC 冷端，不使用热 ClickHouse、不回退实时行情或个人归档；
- `source_kind=agg_trade`、`quality_mode=exact`、`blind_mode=false`；
- 第一刀允许 `warmup_bars=0`，直到服务器 warmup 数据合同另行完成；
- 只复用现有 `ReplaySessionActor`、broker、命令、checkpoint 和外部 DTO，不复制领域模型。

这组限制必须体现在 capability 和拒绝错误中，不能由文档约定但代码默许。

## 2. 全程不可破坏的合同

### 2.1 单写者与 fencing

1. 一个 `session_id` 同时只能由一个持有有效租约的 Worker 推进。
2. Worker 的每次 durable mutation 必须在同一个 PostgreSQL 事务内验证
   `worker_id`、`fencing_epoch`、`lease_token`、数据库时间、snapshot、组织和工作区。
3. 不允许先调用 `require_active()`、释放事务，再在另一个事务中提交 mutation；该做法
   存在检查与写入之间的接管窗口。
4. Actor 只有在 mutation、checkpoint、命令结果和待发布事件全部 durable commit 后才能
   ack 或向订阅者发布。
5. 续租失败、数据库时间已过期或 fencing 不可证明时，Worker 立即停止接受新命令并关闭
   Actor；旧 Worker 的迟到写入仍必须被数据库事务拒绝。

### 2.2 数据与租户 pin

以下字段在一个会话的完整生命周期中不可改变：

- `data_epoch`
- `snapshot_version`
- `manifest_uri`
- `manifest_sha256`
- `organization_id`
- `workspace_id`
- 规范化 market stream
- 回放事件时间范围和 agg_trade ID 闭区间
- 回放配置、broker 配置、代码/合同版本

接管只能改变 Worker 所有者、租约 token、到期时间和递增的 fencing epoch。任何 pin 漂移
都必须在读取行情或恢复 Actor 之前失败。

### 2.3 Profile 与依赖方向

- `personal` 始终是默认 Profile，并继续使用现有 SQLite、本地文件和进程内组合。
- `server` 不得初始化 `ReplaySQLiteStore`、K 线 SQLite、进程内行情总线或本地文件权威源。
- `app.replay` 不得导入 `app.server_runtime`；服务器适配器依赖回放领域端口。
- Profile 差异只存在于组合根和端口适配器中，不能把 `if server_mode` 散落到 Actor、
  broker 或命令处理逻辑。
- Phase 1AH 之前，`DeploymentSettings.runtime_supported` 对 `server` 必须继续返回 false。

### 2.4 安全与公开信息

- `lease_token`、数据库 DSN、内部 bearer、OIDC/JWKS 凭据和对象存储凭据不得进入公开 DTO、
  repr、日志、审计 payload 或 evidence。
- 用户请求中的组织/工作区只用于一致性校验；权威 scope 必须来自已验证身份。
- 内部查询凭据、Worker 凭据和用户 API 凭据必须分开，不能复用同一个 token。
- 所有请求体、结果、checkpoint、事件 outbox、分页和查询扫描都有硬上限。
- API/Worker 对外错误使用稳定错误码，依赖异常和路径信息只进入脱敏内部日志。

## 3. 开始实施前的工作区准备

当前服务器分支可能同时保留插件/发布改动。每个 Phase 都必须使用独立 worktree，或者使用
显式文件 allowlist；禁止 `git add -A`、`git add .` 和隐式带入无关修改。

### 步骤 3.1：记录基线

```bash
git status --short --branch
git diff --name-only
git rev-parse HEAD
git remote -v
```

把 HEAD、分支、未提交文件清单和执行日期记录到该阶段 evidence。若现有未提交文件不属于
Server Phase，保持原样，不格式化、不暂存、不覆盖。

### 步骤 3.2：创建独立 worktree

确认目标路径和分支不存在后执行：

```bash
git fetch server-origin
git worktree add ../CandleScope-server-phase1ad \
  -b codex/server-phase1ad-replay-actor server-origin/main
cd ../CandleScope-server-phase1ad
```

后续阶段从前一阶段已通过门禁的提交创建新分支，不从含插件未提交改动的工作树复制文件。

### 步骤 3.3：运行基线门禁

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase*.py
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_replay_service.py \
  backend/tests/test_replay_trade_service.py \
  backend/tests/test_replay_recovery.py \
  backend/tests/test_replay_shutdown.py
git diff --check
```

基线失败时先记录并定位，不得把既有失败混写成新阶段已通过。

## 4. Phase 1AD：租约保护的回放 Actor 组合根

状态目标：`LEASED_REPLAY_ACTOR_COMPOSITION_COMPLETE_NOT_WORKER`

本阶段只完成一个进程内、可注入依赖的 Server 会话组件。它不启动独立 Worker、不连接
生产 PostgreSQL、不新增外部 API，也不解锁 Server Profile。

### 4.1 计划交付物

| 交付物 | 计划路径 |
| --- | --- |
| 共享 aggTrade Actor 工厂 | `backend/app/replay/session_factory.py` |
| Server 会话与 mutation 端口 | `backend/app/server_runtime/replay_session.py` |
| 内存 fenced mutation store | `backend/app/server_runtime/testing/in_memory_replay_session_store.py` |
| 单元门禁 | `backend/tests/test_server_phase1ad_replay_actor.py` |
| 阶段记录 | `docs/server/CANDLESCOPE_SERVER_PHASE1AD_EXECUTION_zh.md` |
| 机器证据 | `docs/server/evidence/phase1ad-replay-actor-verification.json` |

### 4.2 实施步骤

#### 步骤 1：先抽取共享 Actor 工厂，不改变 personal 行为

1. 从 `ReplayService._actor()` 和 aggTrade 分支的 `_broker()` 中抽取公开、无存储依赖的
   `AggTradeReplaySessionFactory`。
2. 工厂只接收领域输入：`ReplaySessionConfig`、`BrokerConfig`、
   `ReplayTradePageReader`/source factory、明确的回放起止边界、warmup bars、Actor 限额、
   restore checkpoint、recovery target 和 mutation hook。
3. 服务器首切片要求开始时间对齐 `base_interval`，结束时间是完整 base interval 的最后
   1 毫秒；Phase 1Q pin 覆盖同一时间窗，首末成交可以位于边界内部。
4. 让现有 `ReplayService` 的 aggTrade 路径调用该工厂，删除重复构造代码。
5. 运行现有 replay service、trade、checkpoint、recovery 和 shutdown 测试，证明该抽取是
   行为等价重构。

禁止把 `ReplaySQLiteStore`、`ReplaySettings` 全局单例或 `app.server_runtime` 引入工厂。

#### 步骤 2：冻结 Server 启动合同

在 `replay_session.py` 定义严格的 `ServerReplaySessionSpec`，至少包含：

- 完整 `ReplaySessionLease`；
- `ReplayServerSnapshotPin`；
- `ReplaySessionConfig` 与 `BrokerConfig`；
- 明确的 `replay_start_ms` / `replay_end_time_ms`；
- command queue、event buffer、checkpoint cadence 和最大保留量；
- schema version 和代码版本。

构造时逐项校验：

1. lease 与 pin 的 snapshot 一致，config 与 pin 的 market identity 一致，lease scope 有效；
2. `source_kind`、quality、blind、warmup 和 interval 落在首切片 allowlist；
3. 时间范围完整对齐且包含 pin 中所有成交；
4. 所有整数和容量有上下界；
5. public ref 不含 lease token。

#### 步骤 3：定义原子 mutation 端口

定义 `ServerReplaySessionStore` Protocol，至少提供：

```text
create_session(lease, spec, initial_checkpoint, state)
commit_mutation(lease, ActorMutation)
load_recovery(session_id, lease)
read_session(session_id, caller_scope)
close_session(lease, terminal_state)
```

`commit_mutation` 的合同必须明确：实现方在一次原子操作中验证当前租约并提交 mutation；
重复 `command_id` 返回原 durable 结果；相同 revision/sequence 的不同 hash 为完整性冲突；
失败时 Actor mutation hook 抛错，让 Actor 回滚候选状态且不发布事件。

session spec 的持久化副本只能保存 lease public ref；`lease_token` 只存在于当前 lease 行和
Worker 私有内存，不能复制到 session、mutation、checkpoint 或 outbox 表。

#### 步骤 4：组合 `ServerReplaySession`

固定启动顺序：

```text
validate spec and active lease
  -> load_leased_server_snapshot
  -> TradeReplaySource(reader)
  -> shared aggTrade Actor factory
  -> start an unregistered actor
  -> read its initial checkpoint and state
  -> atomically create the durable session under the same lease fence
  -> register actor and publish ready snapshot
```

若 durable create 失败，必须物理取消尚未注册的 Actor，不提交 shutdown mutation，也不把
session 暴露给调用方；若 create 已提交，则后续注册/返回路径不得留下调用方无法发现的孤儿。

固定停止顺序：

```text
stop accepting commands
  -> actor mailbox drain with bounded timeout
  -> fenced final checkpoint/state commit
  -> stop actor tasks
  -> do not release an ownership lease that can no longer be proven
```

读取 snapshot、提交命令和订阅事件全部通过该会话对象，不向上层暴露裸 Actor。

#### 步骤 5：完成负面和恢复测试

单元测试至少覆盖：

- 有效 lease + matching pin 可以启动 Actor 并完成一条 STEP 命令；
- 过期、旧 epoch/token、snapshot 漂移或 scope 漂移在查询前失败；
- 快照加载完成后发生接管，旧 mutation 在 durable commit 时失败；
- mutation 失败后 Actor revision、cursor、state hash、component hash 和事件序列全部回滚；
- 相同 command ID 重试返回同一 durable 结果；
- public ref、异常、repr 和日志不泄露 token；
- `app.replay` 的导入图不出现 `app.server_runtime`；
- personal `ReplayService` 的行为和 checkpoint bytes 保持兼容；
- `CANDLESCOPE_PROFILE=server` 继续拒绝启动。

### 4.3 Phase 1AD 门禁

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
uvx ruff@0.16.2 check \
  backend/app/replay/session_factory.py \
  backend/app/server_runtime/replay_session.py \
  backend/app/server_runtime/testing/in_memory_replay_session_store.py \
  backend/tests/test_server_phase1ad_replay_actor.py
uvx ruff@0.16.2 format --check \
  backend/app/replay/session_factory.py \
  backend/app/server_runtime/replay_session.py \
  backend/app/server_runtime/testing/in_memory_replay_session_store.py \
  backend/tests/test_server_phase1ad_replay_actor.py
backend/.venv/bin/python -m json.tool \
  docs/server/evidence/phase1ad-replay-actor-verification.json >/dev/null
git diff --check
```

退出条件：Actor 的每次可见状态改变都经过租约保护的 durable hook；Server 读取和 personal
回放均通过回归；仍没有 Worker、调度器、API 或 Profile 解锁声明。

## 5. Phase 1AE：PostgreSQL 回放状态与单 Worker 故障接管

状态目标：`REPLAY_WORKER_TAKEOVER_COMPLETE_NOT_SCHEDULER`

### 5.1 计划交付物

| 交付物 | 计划路径 |
| --- | --- |
| 回放运行时迁移 | `deploy/server/postgres/migrations/002_replay_runtime.sql` |
| PostgreSQL session store | `backend/app/server_runtime/storage/postgres_replay_session.py` |
| Worker 配置 | `backend/app/server_runtime/replay_worker_settings.py` |
| Worker 生命周期 | `backend/app/server_runtime/replay_worker.py` |
| Worker 健康合同 | `backend/app/server_runtime/replay_worker_health.py` |
| Worker CLI | `backend/scripts/server_replay_worker.py` |
| 集成 Compose | `deploy/server/compose.phase1ae.yml` |
| 单元门禁 | `backend/tests/test_server_phase1ae_replay_worker.py` |
| 跨进程门禁 | `backend/tests/integration/test_server_phase1ae_replay_worker_takeover.py` |
| 阶段记录与证据 | `docs/server/CANDLESCOPE_SERVER_PHASE1AE_EXECUTION_zh.md`、`docs/server/evidence/phase1ae-replay-worker-verification.json` |

### 5.2 PostgreSQL 模型

迁移 002 必须版本化并由部署身份执行；Worker 运行身份只有最小 DML 权限，不得拥有
`CREATE`、`ALTER`、`DROP` 或 superuser 权限。迁移至少覆盖：

- 既有 `candlescope_replay_session_lease` 的版本与列校验；
- immutable session spec 与 snapshot/scope pin；
- 当前 durable state 摘要；
- append-only mutation journal；
- checkpoint bytes、hash、source sequence 和 command log offset；
- command 结果幂等索引；
- 待发布事件 outbox；
- 独立 schema version 表或与既有迁移注册表一致的版本记录。

每次 mutation 事务按以下顺序执行：

1. 取得 `replay-session:{session_id}` 的事务级 advisory lock；
2. `SELECT ... FOR UPDATE` 当前 lease，使用 `clock_timestamp()` 校验到期；
3. 比对 owner、token、epoch、snapshot 和 scope；
4. 锁定 session state，并校验 expected revision、sequence、command log offset 和 previous hash；
5. 插入 mutation、checkpoint、command result 和 event outbox；
6. 更新 current state/head；
7. commit 后 Actor 才能 ack/publish。

任何一步失败都回滚整个事务。不得把 checkpoint 放在 Worker 本地磁盘后再更新 PostgreSQL
指针；若未来把大 checkpoint 外置对象存储，必须先做 immutable put + hash 校验，并在同一
数据库事务内发布引用。

### 5.3 Worker 生命周期

#### 步骤 1：严格解析配置

至少定义：Worker ID、PostgreSQL DSN、内部 query endpoint/credential、lease TTL、renew
interval、最大 Actor 数、最大并发恢复数、poll interval、shutdown timeout 和可选 loopback
health bind。要求：

- `lease_ttl_ms >= 3 * renew_interval_ms`；
- timeout 和容量均为有界正数；
- health 只允许 loopback；
- DSN/credential 不进入 repr 或 wire；
- 缺少任一 server 依赖时拒绝启动。

#### 步骤 2：实现单会话 Worker

Phase 1AE 的 Worker 从测试/运维明确提供的 assignment 启动，不实现队列选择。启动顺序：

```text
verify migration and privileges
  -> acquire ReplaySessionLease
  -> load and validate immutable spec
  -> load latest valid checkpoint plus contiguous mutation tail
  -> load leased cold snapshot
  -> reconstruct Actor
  -> compare recovery target hashes
  -> start renew loop
  -> report ready
```

新会话先 durable create，再发布 ready。恢复会话必须依次比较 cursor、source event chain、
state hash、component hash、订单、成交和账本，不得只比较最终权益。

#### 步骤 3：处理续租与停止

- 每次 renew 使用数据库返回的租约对象替换内存对象；
- renew 超时、fenced 或数据库不可达时进入 terminal/fenced，停止收命令；
- 不确定是否仍持有租约时不得调用 release；
- 正常关闭且所有权仍可证明时，先提交 final checkpoint，再 release；
- SIGTERM 执行有界 drain；SIGKILL 依赖租约到期和下一 Worker 接管；
- 迟到查询结果或后台任务不能越过已触发的 fenced 状态提交 mutation。

#### 步骤 4：增加健康证据

健康 wire 至少包含：Worker ID、状态、active Actor 数、租约到期时间、最近成功续租时间、
最近 mutation/checkpoint 时间、checkpoint age、恢复次数/失败数、fencing 冲突数和脱敏的
最后错误码。`ready=true` 必须同时满足迁移、数据库、query 端口和租约循环可用。

#### 步骤 5：真实跨进程接管

集成测试固定执行：

1. 启动专用 PostgreSQL、Redpanda、ClickHouse、MinIO；
2. 写入一个 Phase 1Q 可读取的冻结 snapshot；
3. Worker A 获取 epoch 0，执行多条命令并至少写入一个 checkpoint；
4. 在一条已提交命令后 SIGKILL Worker A；另设一个用例在 mutation commit 前注入退出；
5. 等待数据库租约到期，Worker B 以 epoch 1 接管；
6. Worker B 从 checkpoint + tail 恢复并继续执行下一条命令；
7. 使用 Worker A 的旧 token 尝试提交，确认 0 行写入且得到 fenced 错误；
8. 比较接管前后完整 Actor 权威状态和事件链；
9. 确认命令重试只有一个 durable 结果且 outbox sequence 连续。

### 5.4 Phase 1AE 门禁

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
git diff --check
```

`ALLOW_TEST_RESET` 只能接受上述专用 Compose project 和明确的 localhost 端口；测试清理不得
连接非本机数据库、bucket 或 topic。

退出条件：独立 Worker 可在真实 PostgreSQL/对象存储栈上被 SIGKILL 并确定性接管；仍没有
自动分配、多 Worker 调度或外部 API。

## 6. Phase 1AF：PostgreSQL 持久化调度器与 Worker 池

状态目标：`REPLAY_SCHEDULER_POOL_COMPLETE_NOT_EXTERNAL_API`

### 6.1 计划交付物

| 交付物 | 计划路径 |
| --- | --- |
| 调度迁移 | `deploy/server/postgres/migrations/003_replay_scheduler.sql` |
| 调度合同 | `backend/app/server_runtime/replay_scheduler.py` |
| PostgreSQL 调度 store | `backend/app/server_runtime/storage/postgres_replay_scheduler.py` |
| 调度配置与健康 | `backend/app/server_runtime/replay_scheduler_settings.py`、`replay_scheduler_health.py` |
| 调度 CLI | `backend/scripts/server_replay_scheduler.py` |
| 双 Worker Compose | `deploy/server/compose.phase1af.yml` |
| 单元/集成门禁 | `backend/tests/test_server_phase1af_replay_scheduler.py`、`backend/tests/integration/test_server_phase1af_replay_scheduler.py` |
| 阶段记录与证据 | `docs/server/CANDLESCOPE_SERVER_PHASE1AF_EXECUTION_zh.md`、`docs/server/evidence/phase1af-replay-scheduler-verification.json` |

### 6.2 状态机与幂等

冻结以下单向状态机：

```text
PENDING -> ASSIGNED -> STARTING -> RUNNING
   |          |           |          |
   +-------> CANCELLING ------------> CANCELLED
   +--------------------------------> FAILED
RUNNING --------------------------------> COMPLETED
```

终态不可回到运行态。重试必须创建新的 attempt，并保留原 attempt 审计；同一客户端
idempotency key + 相同 payload 返回原 session，payload 不同则冲突。

### 6.3 实施步骤

1. 迁移 003 建立 request、assignment、worker registry/capacity、command inbox/result、
   cancellation/timeout 和 scheduler audit 表；所有 scope 列显式存储，不放入可通配 JSON。
2. Scheduler 只管理元数据和分配，不在自身进程内创建 Actor 或读取市场数据。
3. Worker 以 heartbeat 发布可用容量；过期 Worker 不参与新分配。
4. assignment 使用事务、`FOR UPDATE SKIP LOCKED` 和唯一约束保证一个 session 只有一个活跃
   attempt；Worker 最终仍须通过 Phase 1AA/1AB lease 获取真正写权。
5. 组织/工作区配额至少限制：pending 数、active session 数、每 session 资源上限和命令速率。
6. 优先级只能在同一授权 scope 和容量约束内生效；实现有界 aging，防止普通任务永久饥饿。
7. cancel 先写 durable intent，再由当前 Worker checkpoint 并转终态；Worker 不可达时等待租约
   到期，由接管者完成取消。
8. timeout 使用数据库时间；Scheduler 重启后必须继续扫描，不依赖进程内 timer。
9. 命令 inbox 按 `(session_id, command_id)` 幂等，Worker 只执行已绑定到自己有效 lease 的命令。
10. 每次状态转换写不含凭据和行情 payload 的结构化审计记录。

### 6.4 测试矩阵

- 两个 Worker 竞争同一 session，只有一个获得 lease；
- 两个 session 可分配到不同 Worker，Actor 状态互不共享；
- Worker 容量耗尽时请求保持 PENDING，不超卖；
- 同组织超过 quota 被稳定错误码拒绝，另一组织不受影响；
- 高优先级先运行，普通优先级通过 aging 最终获得调度；
- Scheduler 在 PENDING、ASSIGNED、CANCELLING 各状态重启后可恢复；
- Worker 心跳过期后停止新分配，并触发已有会话接管；
- cancel/timeout 与命令提交并发时只有一个合法终态；
- idempotency key 重放不创建第二个 session；
- PostgreSQL 故障时停止新分配，不在内存继续“成功”。

### 6.5 Phase 1AF 门禁

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
git diff --check
```

退出条件：两个以上 Worker 可由持久化调度器安全分配、取消、超时和接管；外部用户仍无
Server replay API，主 Profile 仍关闭。

## 7. Phase 1AG：Server HTTP replay API、认证、授权与 WebSocket

状态目标：`SERVER_REPLAY_API_COMPLETE_PROFILE_STILL_LOCKED`

### 7.1 计划交付物

| 交付物 | 计划路径 |
| --- | --- |
| Replay application port | `backend/app/replay/application.py` |
| Server facade | `backend/app/server_runtime/replay_api_service.py` |
| Server 身份与权限 | `backend/app/server_runtime/access_identity.py`、`replay_authorization.py` |
| durable WS outbox reader | `backend/app/server_runtime/replay_event_stream.py` |
| API 组合 | `backend/app/server_runtime/replay_api_composition.py` |
| API/安全/WS 门禁 | `backend/tests/test_server_phase1ag_replay_api.py`、`backend/tests/integration/test_server_phase1ag_replay_api.py` |
| 阶段记录与证据 | `docs/server/CANDLESCOPE_SERVER_PHASE1AG_EXECUTION_zh.md`、`docs/server/evidence/phase1ag-replay-api-verification.json` |

### 7.2 先建立应用端口

1. 从 `backend/app/api/v1/replay.py` 和 `stream_replay.py` 实际调用的方法抽出
   `ReplayApplication` Protocol。
2. personal 适配器继续委托现有 `ReplayService`；输出 JSON 必须保持兼容。
3. server 适配器只调用 Scheduler、session store、command journal 和 outbox，不持有 Actor。
4. FastAPI router 依赖该端口，不直接类型检查 `ReplayService`，也不读取 Worker 内存。
5. 对未迁移的高级 replay.v2 能力在 capabilities 中明确关闭，并返回稳定的
   `CAPABILITY_UNAVAILABLE`；不得回退 personal runtime。

首个 Server API 必须闭环支持当前前端运行一个会话所需的最小路径：

| 路径 | Server 行为 |
| --- | --- |
| `GET /api/v1/replay/capabilities` | 返回真实 server capability、限制和降级原因 |
| `GET /api/v1/replay/catalog` | 只返回可固定的 server snapshot/market 范围 |
| `POST /api/v1/replay/runs` | 幂等创建调度 request，返回 PENDING/ASSIGNED 状态 |
| `GET /api/v1/replay/runs` | 只列出调用方 scope 内的 runs |
| `GET /api/v1/replay/runs/{run_id}` | 返回调度和会话状态 |
| `GET /api/v1/replay/runs/session/{session_id}` | 返回 durable 权威 snapshot |
| `POST /api/v1/replay/runs/session/{session_id}/commands` | 写入幂等 command 并返回 durable result |
| `DELETE /api/v1/replay/runs/{run_id}` | durable cancel，不直接杀 Actor |
| `WS /api/v1/stream/replay/{session_id}` | 从 durable outbox 按 sequence 恢复和续传 |

其余 replay.v2 路径必须由一份自动生成/测试的 endpoint inventory 标记为“已实现”或“明确
不可用”，不能意外落入 personal service。

### 7.3 认证与授权

1. 定义 `ServerPrincipal`：subject、organization、team、workspace、user/service-account 类型、
   roles 和 credential ID；不保存原 token。
2. 定义 `IdentityVerifier` 端口；生产适配器验证 issuer、audience、签名、算法 allowlist、
   `exp`/`nbf` 和 key rotation。JWKS 获取必须限制 scheme/host、超时、响应大小和缓存。
3. 客户端提交的 organization/workspace 必须等于 principal scope；缺失、通配或不等全部拒绝。
4. 角色至少覆盖 admin、researcher、trader、read-only。read-only 可查看，不能创建、控制、
   cancel 或删除；权限矩阵写成参数化测试。
5. WebSocket 握手执行相同认证和 scope 校验；连接后每次恢复仍重新验证 session scope。
6. 用户身份、服务间 query 身份和 Worker 身份使用不同 verifier/audience。
7. 认证拒绝、授权拒绝、创建、命令、取消和导出均写审计；不记录 token、完整请求体、行情
   payload 或私有 checkpoint。

### 7.4 WebSocket 一致性与反压

- 首包必须是与 durable session head 对齐的原子 snapshot；
- `after_sequence` 和 `data_epoch` 必须绑定同一会话/快照；
- 断线重连从 outbox 恢复，不要求连接回原 Worker；
- outbox gap、epoch 漂移或 retention 越界发送 reset snapshot，不能猜测增量；
- 每连接使用有界队列，慢客户端被明确断开或 reset，不反压 Worker；
- API 返回丢失后，相同 command ID 重试读取原 durable result。

### 7.5 Phase 1AG 门禁

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ag_replay_api.py \
  backend/tests/test_replay_api.py \
  backend/tests/test_replay_stream.py \
  backend/tests/test_server_phase1x_query_identity.py \
  backend/tests/test_server_phase1y_query_workspace.py

docker compose -p candlescope-phase1ag \
  -f deploy/server/compose.phase1ag.yml up -d --wait
CANDLESCOPE_PHASE1AG_INTEGRATION=1 \
  CANDLESCOPE_PHASE1AG_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1ag_replay_api.py

cd frontend
npm run test:replay
cd ..

docker compose -p candlescope-phase1ag \
  -f deploy/server/compose.phase1ag.yml down -v
git diff --check
```

退出条件：独立 Server API 可通过认证后的现有 DTO 创建、查看、控制和流式读取一个调度
会话；主 `CANDLESCOPE_PROFILE=server` 仍保持锁定。

## 8. Phase 1AH：完整 Server Profile 组合与启动门禁

状态目标：`SERVER_PROFILE_RUNTIME_COMPLETE_NOT_PRODUCTION_READY`

### 8.1 计划交付物

| 交付物 | 计划路径 |
| --- | --- |
| personal/server lifespan 拆分 | `backend/app/deployment/personal_runtime.py`、`server_runtime.py` |
| Server 应用组合 | `backend/app/server_runtime/application.py` |
| 完整组合检查 | 扩展 `backend/app/server_runtime/composition.py` |
| Server Compose | `deploy/server/compose.phase1ah.yml` |
| Profile 门禁 | `backend/tests/test_server_phase1ah_profile.py` |
| 纵向集成门禁 | `backend/tests/integration/test_server_phase1ah_profile.py` |
| 阶段记录与证据 | `docs/server/CANDLESCOPE_SERVER_PHASE1AH_EXECUTION_zh.md`、`docs/server/evidence/phase1ah-server-profile-verification.json` |

### 8.2 重构启动边界

1. 把 `main.py` 中现有 personal startup/shutdown 原样移动到 personal lifespan owner，先用回归
   测试证明默认行为不变。
2. 建立独立 server lifespan owner；它只依赖 server 端口和显式 settings，不执行 personal
   初始化函数。
3. 路由和 DTO 可以共享，runtime state 通过应用端口绑定；不能共享 SQLite store 或进程内
   DataManager 权威状态。
4. `load_deployment_settings()` 必须在任何数据库、任务或监听 socket 创建前完成。
5. 只有 Server 组合检查和集成门禁通过后，才把 `runtime_supported` 对 `server` 改为 true。
6. 保留并升级 `refuse_server_sqlite_boot()`：从“永远拒绝 server”转为验证 server 组合中没有
   SQLite/local/in-process fallback 的负面门禁。

### 8.3 Server 启动顺序

固定为：

```text
parse all settings
  -> verify PostgreSQL migrations and least-privilege roles
  -> verify Redpanda/Kafka, ClickHouse and object-store identity agreement
  -> start query control/router
  -> start scheduler control loop
  -> observe minimum healthy Worker capacity
  -> bind replay application port
  -> bind HTTP/WS listener
  -> readiness=true
```

Collector、writer 和 archiver 可以由独立进程启动，但 API readiness 必须读取它们的真实健康
和 caught-up 状态。任一关键依赖在启动时身份漂移、迁移漂移、权限过大/不足或不可达，整个
Server API 拒绝 ready；不得降级到 personal。

### 8.4 Readiness 与运行时降级

- liveness 只表示进程事件循环仍工作；
- readiness 同时要求 PostgreSQL、query、scheduler、Worker capacity 和迁移合同有效；
- 数据链 lagging 可以返回明确 stale/capability 状态，但不可伪装 caught-up；
- PostgreSQL 控制面故障时，停止创建、控制和未审计读取；
- Worker 全部丢失时，已有会话显示 recovering/unavailable，新请求保持排队或被配额拒绝；
- 对象存储/冷查询不可用时，不切换到热端或 personal 数据；
- 恢复后必须重新验证迁移、权限、snapshot pin 和租约，不能仅因 TCP 恢复而 ready。

### 8.5 完整 Compose 冒烟步骤

1. 启动 PostgreSQL、Redpanda、ClickHouse、MinIO；
2. 运行版本化迁移；
3. 启动 collector、writer、archiver、query、scheduler 和至少两个 Replay Worker；
4. 以 `CANDLESCOPE_PROFILE=server` 启动 FastAPI；
5. 用认证测试身份创建 session，等待 RUNNING；
6. 执行命令并通过 WebSocket 收到相同 revision/sequence；
7. SIGKILL 当前 Worker，确认 API 显示 recovering，接管后从下一 sequence 继续；
8. 重启 API，确认 session 和 command result 不丢失；
9. 扫描进程打开文件、环境与诊断输出，确认没有 SQLite DB、本地 replay archive 或 token；
10. 切回默认无环境变量启动，确认仍为 personal Profile。

### 8.6 Phase 1AH 门禁

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ah_profile.py \
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

PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase*.py
git diff --check
```

退出条件：Server Profile 可以在完整外部栈上启动并完成一次认证回放及 Worker 接管；状态仍
必须明确为“runtime complete, not production ready”，因为公网 24 小时证据尚不存在。

## 9. Phase 1AI：公网 Binance 24 小时连续性与故障注入

状态目标：`PUBLIC_24H_REPLAY_CHAIN_VERIFIED`

本文中的“公网”指从 Binance 公网行情源持续采集，不表示把 CandleScope API 匿名暴露到
互联网。API 仍须处于受认证和受限网络边界内。

### 9.1 计划交付物

| 交付物 | 计划路径 |
| --- | --- |
| 24h 编排与监督器 | 扩展 `backend/app/server_runtime/soak_supervisor.py` |
| 运行 CLI | 扩展 `backend/scripts/server_phase1u_soak_supervisor.py` 或新增 `server_phase1ai_public_soak.py` |
| 故障注入记录器 | `backend/app/server_runtime/soak_faults.py` |
| 部署清单 | `deploy/server/compose.phase1ai.yml` 与环境模板 |
| 门禁测试 | `backend/tests/test_server_phase1ai_public_soak.py` |
| 阶段记录与证据 | `docs/server/CANDLESCOPE_SERVER_PHASE1AI_EXECUTION_zh.md`、`docs/server/evidence/phase1ai-public-24h-verification.json` |

### 9.2 运行前冻结输入

在启动 24 小时时先生成不可修改的 run manifest，记录：

- git commit、dirty=false、镜像 digest 和 Python/OS/依赖版本；
- 主机、CPU、内存、磁盘和网络摘要，不记录用户名或私有路径；
- PostgreSQL/Redpanda/ClickHouse/MinIO 版本和逻辑 cluster identity；
- market stream、起止时间、目标时长、snapshot 和数据保留配置；
- 每个故障注入的计划时间、目标角色、方法和最大等待；
- lease TTL、checkpoint cadence、配额和所有超时；
- 预先定义的验收条件；运行后不得为了通过而改阈值。

启动前必须验证系统时钟同步、磁盘余量、对象存储写入/读取、告警渠道、备份监控和所有
health endpoint。缺一项则不开始计时。

### 9.3 24 小时执行步骤

1. 以正常生产式配置启动完整 Phase 1AH 栈，确认所有角色 ready。
2. 启动 Binance `BTCUSDT aggTrade` collector 和 24h supervisor；记录 UTC 与 monotonic
   起点，目标 elapsed 不少于 `86_400_000 ms`。
3. 创建至少两个固定 snapshot 的回放会话并持续执行有界命令；另保留一个排队会话验证
   scheduler 状态。
4. 每个观测间隔抓取 collector、writer、archiver、query、scheduler、Worker 和 API 健康；
   原始 payload 做有界持久化并串联 hash。
5. 按 run manifest 依次执行一次 collector SIGKILL、writer commit 前退出、archiver commit
   前退出、Replay Worker SIGKILL、Scheduler 重启和 API 重启。一次只注入一个故障，前一项
   恢复并记录 quiet checkpoint 后再继续。
6. 每次故障记录触发时间、最后确认 offset、告警送达、接管 owner/epoch、恢复时间和恢复后
   的第一条 durable 状态；没有观察到预期故障不得标记该注入通过。
7. 运行期间遇到 gap、hash conflict、epoch 回退、snapshot 漂移、审计失败或无法证明租约时，
   对应角色必须 fail closed；不得自动删除异常后继续累计“成功时长”。
8. elapsed 达标后停止新命令，等待 data plane 和 replay outbox 到达 quiet checkpoint。
9. 固定最终 immutable snapshot，并执行 Phase 1R 全链对账。
10. 用最终 snapshot 新建会话，并对已有接管会话再次恢复；比较完整 Actor 权威状态。
11. 运行备份/恢复验证，确认新 replay/scheduler 表和 migration version 被 PostgreSQL PITR
    覆盖；恢复实例不得连接生产消费者或对外监听。
12. 生成证据，停止测试栈前独立重读证据引用的 manifest、对象和数据库 head。

建议的故障时间点可以是 T+2h、T+4h、T+6h、T+8h、T+10h 和 T+12h，但最终时间必须在
run manifest 中预先冻结；失败后重新开始完整 24 小时，不拼接多个短运行窗口。

### 9.4 硬验收条件

以下条件必须全部为 true：

- `source=binance` 且实际连续 elapsed 不少于 24 小时；
- 所有 Kafka durable-acknowledged 事件最终都能按 identity/hash 在权威 Parquet snapshot
  重建；
- duplicate 被幂等吸收，未解决 sequence gap、payload hash 冲突和 producer epoch 回退为 0；
- collector durable offset、writer committed next offset、archiver snapshot version、冷查询和
  replay pin 在 quiet checkpoint 完全一致；
- 每个计划故障真实发生、产生可观察信号并恢复，没有被脚本静默跳过；
- Worker 接管后 cursor、source event chain、state hash、component hash、订单、成交和账本
  全部一致；
- 相同 command ID 在 API 超时、Worker/API 重启后仍只有一个 durable 结果；
- 所有身份拒绝、控制命令、取消和恢复都有脱敏审计；
- personal Profile 回归、全部 server phase tests、Ruff、Compose 解析和 evidence 校验通过；
- evidence 中不存在密钥、token、DSN、私有路径或未脱敏行情 payload。

进程 PID、`ready=true`、单一最终权益、没有报警或“看起来运行了 24 小时”均不能单独作为
成功证据。

### 9.5 执行命令形态

实现完成后，正式入口应只有在双重显式开关下才能运行：

```bash
CANDLESCOPE_PHASE1AI_PUBLIC_SOAK=1 \
  CANDLESCOPE_PHASE1AI_FAULT_INJECTION=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python \
  backend/scripts/server_phase1ai_public_soak.py run \
  --manifest /absolute/path/to/phase1ai-run-manifest.json \
  --output /absolute/path/to/phase1ai-result.json
```

CLI 必须拒绝短于 24 小时、非 Binance source、相对输出路径、覆盖既有 evidence、缺故障计划
或缺双重开关。单元测试使用可注入时钟，不等待真实 24 小时；只有正式运行结果可以设置
`twenty_four_hour_public_continuity=true`。

退出条件：生成可独立复验的 24h evidence，并由另一个进程/工具重算所有 hash、offset、
snapshot 和 Actor 恢复比较。只有此后才评估 README 中的 production readiness 描述；不得
仅凭 Phase 1AH 可启动就更新为生产就绪。

## 10. 每阶段统一完成定义

每个 Phase 合并前必须逐项完成：

1. 对应实现、单元测试、必要的真实基础设施集成测试；
2. `CANDLESCOPE_SERVER_PHASE1<XX>_EXECUTION_zh.md`，明确完成和未完成边界；
3. `docs/server/evidence/phase1<xx>-...-verification.json`，严格 JSON、无重复键；
4. evidence 记录 commit、命令、实际结果和 `claims_not_made`；
5. 新 Python 文件 Ruff check/format 通过；
6. `PYTHONPATH=backend backend/.venv/bin/python -m pytest -q backend/tests/test_server_phase*.py`
   通过；
7. personal replay 定向回归通过；
8. 新 Compose 文件执行 `docker compose ... config` 和真实集成门禁；
9. `git diff --check` 通过；
10. 暂存前使用精确 allowlist，并复核 staged/unstaged 边界。

证据 JSON 至少包含：

```json
{
  "schema_version": "candlescope.server-phase1xx-verification.v1",
  "captured_at_utc": "RFC3339 UTC",
  "branch": "codex/server-phase1xx-*",
  "base_commit": "full sha",
  "phase": "phase1xx-*",
  "phase_passed": false,
  "profile_boundary": {},
  "invariants": {},
  "gates": [],
  "claims_not_made": []
}
```

`phase_passed` 只能根据证据中的实际命令结果写入，不能预填 true。

## 11. 暂存、审查和提交边界

每阶段完成后先列出计划 allowlist，再逐项暂存。例如 Phase 1AD 只能包含：

```text
backend/app/replay/session_factory.py
backend/app/replay/service.py
backend/app/server_runtime/replay_session.py
backend/app/server_runtime/testing/in_memory_replay_session_store.py
backend/tests/test_server_phase1ad_replay_actor.py
必要的既有 replay 回归测试修改
docs/server/CANDLESCOPE_SERVER_PHASE1AD_EXECUTION_zh.md
docs/server/evidence/phase1ad-replay-actor-verification.json
docs/server/CANDLESCOPE_SERVER_PHASE1AD_TO_1AI_EXECUTION_zh.md（仅计划确有修订时）
```

暂存后必须检查：

```bash
git diff --cached --name-status
git diff --cached --stat
git diff --cached --check
git status --short
```

发现插件、release lock、SDK 版本或其他 Phase 文件时立即停止并取消对应暂存项；不要覆盖其
工作树内容。提交和推送仍需单独的明确授权，本文不是自动提交许可。

## 12. 回滚与停止规则

- Phase 1AD–1AG 期间 Server Profile 始终关闭，回滚是移除未启用的新组合，不迁移 personal
  数据。
- 数据库迁移只做向前兼容 additive change；默认不提供自动 destructive downgrade。
- Phase 1AH 运行异常时，把部署 Profile 切回 personal 只影响入口选择，不删除 Server 数据；
  Server 组件应先停止接流量再有界关闭。
- 任何 snapshot、checkpoint、mutation journal、审计或 evidence hash 不一致时立即停止推进，
  保留现场，不自动修复或选择“最新可用”替代。
- 24h 运行失败后保存失败 evidence，并重新开始完整窗口；不得拼接时间、编辑原始采样或
  将失败窗口改名为通过。

## 13. 最终发布检查表

Phase 1AI 完成后仍需一次独立发布审查：

- [ ] `CANDLESCOPE_PROFILE` 未设置时仍为 personal；
- [ ] server 启动不存在 SQLite、本地权威文件和进程内行情 fallback；
- [ ] Server API 的身份、租户、角色和审计负面测试全部通过；
- [ ] Replay Worker 丢租约后无法提交任何 mutation；
- [ ] Scheduler 重启、Worker 接管、API 重启均有真实进程证据；
- [ ] 24h evidence 可由独立 verifier 复算；
- [ ] PostgreSQL 备份/PITR 覆盖 replay 和 scheduler 新表；
- [ ] 告警真实送达且不泄露敏感字段；
- [ ] personal 与 server 的 capability 描述和 README 一致；
- [ ] 生产容量、RPO、RTO 和硬件数量只报告实测值，不从 Phase 0 envelope 推断；
- [ ] 公开文档保留尚未验证的限制，不把“runtime 可启动”等同于“生产就绪”。

完成上述检查后，才能提出 production-ready 状态变更；该状态变更本身应有独立审查、文档
提交和发布授权。
