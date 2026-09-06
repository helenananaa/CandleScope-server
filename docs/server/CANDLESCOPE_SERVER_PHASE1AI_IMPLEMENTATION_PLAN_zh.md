# CandleScope Server Phase 1AI：公网 24 小时回放全链实施手册

状态：`PLANNED_NOT_IMPLEMENTED`

适用基线：Phase 1AD–1AH 已实现并合并；Server Profile 可以在本地外部基础设施上启动，
但真实 collector、writer、archiver、Query Service、scheduler、Replay Worker 和 API 尚未在
同一次运行中组成完整链路，公网 Binance 24 小时连续性与故障注入也尚未执行。

本文是一份可逐项执行的开发手册。它把 Phase 1AI 拆成可以独立检查的短步骤，并有意减少
重复测试：开发过程中只跑与当前改动直接相关的聚焦门禁；全量 Server Phase 回归只在准备
合并前运行一次；正式 24 小时窗口只在短时全链 smoke 稳定后启动。

在正式 24 小时证据通过前：

- `production_ready` 必须保持 `false`；
- 不得把短时 smoke、进程存活或单个 `ready=true` 描述为公网连续性通过；
- 不得把多个失败或中断的短窗口拼接成 24 小时；
- 不得改写已有 evidence 来掩盖失败；
- personal Profile 的默认行为不得改变。

---

## 1. 本阶段最终交付

完成后应形成下面这条真实纵向链路：

```text
Binance BTCUSDT aggTrade
  -> Collector
  -> Redpanda durable topic
  -> ClickHouse Writer
  -> ClickHouse hot projection
  -> Parquet Archiver
  -> MinIO immutable manifest and parquet objects
  -> authenticated Query Service cold read
  -> PostgreSQL Replay Scheduler
  -> scoped Replay Worker
  -> fenced ReplaySessionActor
  -> authenticated Server HTTP/WS API
  -> independent evidence verifier
```

计划新增或修改的文件：

| 类别 | 路径 |
| --- | --- |
| 运行 manifest 合同 | `backend/app/server_runtime/public_soak_manifest.py` |
| 角色进程编排 | `backend/app/server_runtime/public_soak_processes.py` |
| 观测与证据 | `backend/app/server_runtime/public_soak.py` |
| 故障注入 | `backend/app/server_runtime/soak_faults.py` |
| 回放负载驱动 | `backend/app/server_runtime/public_soak_replay.py` |
| 独立 verifier | `backend/app/server_runtime/public_soak_verify.py` |
| 正式 CLI | `backend/scripts/server_phase1ai_public_soak.py` |
| 独立验证 CLI | `backend/scripts/server_phase1ai_verify.py` |
| 基础设施 Compose | `deploy/server/compose.phase1ai.yml` |
| 非秘密环境模板 | `deploy/server/phase1ai.env.example` |
| 聚焦单元测试 | `backend/tests/test_server_phase1ai_public_soak.py` |
| 短时真实集成 | `backend/tests/integration/test_server_phase1ai_smoke.py` |
| 阶段执行记录 | `docs/server/CANDLESCOPE_SERVER_PHASE1AI_EXECUTION_zh.md` |
| 正式机器证据 | `docs/server/evidence/phase1ai-public-24h-verification.json` |

`CANDLESCOPE_SERVER_PHASE1AI_EXECUTION_zh.md` 和正式 evidence 只能在实现或运行发生后记录
事实。本计划文件不能预先把任何门禁标成通过。

---

## 2. 编排方式与边界

### 2.1 本阶段采用两层编排

仓库当前没有 Server 应用镜像 Dockerfile，因此本阶段先使用：

```text
Docker Compose
  -> PostgreSQL / Redpanda / ClickHouse / MinIO

server_phase1ai_public_soak.py
  -> Collector / Writer / Archiver / Query / Scheduler / Worker / FastAPI
```

控制器用 `asyncio.create_subprocess_exec()` 启动 Python 角色，保存 PID、启动时间、退出码和
有界日志路径，并负责按反向依赖顺序停止。禁止使用 `shell=True`，禁止把 token 或 DSN 放进
命令行参数。所有秘密只通过子进程环境传入，并在证据中脱敏。

容器化应用角色是 Phase 1AI 通过后的部署工作，不与本阶段正确性闭环混做。

### 2.2 固定首个公开范围

- source：`binance`；
- exchange/market：`binance:futures`；
- symbol：`BTCUSDT`；
- event kind：`agg_trade`；
- organization/workspace：一个明确的测试租户；
- Replay Worker：两个相同 scope 的 Worker，每个 `max_actors=1`；
- 回放任务：两个运行/完成任务和至少一个排队任务；
- Query：只允许认证的 `preference=cold`；
- evidence：只写绝对路径的新文件，不覆盖已有文件。

不在本阶段增加多交易所、多 symbol、自动扩缩容、Kubernetes、匿名 API、生产 TLS 终止、
PostgreSQL HA 或跨区域对象存储。

---

## 3. Step 0：准备隔离工作区

### 3.1 记录现状

在仓库根目录执行：

```bash
git status --short --branch
git diff --name-only
git rev-parse HEAD
git remote -v
```

当前主工作树可能保留插件、release lock 和 SDK 包改动。不得执行 `git add -A`、
`git add .`，不得格式化或覆盖这些无关文件。

### 3.2 创建独立 worktree

先确认目标分支和目录不存在：

```bash
git branch --list codex/server-phase1ai-public-soak
git worktree list
```

不存在时执行：

```bash
git fetch server-origin
git worktree add ../CandleScope-server-phase1ai \
  -b codex/server-phase1ai-public-soak server-origin/main
cd ../CandleScope-server-phase1ai
```

如果不使用独立 worktree，则后续只能通过本文第 15 节的文件 allowlist 暂存。

### 3.3 只做廉价基线检查

不先跑全量测试。只确认 Python 环境和 Compose 文件当前可用：

```bash
backend/.venv/bin/python --version
PYTHONPATH=backend backend/.venv/bin/python -c \
  "from app.server_runtime.application import STATUS; print(STATUS)"
docker compose -f deploy/server/compose.phase1ah.yml config --quiet
git diff --check
```

任一命令失败时先记录基线问题，不把它混入 Phase 1AI 功能修改。

---

## 4. Step 1：冻结 run manifest 合同

### 4.1 新增 manifest 模型

创建 `public_soak_manifest.py`，定义不可变 `PublicSoakManifest`。至少包含：

```text
schema_version
run_id
source / exchange / market / symbol / event_kind
organization_id / workspace_id
duration_ms
scrape_interval_ms / stale_after_ms
quiet_checkpoint_timeout_ms
started_not_before_utc
git_commit
require_clean_worktree
infrastructure endpoints without credentials
role health endpoints
replay workload limits
fault plan
acceptance thresholds
output policy
```

每个 fault 至少包含：

```text
fault_id
target_role
method
scheduled_elapsed_ms
observation_timeout_ms
recovery_timeout_ms
```

### 4.2 强制校验

构造 manifest 时必须拒绝：

- schema version 不匹配；
- `source != binance`；
- 正式模式 `duration_ms < 86_400_000`；
- 相对 manifest 或 output 路径；
- output 已存在；
- fault ID 重复或时间不递增；
- fault 时间超出运行窗口；
- 缺少六类必选故障；
- health URL 带凭据、query、fragment 或非 loopback host；
- endpoint 字符串包含明显的 userinfo/password；
- replay 数量、采样间隔、日志或 payload 上限超界；
- manifest 中出现 token、secret、password、DSN 值。

### 4.3 development-smoke 与正式模式

同一数据模型支持两个不同入口：

| 模式 | 最短时长 | 允许故障 | 可以声称 24h |
| --- | ---: | --- | --- |
| `development-smoke` | 300 秒 | 只执行 Worker SIGKILL | 否 |
| `run` | 86,400 秒 | 六类故障全部必选 | 只有实际 elapsed 达标后 |

不得通过 manifest 字段把正式入口降级成短跑。`development-smoke` 的结果 schema 必须明确
写入 `twenty_four_hour_public_continuity=false` 和 `production_ready=false`。

### 4.4 本步最小门禁

只新增少量表驱动测试，覆盖有效 manifest 和上述拒绝分支：

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ai_public_soak.py -k manifest
```

通过条件：有效 manifest 可稳定序列化；同一内容产生同一 SHA-256；秘密和不安全路径全部
fail closed。

---

## 5. Step 2：建立 Phase 1AI 基础设施 Compose

### 5.1 从 Phase 1AH 派生

复制 Phase 1AH 的四个真实依赖到 `compose.phase1ai.yml`：

- Redpanda；
- PostgreSQL；
- ClickHouse；
- MinIO。

修改以下内容：

1. 使用新的 project name 和独立端口，避免碰撞 Phase 1AH；
2. 所有服务保留 healthcheck；
3. 为 24 小时运行配置明确的 named volumes；
4. 添加日志轮转，例如 `max-size` 和 `max-file`；
5. 不把真实密码写入 Compose，改用必填环境变量；
6. 不把数据库或对象存储端口绑定到非 loopback 地址；
7. 不添加自动清理 volume 的退出动作。

建议端口只作为本地默认值，最终值由 manifest 冻结：

```text
PostgreSQL  127.0.0.1:29432
Redpanda   127.0.0.1:60092
ClickHouse 127.0.0.1:59123
MinIO      127.0.0.1:60000
```

### 5.2 新增非秘密环境模板

`phase1ai.env.example` 只列变量名和占位符。至少包含：

```text
CANDLESCOPE_PHASE1AI_POSTGRES_PASSWORD
CANDLESCOPE_PHASE1AI_CLICKHOUSE_PASSWORD
CANDLESCOPE_PHASE1AI_MINIO_ACCESS_KEY
CANDLESCOPE_PHASE1AI_MINIO_SECRET_KEY
CANDLESCOPE_PHASE1AI_QUERY_TOKEN
CANDLESCOPE_PHASE1AI_WORKER_CONTROL_TOKEN_A
CANDLESCOPE_PHASE1AI_WORKER_CONTROL_TOKEN_B
CANDLESCOPE_PHASE1AI_API_TOKEN
```

真实 `.env` 推荐放在仓库外。如果必须放在仓库内，则必须先确认它已经被忽略：

```bash
git check-ignore deploy/server/phase1ai.env.local
```

对仓库内文件，如果命令没有输出，就不要继续启动，也不要写入任何秘密；改为先添加安全的
ignore 规则，或直接把文件移到仓库外。

### 5.3 本步最小门禁

```bash
docker compose --env-file /absolute/path/to/phase1ai.env \
  -f deploy/server/compose.phase1ai.yml config --quiet
```

只验证 Compose 可解析，不在这一步跑业务测试。

---

## 6. Step 3：实现角色进程管理器

### 6.1 定义角色

在 `public_soak_processes.py` 定义：

```text
RoleSpec
  name
  argv
  sanitized_environment_keys
  health_url
  startup_timeout_ms
  shutdown_timeout_ms

ManagedRole
  pid
  started_at_utc
  started_at_monotonic_ms
  exit_code
  stdout_log
  stderr_log
```

不得把完整 environment、DSN 或 token 保存到对象 repr 或 evidence。

### 6.2 固定初始化顺序

基础设施 ready 后按顺序执行一次：

```text
collector init-schema
writer init-schema
archiver init-bucket
PostgreSQL query-control migrations
PostgreSQL replay runtime migrations
```

优先复用现有迁移和入口，不在 Phase 1AI 复制 SQL。

### 6.3 固定启动顺序

```text
Collector
  -> ClickHouse Writer
  -> Parquet Archiver
  -> Query Service
  -> Replay Scheduler
  -> Replay Worker A
  -> Replay Worker B
  -> FastAPI Server Profile
```

每启动一个角色都必须等待它自己的 readiness；超时立即停止已经启动的后续依赖并保存失败
记录。禁止一次性启动全部进程后只做最终 sleep。

### 6.4 固定停止顺序

正常结束时：

```text
stop new API workload
  -> wait replay outbox quiet
  -> stop API
  -> stop Workers
  -> stop Scheduler
  -> stop Query Service
  -> wait writer/archive caught up
  -> stop Archiver
  -> stop Writer
  -> stop Collector
```

先发送 SIGTERM，超过每角色 timeout 后才允许 SIGKILL。每个动作都写入 evidence event，不用
`pkill -f` 或模糊进程名杀进程。

### 6.5 本步最小门禁

用无外部依赖的短生命周期子进程测试：启动、ready 超时、SIGTERM、升级 SIGKILL、日志上限
和反向关闭顺序。只运行：

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ai_public_soak.py -k process
```

---

## 7. Step 4：实现观测、hash chain 与 evidence writer

### 7.1 每次采样内容

`public_soak.py` 每个采样周期收集：

- collector health、owner、producer epoch、durable next offset；
- writer health、committed next offset、duplicate/conflict/gap 计数；
- archiver health、snapshot version、manifest hash、archived offset；
- Query Service readiness 和当前可读取 snapshot；
- scheduler readiness、queue depth、live Worker 数；
- Worker A/B owner、lease/fencing epoch、active actor 数；
- API readiness；
- 三个回放任务的状态、revision、cursor 和 state/component hash；
- 进程退出码和最近一次故障状态。

原始响应必须限制字节数。任何响应超限、JSON 无效、时钟回退或 health stale 都使运行失败。

### 7.2 采样 hash chain

每条规范化 sample 写入：

```text
sequence
observed_at_utc
monotonic_elapsed_ms
payload_sha256
previous_sample_sha256
sample_sha256
```

`sample_sha256` 对规范 JSON 计算，并链接上一条 sample。正式 evidence 可以只保留有界摘要，
但必须引用不可变的原始 sample 文件及其最终 hash。

### 7.3 evidence 写入规则

1. 运行中写到与最终 output 同目录的唯一 `.partial` 文件；
2. 定期 flush，并在关键故障事件后 `fsync`；
3. 成功或失败都生成不可变结果；
4. 最终文件使用 exclusive create，禁止覆盖；
5. 失败结果写 `phase_passed=false` 和稳定错误码；
6. 成功前重新读取 manifest 和原始 sample，核对 hash；
7. 公开 evidence 不含 token、DSN、用户名、私有路径或完整行情 payload。

### 7.4 本步最小门禁

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ai_public_soak.py -k 'sample or evidence or redact'
```

---

## 8. Step 5：实现真实 Replay 负载

### 8.1 启动条件

只有 data plane 已产生第一个不可变 snapshot、Query Service 可以用 cold preference 读取，且
两个 Worker 均已注册为 live 后，才能创建回放任务。

### 8.2 固定三个任务

通过认证 Server API 创建：

1. `replay-a`：持续发送有界 STEP 命令；
2. `replay-b`：间隔发送 STEP/PAUSE/RESUME，用于 API/Worker 重启后的恢复；
3. `replay-queued`：在 Worker 容量被占满时保持 queued，用于验证 scheduler 状态。

所有任务都必须使用明确的 `MarketDataSnapshotRef`，禁止 `latest`、version 0 或客户端本地
`query_path`。

### 8.3 幂等探针

为 `replay-b` 预先冻结一个 command ID：

1. 第一次提交时故意让客户端在响应前超时；
2. 使用相同 command ID 重试；
3. API 或 Worker 重启后再次读取结果；
4. PostgreSQL 中必须只有一个 durable command result；
5. revision、cursor 和 state hash 不得重复推进。

### 8.4 本步完成条件

- 两个 active replay 都至少完成一次命令；
- queued replay 确实观察到 queued，而不是脚本自行假定；
- 每个公开 API 调用都有权威组织/工作区 scope；
- Query Worker 始终使用 cold HTTP 服务边界；
- 无 token、lease token 或内部 DSN 进入结果。

---

## 9. Step 6：实现故障状态机

### 9.1 状态模型

`soak_faults.py` 中每个故障只能按下面的方向推进：

```text
planned
  -> trigger_requested
  -> trigger_observed
  -> recovery_observed
  -> quiet_checkpoint_verified
```

任一步超时进入 `failed`，不得继续执行下一故障来掩盖它。

### 9.2 六类正式故障

按 manifest 中预先冻结的时间执行：

| 顺序 | 目标 | 必须观察到的证据 |
| ---: | --- | --- |
| 1 | Collector SIGKILL | 旧 PID 退出，新 owner/epoch 接管，offset 不回退 |
| 2 | Writer commit 前退出 | 未确认消息重放，duplicate 幂等吸收，无 hash conflict |
| 3 | Archiver commit 前退出 | incomplete staging 不成为权威 snapshot，恢复后生成新 manifest |
| 4 | Replay Worker SIGKILL | lease 到期或释放，新 Worker fencing epoch 增加，Actor 状态一致 |
| 5 | Scheduler restart | queued/running 状态保留，Worker heartbeat 重新可见 |
| 6 | API restart |认证边界不变，相同 command ID 仍返回唯一结果 |

Writer/Archiver 的“commit 前退出”必须复用或扩展已有显式 fault hook；禁止用随机时间杀进程后
假装命中了提交窗口。

### 9.3 故障之间的 quiet checkpoint

每个故障完成后，至少满足：

```text
collector durable next offset
  == writer committed next offset
  == archiver snapshot covered next offset

query snapshot == replay pinned snapshot
unresolved gaps == 0
hash conflicts == 0
producer epoch rollback == 0
```

如果当前仍在合理追赶，可等待 manifest 中冻结的 recovery timeout；超时即失败。

---

## 10. Step 7：实现两个 CLI

### 10.1 主运行 CLI

期望形态：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_phase1ai_public_soak.py development-smoke \
  --manifest /absolute/path/to/phase1ai-smoke-manifest.json \
  --output /absolute/path/to/phase1ai-smoke-result.json
```

正式运行必须有双重开关：

```bash
CANDLESCOPE_PHASE1AI_PUBLIC_SOAK=1 \
CANDLESCOPE_PHASE1AI_FAULT_INJECTION=1 \
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_phase1ai_public_soak.py run \
  --manifest /absolute/path/to/phase1ai-run-manifest.json \
  --output /absolute/path/to/phase1ai-result.json
```

CLI 启动前必须检查：

- manifest/output 为绝对路径且 output 不存在；
- 正式双开关均为 `1`；
- git commit 与 manifest 相同；
- `require_clean_worktree=true` 时工作树干净；
- Compose 基础设施可用；
- 磁盘余量、系统时间、对象存储读写和全部 health endpoint 达标；
- manifest 中的故障计划完整；
- 日志与 sample 目录可 exclusive create。

### 10.2 独立验证 CLI

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_phase1ai_verify.py \
  --manifest /absolute/path/to/phase1ai-run-manifest.json \
  --result /absolute/path/to/phase1ai-result.json
```

verifier 不连接正在运行的进程来补齐缺失事实。它只读取冻结 manifest、result、原始 sample、
最终 snapshot manifest 和导出的只读数据库摘要，重新计算全部 hash 和验收布尔值。

---

## 11. Step 8：第一次短时真实全链 smoke

这一步的目标是发现编排错误，不是证明生产就绪。

### 11.1 启动基础设施

```bash
docker compose -p candlescope-phase1ai \
  --env-file /absolute/path/to/phase1ai.env \
  -f deploy/server/compose.phase1ai.yml up -d --wait
```

记录：

```bash
docker compose -p candlescope-phase1ai \
  -f deploy/server/compose.phase1ai.yml ps
```

### 11.2 运行 5–10 分钟 smoke

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_phase1ai_public_soak.py development-smoke \
  --manifest /absolute/path/to/phase1ai-smoke-manifest.json \
  --output /absolute/path/to/phase1ai-smoke-result.json
```

Smoke 只执行一次 Replay Worker SIGKILL，其余正式故障不在这里重复。

### 11.3 独立验证 smoke

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_phase1ai_verify.py \
  --manifest /absolute/path/to/phase1ai-smoke-manifest.json \
  --result /absolute/path/to/phase1ai-smoke-result.json
```

Smoke 通过条件：

- 所有真实角色启动；
- Binance 真实事件进入 Redpanda、ClickHouse 和 MinIO；
- cold Query Service 返回同一 snapshot；
- Server API 创建真实 Replay；
- Worker 被杀后由另一个 Worker 接管；
- 最终 caught up；
- verifier 通过；
- result 仍明确 `twenty_four_hour_public_continuity=false`。

### 11.4 Smoke 失败处理

保存 result、logs、Compose 状态和 volumes，不立即 `down -v`。先执行只读诊断：

```bash
docker compose -p candlescope-phase1ai \
  -f deploy/server/compose.phase1ai.yml ps
docker compose -p candlescope-phase1ai \
  -f deploy/server/compose.phase1ai.yml logs --no-color --tail 200
```

定位并修复后重新创建新的 run ID 和新 output；不得覆盖旧失败结果。

---

## 12. Step 9：最小合并前门禁

只在完整实现和 smoke 已通过后运行一次：

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ai_public_soak.py

PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1ai_smoke.py

PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase*.py

uvx ruff@0.16.2 check \
  backend/app/server_runtime/public_soak_manifest.py \
  backend/app/server_runtime/public_soak_processes.py \
  backend/app/server_runtime/public_soak.py \
  backend/app/server_runtime/soak_faults.py \
  backend/app/server_runtime/public_soak_replay.py \
  backend/app/server_runtime/public_soak_verify.py \
  backend/scripts/server_phase1ai_public_soak.py \
  backend/scripts/server_phase1ai_verify.py \
  backend/tests/test_server_phase1ai_public_soak.py \
  backend/tests/integration/test_server_phase1ai_smoke.py

uvx ruff@0.16.2 format --check \
  backend/app/server_runtime/public_soak_manifest.py \
  backend/app/server_runtime/public_soak_processes.py \
  backend/app/server_runtime/public_soak.py \
  backend/app/server_runtime/soak_faults.py \
  backend/app/server_runtime/public_soak_replay.py \
  backend/app/server_runtime/public_soak_verify.py \
  backend/scripts/server_phase1ai_public_soak.py \
  backend/scripts/server_phase1ai_verify.py \
  backend/tests/test_server_phase1ai_public_soak.py \
  backend/tests/integration/test_server_phase1ai_smoke.py

docker compose --env-file /absolute/path/to/phase1ai.env \
  -f deploy/server/compose.phase1ai.yml config --quiet

git diff --check
```

不要求在开发每一步重复运行 `test_server_phase*.py`。如果最后一次全量回归失败，只修复与
Phase 1AI 相关的回归；既有无关失败必须单独记录。

---

## 13. Step 10：生成正式 24 小时 manifest

### 13.1 固定运行身份

正式运行前记录：

```bash
git status --porcelain
git rev-parse HEAD
docker version
docker compose version
backend/.venv/bin/python --version
timedatectl status
df -h
```

必须满足：

- `git status --porcelain` 无输出；
- manifest 的 `git_commit` 等于当前完整 commit；
- 系统时间同步；
- 数据、日志和 evidence 分区有足够余量；
- 镜像版本和 Compose config hash 已写入 manifest；
- 告警接收端已由人工确认可达；
- 备份/PITR 检查使用隔离恢复目标，不连接生产消费者。

### 13.2 建议故障时间

在 manifest 中预先冻结，例如：

```text
T+02:00 Collector SIGKILL
T+04:00 Writer pre-commit exit
T+06:00 Archiver pre-commit exit
T+08:00 Replay Worker SIGKILL
T+10:00 Scheduler restart
T+12:00 API restart
```

时间可以调整，但正式启动后不得更改。一次只执行一个故障；前一个故障未完成 quiet
checkpoint 时不得进入下一个。

---

## 14. Step 11：执行正式 24 小时窗口

### 14.1 启动

```bash
docker compose -p candlescope-phase1ai \
  --env-file /absolute/path/to/phase1ai.env \
  -f deploy/server/compose.phase1ai.yml up -d --wait

CANDLESCOPE_PHASE1AI_PUBLIC_SOAK=1 \
CANDLESCOPE_PHASE1AI_FAULT_INJECTION=1 \
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_phase1ai_public_soak.py run \
  --manifest /absolute/path/to/phase1ai-run-manifest.json \
  --output /absolute/path/to/phase1ai-result.json
```

不要用终端断开来管理 24 小时任务。应在受监督的持久会话或系统服务中运行，并把 stdout、
stderr 写入 manifest 指定的有界日志文件。

### 14.2 运行期间允许的人工动作

只允许：

- 查看状态和日志；
- 响应磁盘、时钟、网络或安全告警；
- 在确有风险时停止运行并保留失败 evidence。

不允许：

- 修改 manifest 或验收阈值；
- 手工跳过故障；
- 删除 gap/conflict 后继续计时；
- 重置数据库 offset、snapshot 或 lease；
- 修改 result/sample；
- 把中断前后的 elapsed 相加。

### 14.3 正常结束

控制器必须自行：

1. 达到至少 `86_400_000 ms` monotonic elapsed；
2. 停止创建新命令；
3. 等待 writer、archive、replay outbox quiet；
4. 固定最终 immutable snapshot；
5. 执行全链对账；
6. 恢复既有 Replay 并创建一个最终 snapshot Replay；
7. 检查 command ID 幂等；
8. 执行隔离备份恢复验证；
9. 写出 result；
10. 重新读取并验证引用的 manifest、sample 和 snapshot。

---

## 15. Step 12：独立复验与阶段结论

运行结束后，在不同进程中执行：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_phase1ai_verify.py \
  --manifest /absolute/path/to/phase1ai-run-manifest.json \
  --result /absolute/path/to/phase1ai-result.json
```

只有以下条件全部成立，Phase 1AI 才可标为通过：

- source 确为 Binance 且 monotonic elapsed 不少于 24 小时；
- 六个计划故障都真实发生、被观察并完成恢复；
- durable offset、ClickHouse、Parquet、cold query 和 replay pin 在最终 quiet checkpoint 一致；
- unresolved gap、hash conflict 和 producer epoch rollback 均为 0；
- Worker 接管前后 cursor、state hash、component hash、订单、成交和账本一致；
- 相同 command ID 只有一个 durable 结果；
- 身份、租户、控制与取消动作有脱敏审计；
- 备份恢复覆盖 replay/scheduler 新表；
- sample hash chain、manifest hash 和 evidence hash 可独立重算；
- evidence 扫描没有秘密、DSN、私有路径或完整未脱敏行情 payload。

然后才创建或更新：

- `CANDLESCOPE_SERVER_PHASE1AI_EXECUTION_zh.md`；
- `phase1ai-public-24h-verification.json`；
- README 中的服务器状态。

即使 Phase 1AI 通过，也不自动等于生产发布授权。README 是否把 `production_ready` 改为
`true`，应作为独立发布审查处理。

---

## 16. 暂存与提交 allowlist

开发完成后只允许暂存：

```text
backend/app/server_runtime/public_soak_manifest.py
backend/app/server_runtime/public_soak_processes.py
backend/app/server_runtime/public_soak.py
backend/app/server_runtime/soak_faults.py
backend/app/server_runtime/public_soak_replay.py
backend/app/server_runtime/public_soak_verify.py
backend/scripts/server_phase1ai_public_soak.py
backend/scripts/server_phase1ai_verify.py
backend/tests/test_server_phase1ai_public_soak.py
backend/tests/integration/test_server_phase1ai_smoke.py
deploy/server/compose.phase1ai.yml
deploy/server/phase1ai.env.example
docs/server/CANDLESCOPE_SERVER_PHASE1AI_IMPLEMENTATION_PLAN_zh.md
docs/server/CANDLESCOPE_SERVER_PHASE1AI_EXECUTION_zh.md
docs/server/evidence/phase1ai-public-24h-verification.json
README.md
README_zh.md
```

README 和正式 evidence 只有在对应事实已经发生时才纳入。暂存后执行：

```bash
git diff --cached --name-status
git diff --cached --stat
git diff --cached --check
git status --short
```

发现 plugin、package、release lock、SDK 或其他无关文件时停止，不提交。本文不授权 commit、
push、PR 或修改公开生产状态；这些动作仍需单独确认。

---

## 17. 失败与清理规则

### 17.1 任何阶段立即停止的条件

- snapshot、offset、hash、fencing 或 tenant scope 不一致；
- health stale 或系统时钟回退；
- 故障未真实触发却被标成成功；
- 旧 Worker 在失去租约后仍能提交 mutation；
- Query 回退热端、本地文件或未认证路径；
- evidence 出现秘密或绝对私有路径；
- 磁盘余量不足、对象存储不可验证、告警未送达；
- 运行过程中工作树或 manifest 被修改。

### 17.2 保存失败现场

失败时保留：

- manifest；
- partial/final result；
- sample hash chain；
- 各角色有界日志；
- Compose `ps`；
- 数据 volume；
- 最后可证明的 offset、snapshot、lease 和 Actor hash。

默认只停止进程：

```bash
docker compose -p candlescope-phase1ai \
  -f deploy/server/compose.phase1ai.yml down
```

不要执行 `down -v`。只有明确确认不再需要现场、备份和失败证据后，才单独决定是否删除
volumes。

---

## 18. 最小测试策略摘要

为了把时间用于开发而不是重复回归，本阶段按以下频率执行：

| 时点 | 执行内容 |
| --- | --- |
| 每个小步骤 | 只跑当前模块的聚焦测试和 `git diff --check` |
| Compose 修改后 | 只跑一次 `docker compose config --quiet` |
| 完整控制器首次可用 | 跑一次 5–10 分钟真实 smoke |
| 准备合并 | 跑一次 Phase 1AI 单元、一次 Phase 1AI 集成、一次全部 Server Phase 回归和 Ruff |
| 正式验收 | 跑一次不可拼接的真实 24 小时窗口，再由独立 verifier 复验 |

不能省略的不是测试数量，而是四条关键证明：

1. 双开关与 24 小时时长不能绕过；
2. Worker fencing 和 command 幂等在真实重启后仍成立；
3. Kafka、ClickHouse、Parquet、cold query 和 replay snapshot 最终一致；
4. evidence 可独立复算且不泄露秘密。

满足这四条后，再继续增加测试的收益很低；应转入独立发布审查、部署打包和真实运维工作。
