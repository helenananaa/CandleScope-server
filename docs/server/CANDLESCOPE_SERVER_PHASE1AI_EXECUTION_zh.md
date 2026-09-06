# CandleScope Server Phase 1AI 执行记录

状态：`IMPLEMENTED_DEVELOPMENT_SMOKE_VERIFIED_NOT_24H_NOT_PRODUCTION_READY`

适用基线：`codex/server-phase1ai-public-soak`。Phase 1AD–1AH 已在 `server-origin/main`。
本文件只记录已经发生的事实。正式 24 小时公网连续性尚未执行，
`production_ready` 保持 `false`。

## 1. 实现提交

实现在独立 worktree `/home/helenanana/projects/CandleScope-server-phase1ai`
的 `codex/server-phase1ai-public-soak` 分支上完成。未 push、未创建 PR。

| Commit | 说明 |
| --- | --- |
| `7e507300` | 冻结 fail-closed run manifest |
| `3417b854` | Phase 1AI Compose 与非秘密 env 模板 |
| `b9335d70` | 角色进程管理器 |
| `81f75fce` | sample hash chain 与 exclusive evidence |
| `b49f3d09` | snapshot-pinned 三任务 replay 负载 |
| `8165784b` | 单向六故障状态机 |
| `5c70eeaa` | soak / verifier CLI 与双开关预检 |
| `1f1378ed` | development-smoke 控制器接线 |
| `a7d21d3d` | 重置本地 collector lease 并恢复被杀 Worker |
| `3949c006` | quiet checkpoint 追赶重试 |
| `6d9efd38` | loopback health 探测忽略 HTTP proxy |
| `aa65d4b2` | API 启动前等待 archive catch-up |
| `840f1883` | 每 run 独立 MinIO data_epoch / Kafka group |
| `e7d55a5f` | 故障计划完成后不再越界索引 |
| `b6282b72` | live smoke POST 三任务并观察 sibling Actor 接管 |
| `914d1f2c` | 只 pin 连续 agg_trade 前缀 |
| `0c4435a8` | replay 窗口对齐 1m |
| `8b5b69e5` | archive epoch 映射为 actor snapshot digest |
| `336ebfb9` | active 满时排队 PENDING 而不是拒绝 create |
| `99656243` | Worker query 忽略 HTTP proxy 并记录 load cause |
| `0f579b93` | Worker cold query timeout 60s |
| `dbeca227` | parquet segment 500 事件，避免数千小对象 |
| `f5534b5b` | command-id 幂等探针在 PAUSED 上 STEP |
| `88c4d4a2` | Worker `start_new` 加载期间续租 |
| `b4629c7b` | archive catch-up 允许 500 事件段滞后 |
| `225fc10b` | 后续 quiet 失败时仍保留 takeover 证据 |
| `3b957968` | durable command 期间保持 scheduler heartbeat |
| `b8754db1` | quiet checkpoint 在 archive 段对齐处冻结 collector |

## 2. 开发期聚焦检查

- `pytest -k manifest`：24 passed（Step 1）
- `docker compose ... compose.phase1ai.yml config --quiet`：通过（Step 2）
- `pytest -k process`：6 passed（Step 3）
- `pytest -k 'sample or evidence or redact'`：7 passed（Step 4）
- `pytest -k replay`：4 passed（Step 5）
- `pytest -k fault`：10 passed（Step 6）
- `pytest -k 'cli or verif'`：4 passed；真实 CLI 两次拒绝 `DUAL_SWITCH_MISSING` / `RELATIVE_PATH`，help 未声称 24h（Step 7）

## 3. 短时真实全链 development-smoke

基础设施：`docker compose -p candlescope-phase1ai` 四个服务 healthy
（PostgreSQL `127.0.0.1:29432`、Redpanda `127.0.0.1:60092`、
ClickHouse `127.0.0.1:59123`、MinIO `127.0.0.1:60000`）。
Binance `fstream` 可达。系统时钟 NTP 同步。

失败尝试（均未覆盖、未拼接）：

| 结果文件 | 错误 |
| --- | --- |
| smoke-run-1 | `ROLE_EXITED` API identity JSON 被 bash source 展开 |
| smoke-run-2 | `READY_TIMEOUT` API；collector 继承上一轮 lease 出现 aggTrade gap |
| smoke-run-3 | `QUIET_CHECKPOINT_MISMATCH`（单次采样，未等待追赶） |
| smoke-run-4 | `READY_TIMEOUT` API `/health/ready`（archiver catch-up 阻塞） |
| smoke-run-5 | 同上，120s API timeout |
| smoke-run-6 | `ARCHIVE_CATCHUP_TIMEOUT` / `ParquetArchiveConflictError` 复用 MinIO epoch |
| smoke-run-7 | 采样 36 条后 `IndexError`（故障计划 `_index` 越界） |

两次独立成功运行（新 run id、新 output，未覆盖旧文件）：

| 项 | smoke-8 | smoke-9 |
| --- | --- | --- |
| elapsed_ms | 303493 | 305535 |
| sample_count | 134 | 134 |
| phase_passed | true | true |
| twenty_four_hour_public_continuity | false | false |
| production_ready | false | false |
| independent verifier | verified=true | verified=true |

两次成功运行中：Collector 从公网 Binance 发布 `BTCUSDT` `aggTrade`；
Writer 写入 ClickHouse；Archiver 发布 MinIO immutable snapshot；
Query `/health/ready` 与 Server API `/health/ready` 返回 `ready=true` 且
`production_ready=false`；计划内 Worker SIGKILL 后进程被重启且 quiet
checkpoint 收敛。结果 schema 明确 `twenty_four_hour_public_continuity=false`。

Live controller 在后续独立 smoke（未覆盖 8/9）中已能通过认证 API 创建
三个 snapshot-pinned 任务，且 Worker 可将 session 推到 `RUNNING`：

| 结果文件 | 错误 |
| --- | --- |
| smoke-run-10 | `SESSION_NOT_ASSIGNED` 稀疏 pin 跨 1167 笔 |
| smoke-run-11 | `SESSION_NOT_ASSIGNED` `replay_start_ms` 未对齐 1m |
| smoke-run-12 | `SESSION_NOT_ASSIGNED` actor 要求 `sha256:` epoch |
| smoke-run-13 | `REPLAY_CREATE_FAILED` active=2 时拒绝 queued |
| smoke-run-14/15 | `SESSION_NOT_ASSIGNED` Worker query timeout / proxy |
| smoke-run-16 | `SESSION_NOT_ASSIGNED` 过小 parquet segment |
| smoke-run-17 | `COMMAND_FAILED` pause while PAUSED |
| smoke-run-18 | `COMMAND_FAILED` step while PLAYING |
| smoke-run-19 | `COMMAND_FAILED` idempotent STEP while PLAYING |
| smoke-run-20 | `COMMAND_FAILED` `command result was not durable before the wait bound`；Worker `PERSISTENCE_DEGRADED` |
| smoke-run-21 | `ARCHIVE_CATCHUP_TIMEOUT`（500 事件段滞后超过当时 slack） |
| smoke-run-22 | `QUIET_CHECKPOINT_MISMATCH`（500 事件段；takeover 证据曾被 quiet 异常丢掉） |
| smoke-run-23 | `WORKER_TAKEOVER_NOT_OBSERVED`（8s heartbeat 在 command persist 期间过期） |
| smoke-run-25 | `QUIET_CHECKPOINT_MISMATCH`；`worker_takeover_observed=true`（100 事件段，live stream 错过对齐窗口） |

带三任务 replay 与 sibling Actor 接管的两次独立成功 `development-smoke`
（新 run id、新 output，未覆盖 1–25，未拼接）：

| 项 | smoke-24 | smoke-26 |
| --- | --- | --- |
| HEAD | `3b957968` | `b8754db1` |
| elapsed_ms | 305076 | 303581 |
| sample_count | 105 | 91 |
| phase_passed | true | true |
| phase1ai_passed | false | false |
| twenty_four_hour_public_continuity | false | false |
| production_ready | false | false |
| replay_queued_observed | true | true |
| worker_takeover_observed | true | true |
| kill_role / exit | worker_a / -9 | worker_b / -9 |
| independent verifier `--samples` | verified=true | verified=true |

两次成功运行均通过认证 API 创建三个 snapshot-pinned 任务；queued 任务被观察到
`PENDING`；Worker SIGKILL 后 sibling 接管仍活着的 Actor，而不是把被杀进程拉起。
结果 schema 明确 `twenty_four_hour_public_continuity=false`。
独立 verifier 未带 `--samples` 时会按结果旁路路径读取并报 `SAMPLE_COUNT_MISMATCH`，
必须以绝对 `--samples` 路径复验。

这不是 24 小时连续性证明，也不是生产发布授权。

## 4. 合并前门禁（实现后一次）

在 `b8754db1` 与两次已验证 smoke 之后实际运行：

- `pytest backend/tests/test_server_phase1ai_public_soak.py`：61 passed
- `pytest backend/tests/integration/test_server_phase1ai_smoke.py`：1 skipped
  （未设置 `CANDLESCOPE_PHASE1AI_SMOKE=1`；真实 CLI smoke 已在上一节执行）
- `pytest backend/tests/test_server_phase1ah_profile.py`：9 passed
- `uvx ruff@0.16.2 check` 与 `format --check`：allowlist 通过
- `docker compose --env-file deploy/server/phase1ai.env.example -f deploy/server/compose.phase1ai.yml config --quiet`：通过
- `git diff --check`：通过

未把 `backend/tests/test_server_phase*.py` 全量回归当作 24 小时通过条件。

## 5. Step 10 预检（未启动正式 24h）

2026-09-06 15:36 CST 记录：

- 独立 worktree `git status --porcelain` 为空；HEAD `b8754db16e098dbc1335525897204982759d0038`
- Docker 29.1.3；Docker Compose 2.40.3；Python 3.12.13
- 系统时钟 NTP 同步（`System clock synchronized: yes`，`NTP service: active`）
- 根分区约 454G 可用

未满足正式 `run` 启动条件：

- 未人工确认告警接收端可达
- 未做隔离 PITR / 备份恢复（禁止连接生产消费者）
- 未冻结正式 24h manifest（六类故障、`duration_ms=86_400_000`、Compose config hash）
- 未设置 `CANDLESCOPE_PHASE1AI_PUBLIC_SOAK=1` 与 `CANDLESCOPE_PHASE1AI_FAULT_INJECTION=1` 的 `run` 入口
- development-smoke 只执行 Worker SIGKILL，不是六类故障

## 6. 未执行

- 正式 `run` 入口、双开关 24 小时窗口
- 不可拼接的 `86_400_000 ms` monotonic elapsed
- 六类计划故障（collector / writer / archiver / worker / scheduler / API）
- `docs/server/evidence/phase1ai-public-24h-verification.json`
- README `production_ready=true`
- 人工告警可达确认与隔离 PITR 恢复

未声称生产就绪。Phase 1AI 按手册不得标为通过。
