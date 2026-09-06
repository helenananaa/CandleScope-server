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

Live controller 尚未在成功 smoke 中通过 Server API 创建 `replay-a` /
`replay-b` / `replay-queued`，也未在真实链路上证明 command-id 幂等。
这些行为有单元测试（`PublicSoakReplayDriver`），但还不是 live smoke 证据。

这不是 24 小时连续性证明，也不是生产发布授权。

## 4. 合并前门禁（实现后一次）

- `pytest backend/tests/test_server_phase1ai_public_soak.py`：51 passed
- `pytest backend/tests/integration/test_server_phase1ai_smoke.py`：1 skipped
  （未设置 `CANDLESCOPE_PHASE1AI_SMOKE=1`；真实 CLI smoke 已在上一节执行）
- `pytest backend/tests/test_server_phase1ah_profile.py`：9 passed
- `uvx ruff@0.16.2 check` 与 `format --check`：allowlist 通过
- `docker compose --env-file deploy/server/phase1ai.env.example -f deploy/server/compose.phase1ai.yml config --quiet`：通过
- `git diff --check`：通过

## 5. 未执行

- 正式 `run` 入口、双开关 24 小时窗口
- 不可拼接的 `86_400_000 ms` monotonic elapsed
- `docs/server/evidence/phase1ai-public-24h-verification.json`
- README `production_ready=true`
- 人工告警可达确认与隔离 PITR 恢复

未声称生产就绪。
