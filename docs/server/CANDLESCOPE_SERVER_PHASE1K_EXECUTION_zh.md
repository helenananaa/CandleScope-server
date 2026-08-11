# CandleScope Server Phase 1K：多实例写入 Fence 与一致备份窗口

状态：QUERY_CONTROL_WRITE_FENCE_COMPLETE_NOT_DEPLOYABLE

Phase 1K 延续独立快照查询进程，主 FastAPI `server` Profile 仍保持 fail closed。它消除 Phase 1J 依赖操作员口头保证“当前无写入”的缺口，让所有 query-control 写事务与物理备份窗口共享同一个 PostgreSQL 锁协议：

```text
runtime A/B/... write transaction
  -> pg_try_advisory_xact_lock_shared(fixed lock key)
  -> append audit or mutate quarantine
  -> transaction end releases shared permit

backup window operator (auditor role)
  -> validate exact migration and least privilege
  -> pg_advisory_lock(fixed lock key)
  -> wait until existing shared write transactions drain
  -> hold exclusive session lock + heartbeat
  -> anchor -> target time -> backup manifest v2
  -> release exclusive lock

new runtime write while exclusive lock is held
  -> shared try-lock returns false
  -> QueryControlWriteFencedError
  -> HTTP query cannot complete its mandatory audit and returns 503
```

## 1. 锁与失败语义

固定锁名为 `candlescope-query-backup-write-fence-v1`，通过 PostgreSQL `hashtextextended(..., 0)` 映射为 advisory lock key。

以下写路径在事务执行任何业务写入之前必须取得 shared transaction lock：

- 普通查询审计 `emit`；
- hot projection quarantine `latch`；
- generation-fenced quarantine `clear` 及其同事务审计。

shared permit 使用 `pg_try_advisory_xact_lock_shared`。排他窗口已建立时不会排队等待，而是立即抛出 `QueryControlWriteFencedError`；它属于 `QueryControlUnavailableError`，现有查询 API 因无法完成强制审计而 fail closed 为 503。

`PostgresQueryBackupFence.acquire()` 使用 auditor DSN，先验证 Phase 1I 的精确迁移和只读最小权限，再阻塞取得 exclusive session lock。该调用成功即证明所有已经持有 shared permit 的 PostgreSQL 写事务均已结束。第二个协调器受 `lock_timeout` 限制，不能与当前窗口并存。

这不是完整 HTTP request drain：一个尚未进入审计事务的 ClickHouse/Parquet 请求可能仍在计算，但它在 fence 内不能提交审计或成功返回。窗口释放后它才可能重新尝试后续写入。Phase 1K 没有 API gateway admission pause、连接摘流或请求取消。

## 2. Heartbeat 与一次性命令入口

`backend/scripts/server_query_backup_window.py` 是一次性外部调度入口。它：

1. 取得排他 fence 并生成 canonical UUID receipt；
2. 把 `FENCE_ID` 和 `FENCE_ACQUIRED_AT_MS` 注入受信任子进程环境；
3. 在子进程运行期间周期检查本 PostgreSQL session 仍持有唯一 exclusive advisory lock；
4. 超时或 heartbeat 失败时终止直接子进程；
5. 无论子进程成功、失败或超时都尝试释放 fence；
6. 不回显完整 argv，避免把误放在命令行的敏感值复制进 receipt；子进程退出码会原样成为 wrapper 退出码。

必需环境变量：

```text
CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_POSTGRES_AUDITOR_DSN
CANDLESCOPE_SERVER_QUERY_BACKUP_WINDOW_OPERATOR_ID
```

operator ID 固定为 1–32 位 lower-case safe ASCII；连同 31-byte 固定前缀后不会超过 PostgreSQL `application_name` 的 63-byte 标识上限，因此数据库观察值不会被静默截断。

可配置正整数：`CONNECT_TIMEOUT_MS`、`DRAIN_TIMEOUT_MS`、`MAXIMUM_RUNTIME_MS`、`HEARTBEAT_INTERVAL_MS`。默认分别为 5 秒、30 秒、5 分钟和 500 毫秒；heartbeat 必须短于 drain timeout，防止 PostgreSQL 的 idle-in-transaction 保护先终止持锁 session。

子命令必须同步运行且不得 daemonize。wrapper 只能直接管理它启动的进程；受信任命令若自行 fork 后立即退出，后台后代不会继续受 fence 生命周期保护。

## 3. 物理备份 Manifest v2

`candlescope.query-physical-backup.v2` 在 Phase 1J 字段上新增并签名：

- `write_fence_id`；
- `write_fence_acquired_at_ms`。

对象路径升级为 `backups/v2/...`。publisher 在签名前不仅要求 wrapper 注入 receipt，还会让 auditor 从 `pg_stat_activity` 与 `pg_locks` 反查 `candlescope-query-backup-fence:<operator_id>` 正持有唯一 exclusive lock。脱离 window 直接调用 publisher 会 fail closed。

`backend/scripts/server_query_backup_bundle.py` 在同一窗口中按固定顺序执行：

1. 发布 Phase 1I audit anchor；
2. 从 PostgreSQL 取得 canonical UTC 微秒 recovery target time；
3. 运行 Phase 1J `pg_verifybackup`、再次验证数据库等于 anchor、确认 fence 仍存在；
4. 发布包含 fence receipt、target、anchor、WAL prefix 与 artifact hashes 的 manifest v2。

推荐一次性调用形态：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_backup_window.py -- \
  backend/.venv/bin/python backend/scripts/server_query_backup_bundle.py \
  --backup-directory /secure/staging/query-control/backup-id \
  --backup-id 11111111-2222-4333-8444-555555555555
```

base backup 仍须在进入窗口前由 PostgreSQL 18 `pg_basebackup` 捕获，WAL archive 必须已经连续运行。anchor 使用 `CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_` 独立 S3/HMAC 配置；backup 使用 `CANDLESCOPE_SERVER_QUERY_BACKUP_` S3/HMAC 配置，两者可以使用不同 bucket/prefix/credentials。bundle 与凭据均由外部 secret manager 或调度环境提供，仓库不保存生产值。

若进程在最终 manifest 之前失败，对象存储可能留下不可变但不可发现的 anchor/artifact orphan；它们不能被当作完整恢复集合。若 manifest 已发布但随后 heartbeat 失败，整次调度仍报告失败，操作员必须重新运行 bundle verifier 和恢复门禁，不能仅凭对象存在改写任务为成功。

## 4. 真实门禁

PostgreSQL 18.4 真实门禁验证：

1. runtime A 写入第 1 条审计；
2. 人工持有 shared transaction lock，排他协调器在 100 ms 观察窗内保持等待；
3. shared 事务提交后协调器才取得 fence receipt；
4. 第二协调器在 100 ms `lock_timeout` 后失败；
5. runtime A/B 的 `emit` 和 runtime A 的 quarantine `latch` 全部被拒，完整审计链仍严格停在 1 条；
6. auditor 能反查指定 operator 的 exclusive lock，释放后同一检查 fail closed；
7. 释放后 A/B 分别写入，审计链增至 3 条；
8. 真实 CLI wrapper 启动一个 400 ms 子进程，数据库可观察到它持锁，runtime B 写入被拒；
9. CLI 正常退出并释放后，runtime B 写入第 4 条，sequence 连续为 4。

Phase 1J 的完整物理备份/PITR 门禁也改为在 fence 内发布 anchor、目标 WAL 和 manifest v2；窗口内探针写入被拒，释放后写入 target 后事件，恢复库仍只包含 target 前两条记录并能 promote 后继续写。

重跑：

```bash
docker compose -f deploy/server/compose.phase1j.yml --profile recovery down -v
docker compose -f deploy/server/compose.phase1j.yml up -d --wait
CANDLESCOPE_PHASE1K_INTEGRATION=1 \
  CANDLESCOPE_PHASE1K_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1k_backup_window.py

CANDLESCOPE_PHASE1J_INTEGRATION=1 \
  CANDLESCOPE_PHASE1J_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1j_pitr_operations.py
docker compose -f deploy/server/compose.phase1j.yml --profile recovery down -v
```

两个 `ALLOW_TEST_RESET=1` 均只授权重建本地 Phase 1J/1K Compose 数据卷与测试表。

## 5. 明确未完成

- 没有部署 systemd timer、Kubernetes CronJob、常驻 scheduler 或告警路由；当前只是可被这些系统调用的一次性入口；
- 没有 API gateway admission drain、负载均衡摘流、运行中查询取消或跨服务业务事务 fence；
- advisory lock 是数据库 session 状态，不是持久租约；PostgreSQL 重启或连接丢失会释放，wrapper 最迟在下一 heartbeat 才检测到；
- fence receipt 由 backup HMAC 签名但不是第三方不可抵赖证明；拥有 HMAC key 的协调器仍是受信任边界；
- 没有自动 orphan 清理、retention、备份选择索引、恢复审批或定期 restore 调度；
- 没有 24/72 小时连续 WAL、对象存储长故障、磁盘耗尽、时钟漂移、网络分区或大型备份容量证据；
- 没有生产 RPO/RTO、异地复制、PostgreSQL HA、streaming replica 或自动 failover；
- S3 条件创建仍不等于 Object Lock/WORM，HMAC 仍不等于公钥签名、KMS/HSM 或双人控制；
- 主 FastAPI `server` Profile 仍然 fail closed，完整服务器仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1k-backup-window-verification.json`。
