# CandleScope Server Phase 1M：一次性物理备份调度合同

状态：QUERY_BACKUP_SCHEDULE_CONTRACT_COMPLETE_NOT_DEPLOYABLE

后续状态：Phase 1N 已加入有界恢复候选选择；Phase 1O 又要求每次成功先把 v1 核心 receipt 签名并持久化到不可变对象，再输出带历史 URI/hash 的 v2 job result。Phase 1N 可以直接消费明确提供的签名历史 URI。本文件第 5 节关于“没有完整备份索引”的边界仍成立：没有对象列举、不可遗漏证明或全局 latest。当前边界分别以 Phase 1N/1O 执行文档为准。

Phase 1M 延续独立快照查询进程；主 FastAPI `server` Profile 仍保持 fail closed。它把 Phase 1L 已验证但依赖人工拼接的物理备份步骤收敛为一个可由 systemd、CronJob 或其他外部调度器调用的一次性 job：

```text
external scheduler
  -> same-host non-blocking kernel lock
  -> private unique staging directory
  -> bounded pg_basebackup with dedicated replication identity
  -> exact artifact-set validation
  -> Phase 1K exclusive PostgreSQL write fence
     -> Phase 1I audit anchor
     -> Phase 1L manifest v3 + continuous WAL coverage
  -> strict two-receipt validation
  -> immutable HMAC-signed success history
  -> one scheduled-job result with history URI/hash
  -> staging cleanup + lock release
```

Phase 1M 不在 FastAPI 进程内加入 scheduler，也不把 timer 的存在当作生产备份成功。调度、执行、失败信号和恢复验证保持为可分别审计的边界。

## 1. 一次性 job 合同

入口是 `backend/scripts/server_query_backup_job.py`。每次运行使用 canonical UUID `run_id`，并以同一个值作为 backup ID。job 只执行一次，不自行重试或常驻：

1. 校验 PostgreSQL 连接环境、私有 `.pgpass`、staging root、lock root 和 `pg_basebackup` executable；
2. 非阻塞取得同机 kernel file lock；已被占用时输出 `JOB_ALREADY_RUNNING` 并以临时失败退出码 75 结束；
3. 创建 mode `0700` 的唯一 staging 目录；
4. 以固定参数执行 `pg_basebackup`：tar、gzip、streamed WAL、fast checkpoint、SHA-256 manifest checksums、禁止交互式密码提示；
5. 只接受 `backup_manifest`、`base.tar.gz`、`pg_wal.tar.gz` 三个非空 regular file，额外、缺失或 symlink artifact 均失败；
6. 调用已有 Phase 1K window，并在窗口内运行 Phase 1L bundle；
7. 只接受恰好两行 canonical JSON，交叉核对 backup ID、fence ID/时间、成功 exit code、S3 URI、hash、WAL 覆盖数、恢复目标和 operator；
8. 构造 `candlescope.query-backup-job-receipt.v1` 核心 receipt，并将其放入 `candlescope.query-backup-run-history.v1` HMAC envelope，以条件创建写入固定对象路径；
9. 只有历史发布成功后才输出 `candlescope.query-backup-job-result.v2`，其中保留核心字段并加入历史 schema、URI、SHA-256 与 created 标记；最后清理 staging 并释放 lock。

子进程 runtime 和 stdout/stderr 均有界；超时或输出超限会终止整个 process group。失败 receipt 只包含 schema、run ID、phase、稳定错误码和可选 child exit code，不回显命令 stderr、DSN、密码、S3 key 或 HMAC secret。历史发布失败固定为 `SUCCESS_HISTORY_PUBLISH_FAILED`/`history`；staging 清理失败也会使 job 失败，不会把残留物改写成成功。

## 2. 身份、凭据与互斥边界

`pg_basebackup` 必须使用独立的 PostgreSQL `LOGIN REPLICATION` 角色，不需要也不应取得 superuser、createdb 或 createrole。生产 `pg_hba.conf` 只应从备份执行主机允许该角色进入 replication connection，并要求 TLS/SCRAM；测试门禁使用临时 SCRAM 规则，不是生产网络策略。

job 拒绝 `PGPASSWORD`、`PGSERVICE` 和 `PGSERVICEFILE`。`PGPASSFILE` 必须是当前服务账号拥有的绝对路径、非 symlink、非空 `0600` regular file。staging root 和 lock root 也必须由当前账号拥有、路径中不解析 symlink，并禁止 group/world 权限；lock file 使用 `O_NOFOLLOW`（平台支持时）和 `0600`。

环境按阶段收窄：

- `pg_basebackup` 只收到必要 libpq/SSL/locale allowlist，不会收到任何 `CANDLESCOPE_` S3、DSN 或 HMAC 配置；
- backup window 会收到自身需要的 CandleScope 配置，但明确移除 replication 的 `PGHOST`、`PGUSER` 和 `PGPASSFILE` 等变量；
- 签名历史在 window 成功后由父 job 发布，独立的 history S3/HMAC 配置不会传入 window 子进程；
- 同机 file lock 防止同一节点任务重叠；不同节点仍由 Phase 1K PostgreSQL exclusive advisory lock 串行化一致性窗口。

file lock 不是分布式锁，PostgreSQL fence 也只保护 query-control 写事务；它们不停止 ClickHouse/Parquet 读请求，也不替代外部调度器的 missed-run、retry 或 leader-election 语义。

## 3. systemd 模板

`deploy/server/systemd/` 提供：

- hardened `candlescope-query-backup.service` oneshot；
- 每天本地时间 02:00、最多 15 分钟随机延迟且支持 missed-run catch-up 的 timer；
- `OnFailure` 触发的结构化 journal failure signal；
- 完整环境文件示例和部署检查单。

service 使用专用 `candlescope-backup` Unix 用户、私有 runtime/state 目录、`UMask=0077`、25 分钟 systemd 上限以及 filesystem/kernel/namespace hardening。job 内部 base backup 默认 15 分钟、window 默认 6 分钟；外层预算必须大于两者加清理开销。

failure hook 输出 `candlescope.query-backup-failure-signal.v1`，只表示某个 unit 失败并要求人工处置。它没有网络权限，也没有发送邮件、PagerDuty、Slack 或其他外部消息；生产启用前必须由独立日志/监控代理匹配该 schema 并验证实际送达。

模板安装、权限设置、数据库角色、HBA、TLS、secret provider、日志采集和 `systemctl enable` 都不由本阶段自动执行。操作步骤见 `deploy/server/systemd/README_zh.md`。

## 4. 验证门禁

单元门禁覆盖成功顺序和最终 receipt、basebackup/窗口失败、异常 artifact、非 canonical receipt、同机锁冲突、环境隔离、ambient password、passfile/root 权限、child timeout/output limit、failure signal 与 systemd hardening。

显式真实门禁在 PostgreSQL 18 Compose 实例上创建一个临时的非 superuser replication role，使用真实 PostgreSQL 18 `pg_basebackup` 产生三个 artifacts，并让 Phase 1M job 完成 staging 校验、receipt 聚合、清理和锁释放。窗口输出在这个窄门禁中使用固定合法 fixture；完整 fence、MinIO manifest v3、连续 WAL 和 PITR 仍由 Phase 1L 的真实端到端门禁证明，不能把两者合写为一次新的全链生产演练。

重跑 Phase 1M 窄门禁：

```bash
docker compose -f deploy/server/compose.phase1j.yml up -d --wait postgres
CANDLESCOPE_PHASE1M_INTEGRATION=1 \
  CANDLESCOPE_PHASE1M_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1m_scheduled_backup.py
docker compose -f deploy/server/compose.phase1j.yml --profile recovery down -v
```

## 5. 明确未完成

- systemd 模板没有安装或启用，当前没有正在运行的生产 timer；
- journal failure signal 没有接入真实告警路由，也没有验证值班送达；
- 已有不可变签名成功历史，但没有自动 retry/backoff、missed-run SLO、对象 list/完整性证明、orphan staging 扫描、retention/pruning 或恢复审批；
- 没有将数据库/对象存储/HMAC secrets 接入生产 secret manager，环境文件仍只是示例；
- 没有证明 24/72 小时连续调度、长期 WAL 健康、对象存储故障追赶、磁盘耗尽、网络分区、主机重启、多调度节点或大型物理备份容量；
- Phase 1M 窄真实门禁没有重新证明完整 MinIO/PITR 链；该证明仍来自 Phase 1L 门禁；
- 没有生产 RPO/RTO、异地复制、PostgreSQL HA、streaming replica、自动 failover 或 restore scheduler；
- 主 FastAPI `server` Profile 仍然 fail closed，完整 CandleScope server 仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1m-scheduled-backup-verification.json`。
