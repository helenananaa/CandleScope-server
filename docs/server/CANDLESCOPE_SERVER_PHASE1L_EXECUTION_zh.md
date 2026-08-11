# CandleScope Server Phase 1L：连续 WAL 覆盖证明

状态：QUERY_BACKUP_WAL_COVERAGE_COMPLETE_NOT_DEPLOYABLE

后续状态：Phase 1M 已加入一次性 `pg_basebackup` 编排、同机互斥、systemd service/timer 模板和结构化 journal 失败信号；本文件第 5 节关于“没有自动执行 `pg_basebackup`、staging 清理或任务互斥”的描述仅代表 Phase 1L 当时边界。当前边界以 `CANDLESCOPE_SERVER_PHASE1M_EXECUTION_zh.md` 为准。

Phase 1L 延续独立快照查询进程；主 FastAPI `server` Profile 仍保持 fail closed。它修复 Phase 1K 物理备份清单只签名 WAL archive 前缀、却没有证明恢复目标所需具体 WAL 已进入对象存储的缺口。

```text
Phase 1K exclusive write fence
  -> publish audit anchor
  -> publisher uses auditor DSN
  -> one SQL statement captures UTC target time + target LSN
     + pg_walfile_name(target LSN) + wal_segment_size
  -> parse base backup end_lsn/timeline
  -> enumerate every segment from end_lsn through target LSN
  -> wait for each immutable WAL object
  -> verify filename/URI/metadata/hash/exact segment size
  -> re-check live fence + database equals anchor
  -> sign physical-backup manifest v3 with ordered WAL receipts

independent verifier
  -> verify HMAC/canonical manifest/artifacts/PostgreSQL metadata/anchor
  -> re-read every signed WAL object with WAL-specific credentials
  -> require the exact same ordered URI/hash/size receipts
```

## 1. 恢复目标与连续区间

publisher 不再接受操作员传入的 target time 或 target LSN。它通过只读 auditor 连接，在一个 PostgreSQL SQL statement 中取得：

- canonical UTC 微秒 `recovery_target_time`；
- `pg_current_wal_lsn()`；
- `pg_walfile_name(target_lsn)`；
- `pg_size_bytes(current_setting('wal_segment_size'))`。

statement 使用相互依赖的 materialized CTE，先捕获时间、再捕获 LSN，避免把略早于 target time 的 LSN 当作覆盖终点。

目标 tuple 会校验 UTC 形式、64-bit 大写 LSN、24 位大写 WAL filename、合法 PostgreSQL segment size，以及 filename 与 LSN 的确定性对应关系。文件名计算遵循 PostgreSQL `pg_walfile_name()` 的边界语义：LSN 恰好位于 segment boundary 时归入前一段。

覆盖区间从 PostgreSQL `backup_manifest` 唯一 WAL range 的 `end_lsn` 所在段开始，到 target LSN 所在段结束，按 timeline、log、segment 顺序逐段枚举。当前只接受同一 timeline；target 早于 base backup end、非法 segment size、文件名不符或超过默认 4096 段上限都会 fail closed。这里不把对象 key 的字典序当作连续性证据。

## 2. 等待与失败语义

`CANDLESCOPE_SERVER_QUERY_BACKUP_WAL_COVERAGE_TIMEOUT_MS` 默认 120 秒，`WAL_COVERAGE_POLL_INTERVAL_MS` 默认 250 毫秒。只对“对象尚不存在”重试；已读取成功的早期段不会在每轮轮询中重复下载。以下情况立即失败：

生产 PostgreSQL 的 `archive_timeout` 与 archive command 重试/上传延迟必须落在该等待预算内；publisher 不需要、也不会使用 superuser 权限调用 `pg_switch_wal()`。超时意味着本次备份失败，不能把“稍后也许会归档”改写成成功。

- WAL 对象 metadata 的 format、cluster ID、SHA-256 或 size 漂移；
- 内容 hash、receipt filename 或精确 segment size 不符；
- 配置的 `WAL_ARCHIVE_PREFIX_URI` 与 WAL store 实际 URI 不同；
- target filename 与 target LSN/timeline 不同；
- base backup end 到 target 的段数超过上限。

等待结束后 publisher 才以 auditor 从 `pg_stat_activity`/`pg_locks` 重新确认 Phase 1K 指定 operator 的 exclusive fence 仍存在，并验证数据库审计 head 与 anchor 相同。WAL 对象不可变，catalog 在最终发布前仍会再读取完整覆盖区间；独立 verify 也再次读取。

## 3. Manifest v3

schema 升级为 `candlescope.query-physical-backup.v3`，对象路径升级为 `backups/v3/...`。在 v2 全部 fence、PostgreSQL、anchor 和 artifact 字段上新增签名字段：

- `recovery_target_lsn`；
- `wal_segment_size_bytes`；
- 有序 `wal_coverage`，每项包含 `filename`、明确 S3 URI、SHA-256 与 size。

构造和解析 manifest 时会重新计算完整文件名区间，要求 receipt 数量、顺序、timeline、URI prefix 和每段大小逐项一致。旧 v1/v2 清单不会被 v3 verifier 静默接受。

WAL archive 使用独立的 `CANDLESCOPE_SERVER_QUERY_WAL_` endpoint/bucket/prefix/credentials；backup artifacts 使用 `CANDLESCOPE_SERVER_QUERY_BACKUP_`；audit anchor 使用 `CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_`。三组权限可分别收紧。推荐入口仍是 Phase 1K window 内的 bundle：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_backup_window.py -- \
  backend/.venv/bin/python backend/scripts/server_query_backup_bundle.py \
  --backup-directory /secure/staging/query-control/backup-id \
  --backup-id 11111111-2222-4333-8444-555555555555
```

bundle 先发布 anchor，再调用 publisher；publisher 自己捕获 target、等待 WAL、复核 fence/anchor 并发布 v3。JSON 结果回显 target time/LSN/filename、segment size、覆盖段数和 manifest URI/hash，但不包含 DSN、S3 credentials 或 HMAC secret。

## 4. 真实门禁

PostgreSQL 18.4 + MinIO 门禁从全新卷执行：

1. 外部迁移、最小权限角色和 base 审计事件；
2. `pg_basebackup` tar/gzip/streamed WAL 与 `pg_verifybackup`；
3. Phase 1K exclusive fence 内发布 audit anchor，并确认 runtime 探针写入被拒；
4. 使用真实 auditor 角色捕获 target time、LSN、filename 和 16 MiB segment size；
5. 强制归档，逐段上传 MinIO、下载并校验；
6. 从 base backup `end_lsn` 到 target LSN 计算连续文件名列表，读取全部对象并写入 manifest v3；
7. catalog 独立验证 v3、三个 artifacts、完整 WAL receipt 与 anchor；
8. fence 释放后写入 target 后事件；
9. 从空卷恢复到 inclusive target 并 promote，恢复库严格等于 target anchor，target 后事件不存在；
10. 恢复后的 runtime 能继续追加，审计 hash chain 完整。

单元门禁还覆盖 WAL log boundary、LSN 恰好位于 segment boundary、区间倒退、段数上限、非法 segment size/64-bit LSN、缺段等待/超时、错误尺寸、清单确定性以及 WAL 对象缺失或篡改。

重跑：

```bash
docker compose -f deploy/server/compose.phase1j.yml --profile recovery down -v
docker compose -f deploy/server/compose.phase1j.yml up -d --wait postgres minio
CANDLESCOPE_PHASE1J_INTEGRATION=1 \
  CANDLESCOPE_PHASE1J_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1j_pitr_operations.py
docker compose -f deploy/server/compose.phase1j.yml --profile recovery down -v
```

## 5. 明确未完成

- 没有部署 timer、CronJob、常驻 scheduler、告警路由、自动重试策略或任务互斥；
- 没有自动执行 `pg_basebackup`、staging 清理、orphan 清理、retention、备份选择或恢复审批；
- 没有证明跨 timeline 恢复；timeline 变化会 fail closed，而不是自动追踪 `.history`；
- 没有证明 24/72 小时 WAL 连续性、对象存储长故障后的积压追赶、磁盘耗尽、网络分区、并发备份或大型 manifest/backup 容量；
- 没有生产 RPO/RTO、异地复制、PostgreSQL HA、streaming replica 或自动 failover；
- S3 conditional create 不等于 Object Lock/WORM，HMAC 不等于公钥签名、KMS/HSM 或双人控制；
- publisher 仍把三个有上限的 backup artifacts 读入内存，不是大型集群的 streaming/multipart 实现；
- 主 FastAPI `server` Profile 仍然 fail closed，完整 CandleScope server 仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1l-wal-coverage-verification.json`。
