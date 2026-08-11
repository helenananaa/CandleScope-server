# CandleScope Server Phase 1J：WAL 归档与时间点恢复

状态：QUERY_CONTROL_WAL_PITR_DRILL_COMPLETE_NOT_DEPLOYABLE

Phase 1J 延续独立快照查询进程，主 FastAPI `server` Profile 仍保持 fail closed。它把 Phase 1I 的逻辑 dump 恢复证明推进为一个有签名恢复目标的 PostgreSQL 18 物理备份集：

```text
PostgreSQL primary
  -> pg_basebackup tar + gzip + streamed WAL
  -> pg_verifybackup --no-parse-wal

continuous archive_command
  -> strict PostgreSQL WAL filename and size checks
  -> S3-compatible conditional create
  -> same name only accepts identical bytes and metadata

quiesced query-control head
  -> Phase 1I signed audit anchor
  -> exact PostgreSQL UTC recovery_target_time
  -> signed physical-backup manifest + three immutable artifacts

restore drill
  -> verify backup HMAC, artifact hashes, PostgreSQL metadata and anchor HMAC
  -> restore WAL through target time and promote
  -> recovered audit chain equals target anchor
  -> event after target is absent; runtime can append after promotion
```

## 1. 不可变物理备份集

每个 backup ID 只接受三个非空普通文件，拒绝额外文件、symlink 和越界对象：

- `backup_manifest`；
- `base.tar.gz`；
- `pg_wal.tar.gz`。

发布器先把受大小限制的三个输入读入内存，在权限收紧的临时目录重建即将上传的同一组 bytes，再用与服务端同版本的绝对路径执行 `pg_verifybackup --no-parse-wal`，避免原 staging 目录在校验与上传之间被替换；随后解析 PostgreSQL backup manifest v2。外层 RFC 8785/HMAC-SHA256 清单绑定：

- canonical UUID backup ID、cluster ID、创建毫秒时间；
- PostgreSQL 版本、system identifier、timeline、start/end LSN；
- PostgreSQL 可直接接受的 UTC 微秒 `recovery_target_time`，固定格式 `YYYY-MM-DD HH:MM:SS.ffffff+00`；
- WAL archive 的明确 S3 前缀；
- Phase 1I audit anchor 的 URI、对象 SHA-256、head sequence/hash 和迁移版本/hash；
- 三个备份对象各自的 URI、大小和 SHA-256；
- 独立 backup HMAC key ID。

对象与清单均使用条件创建；同 backup ID 重跑只接受逐字节一致内容。verify 会重新读取所有对象，验证清单 HMAC、RFC 8785 canonical bytes、大小/hash、对象路径以及 PostgreSQL manifest 元数据。`server_query_backup_verify.py` 还会验证 audit anchor HMAC，并逐项核对备份清单对锚点的引用。

这些入口是可交给 systemd timer、Kubernetes CronJob 或外部调度器调用的一次性工具，不包含常驻调度器：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_backup_publish.py \
  --backup-directory /secure/staging/query-control/backup-id \
  --backup-id 11111111-2222-4333-8444-555555555555 \
  --created-at-ms 1786453200000 \
  --recovery-target-time '2026-08-11 13:00:00.123456+00' \
  --audit-anchor-uri 's3://bucket/prefix/anchors/v1/key/anchor.json'

PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_backup_verify.py \
  --manifest-uri 's3://bucket/prefix/backups/v1/cluster/id/manifest.json' \
  --audit-anchor-uri 's3://bucket/prefix/anchors/v1/key/anchor.json'
```

环境变量前缀为 `CANDLESCOPE_SERVER_QUERY_BACKUP_`。必需项包括 auditor DSN、S3 endpoint/region/bucket/prefix/credentials、cluster ID、PostgreSQL 版本、WAL prefix URI，以及彼此独立的 anchor/backup HMAC key ID 和严格 Base64 secret。`PG_VERIFYBACKUP_EXECUTABLE` 必须是普通绝对路径。HMAC secret 解码后至少 32 bytes，凭据和 secret 不出现在 JSON 输出中。

## 2. WAL archive 与 restore_command

`server_query_wal_archive.py SOURCE_PATH WAL_FILENAME` 是 `archive_command` 适配器；`server_query_wal_restore.py WAL_FILENAME DESTINATION` 是 `restore_command` 适配器。两者只通过 `CANDLESCOPE_SERVER_QUERY_WAL_` 环境变量取得 S3 配置，不接受命令行凭据。

WAL 对象只接受 PostgreSQL 的三类归档名：24 位大写十六进制 segment、8 位 timeline `.history`、以及 segment 加 8 位 offset 的 `.backup`。路径、斜杠、小写名和未知恢复名均被拒绝。默认单对象上限 64 MiB；归档记录 format、cluster ID、SHA-256 与大小 metadata，同名已存在时必须 bytes 完全一致。restore 会重新验证 metadata、长度和 hash，并在明确的 recovery root 内以临时文件、`fsync`、原子 replace 和 `0600` 权限落盘。

生产配置必须确保 bucket 已预创建、凭据只允许指定前缀、archive command 的失败退出码被 PostgreSQL 重试，并监控 `pg_stat_archiver.failed_count`、最后成功 WAL、归档延迟和对象存储可用性。Phase 1J 的本地 Compose 为便于真实恢复使用本地只读 WAL 镜像；门禁会先把每个 WAL 条件上传到 MinIO、重新下载并验证，再用验证后的 bytes 重写恢复镜像。

## 3. 一致备份顺序

当前工具没有自动暂停查询写入。外部编排必须建立一个受控维护窗口：

1. 保证 WAL archive 已持续运行且没有失败；
2. 用 PostgreSQL 18 `pg_basebackup --format=tar --gzip --wal-method=stream --manifest-checksums=SHA256` 捕获 base backup；
3. 暂停 query-control runtime 写入并确认没有活跃写者；
4. 以 Phase 1I auditor 发布当前审计锚；
5. 从数据库取得 UTC 微秒目标时间，在仍保持静止时运行 backup publisher，使其再次验证数据库与该锚一致；
6. 强制 WAL switch，并确认覆盖目标时间的 WAL 已成功进入不可变归档；
7. verify 备份集与锚点，可靠保存 manifest URI、anchor URI 和各自 SHA-256 后再解除写入暂停。

若第 3 至第 5 步之间仍有查询审计写入，恢复数据库可能超过锚点或发布器观察到不同 head；该次备份不得标记为可恢复集合。Phase 1J 只证明受控窗口流程，没有实现跨实例自动 drain/fence。

## 4. 真实 PostgreSQL 18 门禁

本地门禁使用 `deploy/server/compose.phase1j.yml` 的 PostgreSQL 18.4 与 MinIO：

1. 新卷上运行外部迁移并写入 base 审计事件；
2. 生成 tar/gzip base backup 并通过同版本 `pg_verifybackup`；
3. 写入 target 前事件，验证两条完整审计链，发布 HMAC anchor 并取得数据库目标时间；
4. 写入 target 后事件，主库此时有三条记录；
5. 强制 WAL switch，确认 `failed_count=0`，把所有归档 WAL 上传、下载校验并重写本地恢复镜像；
6. 发布并验证绑定目标时间、WAL 前缀和 anchor 的物理备份清单，备份文件也从对象存储重新下载；
7. 从空恢复卷启动 PITR，只恢复到 inclusive target 后 promote；
8. 恢复库逐字段等于两条记录的 target anchor，target 前事件存在、target 后事件不存在；
9. 恢复后的 runtime 成功追加第 3 条记录，`pg_is_in_recovery()` 为 false。

重跑：

```bash
docker compose -f deploy/server/compose.phase1j.yml --profile recovery down -v
docker compose -f deploy/server/compose.phase1j.yml up -d --wait
CANDLESCOPE_PHASE1J_INTEGRATION=1 \
  CANDLESCOPE_PHASE1J_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1j_pitr_operations.py
docker compose -f deploy/server/compose.phase1j.yml --profile recovery down -v
```

`ALLOW_TEST_RESET=1` 是显式破坏性测试开关，只能用于该 Compose 的一次性数据卷。

真实结果还证明了一个不能隐藏的 PostgreSQL 语义：恢复前 head sequence 为 2，恢复后追加事件得到 sequence 35，而不是 3。identity sequence 不具事务连续性，物理恢复和重放允许消耗但未形成审计行的值。因此合同要求新 sequence 严格大于旧 head，不能要求 `+1`；record count 仍从 2 增至 3，审计 hash chain 与数据库 head 仍完整一致。

## 5. 明确未完成

- 没有部署 systemd timer、CronJob、常驻 scheduler、自动写入 drain/fence、retention/pruning 或备份生命周期控制；
- 没有证明 24/72 小时 WAL 连续性、归档积压恢复、对象存储长故障、磁盘耗尽、时间漂移或并发备份行为；
- 没有测量或承诺生产 RPO/RTO，单次约 10 秒的本地小数据恢复不能外推到生产数据量；
- 没有异地/跨账号复制、PostgreSQL streaming replica、HA、自动 failover 或网络分区证据；
- S3 条件创建不等于 Object Lock/WORM；HMAC 不是公钥签名，没有 KMS/HSM、密钥轮换或双人控制；
- publisher 当前在内存中读取受限大小的三个 artifact，没有 multipart/streaming 上传，不适用于未经重新测量的大型集群；
- `pg_verifybackup --no-parse-wal` 不解析 WAL；本阶段另行逐对象验证 WAL hash，但未替代生产级 WAL 可读性和连续性检查；
- Compose 的 recovery 使用本地文件镜像，不声称已经验证生产 S3 网络上的实时 `restore_command` 吞吐和重试；
- 没有审计表分区、自动 retention、增量验证或长链 compaction；
- API gateway、mTLS/OIDC、终端用户/组织/工作区授权仍未实现；
- 主 FastAPI `server` Profile 仍然 fail closed，完整服务器仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1j-wal-pitr-verification.json`。
