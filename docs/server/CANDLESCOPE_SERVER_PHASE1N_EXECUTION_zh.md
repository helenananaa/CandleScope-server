# CandleScope Server Phase 1N：有界恢复候选选择与新鲜度门禁

状态：QUERY_BACKUP_RECOVERY_SELECTION_COMPLETE_NOT_DEPLOYABLE

Phase 1N 延续独立快照查询进程；主 FastAPI `server` Profile 仍保持 fail closed。它解决 Phase 1M 成功回执存在后仍需人工判断“应该恢复哪一个”的窄问题，但不声称对象存储已有可列举的完整备份索引。Phase 1O 现已补充签名不可变成功历史，因此选择器优先支持明确提供历史 URI；为兼容既有产物，原有私有 v1 receipt 文件入口仍保留。

```text
operator supplies exactly one source:
  1..32 signed Phase 1O history URIs, or private Phase 1M receipt/result files
  -> history HMAC + canonical URI, or strict canonical v1/v2 file
  -> read each canonical signed manifest only
  -> reconcile unsigned job receipt to signed manifest fields
  -> require expected cluster + PostgreSQL system ID + timeline
  -> reject duplicate, future, stale, or tied latest target
  -> choose unique latest target within the supplied set
  -> fully verify only the selected manifest + artifacts + WAL + anchor
  -> emit bounded selection receipt
```

## 1. 两阶段验证

`ImmutablePhysicalBackupCatalog.inspect_signed_manifest()` 只读取 manifest，校验 RFC 8785 bytes、HMAC、schema 和 canonical manifest URI。它不会读取 artifacts 或 WAL，因此方法名明确是 inspect，不代表该备份已经可恢复。现有 `verify()` 先复用 inspect，再继续验证三个物理备份对象、PostgreSQL `backup_manifest` metadata 和完整 WAL coverage，语义没有降级。

`server_query_backup_select.py` 第一阶段对每个输入回执执行 metadata inspect。若输入为历史 URI，会先复验 Phase 1O envelope 的 HMAC、RFC 8785 bytes、cluster 和 canonical URI，再取出其中的 v1 核心 receipt。若输入为本地 v1 receipt 或 v2 job result，文件本身仍不能单独作为真实性证据；v2 的历史引用只做严格形状校验，只有 `--history-uri` 路径会实际验证历史对象。无论来源为何，选择器都会把 receipt 的以下字段逐项对到 HMAC 签名 manifest：

- backup ID 与 canonical manifest URI/hash；
- audit anchor URI/hash；
- recovery target time、LSN、最后 WAL filename 与覆盖段数；
- write fence ID；
- manifest 创建时间和 fence 取得时间必须位于 job start/completion 区间。

任何 unsigned receipt 漂移都会失败。通过 metadata 筛选后，只对唯一选中项调用完整 backup verifier；它会重新下载 artifacts、全部签名 WAL 对象和 anchor。若最新项完整验证失败，命令失败，不会悄悄退回更旧项。

## 2. 选择与新鲜度合同

调用方必须显式提供预期 `cluster_id`、PostgreSQL `system_identifier` 和 `timeline`。默认边界为：

- 最多 32 个候选；
- 每个本地 receipt/result 最大 64 KiB，必须为当前账号拥有的绝对路径、非 symlink、private regular file，并在平台支持时通过 `O_NOFOLLOW` descriptor 读取；
- 本地输入接受 exact canonical v1 receipt 或带严格历史引用的 exact canonical v2 result；历史 URI 输入必须通过独立 history HMAC 和 canonical URI 校验；两种来源不得混用；
- 选中 recovery target 距当前时间最多 30 小时；
- 最多允许 5 分钟未来时钟偏差；
- 同一 backup/manifest 重复、不同备份具有相同最新 target、跨 cluster/system/timeline 或任何字段漂移均 fail closed。

结果 schema 为 `candlescope.query-backup-recovery-selection.v1`，包含输入集合的 RFC 8785 scope hash、评估时间、年龄、预期身份与选中 manifest/anchor/target/fence。它固定输出：

```json
{
  "latest_within_supplied_receipts": true,
  "global_latest_proven": false,
  "selected_backup_fully_verified": true
}
```

scope hash 只是把本次输入集合绑定到结果，不是签名或审批。对象存储 port 没有 list 能力；即使使用签名历史 URI，省略某条输入时选择器也无法证明不存在更新备份，因此绝不输出 global latest。

## 3. 操作入口

推荐直接从 Phase 1M v2 成功结果取出 `success_history_uri`，以一个或多个明确 URI 调用选择器。不要把对象前缀、任意手写 URI 或未验证的日志字段当成完整历史集合：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_backup_select.py \
  --history-uri s3://backup-history/query-operations/backup-runs/v1/production-primary/00000001765500000000-00000000-0000-4000-8000-000000000001.json \
  --expected-cluster-id production-primary \
  --expected-system-identifier 7672760263611183147 \
  --expected-timeline 1
```

兼容入口仍可从受保护日志导出 v1/v2 成功 JSON；文件必须保持 job 输出的单行 canonical JSON 加最终 LF，并设置 owner 为执行选择器的账号、mode `0600`。不要把失败 signal 或松散 JSON 当成候选：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_backup_select.py \
  --receipt /secure/backup-runs/run-1.json \
  --receipt /secure/backup-runs/run-2.json \
  --expected-cluster-id production-primary \
  --expected-system-identifier 7672760263611183147 \
  --expected-timeline 1
```

选择器复用 Phase 1M 环境文件里的 backup、audit-anchor、WAL 和 history 四组只读验证配置；`CANDLESCOPE_SERVER_QUERY_BACKUP_SELECT_` 控制候选/文件/新鲜度上限。生产恢复流程应保存 selection JSON 与输入 scope，随后进入独立人工审批和从空卷 restore 流程，而不是让该命令直接启动或 promote PostgreSQL。

## 4. 验证门禁

单元门禁覆盖 exact/canonical/duplicate-key v1 receipt、v2 result 兼容、签名历史 URI 来源、输入顺序无关的 scope hash、唯一最新选择、过期/未来/并列、错误 cluster/system/timeline、receipt/manifest 漂移、job 时间区间、private file、完整 verify 漂移和 canonical manifest URI。

真实 PostgreSQL 18 + MinIO 门禁从新卷执行 Phase 1J–1L 的 fenced physical backup、连续 WAL、anchor 和 PITR 流程；在 fence 释放后构造真实 Phase 1M 核心 receipt，将其发布成真实签名历史，验证一个显式 cadence 窗口，再由 Phase 1N 从该历史 URI 对真实 S3 manifest 做 metadata 选择，并重新下载 selected artifacts/WAL/anchor 完整验证，之后才继续 target-time 恢复。门禁使用一个候选，证明真实链与历史来源集成；多候选排序由单元 fixture 证明。

## 5. 明确未完成

- Phase 1O 已持久化并签名 Phase 1M 成功核心 receipt，但仍没有对象 list 或自动发现 API；调用方必须显式提供历史 URI；
- 没有 S3 list、完整备份索引、不可遗漏证明或全局 latest；输入集合可能漏掉更新备份；
- Phase 1O 可验证显式输入窗口的成功 gap，但没有证明输入历史集合完整、systemd 精确计划槽位、missed-run SLO、24/72 小时连续调度或真实告警送达；
- 没有自动 fallback；最新候选损坏时必须失败并由受权人员重新评估；
- 没有 restore approval、双人控制、自动从空卷恢复、定期 restore timer 或生产 RPO/RTO；
- 没有 retention/pruning 或删除权限，本阶段不会删除任何 manifest、artifact、WAL 或 anchor；
- systemd timer 仍只是模板，主 FastAPI `server` Profile 仍 fail closed，完整 CandleScope server 仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1n-backup-selection-verification.json`。
