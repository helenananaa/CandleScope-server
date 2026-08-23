# CandleScope Server Phase 1O：签名成功历史与有界 Cadence 门禁

状态：QUERY_BACKUP_SIGNED_HISTORY_COMPLETE_NOT_DEPLOYABLE

Phase 1O 延续外部调度的一次性物理备份模型；主 FastAPI `server` Profile 仍保持 fail closed。它补上 Phase 1N 只能依赖 journal 或人工保存 receipt 的缺口：一次 Phase 1M job 只有在成功核心 receipt 已签名并条件写入不可变对象后，才允许对外报告成功。

```text
Phase 1M verified v1 core receipt
  -> RFC 8785 canonical nested receipt
  -> independent HMAC-SHA256 history envelope
  -> put-if-absent deterministic object key
  -> return v2 job result with history URI/hash
  -> explicit URI set can feed cadence verifier or Phase 1N selector
```

这不是可列举的完整索引，不改变恢复审批边界，也不授予删除权限。

## 1. 不可变签名成功历史

历史 schema 为 `candlescope.query-backup-run-history.v1`，包含 algorithm、key ID、cluster ID、完整 v1 核心 receipt 和 HMAC signature。签名覆盖除 signature 外的全部 RFC 8785 canonical JSON。对象 key 固定为：

```text
backup-runs/v1/<cluster_id>/<completed_at_ms 20-digit>-<backup_uuid>.json
```

repository 只做条件创建。同一成功重复发布必须逐字节一致；若固定 key 已有不同内容则 fail closed。读取时重新验证 strict JSON、exact schema、RFC 8785 bytes、HMAC、key ID 和由内容推导出的 canonical URI。它不依赖对象 list，也不会 overwrite 或 delete。

历史 HMAC 与物理备份 manifest、audit anchor 使用不同配置前缀和 key。生产 IAM 应把该身份限制为历史固定前缀的条件创建和读取；是否具备真正 WORM/Object Lock、版本保留、跨账号复制及独立密钥托管，仍需部署侧证明。

## 2. Phase 1M/1N 接入

Phase 1M 在 basebackup、fence、anchor、连续 WAL、manifest 和双 receipt 校验全部完成后构造 `candlescope.query-backup-job-receipt.v1`。父 job 随后发布签名历史；配置、S3 或 HMAC 失败统一输出 `SUCCESS_HISTORY_PUBLISH_FAILED`、phase `history`，不会产生成功结果。

成功输出升级为 `candlescope.query-backup-job-result.v2`。它保留全部 v1 核心字段，并加入：

- `success_history_schema_version`；
- `success_history_uri`；
- `success_history_sha256`；
- `success_history_created`。

历史对象内部仍嵌套 v1 receipt，避免把外层历史 URI/hash 反向签进自身造成循环引用。Phase 1N 本地文件解析器兼容 exact canonical v1/v2；推荐入口是一个或多个 `--history-uri`，因为该路径会真实复验历史 HMAC 与 canonical URI，再将嵌套 receipt 对账到签名物理 manifest。两种候选来源互斥。

## 3. 显式窗口 Cadence

`server_query_backup_history.py verify-cadence` 接受调用方明确提供的 1–64 个历史 URI、cluster ID 及闭区间起止毫秒。每条历史先独立验证，随后按完成时间和 backup ID 排序，并把以下间隔全部纳入最大 gap：

- 窗口起点到第一个成功；
- 每两个相邻成功；
- 最后一个成功到窗口终点。

默认最大 gap 为 30 小时，未来时钟偏差为 5 分钟。跨 cluster、重复 backup/URI、窗口外成功、未来窗口、数量超限或任一 gap 超限均失败。成功结果 schema 为 `candlescope.query-backup-success-cadence.v1`，包含顺序无关的 scope SHA-256、每次成功的历史 URI/hash 与明确的：

```json
{
  "global_history_completeness_proven": false,
  "scheduled_slot_execution_proven": false
}
```

这项证明只回答“给定这些已签名成功记录，在这个显式窗口中观察到的最大间隔是否合格”。因为没有 list 或不可遗漏证明，它不能回答“所有运行是否都已提供”，也不能把一次成功精确归因到某个 systemd 计划槽位。

示例：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_backup_history.py verify-cadence \
  --history-uri s3://backup-history/query-operations/backup-runs/v1/production-primary/00000001765500000000-00000000-0000-4000-8000-000000000001.json \
  --history-uri s3://backup-history/query-operations/backup-runs/v1/production-primary/00000001765586400000-00000000-0000-4000-8000-000000000002.json \
  --expected-cluster-id production-primary \
  --window-start-ms 1765500000000 \
  --window-end-ms 1765586400000
```

运行时异常只输出 `candlescope.query-backup-history-failure.v1` 和稳定错误码，不回显 DSN、S3 credential 或 HMAC secret。CLI 本身不会自动收集 URI；生产监控系统必须可靠保存每次 v2 结果中的历史引用，再明确构造窗口输入。

## 4. 配置与验证门禁

`CANDLESCOPE_SERVER_QUERY_BACKUP_HISTORY_` 配置 S3 endpoint/region/bucket/prefix/credential、独立 HMAC key、cluster ID、请求超时、最大历史数、最大 gap 与未来偏差。示例已加入 `deploy/server/systemd/query-backup.env.example`。父 job 发布历史时需要这组变量，但会在启动 backup-window 子进程前将其全部移除。

单元门禁覆盖确定性 publish/replay、HMAC 漂移、非 canonical URI、不可变冲突、cluster/重复/数量/未来/gap 边界、窗口首尾、顺序无关 scope、Phase 1M fail-closed 发布和 Phase 1N history URI 来源。

真实 PostgreSQL 18 + MinIO 门禁从新卷完成 fenced physical backup、连续 WAL、anchor、签名历史发布、显式 cadence 验证、从历史 URI 选择、selected artifacts/WAL/anchor 重下载复验及 target-time PITR；独立 Phase 1M 窄门禁使用真实非 superuser replication role 验证 `pg_basebackup` 与 v2 成功结果。

## 5. 明确未完成

- 没有对象 list、完整历史索引、不可遗漏证明或 global latest；
- 没有安装/启用 systemd 模板，没有 24/72 小时连续运行证据，也没有 missed-run SLO；
- cadence verifier 不是常驻监控器，未接入真实告警路由或验证值班送达；
- 没有自动 retry/backoff、跨调度节点 leader election 或 orphan staging 扫描；
- 没有 retention/pruning/delete、WORM/Object Lock、跨账号/异地复制的生产证明；
- 没有自动 fallback、restore approval、双人控制、自动从空卷恢复或定期恢复演练；
- 没有生产 RPO/RTO、PostgreSQL HA、streaming replica 或自动 failover；
- 主 FastAPI `server` Profile 仍 fail closed，完整 CandleScope server 仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1o-backup-history-verification.json`。
