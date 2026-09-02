# CandleScope Server Phase 1P：主机成功清单、Cadence Monitor 与告警投递

状态：QUERY_BACKUP_CADENCE_MONITOR_COMPLETE_NOT_DEPLOYABLE

后续状态：Phase 1Q 不再延伸备份/cadence 控制面，而是回到 Phase 0 纵向链路，把冷端快照查询接到现有回放成交读取入口。备份模板安装、真实值班送达和 72 小时 cadence 观察仍是部署责任；当前产品读取边界以 `CANDLESCOPE_SERVER_PHASE1Q_EXECUTION_zh.md` 为准。

Phase 1P 延续外部 systemd 调度和独立查询进程；主 FastAPI `server` Profile 仍保持 fail closed。它把 Phase 1O 只能由调用方手工提供 history URI 的 cadence verifier 接到一个主机私有引用目录，并提供独立 hourly monitor 与 HMAC HTTPS webhook 传输合同。

```text
Phase 1M remote signed history succeeds
  -> atomically persist one private local history reference
  -> emit Phase 1M v2 success result

hourly monitor
  -> bounded private-directory scan
  -> fetch every in-window signed history by explicit URI
  -> reconcile local URI/hash/backup/time to HMAC history
  -> verify 72h window with 30h maximum gap
  -> healthy JSON, or one HMAC HTTPS alert and non-zero exit
```

本阶段不启用模板、不调用生产 webhook、不执行恢复，也没有 overwrite/delete 权限。

## 1. 主机私有成功引用

成功引用 schema 为 `candlescope.query-backup-success-reference.v1`，只包含 cluster ID、backup ID、完成时间、history URI 和 history SHA-256。文件名固定为：

```text
<completed_at_ms 20-digit>-<backup_uuid>.json
```

引用以 RFC 8785 canonical JSON、mode `0600` 写入 owner/mode `0700` 的绝对目录。实现先写随机私有临时文件并 fsync，再以 hard link 条件创建固定文件、fsync 目录并移除临时链接；同名重放只接受逐字节一致内容。并发 monitor 会忽略严格匹配内部命名规则的临时链接，避免读到发布中间态；其他异常文件名、symlink、共享权限、非 regular file、非 canonical JSON、内容/文件名漂移或正式引用超过默认 4096 都会 fail closed。崩溃遗留临时链接不会被自动清理，正式引用也没有自动删除或修复。

该引用不另行签名，真实性来自回源后的 Phase 1O HMAC history。Phase 1M 只有在远端 history 和本机引用都成功后才输出 v2 success；引用失败固定为 `SUCCESS_INVENTORY_PERSIST_FAILED`、phase `inventory`。由于远端历史先发布，引用失败可能留下不可变远端历史，但这次 job 不报告成功，也不会覆盖或删除该对象。

默认目录为 `/var/lib/candlescope-query-backup-history`，由 backup 与 monitor service 共享同一 `candlescope-backup` Unix identity。history、monitor 配置不会传给 backup-window 子进程，`pg_basebackup` 仍只收到原有 libpq allowlist。

## 2. 独立 Cadence Monitor

入口为 `backend/scripts/server_query_backup_monitor.py`。monitor 默认每次取当前 wall clock 为窗口终点，回看 72 小时；最多读取 4096 个、每个 16 KiB 的本机引用，其中窗口内最多 64 条。每条引用都必须：

- 属于配置的 cluster；
- 从配置 bucket/prefix 的明确 S3 URI 成功读取；
- 通过 strict JSON、RFC 8785、key ID、HMAC 和 canonical URI 验证；
- 与引用中的 history SHA-256、backup ID 和完成时间完全一致。

随后复用 Phase 1O cadence verifier，检查窗口起点到首次成功、相邻成功以及最后成功到窗口终点，默认最大 gap 30 小时。健康结果 schema 为 `candlescope.query-backup-cadence-monitor.v1`，明确输出：

```json
{
  "source": "host-private-success-inventory",
  "host_inventory_completeness_proven": false,
  "global_history_completeness_proven": false
}
```

72 小时窗口需要先积累真实成功引用；新部署不能伪造引用绕过 bootstrap。目录被删除、旧条目损坏、跨主机运行或调度迁移都可能使 monitor 告警。它证明的是指定主机观察到的成功 cadence，不是跨主机完整索引、S3 全局 latest 或 systemd 精确槽位执行证明。

## 3. HMAC HTTPS 告警

cadence、inventory、配置或 S3 验证失败时，monitor 构造 `candlescope.query-backup-cadence-alert.v1` 最小化 JSON，只包含稳定 code、cluster、观察时间、人工处置标记和明确的非完整性声明；异常诊断、URI、DSN、credential 和 secret 不进入 payload。

alert body 使用 RFC 8785 canonical JSON，并由独立 alert HMAC key 产生 `X-CandleScope-Signature`。客户端：

- 生产只接受不带 credential/query/fragment 的 HTTPS URL；
- 不读取环境 proxy，不跟随 redirect；
- 默认单次 10 秒 timeout、最多读取 4 KiB response；
- 只接受 2xx，不回显 response body；
- 不 retry、不 fallback、不去重、不做 cooldown 或 escalation。

成功投递返回 `candlescope.query-backup-cadence-alert-delivery.v1`，但 monitor 仍以非零退出，确保 systemd 状态不会把不健康 cadence 改写为 healthy。投递本身失败则输出净化后的 `BACKUP_CADENCE_ALERT_DELIVERY_FAILED`。持续故障会在后续 hourly timer 再次尝试，因此生产接收端必须具备自己的幂等/抑制策略。

单元门禁使用仅限显式测试开关的 loopback HTTP server，真实接收并复验 body/HMAC，同时证明 response 上限与禁止 redirect；该开关不出现在部署环境模板。这只证明传输实现，不代表 PagerDuty、Slack 或值班平台已经配置或真实送达。

## 4. systemd 与凭据边界

新增：

- `candlescope-query-backup-monitor.service`：2 分钟上限、strict filesystem/kernel/namespace hardening，允许 AF_UNIX/INET 以读取 S3 和投递 webhook；
- `candlescope-query-backup-monitor.timer`：hourly、`Persistent=yes`、最多 5 分钟随机延迟；
- `query-backup-monitor.env.example`：monitor、只读 history 和 alert HMAC/webhook 配置。

backup service 增加共享的 private `StateDirectory=candlescope-query-backup-history`。monitor 环境应使用独立的只读 history S3 identity，只允许固定前缀 GetObject；不应授予 list、put、overwrite 或 delete。alert HMAC key 与 history/manifest/anchor key 分离。

两个 timer 都只是仓库模板，没有复制到 `/etc/systemd/system`、daemon-reload 或 enable。生产启用前必须在真实身份、TLS、DNS、egress、防火墙、secret manager 和接收端上完成故障注入及实际值班送达。

## 5. 验证门禁

单元门禁覆盖：

- 私有目录/文件、canonical schema、原子条件创建、fsync、逐字节 replay 与漂移拒绝；
- 空窗口、gap、hash/cluster/数量/目录异常及远端 HMAC 回源；
- Phase 1M inventory fail-closed、secret 子进程隔离和 staging 清理；
- 真实 loopback HTTP body/HMAC、2xx、response bound、redirect 禁止及净化失败输出；
- systemd hardening、timer cadence 和生产模板不暴露测试开关。

真实 PostgreSQL 18 + MinIO 门禁从新卷完成 fenced physical backup、连续 WAL、anchor、真实签名 history、主机引用、独立 monitor 回源 cadence、Phase 1N selection、全量 artifacts/WAL/anchor 复验和 target-time PITR。Phase 1M 窄门禁使用真实非 superuser replication role，并实际落一个 mode `0600` 成功引用。

## 6. 明确未完成

- systemd 模板未安装/启用，没有 24/72 小时生产运行证据或 missed-run SLO；
- 没有接入或验证真实值班渠道；本地 HTTP 接收门禁不等于生产告警送达；
- 没有跨主机引用汇聚、leader election、共享签名 checkpoint、S3 list 或全局完整性证明；
- 本地引用没有独立 HMAC，拥有服务账号或磁盘写权限者可删除/篡改并触发告警；
- 没有 alert retry、dedupe、cooldown、escalation 或 delivery receipt 的外部持久化；
- 没有 orphan history 自动发现/对账、inventory repair、retention/pruning/delete；
- 没有自动 fallback、恢复审批、双人控制、自动 restore 或定期恢复演练；
- 没有生产 RPO/RTO、异地复制、PostgreSQL HA 或自动 failover；
- 主 FastAPI `server` Profile 仍 fail closed，完整 CandleScope server 仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1p-backup-monitor-verification.json`。
