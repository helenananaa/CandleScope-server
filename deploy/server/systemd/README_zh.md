# Phase 1M–1O systemd 物理备份模板

这些文件是审阅过的部署模板，不会自动安装，也不表示任何主机已经启用备份。只有在 Phase 1L 完整恢复演练和下列检查全部通过后，才应启用 timer。

## 部署前检查

1. 创建无登录 shell 的专用 Unix 用户和组 `candlescope-backup`；代码与虚拟环境放在 `/opt/candlescope`，由 root 管理且该用户不可修改。
2. 安装与服务端主版本匹配的 PostgreSQL client，配置环境文件中的真实、非 symlink `pg_basebackup` 路径。示例固定为 PostgreSQL 18 的版本化路径。
3. 在 PostgreSQL 创建仅 `LOGIN REPLICATION` 的专用角色，明确保持 `NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT`。用窄来源网络、TLS 和 SCRAM 的 `pg_hba.conf` 规则允许 replication connection；不要复用 runtime 或 auditor 角色。
4. 将 `query-backup.env.example` 复制为 `/etc/candlescope/query-backup.env`，替换所有 placeholder，设置 `root:candlescope-backup`、mode `0640`。生产上应由 secret manager 渲染，不应提交真实 secret。`QUERY_BACKUP_HISTORY` 使用独立 HMAC key；其对象存储身份只需对固定历史前缀执行条件创建和读取，不应获得 list、overwrite 或 delete 权限。
5. 创建 `/etc/candlescope/query-backup.pgpass`，owner `candlescope-backup`、mode `0600`。不要设置 `PGPASSWORD`、`PGSERVICE` 或 `PGSERVICEFILE`。
6. 确认 systemd 创建的 `/var/lib/candlescope-query-backup` 和 `/run/candlescope-query-backup` 均归服务账号所有且 mode `0700`；job 会 fail closed 检查。
7. 把三个 unit 复制到 `/etc/systemd/system/` 后运行 `systemd-analyze verify` 与 `systemctl daemon-reload`。先手动启动 service，不要先 enable timer。
8. 从 journal 保存并严格解析唯一 `candlescope.query-backup-job-result.v2`；确认其中的 `success_history_uri` 能通过 HMAC、canonical URI 和内容 hash 复验，再独立运行 backup verifier，并完成一次从空卷恢复到结果中目标的演练。历史发布失败时 job 必须整体失败。
9. 配置外部日志/监控代理匹配 `candlescope.query-backup-failure-signal.v1`，做一次故障注入并确认告警实际送达值班渠道。failure unit 本身只写 journal。
10. 最后才 `systemctl enable --now candlescope-query-backup.timer`，并持续监控 timer 的 last/next trigger、service exit、签名成功历史的新鲜度、WAL archive 健康、磁盘空间和恢复演练年龄。外部监控器应收集明确历史 URI，并以 `server_query_backup_history.py verify-cadence` 验证指定窗口；该命令不会列举对象，也不会证明输入历史集合完整。

## PostgreSQL 权限形状

角色合同示例（密码通过受控交互或 secret 管理设置，不写进 SQL 历史）：

```sql
CREATE ROLE candlescope_backup
  LOGIN REPLICATION
  NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
```

HBA 必须按实际 TLS 和网络拓扑收紧。下面只展示规则形状，不是可直接复制的生产配置：

```text
hostssl replication candlescope_backup <backup-host-cidr> scram-sha-256
```

## 运行语义

- timer 为本地时区每天 02:00，并加入 0–15 分钟随机延迟；`Persistent=yes` 会在关机错过后补触发。
- oneshot 最长 25 分钟，job 内 basebackup/window 默认上限分别为 15/6 分钟。
- 同机重叠运行返回临时失败；跨主机一致窗口由 PostgreSQL exclusive fence 保护。
- job 只有在 v1 核心 receipt 已 HMAC 签名并条件写入不可变历史对象后才输出 v2 成功结果；历史 URI/hash 会随结果写入 journal。历史 HMAC/S3 secret 不会传给 backup-window 子进程。
- 所有成功与失败 JSON 写入 journal。日志保留策略和外部告警不包含在这些模板内。
- timer 不包含自动 retry；是否重试必须结合数据库容量、WAL/对象存储健康和恢复验证另行制定。
- cadence verifier 只证明显式窗口首尾及相邻成功之间的最大间隔；它不证明某个 systemd 计划槽位确实执行，也不证明全局没有遗漏历史。
