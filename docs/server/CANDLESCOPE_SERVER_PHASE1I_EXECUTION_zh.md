# CandleScope Server Phase 1I：查询控制面运维隔离与外部审计锚

状态：QUERY_CONTROL_OPERATIONS_COMPLETE_NOT_DEPLOYABLE

后续状态：Phase 1J 已补上物理 base backup、WAL archive 与 target-time PITR 的受控恢复门禁；本文件第 5 节关于这些能力“尚未实现”的描述仅代表 Phase 1I 当时边界，当前边界以 `CANDLESCOPE_SERVER_PHASE1J_EXECUTION_zh.md` 为准。

Phase 1I 延续独立快照查询进程，仍不解除主 FastAPI `server` Profile。它收紧 Phase 1H 的数据库管理边界，并增加可由数据库之外保存的审计证明：

```text
database administrator
  -> external immutable migration 001_query_control.sql
  -> pre-existing runtime/auditor LOGIN roles

query runtime role
  -> SELECT/UPDATE audit head and quarantine
  -> INSERT audit event + SELECT only audit_sequence
  -> no DDL, no audit event body read, no delete/truncate

auditor role (read-only repeatable-read)
  -> verify migration + complete bounded hash chain + state
  -> HMAC-SHA256 signed canonical anchor
  -> S3-compatible conditional create, never overwrite

pg_dump -> recreate schema from migration -> pg_restore
  -> verify database against the previously published anchor URI
  -> restart runtime and prove identity sequence advances
```

## 1. 外部迁移

运行进程不再建表或补初始行。管理员必须先创建两个不同、非提权的 LOGIN role；密码由部署系统或 secret manager 注入，不写进仓库：

```sql
CREATE ROLE candlescope_query_app LOGIN PASSWORD '<runtime-secret>'
  INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE candlescope_query_audit_reader LOGIN PASSWORD '<auditor-secret>'
  INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
```

随后以数据库管理员 DSN 运行：

```bash
export CANDLESCOPE_SERVER_QUERY_MIGRATION_POSTGRES_DSN='postgresql://...'
export CANDLESCOPE_SERVER_QUERY_MIGRATION_RUNTIME_LOGIN_ROLE='candlescope_query_app'
export CANDLESCOPE_SERVER_QUERY_MIGRATION_AUDITOR_LOGIN_ROLE='candlescope_query_audit_reader'
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_control_migrate.py
```

迁移文件固定为 `deploy/server/postgres/migrations/001_query_control.sql`，当前 SHA-256 为：

```text
b2c29c8300a325248d415ffc3358879beb644a7eddd2f7182a79366bfbebfcb5
```

迁移器使用事务 advisory lock；首次运行记录 version/name/hash，重跑返回 `applied=false`。文件字节或数据库迁移记录与编译契约不一致时拒绝执行。登录角色必须预先存在、可登录且没有 superuser、createdb、createrole、replication 或 bypassrls 权限；group role 也必须保持 NOLOGIN 且非提权。成员授权固定为 `ADMIN FALSE / INHERIT TRUE / SET TRUE`。迁移器不会创建密码身份。

## 2. 运行与审计角色

迁移创建两个 NOLOGIN group role：

- `candlescope_query_runtime`：只能读取迁移版本和当前控制状态、更新 head/quarantine、使用 identity sequence，并仅对审计表的 `audit_sequence` 列拥有 SELECT；INSERT 也只开放 `event_id/schema_version/event_json/previous_hash/event_hash` 五列，不能显式写 identity sequence 或 `recorded_at`；
- `candlescope_query_auditor`：只能 SELECT 迁移记录、事件、head 和 quarantine，无 sequence usage 和任何写权限。

查询服务 DSN 必须使用 runtime LOGIN，不能使用数据库 owner。`PostgresQueryControlStore.start()` 会验证：

- 迁移 version/name/SHA-256 完全匹配；
- runtime group membership 存在且 auditor membership 不存在；
- 当前登录角色无 superuser、createdb、createrole、replication 或 bypassrls 标志；
- schema CREATE、审计 body SELECT、UPDATE/DELETE/TRUNCATE 等有效权限均不存在；
- 必需的 backend row 已由迁移器预置。

审计工具必须使用另一个 auditor LOGIN。它同样验证精确的只读权限，并在 read-only repeatable-read transaction snapshot 内读取 head 与事件，避免并发追加造成假阳性。角色被多授或少授权限都会 fail closed。

## 3. HMAC 外部锚点

锚点把以下字段规范化为 RFC 8785 JSON：

- record count；
- head audit sequence/hash/更新时间；
- query-control migration version/SHA-256；
- HMAC key ID 和算法版本。

大整数使用规范十进制字符串。签名为 `HMAC-SHA256(secret, RFC8785(unsigned_anchor))`。对象键包含 key ID、20 位 sequence 和 head hash；写入使用 `If-None-Match: *`，同 key 重放必须字节完全相同。

发布前，bucket 必须已存在。HMAC secret 使用严格 Base64，解码后至少 32 bytes，并应与 PostgreSQL 管理凭据分开保管：

```bash
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_POSTGRES_DSN='postgresql://auditor:...'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_S3_ENDPOINT_URL='https://...'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_S3_REGION='us-east-1'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_S3_BUCKET='candlescope-audit'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_S3_PREFIX='query-control'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_S3_ACCESS_KEY_ID='...'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_S3_SECRET_ACCESS_KEY='...'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_HMAC_KEY_ID='query-audit-2026-08'
export CANDLESCOPE_SERVER_QUERY_AUDIT_ANCHOR_HMAC_SECRET_BASE64='...'

PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_audit_anchor.py publish
```

输出只包含 anchor URI、content hash、head、migration 和 key ID，不输出 DSN、S3 secret 或 HMAC secret。恢复数据库后，使用发布时保存的明确 URI 校验：

```bash
PYTHONPATH=backend backend/.venv/bin/python \
  backend/scripts/server_query_audit_anchor.py verify \
  --anchor-uri 's3://candlescope-audit/query-control/anchors/v1/...json'
```

解析器拒绝重复 JSON key、非规范十进制、未知/缺失字段、非 RFC 8785 bytes、错误 key ID、HMAC 不匹配以及数据库 head/migration 漂移。

## 4. 备份恢复门禁

真实门禁固定 PostgreSQL 18.4 与同容器版本的 `pg_dump/pg_restore`：

1. 外部迁移并验证重复运行幂等；
2. runtime role 写入查询审计、锁存并 generation-fenced 解除 quarantine；
3. 证明 runtime 不能读取 `event_json` 或 CREATE TABLE，auditor 不能 UPDATE；
4. auditor 验证 4 条链记录与 generation 1 的 inactive 状态；
5. 使用独立 HMAC key 在 MinIO 条件发布锚点并验证幂等；
6. data-only custom dump 保存 migration、event、identity sequence、head 和 quarantine；
7. 删除控制面 schema，以同一 migration 重建并清空 seed data，再 restore；
8. auditor 重算的完整结果与恢复前对象逐字段相同，且通过旧 anchor URI；
9. runtime 重启后追加第 5 条事件，identity sequence 从 4 前进到 5。

重跑：

```bash
docker compose -f deploy/server/compose.phase1i.yml up -d --wait
CANDLESCOPE_PHASE1I_INTEGRATION=1 \
  CANDLESCOPE_PHASE1I_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1i_query_operations.py
docker compose -f deploy/server/compose.phase1i.yml down -v
```

`ALLOW_TEST_RESET=1` 是显式破坏性测试开关；该门禁只能用于 Phase 1I 本地 Compose 数据卷。

## 5. 明确未完成

- 不是生产备份计划：没有定时任务、WAL archive、PITR、异地复制、RPO/RTO 或恢复演练历史；
- HMAC 是共享密钥认证，不是公钥签名或不可抵赖证明；没有 KMS/HSM、双人控制或密钥轮换流程证据；
- S3 条件写不是 Object Lock/WORM；拥有 bucket 管理权者仍可删除对象，调用方也必须可靠保存预期 anchor URI；
- verifier 当前全链读取并受 `MAX_RECORDS` 限制；尚无 checkpoint、增量验证或长链 compaction；
- 尚未启用审计表分区和自动 retention。当前全局 sequence/hash 唯一约束与单链连续性不能在没有新契约和迁移证据时直接改成分区表；
- 没有审计查询 API、法定保留策略、删除批准或隐私生命周期；
- 没有 PostgreSQL HA、网络分区、主从切换、24 小时连续性或生产容量证据；
- API gateway、mTLS/OIDC、终端用户/组织/工作区授权、token 轮换和撤销仍未实现；
- 主 FastAPI `server` Profile 仍然 fail closed，完整服务器仍不可部署。

机器可读结果位于 `docs/server/evidence/phase1i-query-control-operations-verification.json`。
