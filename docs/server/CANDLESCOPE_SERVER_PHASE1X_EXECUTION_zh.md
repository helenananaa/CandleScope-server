# CandleScope Server Phase 1X：查询进程的组织范围身份

状态：ORGANIZATION_SCOPED_QUERY_IDENTITY_COMPLETE_NOT_TENANCY

后续状态：Phase 1Y 已把同一内部 bearer 同时绑到 `workspace_id`；组织与工作区必须成对出现。当前范围合同以 `CANDLESCOPE_SERVER_PHASE1Y_EXECUTION_zh.md` 为准。1X 关于“不是完整租户/OIDC”的判断仍然成立。

Phase 1X 把 Phase 1G 的内部 bearer 从“单一 principal”推进到 **服务端绑定的组织范围**。客户端不能自报 principal 或 organization；请求里的 `organization_id` 必须等于 credential 绑定的组织。

```text
Authorization: Bearer <internal token>
  -> constant-time compare
  -> QueryCallerIdentity { principal, organization_id }
  -> request.organization_id must match
  -> audit records organization_id, never the token
```

这不是完整租户模型、OIDC、用户角色或工作区授权。主 FastAPI `server` Profile 仍 fail closed。控制面 token 仍不带组织范围。

## 1. 身份合同

`candlescope.query-caller-identity.v1`：

- `principal` 与 `organization_id` 均为有界小写标识；
- organization 拒绝 `*`, `all`, `any`, `public`, `global`, `shared`, `wildcard`, `default`；
- token 仍至少 32 字符，settings repr 与审计不含 token。

`BearerTokenAuthenticator` 可带 `organization_id`。未绑定时保持 Phase 1G 行为；若未绑定却在请求中出现 `organization_id`，返回 422 `ORGANIZATION_SCOPE_NOT_BOUND`，防止客户端自报组织。

绑定后：

| 请求 | 结果 |
| --- | --- |
| 缺少 organization_id | 422 `ORGANIZATION_SCOPE_REQUIRED` |
| 非法/保留名 | 422 `ORGANIZATION_SCOPE_INVALID` |
| 与 token 不一致 | 403 `ORGANIZATION_SCOPE_DENIED` |
| 完全一致 | 进入既有 snapshot query |

`GET /metrics` 仍只验证 bearer，不要求 organization。`/health/*` 仍无认证。

生产查询进程通过 `CANDLESCOPE_SERVER_QUERY_AUTH_ORGANIZATION_ID` 绑定；未设置则保持 1G 无组织范围兼容，供既有门禁使用。1X 不把该变量变成 from_env 必填，以免假装租户已交付。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 身份合同 | `backend/app/server_runtime/query_identity.py` |
| bearer 绑定 | `backend/app/server_runtime/query_security.py` |
| 查询请求/审计 | `backend/app/server_runtime/query_api.py` |
| 设置 | `backend/app/server_runtime/query_settings.py` |
| 查询入口 | `backend/scripts/server_snapshot_query.py` |
| 单元门禁 | `backend/tests/test_server_phase1x_query_identity.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1x_query_identity.py \
  backend/tests/test_server_phase1g_query_hardening.py
```

## 4. 明确未完成

- 没有多 token、轮换、撤销、用户/团队 RBAC（工作区绑定见 Phase 1Y）；
- 没有 OIDC/mTLS，也没有把组织目录放进 PostgreSQL；
- 没有打开 FastAPI `server` Profile 或接入网关。

机器可读结果位于 `docs/server/evidence/phase1x-query-identity-verification.json`。
