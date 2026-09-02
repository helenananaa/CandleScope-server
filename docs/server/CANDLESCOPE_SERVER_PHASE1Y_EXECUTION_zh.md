# CandleScope Server Phase 1Y：查询进程的工作区范围身份

状态：WORKSPACE_SCOPED_QUERY_IDENTITY_COMPLETE_NOT_TENANCY

后续状态：Phase 1Z 已把 FastAPI SQLite 控制/行情启动路径写成可复验清单，并在误开 `runtime_supported` 时仍拒绝 server 启动。1Y 的组织+工作区身份合同不变；当前 FastAPI 解锁边界以 `CANDLESCOPE_SERVER_PHASE1Z_EXECUTION_zh.md` 为准。

Phase 1Y 把 Phase 1X 的组织范围推进到 **同一内部 credential 上的组织 + 工作区成对绑定**。只绑 `organization_id` 会变成组织内通配，因此禁止。客户端不能自报 principal、organization 或 workspace；请求里的两者必须同时等于 token 绑定值。

```text
Authorization: Bearer <internal token>
  -> constant-time compare
  -> QueryCallerIdentity { principal, organization_id, workspace_id }
  -> request.organization_id and request.workspace_id must match
  -> audit records both ids, never the token
```

这不是用户/团队 RBAC、OIDC、工作区目录或网关租户模型。主 FastAPI `server` Profile 仍 fail closed。控制面 token 仍不带组织/工作区范围。

## 1. 身份合同

`candlescope.query-caller-identity.v2`：

- `principal`、`organization_id`、`workspace_id` 均为有界小写标识；
- 组织与工作区拒绝同一组保留名：`*`, `all`, `any`, `public`, `global`, `shared`, `wildcard`, `default`；
- token 仍至少 32 字符，settings repr 与审计不含 token。

`BearerTokenAuthenticator` 与 `QueryServiceSettings` 要求 `organization_id` 与 `workspace_id` 同时出现或同时缺省。未绑定时保持 Phase 1G 行为；若未绑定却在请求中出现 `workspace_id`，返回 422 `WORKSPACE_SCOPE_NOT_BOUND`，防止客户端自报工作区。组织侧仍返回 `ORGANIZATION_SCOPE_NOT_BOUND`。

绑定后：

| 请求 | 结果 |
| --- | --- |
| 缺少 workspace_id | 422 `WORKSPACE_SCOPE_REQUIRED` |
| 非法/保留名 | 422 `WORKSPACE_SCOPE_INVALID` |
| 与 token 不一致 | 403 `WORKSPACE_SCOPE_DENIED` |
| 组织与工作区均一致 | 进入既有 snapshot query |

`GET /metrics` 仍只验证 bearer，不要求组织或工作区。`/health/*` 仍无认证。

生产查询进程通过 `CANDLESCOPE_SERVER_QUERY_AUTH_ORGANIZATION_ID` 与 `CANDLESCOPE_SERVER_QUERY_AUTH_WORKSPACE_ID` 成对绑定；两者都未设置则保持 1G 无范围兼容。1Y 不把它们变成 from_env 必填，以免假装租户已交付。

## 2. 交付物

| 交付物 | 路径 |
| --- | --- |
| 身份合同 | `backend/app/server_runtime/query_identity.py` |
| bearer 绑定 | `backend/app/server_runtime/query_security.py` |
| 查询请求/审计 | `backend/app/server_runtime/query_api.py` |
| 设置 | `backend/app/server_runtime/query_settings.py` |
| 查询入口 | `backend/scripts/server_snapshot_query.py` |
| 单元门禁 | `backend/tests/test_server_phase1y_query_workspace.py` |

## 3. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1x_query_identity.py \
  backend/tests/test_server_phase1y_query_workspace.py \
  backend/tests/test_server_phase1g_query_hardening.py
```

## 4. 明确未完成

- 没有多 token、轮换、撤销、用户/团队 RBAC；
- 没有 OIDC/mTLS，也没有把组织或工作区目录放进 PostgreSQL；
- 没有打开 FastAPI `server` Profile 或接入网关。

机器可读结果位于 `docs/server/evidence/phase1y-query-workspace-verification.json`。
