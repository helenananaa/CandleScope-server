# CandleScope Server Phase 1AG：Server HTTP replay API、认证与 WebSocket

状态：SERVER_REPLAY_API_COMPLETE_PROFILE_STILL_LOCKED

Phase 1AG 抽出 `ReplayApplication` 端口，并实现独立的 Server replay facade：认证后的 HTTP/WS 走调度器、session store 与 durable outbox，不持有 Actor。未迁移的 replay.v2 路径在 capability inventory 中明确为 `CAPABILITY_UNAVAILABLE`，不得回退 personal runtime。主 `CANDLESCOPE_PROFILE=server` 仍保持锁定。

## 1. 交付物

| 交付物 | 路径 |
| --- | --- |
| Replay application port | `backend/app/replay/application.py` |
| Server facade / 组合 | `replay_api_service.py`、`replay_api_composition.py` |
| 身份与权限 | `access_identity.py`、`replay_authorization.py` |
| durable WS outbox | `replay_event_stream.py` |
| 门禁 | `test_server_phase1ag_replay_api.py`、`integration/test_server_phase1ag_replay_api.py` |

## 2. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ag_replay_api.py \
  backend/tests/test_replay_api.py \
  backend/tests/test_replay_stream.py \
  backend/tests/test_server_phase1x_query_identity.py \
  backend/tests/test_server_phase1y_query_workspace.py

docker compose -p candlescope-phase1ag \
  -f deploy/server/compose.phase1ag.yml up -d --wait
CANDLESCOPE_PHASE1AG_INTEGRATION=1 \
  CANDLESCOPE_PHASE1AG_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1ag_replay_api.py
cd frontend && npm run test:replay && cd ..
docker compose -p candlescope-phase1ag \
  -f deploy/server/compose.phase1ag.yml down -v
```

## 3. 明确未完成

- 主 FastAPI `CANDLESCOPE_PROFILE=server` 仍锁定；
- 没有公网 24 小时连续性证据，不得声称生产就绪。

机器可读结果位于 `docs/server/evidence/phase1ag-replay-api-verification.json`。
