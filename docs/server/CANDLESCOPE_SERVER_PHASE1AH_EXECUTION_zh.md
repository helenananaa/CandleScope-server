# CandleScope Server Phase 1AH：Server Profile 组合与启动门禁

状态：SERVER_PROFILE_RUNTIME_COMPLETE_NOT_PRODUCTION_READY

Phase 1AH 把 personal/server FastAPI lifespan 拆开，并把 `CANDLESCOPE_PROFILE=server` 的 `runtime_supported` 打开。Server 启动必须先加载 settings、通过数据平面组合检查，再把 `refuse_server_sqlite_boot` 当作负面清单：证明组合不是 SQLite / 本地文件 / 进程内总线。公网 24 小时连续性仍不存在，因此不得声称生产就绪。

```text
load_deployment_settings
  -> require_runtime_support
  -> personal: refuse noop + personal SQLite lifespan
  -> server: load composition (no db) + refuse sqlite unless composition proves otherwise
  -> start_server_runtime: scheduler, identity, replay API, readiness
```

## 1. 交付物

| 交付物 | 路径 |
| --- | --- |
| personal lifespan | `backend/app/deployment/personal_runtime.py` |
| server lifespan | `backend/app/deployment/server_runtime.py` |
| Server 应用组合 | `backend/app/server_runtime/application.py` |
| Worker 池循环 | `backend/app/server_runtime/replay_worker_pool.py` |
| Compose | `deploy/server/compose.phase1ah.yml` |
| 门禁 | `test_server_phase1ah_profile.py`、`integration/test_server_phase1ah_profile.py` |

## 2. 本地重跑

```bash
PYTHONPATH=backend backend/.venv/bin/python -m pytest -q \
  backend/tests/test_server_phase1ah_profile.py \
  backend/tests/test_server_phase1w_composition.py \
  backend/tests/test_server_phase1z_fastapi_sqlite_boot.py \
  backend/tests/test_server_phase1ag_replay_api.py

docker compose -p candlescope-phase1ah \
  -f deploy/server/compose.phase1ah.yml up -d --wait
CANDLESCOPE_PHASE1AH_INTEGRATION=1 \
  CANDLESCOPE_PHASE1AH_ALLOW_TEST_RESET=1 \
  PYTHONPATH=backend \
  backend/.venv/bin/python -m pytest -q \
  backend/tests/integration/test_server_phase1ah_profile.py
docker compose -p candlescope-phase1ah \
  -f deploy/server/compose.phase1ah.yml down -v
```

## 3. 明确未完成

- 没有公网 Binance 24 小时连续性证据；
- `production_ready` 必须保持 false；
- Phase 1AI 的双重开关 soak 尚未运行。

机器可读结果位于 `docs/server/evidence/phase1ah-server-profile-verification.json`。
