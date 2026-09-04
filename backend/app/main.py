"""
CandleScope backend entrypoint.

Startup sequence:
  1. Initialize SQLite storage (klines_repo).
  2. Restore the local symbol catalog snapshot.
  3. Start the opt-in script-runtime plugin host from resolved activation state.
  4. Start the DataEngine runtime and attach its public handles to
     ``app.state`` for API/WS endpoints.
  5. Refresh exchange metadata asynchronously on a best-effort basis.
  6. Bridge the IndicatorEngine to DataManager events.
  7. On shutdown, stop IndicatorEngine, plugin sidecars, and DataEngine.

When DataManager fails to initialize, the application can still expose
health endpoints, but data APIs report explicit service-unavailable errors.
"""

import logging

# ── Monkey-patch: websockets recv_messages bug ──────────────────
# websockets ≥15 initializes ``recv_messages`` in ``connection_made``,
# but if the TCP connection is reset (e.g. GFW) *before* that callback
# fires, ``connection_lost`` crashes with:
#   AttributeError: 'ClientConnection' object has no attribute 'recv_messages'
# This patch makes ``connection_lost`` safe when the connection was
# never fully established.
try:
    from websockets.asyncio.connection import Connection as _WsConnection

    _orig_connection_lost = _WsConnection.connection_lost

    def _safe_connection_lost(self, exc):
        if not hasattr(self, "recv_messages"):
            # Connection was reset before handshake; nothing to clean up.
            return
        _orig_connection_lost(self, exc)

    _WsConnection.connection_lost = _safe_connection_lost
except Exception:
    pass
# ── End monkey-patch ────────────────────────────────────────────


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware

from app.api.v1.alerts import router as alerts_router
from app.api.v1.indicators import router as indicators_router  # indicator engine v2
from app.api.v1.exchanges import router as exchanges_router
from app.api.v1.full_order_book import router as full_order_book_router
from app.api.v1.klines import router as klines_router
from app.api.v1.liquidations import router as liquidations_router
from app.api.v1.market import router as market_router
from app.api.v1.order_book import router as order_book_router
from app.api.v1.replay import router as replay_router
from app.api.v1.trade_flow import router as trade_flow_router
from app.api.v1.settings import router as settings_router
from app.api.v1.stream import router as stream_router
from app.api.v1.subscriptions import router as subscriptions_router
from app.api.v1.subscriptions import price_ws_router
from app.api.v1.symbols import router as symbols_router
from app.core.config import CORS_ORIGINS
from app.core.executors import executors_snapshot
from app.core.runtime_metrics import ws_runtime_metrics
from app.data_engine.data_manager.capacity import build_capacity_snapshot
from app.deployment import (
    DeploymentProfile,
    load_deployment_settings,
    refuse_server_sqlite_boot,
)
from app.deployment.personal_runtime import (
    start_personal_runtime,
    stop_personal_runtime,
)
from app.deployment.server_runtime import start_server_runtime, stop_server_runtime
from app.plugin_core_v2 import create_core_plugin_router

logger = logging.getLogger("candlescope")

APP_NAME = "CandleScope"
APP_VERSION = "0.3.0"
PLUGIN_PLATFORM_V2_HOST_VERSION = "0.4.0"

app = FastAPI(
    title="CandleScope API",
    description="Backend API for CandleScope",
    version=APP_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(
    GZipMiddleware,
    minimum_size=1024,
    compresslevel=5,
)

app.include_router(klines_router, prefix="/api/v1")
app.include_router(market_router, prefix="/api/v1")
app.include_router(trade_flow_router, prefix="/api/v1")
app.include_router(liquidations_router, prefix="/api/v1")
app.include_router(order_book_router, prefix="/api/v1")
app.include_router(full_order_book_router, prefix="/api/v1")
app.include_router(stream_router, prefix="/api/v1")
app.include_router(indicators_router, prefix="/api/v1")
app.include_router(alerts_router, prefix="/api/v1")
app.include_router(settings_router, prefix="/api/v1")
app.include_router(exchanges_router, prefix="/api/v1")
app.include_router(symbols_router, prefix="/api/v1")
app.include_router(subscriptions_router, prefix="/api/v1")
app.include_router(price_ws_router, prefix="/api/v1")
app.include_router(replay_router, prefix="/api/v1")
app.include_router(create_core_plugin_router())


# ═══════════════════════════════════════════════════════════════
#  DataManager bootstrap
# ═══════════════════════════════════════════════════════════════


async def _init_data_manager() -> None:
    """Create and start the DataEngine runtime."""
    from app.data_engine.runtime import (
        FullOrderBookConfigurationError,
        LiquidationConfigurationError,
        OrderBookConfigurationError,
        TradeFlowConfigurationError,
        start_data_engine,
    )

    try:
        from app.alerts.facade import AlertFacade
        from app.alerts.runtime import AlertRuntimeEngine
        from app.indicator.data_manager_bridge import bridge_indicator_engine
        from app.indicator.range_result_service import IndicatorRangeResultService
        from app.indicator.series_revision import SeriesRevisionRegistry

        runtime = await start_data_engine()
        runtime.attach_to_app_state(app.state)

        try:
            revision_registry = SeriesRevisionRegistry()
            indicator_range_service = IndicatorRangeResultService.from_config(
                revision_registry=revision_registry,
            )
            # One authoritative revision registry is shared by WS events,
            # range cache entries and HTTP response metadata.
            app.state.indicator_series_revisions = revision_registry
            app.state.indicator_range_service = indicator_range_service
            indicator_engine = bridge_indicator_engine(
                runtime.data_manager,
                backfill_coordinator=runtime.backfill_coordinator,
                result_service=indicator_range_service,
            )
            app.state.indicator_engine = indicator_engine
            print("[startup] IndicatorEngine bridged to DataManager [ok]")
        except Exception as exc:
            logger.warning("IndicatorEngine bridge failed: %s", exc)
            print(f"[startup] IndicatorEngine bridge failed: {exc}")

        try:
            alert_facade = AlertFacade()
            alert_runtime = AlertRuntimeEngine(
                facade=alert_facade, data_manager=runtime.data_manager
            )
            app.state.alert_facade = alert_facade
            app.state.alert_runtime = alert_runtime
            await alert_runtime.start()
            print("[startup] AlertRuntime bridged to DataManager [ok]")
        except Exception as exc:
            logger.warning("AlertRuntime bridge failed: %s", exc, exc_info=True)
            print(f"[startup] AlertRuntime bridge failed: {exc}")

    except (
        TradeFlowConfigurationError,
        LiquidationConfigurationError,
        OrderBookConfigurationError,
        FullOrderBookConfigurationError,
    ) as exc:
        logger.critical(
            "Advanced market-data configuration prevents safe startup: %s",
            exc,
            exc_info=True,
        )
        raise
    except Exception as exc:
        logger.error("DataManager initialization failed: %s", exc, exc_info=True)
        print(f"[startup] DataManager init failed: {exc}")
        app.state.data_manager = None


async def _init_replay_runtime() -> None:
    """Start replay as an application sibling, independent of live DataEngine."""

    from app.replay.runtime import start_replay_runtime

    runtime = await start_replay_runtime()
    app.state.replay_runtime = runtime
    app.state.replay_service = runtime.service


def _schedule_symbol_catalog_refresh():
    from app.deployment.personal_runtime import schedule_symbol_catalog_refresh

    return schedule_symbol_catalog_refresh(app)


# ═══════════════════════════════════════════════════════════════
#  Application Lifecycle
# ═══════════════════════════════════════════════════════════════


@app.on_event("startup")
async def startup_event() -> None:
    """Load profile settings before any database or socket, then dispatch."""
    deployment_settings = load_deployment_settings()
    deployment_settings.require_runtime_support()
    app.state.deployment_profile = deployment_settings.profile.value
    app.state.deployment_contract_version = deployment_settings.contract_version
    if deployment_settings.profile is DeploymentProfile.PERSONAL:
        refuse_server_sqlite_boot(deployment_settings)
        await start_personal_runtime(
            app,
            init_replay_runtime=_init_replay_runtime,
            init_data_manager=_init_data_manager,
            app_name=APP_NAME,
            app_version=APP_VERSION,
            plugin_platform_v2_host_version=PLUGIN_PLATFORM_V2_HOST_VERSION,
            schedule_catalog=_schedule_symbol_catalog_refresh,
        )
        return
    composition = None
    try:
        from app.server_runtime.composition import (
            ServerCompositionError,
            load_server_data_plane_composition,
        )

        composition = load_server_data_plane_composition()
    except ServerCompositionError:
        composition = None
    refuse_server_sqlite_boot(deployment_settings, composition=composition)
    await start_server_runtime(app, deployment_settings, composition)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Dispatch shutdown to the Profile that started this process."""
    profile = getattr(app.state, "deployment_profile", DeploymentProfile.PERSONAL.value)
    if profile == DeploymentProfile.SERVER.value:
        await stop_server_runtime(app)
        return
    await stop_personal_runtime(app)


# ═══════════════════════════════════════════════════════════════
#  System Endpoints
# ═══════════════════════════════════════════════════════════════


@app.get("/", tags=["system"])
async def root() -> dict:
    dm = getattr(app.state, "data_manager", None)
    return {
        "name": "CandleScope API",
        "version": APP_VERSION,
        "status": "running",
        "data_manager": "active" if dm is not None else "not_initialized",
    }


@app.get("/health", tags=["system"])
async def health_check() -> dict:
    dm = getattr(app.state, "data_manager", None)
    result: dict = {"status": "ok"}
    plugin_runtime_host = getattr(app.state, "plugin_runtime_host", None)
    if plugin_runtime_host is not None:
        result["plugin_runtimes"] = plugin_runtime_host.health_summary()
    plugin_platform_v2 = getattr(app.state, "plugin_platform_v2", None)
    if plugin_platform_v2 is not None:
        result["plugin_platform_v2"] = plugin_platform_v2.health_summary()
    first_party_bootstrap = getattr(
        app.state,
        "first_party_plugin_bootstrap",
        None,
    )
    if isinstance(first_party_bootstrap, dict):
        result["first_party_plugin_bootstrap"] = {
            key: first_party_bootstrap[key]
            for key in ("status", "runtimeId", "version", "changed", "downloaded")
            if key in first_party_bootstrap
        }
    indicator_runtime_service = getattr(
        app.state,
        "indicator_runtime_service",
        None,
    )
    if indicator_runtime_service is not None:
        routing = indicator_runtime_service.snapshot()
        result["indicator_runtime_routing"] = {
            "started": routing["started"],
            "routes": routing["routes"],
            "counts": routing["counts"],
        }
    if dm is not None:
        try:
            result["data_manager"] = dm.health_snapshot()
            lag_monitor = getattr(app.state, "event_loop_lag_monitor", None)
            if lag_monitor is not None:
                result["event_loop_lag"] = lag_monitor.snapshot()
        except Exception:
            result["data_manager"] = {"status": "error"}
    else:
        result["data_manager"] = {"status": "not_initialized"}
    return result


@app.get("/debug/snapshot", tags=["system"])
async def debug_snapshot() -> dict:
    """Full diagnostic snapshot of the DataManager (dev/debug only)."""
    dm = getattr(app.state, "data_manager", None)
    try:
        snapshot = (
            {"error": "DataManager not initialized"} if dm is None else dm.snapshot()
        )
        snapshot["executors"] = executors_snapshot()
        lag_monitor = getattr(app.state, "event_loop_lag_monitor", None)
        snapshot["runtime"] = {
            "event_loop_lag": lag_monitor.snapshot()
            if lag_monitor is not None
            else None,
            "websocket": ws_runtime_metrics.snapshot(),
        }
        replay_runtime = getattr(app.state, "replay_runtime", None)
        replay_service = getattr(app.state, "replay_service", None)
        if replay_runtime is not None:
            snapshot["replay"] = replay_runtime.diagnostics(redact_paths=True)
        elif replay_service is not None:
            snapshot["replay"] = replay_service.diagnostics(redact_paths=True)
        else:
            snapshot["replay"] = {
                "enabled": False,
                "available": False,
                "reason": "REPLAY_DISABLED",
                "sessions": {},
            }
        return snapshot
    except Exception as exc:
        return {"error": str(exc)}


@app.get("/debug/capacity", tags=["system"])
async def capacity_snapshot(
    include_database_hash: bool = False,
    detail_offset: int = 0,
    detail_limit: int = 20,
    event_loop_after_sequence: int | None = None,
) -> dict:
    """Return a read-only, multi-chart-oriented capacity snapshot."""

    return await build_capacity_snapshot(
        app.state,
        include_database_hash=include_database_hash,
        detail_offset=detail_offset,
        detail_limit=detail_limit,
        event_loop_after_sequence=event_loop_after_sequence,
    )
