"""Personal Profile FastAPI lifespan. SQLite and in-process ownership stay here."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable

from app.core.config import (
    EVENT_LOOP_LAG_INTERVAL_SECONDS,
    LIQUIDATION_DB_PATH,
    LIQUIDATION_ROLLUP_BACKEND,
    SYMBOL_CATALOG_FOREGROUND_DWELL_SECONDS,
    SYMBOL_CATALOG_FOREGROUND_RECHECK_SECONDS,
    TRADE_FLOW_DB_PATH,
    TRADE_FLOW_ROLLUP_BACKEND,
)
from app.core.runtime_metrics import EventLoopLagMonitor
from app.data_engine.storage import (
    init_klines_storage,
    init_liquidation_storage,
    init_market_metrics_storage,
    init_trade_flow_storage,
)

logger = logging.getLogger("candlescope")

InitHook = Callable[[], Awaitable[None]]


async def start_personal_runtime(
    app: object,
    *,
    init_replay_runtime: InitHook,
    init_data_manager: InitHook,
    app_name: str,
    app_version: str,
    plugin_platform_v2_host_version: str,
    schedule_catalog: Callable[[], object] | None = None,
) -> None:
    """Existing personal startup, including inventoried SQLite control/market paths."""

    lag_monitor = EventLoopLagMonitor(interval_seconds=EVENT_LOOP_LAG_INTERVAL_SECONDS)
    lag_monitor.start()
    app.state.event_loop_lag_monitor = lag_monitor

    init_klines_storage()
    init_market_metrics_storage()
    if TRADE_FLOW_ROLLUP_BACKEND == "sqlite":
        init_trade_flow_storage(TRADE_FLOW_DB_PATH)
    if LIQUIDATION_ROLLUP_BACKEND == "sqlite":
        init_liquidation_storage(LIQUIDATION_DB_PATH)

    from app.api.v1.symbols import initialize_exchange_metadata_cache

    restored_catalog = initialize_exchange_metadata_cache()
    if restored_catalog:
        logger.info("Restored last-known-good symbol catalog snapshot")

    from app.first_party_plugin_bootstrap import (
        ensure_first_party_plugins_from_environment,
    )
    from app.indicator.runtime_service import (
        build_indicator_runtime_service_from_environment,
    )
    from app.plugin_compat_v1 import V1ScriptRuntimeCompatibilityBridge
    from app.plugin_core_v2 import (
        build_core_plugin_platform_from_environment,
        build_management_guard_from_environment,
    )
    from app.plugin_core_v2.bootstrap import default_platform_root
    from app.plugin_runtime import build_runtime_host_from_environment

    plugin_runtime_host = None
    indicator_runtime_service = None
    plugin_platform_v2 = None
    try:
        first_party_bootstrap = await asyncio.to_thread(
            ensure_first_party_plugins_from_environment,
            host_name=app_name,
            host_version=app_version,
        )
        app.state.first_party_plugin_bootstrap = first_party_bootstrap.to_wire()
        plugin_runtime_host = build_runtime_host_from_environment(
            host_name=app_name,
            host_version=app_version,
        )
        await plugin_runtime_host.start()
        indicator_runtime_service = build_indicator_runtime_service_from_environment(
            host=plugin_runtime_host,
        )
        await indicator_runtime_service.start()
        plugin_platform_v2 = build_core_plugin_platform_from_environment(
            host_name=app_name,
            host_version=plugin_platform_v2_host_version,
        )
        v1_compatibility = V1ScriptRuntimeCompatibilityBridge(
            root=getattr(
                plugin_platform_v2,
                "root",
                default_platform_root(os.environ),
            ),
            indicator_source=indicator_runtime_service,
            runtime_host=plugin_runtime_host,
        )
        indicator_runtime_service.bind_catalog_projector(
            v1_compatibility.project_indicator_catalog
        )
        plugin_platform_v2.bind_v1_compatibility(v1_compatibility)
        plugin_platform_v2_guard = build_management_guard_from_environment(
            platform=plugin_platform_v2,
        )
    except BaseException:
        if plugin_platform_v2 is not None:
            await plugin_platform_v2.stop()
        if indicator_runtime_service is not None:
            await indicator_runtime_service.stop()
        if plugin_runtime_host is not None:
            await plugin_runtime_host.stop()
        await lag_monitor.stop()
        raise
    app.state.plugin_runtime_host = plugin_runtime_host
    app.state.indicator_runtime_service = indicator_runtime_service
    app.state.plugin_v1_compatibility = v1_compatibility
    app.state.plugin_platform_v2 = plugin_platform_v2
    app.state.plugin_platform_v2_management_guard = plugin_platform_v2_guard
    plugin_summary = plugin_runtime_host.health_summary()
    print(
        "[startup] Runtime plugin host "
        f"{plugin_summary['status']} "
        f"({plugin_summary['ready']}/{plugin_summary['enabled']} ready)"
    )
    print(
        "[startup] First-party plugin bootstrap "
        f"{first_party_bootstrap.status}"
        + (
            f" ({first_party_bootstrap.runtime_id} {first_party_bootstrap.version})"
            if first_party_bootstrap.runtime_id
            else ""
        )
    )

    try:
        await init_replay_runtime()
        await init_data_manager()
        data_manager = getattr(app.state, "data_manager", None)
        if data_manager is not None:
            from app.plugin_market_v2 import DataManagerConsumerPort

            plugin_platform_v2.bind_market_data(DataManagerConsumerPort(data_manager))
        from app.api.v1.symbols import (
            evict_exchange_metadata,
            refresh_exchange_metadata,
        )

        plugin_platform_v2.bind_symbol_refresher(
            refresh_exchange_metadata,
            evictor=evict_exchange_metadata,
        )
        await plugin_platform_v2.start()
    except BaseException:
        await plugin_platform_v2.stop()
        data_runtime = getattr(app.state, "data_engine_runtime", None)
        if data_runtime is not None:
            await data_runtime.shutdown()
        replay_runtime = getattr(app.state, "replay_runtime", None)
        if replay_runtime is not None:
            await replay_runtime.shutdown()
        await indicator_runtime_service.stop()
        await plugin_runtime_host.stop()
        await lag_monitor.stop()
        raise
    plugin_platform_v2.publish_event(
        "candlescope.app.ready/1",
        {"hostVersion": plugin_platform_v2_host_version},
    )

    from app.api.v1.symbols import configure_exchange_metadata_foreground_probe

    runtime = getattr(app.state, "data_engine_runtime", None)
    configure_exchange_metadata_foreground_probe(
        getattr(runtime, "backfill_coordinator", None)
    )
    if schedule_catalog is not None:
        schedule_catalog()
    else:
        schedule_symbol_catalog_refresh(app)


def schedule_symbol_catalog_refresh(app: object) -> asyncio.Task[None]:
    async def _refresh() -> None:
        try:
            from app.api.v1.symbols import refresh_exchange_metadata

            await _wait_for_catalog_foreground_quiet(app)
            counts = await refresh_exchange_metadata()
            print(f"[startup] Exchange info loaded [ok] {counts}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Exchange info load failed (non-critical): %s",
                exc,
                exc_info=True,
            )
            print(f"[startup] Exchange info load failed (non-critical): {exc}")

    task = asyncio.create_task(_refresh(), name="startup:symbol-catalog-refresh")
    app.state.symbol_catalog_refresh_task = task
    return task


async def _wait_for_catalog_foreground_quiet(app: object) -> None:
    dwell = SYMBOL_CATALOG_FOREGROUND_DWELL_SECONDS
    if dwell > 0:
        await asyncio.sleep(dwell)
    runtime = getattr(app.state, "data_engine_runtime", None)
    coordinator = getattr(runtime, "backfill_coordinator", None)
    has_foreground_work = getattr(coordinator, "has_foreground_work", None)
    foreground_idle_seconds = getattr(coordinator, "foreground_idle_seconds", None)
    if not callable(has_foreground_work):
        return
    while True:
        try:
            busy = bool(has_foreground_work())
            idle_for = (
                float(foreground_idle_seconds())
                if callable(foreground_idle_seconds)
                else float("inf")
            )
        except Exception:  # noqa: BLE001
            busy = True
            idle_for = 0.0
        if not busy and idle_for >= dwell:
            return
        await asyncio.sleep(SYMBOL_CATALOG_FOREGROUND_RECHECK_SECONDS)


async def stop_personal_runtime(app: object) -> None:
    """Existing personal shutdown. Does not run for the server Profile."""

    symbol_catalog_task = getattr(app.state, "symbol_catalog_refresh_task", None)
    if symbol_catalog_task is not None and not symbol_catalog_task.done():
        symbol_catalog_task.cancel()
        try:
            await symbol_catalog_task
        except asyncio.CancelledError:
            pass
    try:
        from app.api.v1.symbols import (
            cancel_exchange_metadata_refreshes,
            configure_exchange_metadata_foreground_probe,
        )

        await cancel_exchange_metadata_refreshes()
        configure_exchange_metadata_foreground_probe(None)
    except Exception as exc:
        logger.warning("Symbol catalog shutdown failed: %s", exc, exc_info=True)

    lag_monitor = getattr(app.state, "event_loop_lag_monitor", None)
    if lag_monitor is not None:
        await lag_monitor.stop()

    plugin_platform_v2 = getattr(app.state, "plugin_platform_v2", None)
    if plugin_platform_v2 is not None:
        try:
            plugin_platform_v2.publish_event(
                "candlescope.app.stopping/1", {"reason": "Application shutdown"}
            )
            await plugin_platform_v2.stop()
        except Exception as exc:
            logger.warning("Plugin Platform v2 shutdown error: %s", exc, exc_info=True)

    indicator_engine = getattr(app.state, "indicator_engine", None)
    if indicator_engine is not None:
        try:
            indicator_engine.stop()
            print("[shutdown] IndicatorEngine shut down [ok]")
        except Exception as exc:  # noqa: BLE001
            print(f"[shutdown] IndicatorEngine shutdown error: {exc}")

    indicator_range_service = getattr(app.state, "indicator_range_service", None)
    if indicator_range_service is not None:
        indicator_range_service.unbind_all()
        indicator_range_service.clear()

    alert_runtime = getattr(app.state, "alert_runtime", None)
    if alert_runtime is not None:
        try:
            await alert_runtime.stop()
            print("[shutdown] AlertRuntime shut down [ok]")
        except Exception as exc:  # noqa: BLE001
            print(f"[shutdown] AlertRuntime shutdown error: {exc}")

    indicator_runtime_service = getattr(app.state, "indicator_runtime_service", None)
    if indicator_runtime_service is not None:
        try:
            await indicator_runtime_service.stop()
        except Exception as exc:
            logger.warning(
                "Indicator runtime routing shutdown error: %s",
                exc,
                exc_info=True,
            )

    plugin_runtime_host = getattr(app.state, "plugin_runtime_host", None)
    if plugin_runtime_host is not None:
        try:
            await plugin_runtime_host.stop()
            print("[shutdown] Runtime plugin host shut down [ok]")
        except Exception as exc:
            logger.warning("Runtime plugin host shutdown error: %s", exc, exc_info=True)
            print(f"[shutdown] Runtime plugin host shutdown error: {exc}")

    runtime = getattr(app.state, "data_engine_runtime", None)
    if runtime is not None:
        await runtime.shutdown(step_timeout=5)
    replay_runtime = getattr(app.state, "replay_runtime", None)
    if replay_runtime is not None:
        await replay_runtime.shutdown(step_timeout=5)

    print("[shutdown] All components shut down")


__all__ = ["start_personal_runtime", "stop_personal_runtime"]
