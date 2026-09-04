"""Server Profile FastAPI lifespan. Never opens personal SQLite stores."""

from __future__ import annotations

from collections.abc import Mapping

from app.deployment.profile import DeploymentSettings
from app.server_runtime.composition import ServerDataPlaneComposition


async def start_server_runtime(
    app: object,
    settings: DeploymentSettings,
    composition: ServerDataPlaneComposition,
    *,
    environment: Mapping[str, str] | None = None,
) -> None:
    """Bind server ports onto the FastAPI app. Does not call SQLite initializers."""

    from app.server_runtime.application import attach_server_profile

    await attach_server_profile(
        app,
        settings,
        composition,
        environment=environment,
    )


async def stop_server_runtime(app: object) -> None:
    runtime = getattr(app.state, "server_profile_runtime", None)
    if runtime is not None:
        await runtime.stop()
        app.state.server_profile_runtime = None


__all__ = ["start_server_runtime", "stop_server_runtime"]
