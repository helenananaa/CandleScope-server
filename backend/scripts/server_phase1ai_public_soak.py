"""Phase 1AI public-soak controller CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

from app.server_runtime.public_soak_manifest import (
    MODE_DEVELOPMENT_SMOKE,
    MODE_RUN,
    PUBLIC_SOAK_DURATION_MS,
    PublicSoakManifest,
    PublicSoakManifestError,
    load_manifest,
)

PUBLIC_SOAK_ENV = "CANDLESCOPE_PHASE1AI_PUBLIC_SOAK"
FAULT_INJECTION_ENV = "CANDLESCOPE_PHASE1AI_FAULT_INJECTION"


class PublicSoakCliError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = dict(details or {})

    def to_wire(self) -> dict[str, object]:
        return {
            "phase_passed": False,
            "phase1ai_passed": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
            "twenty_four_hour_public_continuity": False,
            "production_ready": False,
        }


def current_git_commit(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise PublicSoakCliError("GIT_COMMIT_UNAVAILABLE", "git rev-parse HEAD failed")
    return result.stdout.strip().lower()


def worktree_is_clean(repo_root: Path) -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise PublicSoakCliError("GIT_STATUS_UNAVAILABLE", "git status failed")
    return result.stdout.strip() == ""


def preflight(
    *,
    mode: str,
    manifest_path: str,
    output_path: str,
    environ: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
) -> PublicSoakManifest:
    values = os.environ if environ is None else environ
    root = repo_root or Path.cwd()
    manifest_file = Path(manifest_path)
    output_file = Path(output_path)
    if not manifest_file.is_absolute():
        raise PublicSoakCliError("RELATIVE_PATH", "manifest path must be absolute")
    if not output_file.is_absolute():
        raise PublicSoakCliError("RELATIVE_PATH", "output path must be absolute")
    if output_file.exists():
        raise PublicSoakCliError("OUTPUT_EXISTS", "output already exists")
    if mode == MODE_RUN and (
        values.get(PUBLIC_SOAK_ENV) != "1" or values.get(FAULT_INJECTION_ENV) != "1"
    ):
        raise PublicSoakCliError(
            "DUAL_SWITCH_MISSING",
            "run requires CANDLESCOPE_PHASE1AI_PUBLIC_SOAK=1 and "
            "CANDLESCOPE_PHASE1AI_FAULT_INJECTION=1",
        )
    try:
        manifest = load_manifest(manifest_file, mode=mode)
    except PublicSoakManifestError as exc:
        raise PublicSoakCliError(exc.code, exc.message, details=exc.details) from exc
    if Path(manifest.output.result_path) != output_file:
        raise PublicSoakCliError(
            "OUTPUT_PATH_MISMATCH",
            "CLI --output must equal manifest.output.result_path",
        )
    if mode == MODE_RUN and manifest.duration_ms < PUBLIC_SOAK_DURATION_MS:
        raise PublicSoakCliError(
            "DURATION_BELOW_PUBLIC_SOAK",
            "run mode cannot be downgraded below 86_400_000 ms",
        )
    commit = current_git_commit(root)
    if commit != manifest.git_commit:
        raise PublicSoakCliError(
            "GIT_COMMIT_MISMATCH",
            "manifest git_commit does not match HEAD",
            details={"head": commit, "manifest": manifest.git_commit},
        )
    if manifest.require_clean_worktree and not worktree_is_clean(root):
        raise PublicSoakCliError(
            "WORKTREE_DIRTY",
            "require_clean_worktree is true but the worktree is dirty",
        )
    compose = Path(manifest.infrastructure.compose_file)
    if shutil.which("docker") is None:
        raise PublicSoakCliError("DOCKER_UNAVAILABLE", "docker is not on PATH")
    if not compose.is_file():
        raise PublicSoakCliError(
            "COMPOSE_MISSING",
            "manifest compose_file does not exist",
        )
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="CandleScope Phase 1AI public soak controller",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name in (MODE_DEVELOPMENT_SMOKE, MODE_RUN):
        command = sub.add_parser(name)
        command.add_argument("--manifest", required=True)
        command.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    try:
        manifest = preflight(
            mode=args.command,
            manifest_path=args.manifest,
            output_path=args.output,
        )
    except PublicSoakCliError as exc:
        print(json.dumps(exc.to_wire(), sort_keys=True), flush=True)
        return 1
    from app.server_runtime.public_soak import execute_public_soak

    payload = asyncio.run(
        execute_public_soak(
            manifest,
            mode=args.command,
            environ=os.environ,
            repo_root=Path.cwd(),
            python_executable=sys.executable,
        )
    )
    print(json.dumps(payload, sort_keys=True), flush=True)
    if payload.get("twenty_four_hour_public_continuity") is True:
        return 0 if payload.get("phase_passed") is True else 1
    return 0 if payload.get("phase_passed") is True else 1


if __name__ == "__main__":
    sys.exit(main())
