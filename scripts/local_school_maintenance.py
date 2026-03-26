#!/usr/bin/env python3
"""Run school scrape, repair, build, and optional deploy from the local machine."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent


def default_repair_command() -> str:
    return "bash scripts/local_codex_repair.sh {prompt_path}"


def build_commands(
    *,
    workers: int,
    proxy_profile: str | None,
    dry_run: bool,
    repair: bool,
    repair_command: str | None,
    deploy_ha1: bool,
) -> list[list[str]]:
    commands: list[list[str]] = [
        [
            "uv",
            "run",
            "python",
            "scripts/run_school_scrapes.py",
            "--workers",
            str(max(1, workers)),
        ]
    ]

    if proxy_profile:
        commands[0].extend(["--proxy-profile", proxy_profile])

    if dry_run:
        commands[0].append("--dry-run")
        return commands

    if repair:
        repair_args = [
            "uv",
            "run",
            "python",
            "scripts/run_repair_queue.py",
            "--drain-until-empty",
            "--repair-command",
            repair_command or default_repair_command(),
        ]
        if proxy_profile:
            repair_args.extend(["--proxy-profile", proxy_profile])
        commands.append(repair_args)

    commands.append(["uv", "run", "python", "scripts/build_site.py"])

    if deploy_ha1:
        commands.append(["bash", "scripts/deploy-ha1.sh", "--skip-build"])

    return commands


def run_commands(commands: list[list[str]]) -> int:
    for command in commands:
        print(f"Running: {' '.join(command)}")
        result = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
        if result.returncode != 0:
            return result.returncode
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local school maintenance workflow")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent scraper workers")
    parser.add_argument(
        "--proxy-profile",
        choices=["mobile", "datacenter"],
        default="datacenter",
        help="Proxy profile for scrape and repair runs",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show scrape work without executing repairs or deploy")
    parser.add_argument(
        "--no-repair",
        action="store_true",
        help="Skip draining the repair queue after scrape runs",
    )
    parser.add_argument(
        "--repair-command",
        help="Override the repair launcher command template passed to run_repair_queue.py",
    )
    parser.add_argument(
        "--deploy-ha1",
        action="store_true",
        help="Upload rebuilt site and database to ha1 after local build",
    )
    args = parser.parse_args()

    commands = build_commands(
        workers=args.workers,
        proxy_profile=args.proxy_profile,
        dry_run=args.dry_run,
        repair=not args.no_repair,
        repair_command=args.repair_command,
        deploy_ha1=args.deploy_ha1,
    )
    return run_commands(commands)


if __name__ == "__main__":
    raise SystemExit(main())
