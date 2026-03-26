#!/usr/bin/env python3
"""Weekly deterministic school scrape entry point."""

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path


def setup_logging(dry_run: bool = False):
    """Configure logging for the weekly scrape."""
    log_dir = Path("/var/log/best.football")

    handlers = [logging.StreamHandler(sys.stdout)]

    if not dry_run and log_dir.exists():
        handlers.append(
            logging.FileHandler(log_dir / "scrape.log")
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )


def build_weekly_commands(
    *,
    dry_run: bool,
    repair_command: str | None,
    workers: int,
    proxy_profile: str | None,
) -> list[list[str]]:
    commands: list[list[str]] = [
        [
            "uv",
            "run",
            "python",
            "scripts/run_school_scrapes.py",
            "--workers",
            str(workers),
        ]
    ]

    if proxy_profile:
        commands[0].extend(["--proxy-profile", proxy_profile])

    if dry_run:
        commands[0].append("--dry-run")
        return commands

    if repair_command:
        repair_args = [
            "uv",
            "run",
            "python",
            "scripts/run_repair_queue.py",
            "--drain-until-empty",
            "--repair-command",
            repair_command,
        ]
        if proxy_profile:
            repair_args.extend(["--proxy-profile", proxy_profile])
        commands.append(repair_args)

    commands.append(["uv", "run", "python", "scripts/build_site.py"])
    return commands


def run_weekly_commands(commands: list[list[str]]) -> int:
    logger = logging.getLogger(__name__)
    project_root = Path(__file__).parent.parent

    for command in commands:
        logger.info("Running: %s", " ".join(command))
        result = subprocess.run(command, cwd=project_root, check=False)
        if result.returncode != 0:
            logger.error("Command failed with exit code %s", result.returncode)
            return result.returncode

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run weekly deterministic school scrapes")
    parser.add_argument("--dry-run", "-n", action="store_true", help="List work without executing scrapers")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent scraper workers")
    parser.add_argument(
        "--proxy-profile",
        choices=["mobile", "datacenter"],
        help="Proxy profile for runtime and repair queue",
    )
    parser.add_argument(
        "--repair-command",
        help="Optional repair command template. Defaults to BEST_FOOTBALL_REPAIR_COMMAND if set.",
    )
    args = parser.parse_args()

    setup_logging(args.dry_run)
    logger = logging.getLogger(__name__)
    repair_command = args.repair_command or os.environ.get("BEST_FOOTBALL_REPAIR_COMMAND")

    logger.info("=" * 60)
    logger.info("Starting weekly deterministic scrape")
    logger.info("Dry run: %s", args.dry_run)
    logger.info("Repair queue enabled: %s", bool(repair_command) and not args.dry_run)
    logger.info("=" * 60)

    commands = build_weekly_commands(
        dry_run=args.dry_run,
        repair_command=repair_command,
        workers=max(1, args.workers),
        proxy_profile=args.proxy_profile,
    )
    return run_weekly_commands(commands)


if __name__ == "__main__":
    raise SystemExit(main())
