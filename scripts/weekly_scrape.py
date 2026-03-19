#!/usr/bin/env python3
"""Weekly scrape cron entry point.

This script is run by cron (Sundays at 2 AM) to:
1. Run all scrapers sequentially
2. Detect changes in athletic data
3. Update affected school pages
4. Trigger repair agents if scrapers fail

Usage:
    python scripts/weekly_scrape.py [--dry-run]

Cron entry:
    0 2 * * 0 cd /home/dd/code/sites/best.football && uv run python scripts/weekly_scrape.py >> /var/log/best.football/scrape.log 2>&1
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from orchestrator.runner import SequentialOrchestrator


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


async def main():
    """Main entry point for weekly scrape."""
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv

    setup_logging(dry_run)
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("Starting weekly scrape")
    logger.info("Dry run: %s", dry_run)
    logger.info("=" * 60)

    try:
        orchestrator = SequentialOrchestrator(dry_run=dry_run)
        results = await orchestrator.run_weekly_scrape()

        # Exit with error code if any scrapers failed
        failed = any(r.get("status") == "failed" for r in results.values())

        if failed:
            logger.error("Some scrapers failed - check logs")
            sys.exit(1)
        else:
            logger.info("All scrapers completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.exception("Fatal error during weekly scrape")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
