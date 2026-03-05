#!/usr/bin/env python3
"""Parallel runtime for deterministic per-school scraper scripts."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import school_scraper_queue as queue
from scrapers.schools.runtime import (
    BlocklistedDomainError,
    ProxyNotConfiguredError,
    assert_not_blocklisted,
    require_proxy_credentials,
    run_scraper_file,
)

logger = logging.getLogger(__name__)


async def _run_one(row: dict, semaphore: asyncio.Semaphore, dry_run: bool) -> dict:
    async with semaphore:
        nces_id = row["nces_id"]
        script_path = Path(row["scraper_file"])
        started = datetime.now().isoformat(timespec="seconds")

        if dry_run:
            logger.info("[dry-run] would scrape %s (%s)", nces_id, script_path)
            return {"nces_id": nces_id, "status": "dry_run"}

        if not script_path.exists():
            reason = f"missing_script_file:{script_path}"
            queue.mark_failed(nces_id, reason)
            queue.add_scrape_run(
                nces_id,
                "failed",
                script_path=str(script_path),
                started_at=started,
                ended_at=datetime.now().isoformat(timespec="seconds"),
                error_message=reason,
            )
            return {"nces_id": nces_id, "status": "failed", "reason": reason}

        try:
            assert_not_blocklisted([row.get("website") or ""])
            run = await run_scraper_file(script_path, website=row.get("website"))

            if not run.valid:
                reason = f"validation_failed:{'; '.join(run.validation_errors)}"
                queue.mark_failed(nces_id, reason)
                status = "failed"
                payload = run.payload
            elif not run.non_empty_extraction:
                reason = "validation_empty_extraction"
                queue.mark_failed(nces_id, reason)
                status = "failed"
                payload = run.payload
            else:
                queue.mark_complete(nces_id, str(script_path))
                reason = None
                status = "success"
                payload = run.payload

            queue.add_scrape_run(
                nces_id,
                status,
                script_path=str(script_path),
                started_at=started,
                ended_at=datetime.now().isoformat(timespec="seconds"),
                error_message=reason,
                payload=payload,
            )

            return {
                "nces_id": nces_id,
                "status": status,
                "reason": reason,
            }

        except BlocklistedDomainError as exc:
            reason = f"blocklisted_domain:{exc}"
            queue.mark_blocked(nces_id, reason)
            queue.add_scrape_run(
                nces_id,
                "blocked",
                script_path=str(script_path),
                started_at=started,
                ended_at=datetime.now().isoformat(timespec="seconds"),
                error_message=reason,
            )
            return {"nces_id": nces_id, "status": "blocked", "reason": reason}

        except Exception as exc:
            reason = f"runtime_error:{exc}"
            queue.mark_failed(nces_id, reason)
            queue.add_scrape_run(
                nces_id,
                "failed",
                script_path=str(script_path),
                started_at=started,
                ended_at=datetime.now().isoformat(timespec="seconds"),
                error_message=reason,
            )
            return {"nces_id": nces_id, "status": "failed", "reason": reason}


async def _run_all(rows: list[dict], workers: int, dry_run: bool) -> list[dict]:
    semaphore = asyncio.Semaphore(max(1, workers))
    tasks = [asyncio.create_task(_run_one(row, semaphore, dry_run=dry_run)) for row in rows]
    return await asyncio.gather(*tasks)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run complete school scrapers with worker pool")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent scraper workers")
    parser.add_argument("--state", help="Optional state filter")
    parser.add_argument("--limit", type=int, help="Optional max scripts to run")
    parser.add_argument("--dry-run", action="store_true", help="List work without executing scripts")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    queue.init_tables()

    try:
        require_proxy_credentials()
    except ProxyNotConfiguredError as exc:
        logger.error(str(exc))
        return 2

    rows = queue.get_complete_rows(state=args.state, limit=args.limit)
    logger.info("Selected %d school scripts", len(rows))

    if not rows:
        return 0

    results = asyncio.run(_run_all(rows, workers=args.workers, dry_run=args.dry_run))

    success = sum(1 for row in results if row["status"] == "success")
    failed = sum(1 for row in results if row["status"] == "failed")
    blocked = sum(1 for row in results if row["status"] == "blocked")
    dry = sum(1 for row in results if row["status"] == "dry_run")

    logger.info(
        "Run complete: success=%d failed=%d blocked=%d dry_run=%d",
        success,
        failed,
        blocked,
        dry,
    )

    if failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
