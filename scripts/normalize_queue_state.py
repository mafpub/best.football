#!/usr/bin/env python3
"""Normalize scraper queue state for clean production operation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.database import get_db
from pipeline import school_scraper_queue as queue


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize school scraper queue state")
    parser.add_argument("--clear-blocked", action="store_true", help="Move all blocked rows back to pending")
    parser.add_argument(
        "--reset-in-progress",
        action="store_true",
        help="Move in_progress rows back to pending",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing")
    args = parser.parse_args()

    queue.init_tables()

    with get_db() as conn:
        blocked = conn.execute(
            "SELECT COUNT(*) FROM school_scraper_status WHERE status = ?",
            (queue.STATUS_BLOCKED,),
        ).fetchone()[0]
        restricted = conn.execute(
            "SELECT COUNT(*) FROM school_scraper_status WHERE status = ?",
            (queue.STATUS_RESTRICTED,),
        ).fetchone()[0]
        in_progress = conn.execute(
            "SELECT COUNT(*) FROM school_scraper_status WHERE status = ?",
            (queue.STATUS_IN_PROGRESS,),
        ).fetchone()[0]

    print(f"Blocked rows: {blocked}")
    print(f"Restricted rows: {restricted}")
    print(f"In-progress rows: {in_progress}")

    if args.dry_run:
        print("Dry run: no changes applied")
        return 0

    if args.clear_blocked:
        moved = queue.clear_blocked()
        print(f"Cleared blocked -> pending: {moved}")

    if args.reset_in_progress:
        with get_db() as conn:
            conn.execute(
                """
                UPDATE school_scraper_status
                SET status = ?,
                    started_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE status = ?
                """,
                (queue.STATUS_PENDING, queue.STATUS_IN_PROGRESS),
            )
            changed = conn.total_changes
        print(f"Reset in_progress -> pending: {changed}")

    report = queue.get_status_report()
    print(
        "Post-normalization status: "
        f"pending={report[queue.STATUS_PENDING]} "
        f"complete={report[queue.STATUS_COMPLETE]} "
        f"no_football={report[queue.STATUS_NO_FOOTBALL]} "
        f"blocked={report[queue.STATUS_BLOCKED]} "
        f"restricted={report[queue.STATUS_RESTRICTED]} "
        f"failed={report[queue.STATUS_FAILED]} "
        f"needs_repair={report[queue.STATUS_NEEDS_REPAIR]}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
