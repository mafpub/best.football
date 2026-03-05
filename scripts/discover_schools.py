#!/usr/bin/env python3
"""School scraper queue orchestration.

Lifecycle states:
- pending
- in_progress
- complete
- blocked
- failed
- needs_repair

This script is the control plane for deterministic per-school scraper work.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import school_scraper_queue as queue


def _parse_statuses(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return (queue.STATUS_PENDING,)
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    return values or (queue.STATUS_PENDING,)


def main() -> int:
    parser = argparse.ArgumentParser(description="School scraper queue control")

    parser.add_argument("--seed", action="store_true", help="Seed queue from schools with websites")
    parser.add_argument("--limit", type=int, help="Optional max rows for seed/requeue/list operations")

    parser.add_argument("--next-batch", action="store_true", help="List next schools in queue")
    parser.add_argument("--count", type=int, default=10, help="Batch size for --next-batch")
    parser.add_argument("--claim-next", action="store_true", help="Claim one school and move to in_progress")

    parser.add_argument("--state", help="Filter by state abbreviation")
    parser.add_argument(
        "--statuses",
        help="Comma-separated statuses for --next-batch/--claim-next (default: pending)",
    )

    parser.add_argument("--complete", metavar="NCES_ID", help="Mark school as complete")
    parser.add_argument("--scraper-file", metavar="PATH", help="Path to generated scraper script")

    parser.add_argument("--blocked", metavar="NCES_ID", help="Mark school as blocked")
    parser.add_argument("--failed", metavar="NCES_ID", help="Mark school as failed")
    parser.add_argument("--needs-repair", metavar="NCES_ID", help="Mark school as needs_repair")

    parser.add_argument("--reason", help="Failure/blocked reason")
    parser.add_argument("--notes", help="Additional notes")

    parser.add_argument("--requeue-due-blocked", action="store_true", help="Requeue blocked rows whose recheck time is due")
    parser.add_argument("--clear-blocked", action="store_true", help="Move blocked rows back to pending immediately")
    parser.add_argument("--status", action="store_true", help="Show queue status report")

    args = parser.parse_args()

    queue.init_tables()

    if args.seed:
        inserted = queue.seed_queue(state=args.state, limit=args.limit)
        print(f"Seeded {inserted} school(s)")
        return 0

    if args.next_batch:
        rows = queue.get_next_batch(
            count=max(1, args.count),
            state=args.state,
            statuses=_parse_statuses(args.statuses),
        )
        print(json.dumps(rows, indent=2))
        return 0

    if args.claim_next:
        row = queue.claim_next_school(
            state=args.state,
            statuses=_parse_statuses(args.statuses),
        )
        print(json.dumps(row, indent=2) if row else "null")
        return 0

    if args.complete:
        if not args.scraper_file:
            print("--complete requires --scraper-file", file=sys.stderr)
            return 2
        queue.mark_complete(args.complete, args.scraper_file)
        print(f"Marked {args.complete} as complete")
        print(f"  Scraper: {args.scraper_file}")
        return 0

    if args.blocked:
        queue.mark_blocked(args.blocked, args.reason or "blocked_no_reason")
        print(f"Marked {args.blocked} as blocked: {args.reason or 'blocked_no_reason'}")
        return 0

    if args.failed:
        queue.mark_failed(args.failed, args.reason or "failed_no_reason", notes=args.notes)
        print(f"Marked {args.failed} as failed")
        return 0

    if args.needs_repair:
        queue.mark_needs_repair(args.needs_repair, args.reason)
        print(f"Marked {args.needs_repair} as needs_repair")
        return 0

    if args.requeue_due_blocked:
        count = queue.requeue_due_blocked(limit=args.limit)
        print(f"Requeued {count} blocked school(s)")
        return 0

    if args.clear_blocked:
        count = queue.clear_blocked(state=args.state, limit=args.limit)
        print(f"Cleared {count} blocked school(s) back to pending")
        return 0

    if args.status:
        report = queue.get_status_report(state=args.state)
        print("\n=== School Scraper Queue Status ===")
        print(f"Total schools with websites: {report['total_schools']}")
        print(f"Pending: {report[queue.STATUS_PENDING]}")
        print(f"In Progress: {report[queue.STATUS_IN_PROGRESS]}")
        print(f"Complete: {report[queue.STATUS_COMPLETE]}")
        print(f"Blocked: {report[queue.STATUS_BLOCKED]}")
        print(f"Failed: {report[queue.STATUS_FAILED]}")
        print(f"Needs Repair: {report[queue.STATUS_NEEDS_REPAIR]}")
        print(f"\nProgress (complete only): {report['progress_complete_pct']:.1f}%")
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
