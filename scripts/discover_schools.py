#!/usr/bin/env python3
"""
School Athletic Website Discovery Orchestrator

This script coordinates the discovery and scraper generation process.
Claude (the human operator) uses Playwright MCP to:
1. Browse each school website
2. Find athletic content
3. Generate deterministic scraper scripts

Usage:
    # Get next batch of schools to discover
    python scripts/discover_schools.py --next-batch --count 10

    # Mark a school as complete
    python scripts/discover_schools.py --complete {nces_id}

    # Mark a school as blocked (Cloudflare, etc.)
    python scripts/discover_schools.py --blocked {nces_id} --reason "cloudflare"

    # Status report
    python scripts/discover_schools.py --status
"""

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.database import get_db


def get_next_batch(count: int = 10, state: str = None) -> list[dict]:
    """Get next batch of schools needing scraper discovery.

    Returns:
        List of school dicts with nces_id, name, website, state, city
    """
    with get_db() as conn:
        if state:
            schools = conn.execute("""
                SELECT nces_id, name, website, city, state
                FROM schools
                WHERE state = ?
                AND website IS NOT NULL AND website != ''
                AND nces_id NOT IN (
                    SELECT nces_id FROM school_scraper_status
                    WHERE status IN ('complete', 'blocked')
                )
                ORDER BY state, name
                LIMIT ?
            """, (state, count)).fetchall()
        else:
            schools = conn.execute("""
                SELECT nces_id, name, website, city, state
                FROM schools
                WHERE website IS NOT NULL AND website != ''
                AND nces_id NOT IN (
                    SELECT nces_id FROM school_scraper_status
                    WHERE status IN ('complete', 'blocked')
                )
                ORDER BY state, name
                LIMIT ?
            """, (count,)).fetchall()

        return [dict(row) for row in schools]


def mark_status(nces_id: str, status: str, scraper_file: str = None, reason: str = None, notes: str = None):
    """Mark a school's scraper status.

    Args:
        nces_id: School NCES ID
        status: 'pending', 'in_progress', 'complete', 'blocked', 'failed'
        scraper_file: Path to generated scraper script
        reason: Reason for blocked/failed status
        notes: Additional notes
    """
    with get_db() as conn:
        # Check if table exists
        table_exists = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='school_scraper_status'
        """).fetchone()

        if not table_exists:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS school_scraper_status (
                    nces_id TEXT PRIMARY KEY REFERENCES schools(nces_id),
                    status TEXT DEFAULT 'pending',
                    scraper_file TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    failure_reason TEXT,
                    notes TEXT,
                    attempts INTEGER DEFAULT 0
                )
            """)

        if status == 'in_progress':
            conn.execute("""
                INSERT OR REPLACE INTO school_scraper_status (nces_id, status, started_at, attempts)
                VALUES (?, 'in_progress', CURRENT_TIMESTAMP, COALESCE((SELECT attempts FROM school_scraper_status WHERE nces_id = ?), 0) + 1)
            """, (nces_id, nces_id))
        elif status == 'complete':
            conn.execute("""
                INSERT OR REPLACE INTO school_scraper_status (nces_id, status, scraper_file, completed_at)
                VALUES (?, 'complete', ?, CURRENT_TIMESTAMP)
                ON CONFLICT(nces_id) DO UPDATE SET status = 'complete', scraper_file = excluded.scraper_file, completed_at = excluded.completed_at
            """, (nces_id, scraper_file))
        elif status == 'blocked':
            conn.execute("""
                INSERT OR REPLACE INTO school_scraper_status (nces_id, status, failure_reason, completed_at)
                VALUES (?, 'blocked', ?, CURRENT_TIMESTAMP)
            """, (nces_id, reason))
        elif status == 'failed':
            conn.execute("""
                UPDATE school_scraper_status
                SET status = 'failed', notes = ?
                WHERE nces_id = ?
            """, (notes, nces_id))

        conn.commit()


def save_scraper_script(nces_id: str, state: str, script_content: str) -> Path:
    """Save a generated scraper script.

    Args:
        nces_id: School NCES ID
        state: State abbreviation
        script_content: Python script content

    Returns:
        Path to saved script
    """
    state_dir = PROJECT_ROOT / "scrapers" / "schools" / state.lower()
    state_dir.mkdir(parents=True, exist_ok=True)

    script_path = state_dir / f"{nces_id}.py"
    script_path.write_text(script_content)

    return script_path


def get_status_report() -> dict:
    """Get overall discovery status.

    Returns:
        Dict with counts by status
    """
    with get_db() as conn:
        # Check table exists
        table_exists = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='school_scraper_status'
        """).fetchone()

        if not table_exists:
            return {
                "total_schools": 0,
                "pending": 0,
                "in_progress": 0,
                "complete": 0,
                "blocked": 0,
                "failed": 0,
            }

        # Get total schools with websites
        total = conn.execute("""
            SELECT COUNT(*) FROM schools
            WHERE website IS NOT NULL AND website != ''
        """).fetchone()[0]

        # Get status counts
        stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END) as complete,
                SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM school_scraper_status
        """).fetchone()

        in_progress = stats["in_progress"] or 0
        complete = stats["complete"] or 0
        blocked = stats["blocked"] or 0
        failed = stats["failed"] or 0

        return {
            "total_schools": total,
            "pending": total - (in_progress + complete + blocked + failed),
            "in_progress": in_progress,
            "complete": complete,
            "blocked": blocked,
            "failed": failed,
        }


def main():
    parser = argparse.ArgumentParser(description="School discovery orchestrator")
    parser.add_argument("--next-batch", action="store_true", help="Get next batch of schools to discover")
    parser.add_argument("--count", type=int, default=10, help="Batch size")
    parser.add_argument("--state", help="Filter by state")
    parser.add_argument("--complete", metavar="NCES_ID", help="Mark school as complete")
    parser.add_argument("--scraper-file", metavar="PATH", help="Path to scraper script (for --complete)")
    parser.add_argument("--blocked", metavar="NCES_ID", help="Mark school as blocked")
    parser.add_argument("--failed", metavar="NCES_ID", help="Mark school as failed")
    parser.add_argument("--reason", help="Reason for blocked/failed")
    parser.add_argument("--notes", help="Additional notes")
    parser.add_argument("--status", action="store_true", help="Show status report")

    args = parser.parse_args()

    # Initialize status table
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS school_scraper_status (
                nces_id TEXT PRIMARY KEY REFERENCES schools(nces_id),
                status TEXT DEFAULT 'pending',
                scraper_file TEXT,
                started_at TEXT,
                completed_at TEXT,
                failure_reason TEXT,
                notes TEXT,
                attempts INTEGER DEFAULT 0
            )
        """)
        conn.commit()

    if args.next_batch:
        schools = get_next_batch(args.count, args.state)
        print(json.dumps(schools, indent=2))

    elif args.complete:
        mark_status(args.complete, "complete", scraper_file=args.scraper_file)
        print(f"Marked {args.complete} as complete")
        if args.scraper_file:
            print(f"  Scraper: {args.scraper_file}")

    elif args.blocked:
        mark_status(args.blocked, "blocked", args.reason or "Unknown")
        print(f"Marked {args.blocked} as blocked: {args.reason}")

    elif args.failed:
        mark_status(args.failed, "failed", notes=args.notes)
        print(f"Marked {args.failed} as failed: {args.notes}")

    elif args.status:
        report = get_status_report()
        print("\n=== Discovery Status ===")
        print(f"Total schools with websites: {report['total_schools']}")
        print(f"Pending: {report['pending']}")
        print(f"In Progress: {report['in_progress']}")
        print(f"Complete: {report['complete']}")
        print(f"Blocked: {report['blocked']}")
        print(f"Failed: {report['failed']}")

        if report['total_schools'] > 0:
            pct = (report['complete'] / report['total_schools']) * 100
            print(f"\nProgress: {pct:.1f}%")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
