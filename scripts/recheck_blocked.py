#!/usr/bin/env python3
"""Requeue blocked schools whose scheduled recheck date is due."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import school_scraper_queue as queue


def main() -> int:
    parser = argparse.ArgumentParser(description="Requeue due blocked schools")
    parser.add_argument("--limit", type=int, help="Optional max rows to requeue")
    args = parser.parse_args()

    queue.init_tables()
    count = queue.requeue_due_blocked(limit=args.limit)
    print(f"Requeued {count} blocked school(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
