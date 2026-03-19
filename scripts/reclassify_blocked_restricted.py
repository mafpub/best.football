#!/usr/bin/env python3
"""Relabel proxy-restricted blocked schools as restricted."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import school_scraper_queue as queue
from pipeline.database import get_db
from pipeline.proxy import get_oxylabs_proxy_servers
from scrapers.schools.runtime import append_blocklist_domain

RESTRICTED_HEADER_MARKER = "x-error-description: access denied: restricted target"


def _normalize_url(value: str) -> str:
    raw = value.strip()
    if not raw:
        return raw
    if "://" not in raw:
        return f"https://{raw}"
    return raw


def _list_blocked_rows(limit: int | None = None) -> list[dict]:
    params: list[object] = [queue.STATUS_BLOCKED]
    limit_clause = ""
    if limit and limit > 0:
        limit_clause = " LIMIT ?"
        params.append(limit)

    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT q.nces_id, s.name, s.website, q.failure_reason
            FROM school_scraper_status q
            JOIN schools s ON s.nces_id = q.nces_id
            WHERE q.status = ?
              AND s.website IS NOT NULL
              AND TRIM(s.website) != ''
            ORDER BY q.updated_at DESC, s.state, s.name
            {limit_clause}
            """,
            tuple(params),
        ).fetchall()
    return [dict(row) for row in rows]


def _probe(proxy: str, url: str, timeout_seconds: int) -> tuple[bool, str]:
    proc = subprocess.run(
        [
            "curl",
            "-skI",
            "-x",
            proxy,
            "--max-time",
            str(timeout_seconds),
            url,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    text = f"{proc.stdout}\n{proc.stderr}"
    return _is_restricted_output(text), text


def _is_restricted_output(payload: str) -> bool:
    lines = payload.splitlines()
    for line in lines:
        if RESTRICTED_HEADER_MARKER in line.lower():
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Relabel blocked rows that are proxy-restricted")
    parser.add_argument(
        "--proxy-profile",
        choices=["mobile", "datacenter"],
        help="Select proxy profile (mobile|datacenter). Defaults to OXYLABS_PROXY_PROFILE/datacenter.",
    )
    parser.add_argument("--limit", type=int, help="Optional max rows to inspect")
    parser.add_argument("--timeout", type=int, default=20, help="curl max-time per proxy")
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing")
    args = parser.parse_args()

    queue.init_tables()
    proxies = list(get_oxylabs_proxy_servers(profile=args.proxy_profile))
    rows = _list_blocked_rows(limit=args.limit)

    inspected = 0
    relabeled = 0
    for row in rows:
        url = _normalize_url(row["website"] or "")
        if not url:
            continue

        inspected += 1
        results = [_probe(proxy, url, args.timeout) for proxy in proxies]
        if not results or not all(is_restricted for is_restricted, _ in results):
            print(f"KEEP_BLOCKED {row['nces_id']} {row['name']} {url}")
            continue

        host = urlparse(url).hostname or url
        reason = f"restricted_target_via_oxylabs:{host}"
        print(f"RESTRICTED {row['nces_id']} {row['name']} {url}")
        if not args.dry_run:
            queue.mark_restricted(row["nces_id"], reason)
            append_blocklist_domain(url, profile=args.proxy_profile, reason=reason)
        relabeled += 1

    print(f"\nInspected: {inspected}")
    print(f"Relabeled restricted: {relabeled}")
    print(f"Unchanged blocked: {inspected - relabeled}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
