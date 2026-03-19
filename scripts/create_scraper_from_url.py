#!/usr/bin/env python3
"""Run one creator-agent session for a school selected by website URL."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import school_scraper_queue as queue
from pipeline.database import get_db
from pipeline.proxy import get_browser_proxy_env
from scrapers.schools.runtime import (
    BlocklistedDomainError,
    ProxyNotConfiguredError,
    assert_not_blocklisted,
    require_proxy_credentials,
    run_scraper_file_sync,
)


def _norm_url(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    p = urlparse(raw)
    host = (p.hostname or "").lower()
    path = (p.path or "/").rstrip("/")
    return f"{host}{path}"


def _find_school_by_url(url: str, nces_id: str | None = None) -> dict:
    needle = _norm_url(url)
    if not needle:
        raise ValueError("URL is empty")

    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT s.nces_id, s.name, s.website, s.city, s.state
            FROM schools s
            WHERE s.website IS NOT NULL AND TRIM(s.website) != ''
            """
        ).fetchall()

    matches = []
    for row in rows:
        row_dict = dict(row)
        hay = _norm_url(row_dict["website"])
        if hay == needle:
            matches.append(row_dict)

    if nces_id:
        for row in matches:
            if row["nces_id"] == nces_id:
                return row
        raise ValueError(f"No match for URL {url} with NCES {nces_id}")

    if not matches:
        raise ValueError(f"No school found for URL {url}")
    if len(matches) > 1:
        ids = ", ".join(item["nces_id"] for item in matches[:10])
        raise ValueError(f"Multiple schools match URL {url}; pass --nces-id. Candidates: {ids}")

    return matches[0]


def _run_adapter(row: dict, launcher_command: str, proxy_profile: str | None) -> dict:
    script_path = queue.resolve_script_path(PROJECT_ROOT, row["nces_id"], row["state"])
    cmd = [
        "uv",
        "run",
        "python",
        "scripts/agent_session_adapter.py",
        "--mode",
        "create",
        "--launcher-command",
        launcher_command,
        "--nces-id",
        row["nces_id"],
        "--school-name",
        row["name"],
        "--state",
        row["state"],
        "--website",
        row.get("website") or "",
        "--city",
        row.get("city") or "",
        "--script-path",
        str(script_path),
    ]
    if proxy_profile:
        cmd.extend(["--proxy-profile", proxy_profile])

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.update(get_browser_proxy_env(profile=proxy_profile))
    proc = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()[-300:]
        return {
            "status": "failed",
            "script_path": str(script_path),
            "reason": f"adapter_failed:{stderr or 'no_stderr'}",
        }

    text = (proc.stdout or "").strip()
    try:
        return json.loads(text.splitlines()[-1]) if text else {
            "status": "failed",
            "script_path": str(script_path),
            "reason": "adapter_no_output",
        }
    except json.JSONDecodeError:
        return {
            "status": "failed",
            "script_path": str(script_path),
            "reason": "adapter_invalid_json",
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Create one school scraper by website URL")
    parser.add_argument("--url", required=True, help="School website URL")
    parser.add_argument("--launcher-command", required=True, help="Underlying one-shot launcher command")
    parser.add_argument("--nces-id", help="Disambiguate when URL is shared by multiple schools")
    parser.add_argument(
        "--proxy-profile",
        choices=["mobile", "datacenter"],
        help="Select proxy profile (mobile|datacenter). Defaults to OXYLABS_PROXY_PROFILE/datacenter.",
    )
    args = parser.parse_args()

    queue.init_tables()

    try:
        require_proxy_credentials(profile=args.proxy_profile)
    except ProxyNotConfiguredError as exc:
        print(f"proxy_error: {exc}", file=sys.stderr)
        return 2

    row = _find_school_by_url(args.url, nces_id=args.nces_id)
    claimed = queue.claim_school(row["nces_id"])
    if not claimed:
        print("failed to claim school", file=sys.stderr)
        return 1

    try:
        assert_not_blocklisted([claimed.get("website") or args.url], profile=args.proxy_profile)
    except BlocklistedDomainError as exc:
        queue.mark_blocked(claimed["nces_id"], f"blocklisted_domain:{exc}")
        print(json.dumps({"status": "blocked", "reason": str(exc), "nces_id": claimed["nces_id"]}))
        return 0

    result = _run_adapter(claimed, args.launcher_command, proxy_profile=args.proxy_profile)
    status = str(result.get("status") or "failed")
    reason = str(result.get("reason") or "")
    script_path = Path(str(result.get("script_path") or queue.resolve_script_path(PROJECT_ROOT, claimed["nces_id"], claimed["state"])))

    if status == "complete":
        if not script_path.exists():
            queue.mark_failed(claimed["nces_id"], f"missing_script_file:{script_path}")
            print(json.dumps({"status": "failed", "reason": f"missing_script_file:{script_path}", "nces_id": claimed["nces_id"]}))
            return 0

        try:
            run = run_scraper_file_sync(
                script_path,
                website=claimed.get("website") or args.url,
                profile=args.proxy_profile,
            )
            if run.valid and run.non_empty_extraction:
                queue.mark_complete(claimed["nces_id"], str(script_path))
                print(json.dumps({"status": "complete", "reason": reason, "nces_id": claimed["nces_id"], "script_path": str(script_path)}))
                return 0

            err = "; ".join(run.validation_errors) if run.validation_errors else "empty_extraction"
            queue.mark_failed(claimed["nces_id"], f"validation_failed:{err}")
            print(json.dumps({"status": "failed", "reason": f"validation_failed:{err}", "nces_id": claimed["nces_id"]}))
            return 0
        except Exception as exc:  # noqa: BLE001
            queue.mark_failed(claimed["nces_id"], f"validation_error:{exc}")
            print(json.dumps({"status": "failed", "reason": f"validation_error:{exc}", "nces_id": claimed["nces_id"]}))
            return 0

    if status == "blocked":
        queue.mark_blocked(claimed["nces_id"], reason or "blocked_by_creator")
        print(json.dumps({"status": "blocked", "reason": reason or "blocked_by_creator", "nces_id": claimed["nces_id"]}))
        return 0

    queue.mark_failed(claimed["nces_id"], reason or "creator_failed")
    print(json.dumps({"status": "failed", "reason": reason or "creator_failed", "nces_id": claimed["nces_id"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
