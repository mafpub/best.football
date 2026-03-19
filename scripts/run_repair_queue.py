#!/usr/bin/env python3
"""Sequential repair loop for school scripts in needs_repair."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import school_scraper_queue as queue
from pipeline.proxy import get_browser_proxy_env
from scrapers.schools.runtime import (
    BlocklistedDomainError,
    ProxyNotConfiguredError,
    assert_not_blocklisted,
    require_proxy_credentials,
    run_scraper_file_sync,
)

LOCK_PATH = PROJECT_ROOT / "data" / "repair_browser.lock"


def _acquire_lock(path: Path):
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8")
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    return handle


def _parse_json_stdout(stdout: str) -> dict | None:
    text = (stdout or "").strip()
    if not text:
        return None

    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in reversed(lines):
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                return value
        except json.JSONDecodeError:
            continue

    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        return None


def _build_command(
    template: str,
    row: dict,
    script_path: Path,
    proxy_profile: str | None,
) -> list[str]:
    rendered = template.format(
        nces_id=row["nces_id"],
        name=row["name"],
        website=row.get("website") or "",
        state=row["state"],
        city=row.get("city") or "",
        script_path=str(script_path),
        failure_reason=row.get("failure_reason") or "",
        proxy_profile=proxy_profile or "",
    )
    return shlex.split(rendered)


def _process_one(
    repair_template: str,
    state: str | None,
    dry_run: bool,
    proxy_profile: str | None,
) -> bool:
    row = queue.claim_next_school(state=state, statuses=(queue.STATUS_NEEDS_REPAIR,))
    if not row:
        print("No schools in needs_repair")
        return False

    nces_id = row["nces_id"]
    script_path = Path(row["scraper_file"]) if row.get("scraper_file") else queue.resolve_script_path(
        PROJECT_ROOT, nces_id, row["state"]
    )

    try:
        require_proxy_credentials(profile=proxy_profile)
        assert_not_blocklisted([row.get("website") or ""], profile=proxy_profile)
    except ProxyNotConfiguredError as exc:
        queue.mark_needs_repair(nces_id, f"proxy_not_configured:{exc}")
        print(f"{nces_id}: remains needs_repair (proxy not configured)")
        return True
    except BlocklistedDomainError as exc:
        queue.mark_blocked(nces_id, f"blocklisted_domain:{exc}")
        print(f"{nces_id}: blocked ({exc})")
        return True

    command = _build_command(repair_template, row, script_path, proxy_profile)
    print(f"Running repair for {nces_id}: {' '.join(shlex.quote(part) for part in command)}")

    if dry_run:
        queue.mark_needs_repair(nces_id, "dry_run_repair")
        print(f"{nces_id}: dry run")
        return True

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.update(get_browser_proxy_env(profile=proxy_profile))
    proc = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    payload = _parse_json_stdout(proc.stdout)

    if proc.returncode != 0:
        stderr_tail = (proc.stderr or "").strip()[-300:]
        queue.mark_needs_repair(nces_id, f"repair_command_failed:{stderr_tail or 'no_stderr'}")
        print(f"{nces_id}: repair command failed", file=sys.stderr)
        return True

    status = (payload or {}).get("status", "complete")
    reason = (payload or {}).get("reason")
    payload_script = (payload or {}).get("script_path")
    if payload_script:
        script_path = Path(payload_script)

    if status == queue.STATUS_BLOCKED:
        queue.mark_blocked(nces_id, reason or "blocked_by_repair")
        print(f"{nces_id}: blocked")
        return True

    if status in {queue.STATUS_FAILED, queue.STATUS_NEEDS_REPAIR}:
        queue.mark_needs_repair(nces_id, reason or "repair_failed")
        print(f"{nces_id}: remains needs_repair")
        return True

    if not script_path.exists():
        queue.mark_needs_repair(nces_id, f"repaired_script_missing:{script_path}")
        print(f"{nces_id}: repaired script missing", file=sys.stderr)
        return True

    try:
        run = run_scraper_file_sync(
            script_path,
            website=row.get("website"),
            profile=proxy_profile,
        )
    except Exception as exc:
        queue.mark_needs_repair(nces_id, f"repair_validation_error:{exc}")
        print(f"{nces_id}: repair validation failed ({exc})", file=sys.stderr)
        return True

    if not run.valid:
        queue.mark_needs_repair(
            nces_id,
            f"repair_validation_contract_failed:{'; '.join(run.validation_errors)}",
        )
        print(f"{nces_id}: repair output invalid")
        return True

    if not run.non_empty_extraction:
        queue.mark_needs_repair(nces_id, "repair_validation_empty_extraction")
        print(f"{nces_id}: repair output empty")
        return True

    queue.mark_complete(nces_id, str(script_path))
    print(f"{nces_id}: repaired and complete")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Run repair sessions for needs_repair schools")
    parser.add_argument(
        "--repair-command",
        required=True,
        help=(
            "Command template for one-shot repair session. "
            "Supports {nces_id} {name} {website} {state} {city} {script_path}"
            " {failure_reason} {proxy_profile}."
        ),
    )
    parser.add_argument("--state", help="Optional state filter")
    parser.add_argument("--continuous", action="store_true", help="Run continuously")
    parser.add_argument("--sleep-seconds", type=int, default=30, help="Idle sleep seconds")
    parser.add_argument(
        "--proxy-profile",
        choices=["mobile", "datacenter"],
        help="Select proxy profile (mobile|datacenter). Defaults to OXYLABS_PROXY_PROFILE/mobile.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not invoke repair command")

    args = parser.parse_args()

    queue.init_tables()

    lock = _acquire_lock(LOCK_PATH)
    try:
        if args.continuous:
            while True:
                handled = _process_one(
                    args.repair_command,
                    args.state,
                    args.dry_run,
                    args.proxy_profile,
                )
                if not handled:
                    time.sleep(max(1, args.sleep_seconds))
        else:
            _process_one(
                args.repair_command,
                args.state,
                args.dry_run,
                args.proxy_profile,
            )
    finally:
        lock.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
