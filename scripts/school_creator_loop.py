#!/usr/bin/env python3
"""Sequential creator loop for one-school deterministic scraper generation.

This runner processes exactly one school at a time (or loops continuously),
enforcing proxy-only execution and provider blocklist checks.
"""

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

LOCK_PATH = PROJECT_ROOT / "data" / "creator_browser.lock"


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
    school: dict,
    script_path: Path,
    proxy_profile: str | None,
) -> list[str]:
    rendered = template.format(
        nces_id=school["nces_id"],
        name=school["name"],
        website=school.get("website") or "",
        state=school["state"],
        city=school.get("city") or "",
        script_path=str(script_path),
        proxy_profile=proxy_profile or "",
    )
    return shlex.split(rendered)


def _run_creator_command(command: list[str], proxy_profile: str | None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.update(get_browser_proxy_env(profile=proxy_profile))
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _process_one(
    creator_command_template: str,
    state: str | None,
    dry_run: bool,
    proxy_profile: str | None,
    creator_survey_run_id: int,
) -> bool:
    school = queue.claim_next_school(
        state=state,
        statuses=(queue.STATUS_PENDING,),
        survey_run_id=creator_survey_run_id,
    )
    if not school:
        print(
            "No pending schools available from the latest datacenter survey success list"
        )
        return False

    nces_id = school["nces_id"]
    script_path = queue.resolve_script_path(PROJECT_ROOT, nces_id, school["state"])

    try:
        require_proxy_credentials(profile=proxy_profile)
        assert_not_blocklisted([school.get("website") or ""], profile=proxy_profile)
    except ProxyNotConfiguredError as exc:
        queue.mark_failed(nces_id, f"proxy_not_configured:{exc}")
        print(f"{nces_id}: failed (proxy not configured)", file=sys.stderr)
        return True
    except BlocklistedDomainError as exc:
        queue.mark_blocked(nces_id, f"blocklisted_domain:{exc}")
        print(f"{nces_id}: blocked ({exc})")
        return True

    command = _build_command(
        creator_command_template,
        school,
        script_path,
        proxy_profile=proxy_profile,
    )
    print(f"Running creator for {nces_id}: {' '.join(shlex.quote(part) for part in command)}")

    if dry_run:
        print(f"{nces_id}: dry run (would invoke creator and validate)")
        queue.upsert_status(nces_id, queue.STATUS_PENDING, notes="dry_run_creator")
        return True

    result = _run_creator_command(command, proxy_profile)
    creator_payload = _parse_json_stdout(result.stdout)

    if result.returncode != 0:
        stderr_tail = (result.stderr or "").strip()[-300:]
        queue.mark_failed(nces_id, f"creator_command_failed:{stderr_tail or 'no_stderr'}")
        print(f"{nces_id}: failed (creator command exit {result.returncode})", file=sys.stderr)
        return True

    status = (creator_payload or {}).get("status", "complete")
    reason = (creator_payload or {}).get("reason")
    payload_script = (creator_payload or {}).get("script_path")

    if payload_script:
        script_path = Path(payload_script)

    if status == queue.STATUS_BLOCKED:
        queue.mark_blocked(nces_id, reason or "blocked_by_creator")
        print(f"{nces_id}: blocked ({reason or 'blocked_by_creator'})")
        return True

    if status in {queue.STATUS_FAILED, queue.STATUS_NEEDS_REPAIR}:
        queue.mark_failed(nces_id, reason or "failed_by_creator")
        print(f"{nces_id}: failed ({reason or 'failed_by_creator'})")
        return True

    if not script_path.exists():
        queue.mark_failed(nces_id, f"missing_script_file:{script_path}")
        print(f"{nces_id}: failed (script not found at {script_path})", file=sys.stderr)
        return True

    try:
        run = run_scraper_file_sync(
            script_path,
            website=school.get("website"),
            profile=proxy_profile,
        )
    except Exception as exc:
        queue.mark_failed(nces_id, f"validation_run_error:{exc}")
        print(f"{nces_id}: failed validation run ({exc})", file=sys.stderr)
        return True

    if not run.valid:
        queue.mark_failed(nces_id, f"validation_failed:{'; '.join(run.validation_errors)}")
        print(f"{nces_id}: failed validation contract")
        return True

    if not run.non_empty_extraction:
        queue.mark_failed(nces_id, "validation_empty_extraction")
        print(f"{nces_id}: failed (empty extraction)")
        return True

    queue.mark_complete(nces_id, str(script_path))
    print(f"{nces_id}: complete ({script_path})")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one-school creator sessions sequentially")
    parser.add_argument(
        "--creator-command",
        required=True,
        help=(
            "Command template for one-shot creator session. "
            "Supports {nces_id} {name} {website} {state} {city} {script_path}"
            " {proxy_profile}"
        ),
    )
    parser.add_argument("--state", help="Optional state filter")
    parser.add_argument("--continuous", action="store_true", help="Run continuously")
    parser.add_argument("--sleep-seconds", type=int, default=15, help="Idle sleep between iterations")
    parser.add_argument(
        "--proxy-profile",
        choices=["mobile", "datacenter"],
        help="Select proxy profile (mobile|datacenter). Defaults to OXYLABS_PROXY_PROFILE/datacenter.",
    )
    parser.add_argument("--seed-missing", action="store_true", help="Seed queue from schools before running")
    parser.add_argument("--dry-run", action="store_true", help="Do not invoke command or write final statuses")

    args = parser.parse_args()

    queue.init_tables()
    try:
        creator_survey_run_id = queue.require_latest_creator_survey_run_id()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    if args.seed_missing:
        seeded = queue.seed_queue(state=args.state, survey_run_id=creator_survey_run_id)
        print(
            f"Seeded {seeded} queue rows from datacenter survey success run "
            f"{creator_survey_run_id}"
        )

    lock = _acquire_lock(LOCK_PATH)
    try:
        if args.continuous:
            while True:
                handled = _process_one(
                    creator_command_template=args.creator_command,
                    state=args.state,
                    dry_run=args.dry_run,
                    proxy_profile=args.proxy_profile,
                    creator_survey_run_id=creator_survey_run_id,
                )
                if not handled:
                    time.sleep(max(1, args.sleep_seconds))
        else:
            _process_one(
                creator_command_template=args.creator_command,
                state=args.state,
                dry_run=args.dry_run,
                proxy_profile=args.proxy_profile,
                creator_survey_run_id=creator_survey_run_id,
            )
    finally:
        lock.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
