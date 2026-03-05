#!/usr/bin/env python3
"""Adapter for one-shot creator/repair agent launcher commands.

This script standardizes launcher output into a strict JSON contract:
{"status":"complete|blocked|failed","script_path":"...","reason":"..."}
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

ALLOWED_STATUSES = {"complete", "blocked", "failed"}


def _prompt_text(
    *,
    mode: str,
    nces_id: str,
    school_name: str,
    state: str,
    website: str,
    city: str,
    script_path: str,
    failure_reason: str,
) -> str:
    shared = f"""
School: {school_name}
NCES ID: {nces_id}
State: {state}
City: {city}
Website: {website}
Script path: {script_path}

Hard requirements:
- Use Playwright with Oxylabs proxy only. No direct/non-proxy requests.
- Respect provider blocklist and school website constraints.
- Build/repair deterministic selectors for this single school only.
- Output must be one JSON object on the last line:
  {{"status":"complete|blocked|failed","script_path":"{script_path}","reason":"..."}}
""".strip()

    if mode == "create":
        return (
            "Create a new deterministic per-school scraper script.\n\n"
            f"{shared}\n\n"
            "Actions:\n"
            "1. Manually navigate the school site and athletics-related pages/subdomains.\n"
            "2. Capture high-value locations (news, schedule, roster, coaches, contacts, camp pages where present).\n"
            "3. Write scraper script to the exact script path above.\n"
            "4. Script must return the shared envelope:\n"
            "   nces_id, school_name, state, source_pages, extracted_items, scrape_meta, errors\n"
            "5. Run once and verify non-empty extraction.\n"
            "6. If no athletics program/public content exists, return status=blocked with reason."
        )

    return (
        "Repair an existing deterministic per-school scraper script.\n\n"
        f"{shared}\n\n"
        f"Last failure reason: {failure_reason}\n\n"
        "Actions:\n"
        "1. Compare live site DOM/structure with current script selectors.\n"
        "2. Patch only this school script to restore deterministic extraction.\n"
        "3. Preserve proxy-only behavior.\n"
        "4. Run once and verify non-empty extraction + required envelope.\n"
        "5. If school no longer has accessible athletics content, return status=blocked with reason."
    )


def _parse_json_from_stdout(stdout: str) -> dict | None:
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


def _normalize_result(raw: dict | None, script_path: str) -> dict:
    if not isinstance(raw, dict):
        return {
            "status": "failed",
            "script_path": script_path,
            "reason": "launcher_no_json_result",
        }

    status = str(raw.get("status", "failed")).strip().lower()
    if status not in ALLOWED_STATUSES:
        status = "failed"

    result = {
        "status": status,
        "script_path": str(raw.get("script_path") or script_path),
        "reason": raw.get("reason"),
    }

    if status == "complete" and not Path(result["script_path"]).exists():
        result["status"] = "failed"
        result["reason"] = f"script_not_found:{result['script_path']}"

    return result


def _build_command(template: str, values: dict[str, str]) -> list[str]:
    rendered = template.format(**values)
    return shlex.split(rendered)


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize creator/repair launcher output")
    parser.add_argument("--mode", choices=["create", "repair"], required=True)
    parser.add_argument("--launcher-command", required=True)

    parser.add_argument("--nces-id", required=True)
    parser.add_argument("--school-name", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--website", default="")
    parser.add_argument("--city", default="")
    parser.add_argument("--script-path", required=True)
    parser.add_argument("--failure-reason", default="")
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    prompt = _prompt_text(
        mode=args.mode,
        nces_id=args.nces_id,
        school_name=args.school_name,
        state=args.state,
        website=args.website,
        city=args.city,
        script_path=args.script_path,
        failure_reason=args.failure_reason,
    )

    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as tmp:
        tmp.write(prompt)
        prompt_path = tmp.name

    values = {
        "mode": args.mode,
        "nces_id": args.nces_id,
        "school_name": args.school_name,
        "state": args.state,
        "website": args.website,
        "city": args.city,
        "script_path": args.script_path,
        "failure_reason": args.failure_reason,
        "prompt_path": prompt_path,
    }

    command = _build_command(args.launcher_command, values)
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")

    try:
        proc = subprocess.run(
            command,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(1, args.timeout_seconds),
        )
    except subprocess.TimeoutExpired:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "script_path": args.script_path,
                    "reason": "launcher_timeout",
                }
            )
        )
        return 0

    if args.verbose and proc.stdout:
        print(proc.stdout, file=sys.stderr)
    if args.verbose and proc.stderr:
        print(proc.stderr, file=sys.stderr)

    if proc.returncode != 0:
        stderr_tail = (proc.stderr or "").strip()[-300:]
        print(
            json.dumps(
                {
                    "status": "failed",
                    "script_path": args.script_path,
                    "reason": f"launcher_exit_{proc.returncode}:{stderr_tail or 'no_stderr'}",
                }
            )
        )
        return 0

    parsed = _parse_json_from_stdout(proc.stdout)
    normalized = _normalize_result(parsed, args.script_path)
    print(json.dumps(normalized))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
