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

from pipeline.proxy import describe_oxylabs_proxy_mode

ALLOWED_STATUSES = {"complete", "blocked", "failed"}
PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_TEMPLATE_BY_MODE = {
    "create": PROJECT_ROOT / "templates" / "agent_prompts" / "school_creator.md",
    "repair": PROJECT_ROOT / "templates" / "agent_prompts" / "school_repair.md",
}


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
    template_path = DEFAULT_TEMPLATE_BY_MODE[mode]
    template = template_path.read_text(encoding="utf-8")
    proxy_mode = describe_oxylabs_proxy_mode()
    return template.format(
        mode=mode,
        nces_id=nces_id,
        school_name=school_name,
        state=state,
        website=website,
        city=city,
        script_path=script_path,
        failure_reason=failure_reason,
        proxy_servers=", ".join(proxy_mode["servers"]),
        proxy_auth_mode=proxy_mode["auth_mode"],
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
