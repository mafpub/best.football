"""Repo-local environment loader.

Loads key/value pairs from project .env files into os.environ so scripts can run
without shell-level export setup.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILES = (PROJECT_ROOT / ".env", PROJECT_ROOT / ".env.local")


def _strip_wrapping_quotes(value: str) -> str:
    if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
        return value[1:-1]
    return value


def _parse_env_line(line: str) -> tuple[str, str] | None:
    text = line.strip()
    if not text or text.startswith("#"):
        return None

    if text.startswith("export "):
        text = text[len("export ") :].strip()

    if "=" not in text:
        return None

    key, value = text.split("=", 1)
    key = key.strip()
    if not key:
        return None

    return key, _strip_wrapping_quotes(value.strip())


def load_repo_env(*, override: bool = False) -> None:
    """Load .env values from this repo into process environment."""
    for path in ENV_FILES:
        if not path.exists():
            continue

        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue

        for line in lines:
            parsed = _parse_env_line(line)
            if not parsed:
                continue
            key, value = parsed
            if override or key not in os.environ:
                os.environ[key] = value
