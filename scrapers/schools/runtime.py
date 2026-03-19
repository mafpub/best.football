"""Runtime helpers for deterministic per-school scraper scripts."""

from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse

from pipeline.env import load_repo_env
from pipeline.proxy import (
    describe_oxylabs_proxy_mode,
    get_oxylabs_proxy_servers,
    get_playwright_proxy_config as _get_playwright_proxy_config,
    require_oxylabs_proxy_configuration,
)

load_repo_env()

BLOCKLIST_FILE = Path.home() / ".web_scraper_blocklist.json"
REQUIRED_KEYS = {
    "nces_id",
    "school_name",
    "state",
    "source_pages",
    "extracted_items",
    "scrape_meta",
    "errors",
}


class ProxyNotConfiguredError(RuntimeError):
    """Raised when Oxylabs proxy configuration is missing."""


class BlocklistedDomainError(RuntimeError):
    """Raised when a target URL is on the provider blocklist."""


@dataclass
class ScrapeRunResult:
    """Normalized scrape result for one school script."""

    payload: dict[str, Any]
    valid: bool
    validation_errors: list[str]

    @property
    def non_empty_extraction(self) -> bool:
        extracted = self.payload.get("extracted_items")
        if isinstance(extracted, dict):
            return any(_has_data(value) for value in extracted.values())
        return _has_data(extracted)


def _has_data(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def require_proxy_credentials() -> None:
    """Fail fast if neither proxy endpoints nor auth mode are configured."""
    try:
        require_oxylabs_proxy_configuration()
    except ValueError as exc:
        raise ProxyNotConfiguredError(
            "Oxylabs proxy not configured. "
            "Set OXYLABS_PROXY_SERVERS/OXYLABS_PROXY_SERVER, and optionally "
            "OXYLABS_USERNAME/OXYLABS_PASSWORD when not using IP whitelist."
        ) from exc


def get_playwright_proxy_config(proxy_index: int | None = None) -> dict[str, str]:
    """Return the shared Playwright proxy config for school scrapers."""
    require_proxy_credentials()
    return _get_playwright_proxy_config(proxy_index)


def get_proxy_runtime_meta() -> dict[str, Any]:
    """Return lightweight proxy metadata for scraper diagnostics."""
    details = describe_oxylabs_proxy_mode()
    return {
        "proxy_servers": details["servers"],
        "proxy_auth_mode": details["auth_mode"],
    }


def get_proxy_server_list() -> list[str]:
    """Return the configured proxy server pool as a list."""
    return list(get_oxylabs_proxy_servers())


def _normalize_url(url: str) -> str:
    value = url.strip()
    if not value:
        return value
    if "://" not in value:
        return f"https://{value}"
    return value


def _load_blocklist_domains() -> set[str]:
    if not BLOCKLIST_FILE.exists():
        return set()

    try:
        data = json.loads(BLOCKLIST_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return set()

    domains = data.get("domains", [])
    if not isinstance(domains, list):
        return set()

    values = set()
    for value in domains:
        if isinstance(value, str) and value.strip():
            values.add(value.strip().lower())
    return values


def _extract_domain(url: str) -> str:
    normalized = _normalize_url(url)
    parsed = urlparse(normalized)
    return (parsed.hostname or "").lower()


def assert_not_blocklisted(urls: list[str]) -> None:
    """Raise if any URL domain is blocklisted."""
    blocked = _load_blocklist_domains()
    if not blocked:
        return

    for raw_url in urls:
        if not isinstance(raw_url, str) or not raw_url.strip():
            continue
        domain = _extract_domain(raw_url)
        if not domain:
            continue
        if domain in blocked or any(domain.endswith(f".{d}") for d in blocked):
            raise BlocklistedDomainError(
                f"Domain is blocklisted by proxy provider policy: {domain}"
            )


def validate_payload(payload: dict[str, Any]) -> list[str]:
    """Return validation errors for payload contract."""
    errors: list[str] = []
    missing = sorted(REQUIRED_KEYS - set(payload.keys()))
    if missing:
        errors.append(f"Missing required keys: {', '.join(missing)}")

    if "source_pages" in payload and not isinstance(payload["source_pages"], list):
        errors.append("source_pages must be a list")

    if "extracted_items" in payload and not isinstance(payload["extracted_items"], dict):
        errors.append("extracted_items must be a dict")

    if "scrape_meta" in payload and not isinstance(payload["scrape_meta"], dict):
        errors.append("scrape_meta must be a dict")

    if "errors" in payload and not isinstance(payload["errors"], list):
        errors.append("errors must be a list")

    return errors


def _legacy_to_envelope(raw: dict[str, Any]) -> dict[str, Any]:
    source_pages: list[str] = []
    for key in ("athletic_url", "website", "source_url"):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            source_pages.append(_normalize_url(value))

    extracted_items = {
        key: value
        for key, value in raw.items()
        if key
        not in {
            "nces_id",
            "school_name",
            "name",
            "state",
            "athletic_url",
            "website",
            "source_url",
            "scraped_at",
            "errors",
        }
    }

    payload = {
        "nces_id": raw.get("nces_id"),
        "school_name": raw.get("school_name") or raw.get("name"),
        "state": raw.get("state"),
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": raw.get("scraped_at") or datetime.now().isoformat(),
            "script_version": "legacy-adapter",
        },
        "errors": raw.get("errors") if isinstance(raw.get("errors"), list) else [],
    }

    return payload


def normalize_payload(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize raw scraper return into required envelope."""
    if REQUIRED_KEYS.issubset(raw.keys()):
        payload = dict(raw)
    else:
        payload = _legacy_to_envelope(raw)

    if payload.get("state"):
        payload["state"] = str(payload["state"]).upper()

    pages = payload.get("source_pages")
    if not isinstance(pages, list):
        payload["source_pages"] = []
    else:
        normalized_pages = []
        for page in pages:
            if isinstance(page, str) and page.strip():
                normalized_pages.append(_normalize_url(page))
        payload["source_pages"] = normalized_pages

    payload.setdefault("errors", [])
    payload.setdefault("extracted_items", {})
    payload.setdefault("scrape_meta", {})

    meta = payload["scrape_meta"]
    if isinstance(meta, dict):
        meta.setdefault("scraped_at", datetime.now().isoformat())

    return payload


def _load_module(scraper_path: Path):
    module_name = f"school_scraper_{scraper_path.stem}_{abs(hash(str(scraper_path)))}"
    spec = importlib.util.spec_from_file_location(module_name, scraper_path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Could not load scraper module: {scraper_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _discover_entrypoint(module) -> Callable[[], Awaitable[dict[str, Any]]]:
    preferred_names = [
        "scrape_school",
        "scrape_athletics",
    ]
    for name in preferred_names:
        candidate = getattr(module, name, None)
        if inspect.iscoroutinefunction(candidate):
            return candidate

    candidates = []
    for name, value in vars(module).items():
        if name.startswith("scrape_") and inspect.iscoroutinefunction(value):
            candidates.append((name, value))

    if not candidates:
        raise RuntimeError("No async scraper entrypoint found (expected scrape_* function)")

    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


async def run_scraper_file(scraper_path: Path, website: str | None = None) -> ScrapeRunResult:
    """Load and execute one deterministic school scraper script."""
    require_proxy_credentials()
    if website:
        assert_not_blocklisted([website])

    module = _load_module(scraper_path)
    fn = _discover_entrypoint(module)
    raw = await fn()

    if not isinstance(raw, dict):
        raise RuntimeError("Scraper did not return a dict payload")

    payload = normalize_payload(raw)
    errors = validate_payload(payload)
    return ScrapeRunResult(payload=payload, valid=not errors, validation_errors=errors)


def run_scraper_file_sync(scraper_path: Path, website: str | None = None) -> ScrapeRunResult:
    """Synchronous wrapper for CLI scripts."""
    return asyncio.run(run_scraper_file(scraper_path, website=website))
