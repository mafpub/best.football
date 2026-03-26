"""Deterministic football scraper for American High (CA)."""

from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timezone
from typing import Any

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "061440001654"
SCHOOL_NAME = "American High"
STATE = "CA"

LOCAL_ATHLETICS_URL = "https://fremontunified.org/american/students-community/activities-athletics/"
GOOGLE_ATHLETICS_URL = "https://sites.google.com/fusdk12.net/ahs-athletics/"
TARGET_URLS = [LOCAL_ATHLETICS_URL, GOOGLE_ATHLETICS_URL]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
PROXY_URL = (
    f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_SERVER}"
    if PROXY_USERNAME and PROXY_PASSWORD
    else None
)

FOOTBALL_TERMS = (
    "football",
    "varsity",
    "jv",
    "freshman",
    "coach",
    "schedule",
    "camp",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _strip_html(html: str) -> str:
    html = re.sub(r"(?is)<script.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?</style>", " ", html)
    html = re.sub(r"<[^>]+>", " ", html)
    return _clean(html)


def _extract_keyword_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if "football" in lower or any(term in lower for term in ("varsity", "jv", "freshman")):
            lines.append(line)
    return _dedupe_keep_order(lines)[:40]


def _extract_links(html: str) -> list[str]:
    matches = re.findall(r'href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.I | re.S)
    kept: list[str] = []
    seen: set[str] = set()
    for href, raw_text in matches:
        text = _clean(re.sub(r"<[^>]+>", " ", raw_text))
        combo = f"{text} {href}".lower()
        if not any(term in combo for term in FOOTBALL_TERMS):
            continue
        value = f"{text}|{href}"
        if value in seen:
            continue
        seen.add(value)
        kept.append(value)
    return _dedupe_keep_order(kept)


def _fetch_via_curl(url: str) -> str:
    if not PROXY_URL:
        raise RuntimeError("Oxylabs credentials not set")
    proc = subprocess.run(
        ["curl", "-L", "--max-time", "45", "-s", "--proxy", PROXY_URL, url],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout


async def scrape_school() -> dict[str, Any]:
    """Fetch the public athletics hub and extract football evidence only."""
    require_proxy_credentials()
    assert_not_blocklisted(TARGET_URLS)

    errors: list[str] = []
    source_pages: list[str] = []
    page_texts: list[str] = []

    try:
        for url in TARGET_URLS:
            html = _fetch_via_curl(url)
            source_pages.append(url)
            page_texts.append(_strip_html(html))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"fetch_failed:{type(exc).__name__}:{exc}")

    football_lines: list[str] = []
    football_links: list[str] = []
    for text in page_texts:
        football_lines.extend(_extract_keyword_lines(text))
        football_links.extend(_extract_links(text))

    football_lines = _dedupe_keep_order(football_lines)
    football_links = _dedupe_keep_order(football_links)

    football_program_available = bool(football_lines or football_links)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_hub_url": LOCAL_ATHLETICS_URL,
        "google_athletics_url": GOOGLE_ATHLETICS_URL,
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "football_schedule_public": False,
        "football_schedule_note": "No public football schedule or camp page was exposed on the athletics hub.",
        "summary": (
            "American High's athletics hub publicly lists football and links to the Google Sites athletics page."
            if football_program_available
            else ""
        ),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_server": PROXY_SERVER,
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_texts),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
