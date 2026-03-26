"""Deterministic football scraper for Dana Hills High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    get_proxy_server_list,
    require_proxy_credentials,
)

NCES_ID = "060744000689"
SCHOOL_NAME = "Dana Hills High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ACTIVITIES_HOME_URL = "https://danahills.capousd.org/Activities/Activities-Home/index.html"
STAFF_DIRECTORY_URL = "https://danahills.capousd.org/Staff-Directory/"
SCHOOL_PROFILE_URL = "https://danahills.capousd.org/documents/School-Profile.pdf"

TARGET_URLS = [
    ACTIVITIES_HOME_URL,
    STAFF_DIRECTORY_URL,
    SCHOOL_PROFILE_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line or line in lines:
            continue
        lines.append(line)
    return lines


def _is_blocked(title: str, text: str) -> bool:
    normalized = f"{title}\n{text}".lower()
    return any(
        token in normalized
        for token in (
            "403",
            "forbidden",
            "cloudflare",
            "attention required",
            "access denied",
            "blocked",
        )
    )


def _parse_football_events(lines: list[str]) -> list[dict[str, str]]:
    pattern = re.compile(
        r"^Football\s+(?P<kind>vs\.|@)\s+(?P<opponent>.+?)\s+"
        r"(?P<date>[A-Z][a-z]{2}\.?\s*\d{1,2},\s+\d{4})$"
    )
    events: list[dict[str, str]] = []
    for line in lines:
        if "football" not in line.lower():
            continue
        match = pattern.match(line)
        if match:
            event_kind = "home" if match.group("kind") == "vs." else "away"
            events.append(
                {
                    "event_type": "football_game",
                    "location": event_kind,
                    "opponent": _clean(match.group("opponent")),
                    "date": _clean(match.group("date")),
                    "raw_line": line,
                }
            )
        else:
            events.append(
                {
                    "event_type": "football_related",
                    "raw_line": line,
                }
            )
    return events


async def _snapshot(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=20000)
    title = _clean(await page.title())
    lines = _extract_lines(body_text)
    return {
        "url": page.url,
        "title": title,
        "text": body_text,
        "lines": lines,
        "football_lines": [line for line in lines if "football" in line.lower()],
        "blocked": _is_blocked(title, body_text),
    }


async def _load_with_proxy(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1500)
    return await _snapshot(page)


async def _scrape_with_proxy_index(proxy_index: int) -> dict[str, Any]:
    source_pages: list[str] = []
    errors: list[str] = []
    snapshots: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE, proxy_index=proxy_index),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            activities_snapshot = await _load_with_proxy(page, ACTIVITIES_HOME_URL)
            snapshots.append(activities_snapshot)
            source_pages.append(activities_snapshot["url"])

            football_events = _parse_football_events(activities_snapshot["lines"])
            football_lines = activities_snapshot["football_lines"]
            if not football_events:
                errors.append("football_events_not_found_on_activities_calendar")

            athletics_summary = _dedupe_keep_order(
                [
                    line
                    for line in activities_snapshot["lines"]
                    if "dana hills high school fields 25 cif sports" in line.lower()
                    or "fall sports" in line.lower()
                    or "football" in line.lower()
                    or "south coast" in line.lower()
                    or "sea view leagues" in line.lower()
                ]
            )

            if not football_lines:
                errors.append("football_lines_not_found_on_activities_page")

            extracted_items = {
                "activities_home_url": ACTIVITIES_HOME_URL,
                "football_events": football_events,
                "football_lines": football_lines,
                "athletics_summary_lines": athletics_summary,
                "page_titles": [activities_snapshot["title"]],
            }

            payload = {
                "nces_id": NCES_ID,
                "school_name": SCHOOL_NAME,
                "state": STATE,
                "source_pages": _dedupe_keep_order(source_pages),
                "extracted_items": extracted_items,
                "scrape_meta": {
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "proxy_profile": PROXY_PROFILE,
                    "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
                    "proxy_index": proxy_index,
                    "navigation_steps": [
                        "goto_activities_home",
                        "extract_calendar_lines",
                    ],
                    "snapshot_count": len(snapshots),
                },
                "errors": errors,
            }
            return payload
        finally:
            await browser.close()


async def scrape_school() -> dict[str, Any]:
    """Scrape Dana Hills High's public football evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy_servers = get_proxy_server_list(profile=PROXY_PROFILE)
    last_error: str | None = None

    for proxy_index in range(max(1, len(proxy_servers))):
        try:
            payload = await _scrape_with_proxy_index(proxy_index)
        except Exception as exc:  # noqa: BLE001
            last_error = f"{type(exc).__name__}:{exc}"
            continue

        if payload["errors"] and not payload["extracted_items"].get("football_events"):
            last_error = "; ".join(payload["errors"])
            continue

        if payload["extracted_items"].get("football_events"):
            return payload

        last_error = "; ".join(payload["errors"]) or "no_football_content_found"

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": [],
        "extracted_items": {
            "activities_home_url": ACTIVITIES_HOME_URL,
            "football_events": [],
            "football_lines": [],
            "athletics_summary_lines": [],
        },
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": PROXY_PROFILE,
            "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "proxy_servers": proxy_servers,
            "proxy_retry_error": last_error or "unknown_error",
        },
        "errors": [last_error] if last_error else ["unknown_error"],
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
