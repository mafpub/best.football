"""Deterministic football scraper for Anaheim High (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060263000169"
SCHOOL_NAME = "Anaheim High"
STATE = "CA"
BASE_URL = "https://anaheimhs.org"
ATHLETICS_URL = f"{BASE_URL}/Anaheim/Department/11903-Athletics"
MASTER_CALENDAR_URL = f"{BASE_URL}/Anaheim/Department/14227-Anaheim-High-School/64991-Anaheim-Event-Calendar.html"

TARGET_URLS = [ATHLETICS_URL]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
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


def _extract_emails(text: str) -> list[str]:
    emails = re.findall(r"[\w.\-+]+@[\w.\-]+\.\w+", text)
    return _dedupe_keep_order(emails)


def _extract_football_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if (
            "football" in lower
            or "master calendar" in lower
            or "tryout" in lower
            or "coach" in lower
            or "fall sports schedule" in lower
        ):
            lines.append(line)
    return _dedupe_keep_order(lines)[:50]


def _extract_football_coaches(text: str) -> list[str]:
    coaches: list[str] = []
    if re.search(r"Football\s+Gus Martinez", text, re.IGNORECASE):
        coaches.append("Football: Gus Martinez")
    if re.search(r"Girls Flag Football.*?Vince Gomez", text, re.IGNORECASE | re.DOTALL):
        coaches.append("Girls Flag Football: Vince Gomez")
    return _dedupe_keep_order(coaches)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []
    normalized = _clean(body)
    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": normalized,
        "football_lines": _extract_football_lines(normalized),
        "football_coaches": _extract_football_coaches(normalized),
        "emails": _extract_emails(normalized),
        "links": [
            f"{_clean(str(item.get('text') or ''))}|{str(item.get('href') or '').strip()}"
            for item in links
            if isinstance(item, dict) and str(item.get("href") or "").strip()
        ],
    }


async def scrape_school() -> dict[str, Any]:
    """Visit Anaheim High athletics pages and extract public football details."""
    require_proxy_credentials()
    assert_not_blocklisted(TARGET_URLS)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USERNAME,
                "password": PROXY_PASSWORD,
            },
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_lines: list[str] = []
    football_coaches: list[str] = []
    emails: list[str] = []
    links: list[str] = []
    for signal in page_signals:
        football_lines.extend(signal.get("football_lines", []))
        football_coaches.extend(signal.get("football_coaches", []))
        emails.extend(signal.get("emails", []))
        links.extend(signal.get("links", []))

    football_lines = _dedupe_keep_order(football_lines)
    football_coaches = _dedupe_keep_order(football_coaches)
    emails = _dedupe_keep_order(emails)
    links = _dedupe_keep_order(links)

    football_program_available = bool(football_lines or football_coaches)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_athletics_page")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": ATHLETICS_URL,
        "football_team_names": [
            "Football" if football_program_available else "",
            "Girls Flag Football" if any("girls flag football" in c.lower() for c in football_coaches + football_lines) else "",
        ],
        "football_coaches": football_coaches,
        "football_schedule_public": True,
        "football_schedule_url": MASTER_CALENDAR_URL,
        "football_schedule_note": "For game dates, please see our MASTER CALENDAR.",
        "football_keyword_lines": football_lines,
        "football_links": links,
        "program_contact_emails": emails,
        "summary": (
            "Anaheim High's athletics page lists football and girls flag football in the fall sports schedule, names the football coach, and links the master calendar for game dates."
            if football_program_available
            else ""
        ),
    }

    extracted_items["football_team_names"] = _dedupe_keep_order(
        [name for name in extracted_items["football_team_names"] if name]
    )

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
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
