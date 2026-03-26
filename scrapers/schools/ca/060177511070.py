"""Deterministic football scraper for Futures High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060177511070"
SCHOOL_NAME = "Futures High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.fhscharter.org/"
ATHLETICS_YEAR_URL = (
    "https://www.fhscharter.org/Student-Services/Athletics/FHS-Athletics-Year-Around/index.html"
)
SPORTS_CALENDAR_URL = "https://www.fhscharter.org/Student-Services/Athletics/Sports-Calendar/"
FLAG_FOOTBALL_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/1bA5_al__uBnArNlqSzn5w6drFC6j676apzyuS6OOW38/edit?usp=sharing"
)

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_YEAR_URL,
    SPORTS_CALENDAR_URL,
    FLAG_FOOTBALL_SHEET_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_TERMS = (
    "football",
    "flag football",
    "athletics",
    "coach",
    "calendar",
    "schedule",
    "practice",
    "scrimmage",
    "contest",
    "varsity",
    "game",
)

SHEET_ROW_RE = re.compile(
    r"^(?P<date>[A-Za-z]{3}-\d{1,2}/\d{1,2})\s+"
    r"(?P<opponent>.+?)\s+"
    r"(?P<level>Varsity|JV|Junior Varsity|Freshman|Frosh)\s+"
    r"(?P<location>.+?)\s+"
    r"(?P<time>\d{1,2}:\d{2}(?:\s?[AP]M)?)$",
    re.IGNORECASE,
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


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


def _extract_lines(text: str, *, limit: int = 60) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(term in lowered for term in FOOTBALL_TERMS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_links(links: list[dict[str, Any]]) -> list[str]:
    values: list[str] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if not any(term in blob for term in FOOTBALL_TERMS):
            continue
        values.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(values)


def _extract_sheet_entries(text: str) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        match = SHEET_ROW_RE.match(line)
        if not match:
            continue
        entries.append(
            {
                "date": _clean(match.group("date")),
                "opponent": _clean(match.group("opponent")),
                "level": _clean(match.group("level")),
                "location": _clean(match.group("location")),
                "time": _clean(match.group("time")),
                "raw": line,
            }
        )
    return entries


def _extract_calendar_notes(text: str) -> list[str]:
    notes: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        lowered = line.lower()
        if not line:
            continue
        if any(
            phrase in lowered
            for phrase in (
                "practice starts",
                "first scrimmage",
                "first contest",
                "sit out period",
                "dead period",
            )
        ):
            notes.append(line)
    return _dedupe_keep_order(notes)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    try:
        body_text = await page.locator("body").inner_text()
    except Exception:  # noqa: BLE001
        body_text = ""

    try:
        links = await page.eval_on_selector_all(
            "a[href]",
            """els => els.map(e => ({
                text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: e.href || ''
            }))""",
        )
    except Exception:  # noqa: BLE001
        links = []

    if not isinstance(links, list):
        links = []

    normalized = _clean(body_text)
    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": normalized,
        "football_lines": _extract_lines(normalized),
        "football_links": _extract_links([item for item in links if isinstance(item, dict)]),
        "schedule_entries": _extract_sheet_entries(normalized),
        "calendar_notes": _extract_calendar_notes(normalized),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit the athletics pages and extract public football evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy_meta = get_proxy_runtime_meta(PROXY_PROFILE)
    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_500)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_lines: list[str] = []
    football_links: list[str] = []
    schedule_entries: list[dict[str, str]] = []
    calendar_notes: list[str] = []

    for signal in page_signals:
        football_lines.extend(signal.get("football_lines", []))
        football_links.extend(signal.get("football_links", []))
        schedule_entries.extend(signal.get("schedule_entries", []))
        calendar_notes.extend(signal.get("calendar_notes", []))

    football_lines = _dedupe_keep_order(football_lines)
    football_links = _dedupe_keep_order(football_links)
    calendar_notes = _dedupe_keep_order(calendar_notes)

    football_program_available = bool(football_lines or football_links or schedule_entries)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_team_names = _dedupe_keep_order(
        [
            "Girls Flag Football"
            if any("girl flag football" in line.lower() or "girls flag football" in line.lower() for line in football_lines)
            else "",
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_names": football_team_names,
        "football_hub_url": ATHLETICS_YEAR_URL,
        "football_calendar_url": SPORTS_CALENDAR_URL,
        "football_schedule_url": FLAG_FOOTBALL_SHEET_URL,
        "football_schedule_public": bool(schedule_entries),
        "football_schedule_entries": schedule_entries,
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "football_calendar_notes": calendar_notes,
        "summary": (
            "Futures High publishes a live athletics calendar with Girl Flag Football and links a public Google Sheet containing the flag football schedule."
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
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
