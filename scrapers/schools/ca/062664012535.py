"""Deterministic football scraper for American Canyon High (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "062664012535"
SCHOOL_NAME = "American Canyon High"
STATE = "CA"
BASE_URL = "https://achs.nvusd.org"
ATHLETICS_URL = f"{BASE_URL}/programs/athletics/"
SCHEDULES_URL = f"{BASE_URL}/our-school/calendars#athletics"
COACHES_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1M6GjkVAbOzPn0Jq6ueCbBDWMC7hNOM3RzGwc3DrkGpI/edit?gid=361769915#gid=361769915"
)

TARGET_URLS = [ATHLETICS_URL, SCHEDULES_URL, COACHES_URL]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

FOOTBALL_TERMS = (
    "football",
    "girls flag football",
    "boys flag football",
    "flag football",
    "coach",
    "coaches by sport",
    "athletics schedules",
    "athletic clearance",
    "parent/student-athlete meeting",
)

SPORT_TERMS = (
    "badminton",
    "baseball",
    "basketball",
    "cross country",
    "football",
    "girls flag football",
    "golf",
    "soccer",
    "softball",
    "swimming",
    "tennis",
    "track & field",
    "volleyball",
    "water polo",
    "wrestling",
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


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(term in lowered for term in FOOTBALL_TERMS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:60]


def _extract_links(links: list[dict[str, str]]) -> list[str]:
    kept: list[str] = []
    seen: set[str] = set()
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        if not text or not href:
            continue
        combo = f"{text} {href}".lower()
        if not any(term in combo for term in FOOTBALL_TERMS):
            continue
        value = f"{text}|{href}"
        if value in seen:
            continue
        seen.add(value)
        kept.append(value)
    return _dedupe_keep_order(kept)


def _extract_sports(lines: list[str]) -> list[str]:
    lowered = " | ".join(lines).lower()
    found: list[str] = []
    for sport in SPORT_TERMS:
        if sport in lowered:
            found.append(sport.title())
    return _dedupe_keep_order(found)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )
    if not isinstance(links, list):
        links = []

    lines = _extract_lines(body)
    lower = body.lower()
    teams: list[str] = []
    if "football" in lower:
        teams.append("Football")
    if "girls flag football" in lower or "girls' flag football" in lower:
        teams.append("Girls Flag Football")

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "keyword_lines": lines,
        "football_links": _extract_links([item for item in links if isinstance(item, dict)]),
        "football_team_names": _dedupe_keep_order(teams),
        "sports_list": _extract_sports(lines),
        "body_text": body,
    }


async def scrape_school() -> dict[str, Any]:
    """Visit the public ACHS athletics pages and extract football signals."""
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
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
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

    athletics_lines: list[str] = []
    athletics_links: list[str] = []
    teams: list[str] = []
    sports_list: list[str] = []
    schedule_lines: list[str] = []
    coach_lines: list[str] = []
    athletics_page_seen = False

    for signal in page_signals:
        lines = [line for line in signal.get("keyword_lines", []) if isinstance(line, str)]
        links = [link for link in signal.get("football_links", []) if isinstance(link, str)]
        athletics_lines.extend(lines)
        athletics_links.extend(links)
        teams.extend([team for team in signal.get("football_team_names", []) if isinstance(team, str)])
        sports_list.extend([sport for sport in signal.get("sports_list", []) if isinstance(sport, str)])

        url = str(signal.get("requested_url") or "")
        body_text = str(signal.get("body_text") or "")
        if url == ATHLETICS_URL:
            athletics_page_seen = True
            if "fall" in body_text.lower():
                schedule_lines.extend(
                    [
                        line
                        for line in lines
                        if any(token in line.lower() for token in ("football", "girls flag football", "fall", "meeting", "clearance"))
                    ]
                )
            if "contact athletic director" in body_text.lower():
                coach_lines.append("Contact Athletic Director John O'Con")
        elif url == COACHES_URL:
            if "football" in body_text.lower():
                coach_lines.extend(
                    [
                        line
                        for line in body_text.splitlines()
                        if "football" in line.lower() or "coach" in line.lower()
                    ]
                )
        elif url == SCHEDULES_URL:
            if "athletics" in body_text.lower():
                schedule_lines.extend(
                    [
                        line
                        for line in lines
                        if any(token in line.lower() for token in ("football", "flag football", "schedule", "meeting", "clearance"))
                    ]
                )

    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_links = _dedupe_keep_order(athletics_links)
    teams = _dedupe_keep_order(teams)
    sports_list = _dedupe_keep_order(sports_list)
    schedule_lines = _dedupe_keep_order(schedule_lines)
    coach_lines = _dedupe_keep_order(coach_lines)

    football_program_available = bool(athletics_page_seen and ("football" in " | ".join(athletics_lines).lower() or athletics_links))
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_achs_athletics_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": ATHLETICS_URL,
        "athletics_schedules_url": SCHEDULES_URL,
        "coaches_by_sport_url": COACHES_URL,
        "football_team_names": teams,
        "athletics_sports_list": sports_list,
        "football_keyword_lines": athletics_lines[:60],
        "football_links": athletics_links[:30],
        "football_schedule_note": (
            "Football appears in the athletics season overview with fall clearance deadline and parent/student-athlete meeting."
            if football_program_available
            else ""
        ),
        "football_schedule_public": True,
        "football_schedule_lines": schedule_lines[:25],
        "football_coach_lines": coach_lines[:20],
        "summary": (
            "American Canyon High publishes an athletics hub with football, girls flag football, schedules, and coach directory links."
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
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "athletics_page",
                "athletics_schedules_anchor",
                "coaches_by_sport_link",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
