"""Deterministic football scraper for Fontana High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.schools.runtime import (  # noqa: E402
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061392001589"
SCHOOL_NAME = "Fontana High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://fohi.fusd.net/"
ATHLETICS_URL = "https://fohi.fusd.net/athletics"
SPORTS_OFFERED_URL = "https://fohi.fusd.net/athletics/sports-offered"
FALL_SPORTS_URL = "https://fohi.fusd.net/athletics/sports-offered/fall-sports"
FOOTBALL_URL = "https://fohi.fusd.net/athletics/sports-offered/fall-sports/football"
UPCOMING_GAME_CALENDAR_URL = "https://fohi.fusd.net/athletics/upcoming-game-calendar"
LEAGUE_STANDINGS_URL = "https://fohi.fusd.net/athletics/league-standings"
FACILITIES_URL = "https://fohi.fusd.net/athletics/facilities"

MANUAL_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    SPORTS_OFFERED_URL,
    FALL_SPORTS_URL,
    FOOTBALL_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        key = repr(value) if isinstance(value, dict) else _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _extract_lines(text: str, *, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_email_from_links(links: list[dict[str, Any]]) -> str:
    for item in links:
        href = _clean(str(item.get("href") or ""))
        text = _clean(str(item.get("text") or ""))
        if href.lower().startswith("mailto:"):
            return href.split(":", 1)[1]
        if "@" in text and "." in text:
            return text
    return ""


def _extract_relevant_links(links: list[dict[str, Any]]) -> list[dict[str, str]]:
    related: list[dict[str, str]] = []
    for item in links:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        if any(
            term in f"{text} {href}".lower()
            for term in (
                "football",
                "athletic",
                "coach",
                "calendar",
                "league",
                "schedule",
                "cif",
                "field",
                "contact",
                "physical",
            )
        ):
            related.append({"text": text, "href": href})
    return _dedupe_keep_order(related)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    main = page.locator("main#fsPageContent")
    body_text = _clean(await main.inner_text())
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    coach_name = ""
    assistant_coaches: list[str] = []
    coach_heading = main.locator("h3:has-text('Head Coach')")
    if await coach_heading.count():
        coach_name = _clean(await coach_heading.first.inner_text()).replace(" - Head Coach", "").strip()

    assistant_list = main.locator(".fsElementContent > ul > li")
    if await assistant_list.count():
        for index in range(await assistant_list.count()):
            assistant_coaches.append(_clean(await assistant_list.nth(index).inner_text()))
    assistant_coaches = _dedupe_keep_order(assistant_coaches)

    if not coach_name:
        match = re.search(r"([A-Z][A-Za-z'\-.]+(?:\s+[A-Z][A-Za-z'\-.]+)+?)\s*-\s*Head Coach", body_text)
        if match:
            coach_name = _clean(match.group(1))

    if not assistant_coaches:
        match = re.search(
            r"Assistant Coaches:\s*(.+?)\s*(?:CIF-SS Champions|League Champions|Retired Numbers|CIF-SS Runners Up)",
            body_text,
        )
        if match:
            assistant_text = _clean(match.group(1))
            assistant_coaches = _dedupe_keep_order(
                [
                    _clean(item)
                    for item in re.split(
                        r"\s{2,}|\s(?=[A-Z][a-z]+(?:\s+(?:[A-Z][a-z]+|de|del|da|di|van|von|la|le|y))+)",
                        assistant_text,
                    )
                    if _clean(item)
                ]
            )

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "links": _extract_relevant_links(links),
        "football_lines": _extract_lines(
            body_text,
            keywords=("football", "head coach", "assistant coaches", "skyline league", "cif-ss", "league champions"),
        ),
        "head_coach_name": coach_name,
        "assistant_coaches": assistant_coaches,
        "coach_email": _extract_email_from_links(links),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Fontana High's public football page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(
        [
            HOME_URL,
            ATHLETICS_URL,
            SPORTS_OFFERED_URL,
            FALL_SPORTS_URL,
            FOOTBALL_URL,
            UPCOMING_GAME_CALENDAR_URL,
            LEAGUE_STANDINGS_URL,
            FACILITIES_URL,
        ],
        profile=PROXY_PROFILE,
    )

    source_pages: list[str] = []
    errors: list[str] = []
    page_data: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(user_agent=USER_AGENT, locale="en-US")
        page = await context.new_page()

        try:
            for url in MANUAL_URLS:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)

            await page.goto(FOOTBALL_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            page_data = await _collect_page(page, FOOTBALL_URL)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(page_data),
        "football_page_url": FOOTBALL_URL,
        "athletics_page_url": ATHLETICS_URL,
        "sports_offered_page_url": SPORTS_OFFERED_URL,
        "fall_sports_page_url": FALL_SPORTS_URL,
        "upcoming_game_calendar_url": UPCOMING_GAME_CALENDAR_URL,
        "league_standings_url": LEAGUE_STANDINGS_URL,
        "facilities_url": FACILITIES_URL,
        "football_team_name": "Football (V - JV - F)",
        "football_league": "Fall - Skyline League",
        "head_coach_name": page_data.get("head_coach_name", ""),
        "coach_email": page_data.get("coach_email", ""),
        "assistant_coaches": page_data.get("assistant_coaches", []),
        "football_lines": page_data.get("football_lines", []),
        "football_related_links": page_data.get("links", []),
    }

    if not extracted_items["head_coach_name"] and not extracted_items["assistant_coaches"]:
        errors.append("no_public_football_content_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "home",
                "athletics",
                "sports_offered",
                "fall_sports",
                "football",
            ],
        },
        "errors": errors,
    }


async def main() -> None:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
