"""Deterministic football scraper for Eastlake High (CA)."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "063864001223"
SCHOOL_NAME = "Eastlake High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

PROGRAM_URL = "https://www.eastlakeathletics.com/sports/boys-football"
SCHEDULE_URL = (
    "https://www.eastlakeathletics.com/sports/boys-football/schedule"
    "?team=boys-football-5199524&year=2025-2026"
)

TARGET_URLS = [PROGRAM_URL, SCHEDULE_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


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


def _absolute_url(href: str, base_url: str) -> str:
    clean = _clean(href)
    if not clean:
        return ""
    return urljoin(base_url, clean)


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if line and line not in lines:
            lines.append(line)
    return lines


def _parse_event_card(text: str, watch_urls: list[str]) -> dict[str, Any] | None:
    lines = _extract_lines(text)
    if len(lines) < 7:
        return None

    date_label = lines[1]
    time_label = lines[2]
    location_label = lines[3]
    sport_label = lines[4]
    team_label = lines[5]
    matchup_label = lines[6]

    venue_label = ""
    if len(lines) >= 8 and not lines[7].startswith("WATCH"):
        venue_label = lines[7]

    matchup_match = re.match(r"^(vs|at)\s+(.+)$", matchup_label, flags=re.I)
    opponent = _clean(matchup_match.group(2)) if matchup_match else matchup_label
    game_side = "home" if matchup_label.lower().startswith("vs ") else "away"

    return {
        "date_label": date_label,
        "time_label": time_label,
        "location": location_label,
        "sport": sport_label,
        "team_level": team_label,
        "opponent": opponent,
        "game_side": game_side,
        "venue": venue_label,
        "has_stream": bool(watch_urls),
        "watch_urls": _dedupe_keep_order(watch_urls),
        "raw_text": _clean(text),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Eastlake High's public football schedule from the athletics site."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    errors: list[str] = []
    games: list[dict[str, Any]] = []
    schedule_title = ""
    season_label = ""
    previous_year_url = ""
    next_year_url = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            locale="en-US",
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()
        try:
            await page.goto(SCHEDULE_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(2000)
            source_pages.extend([PROGRAM_URL, page.url])
            schedule_title = _clean(await page.title())

            season_heading = page.locator("div.text-center.font-heading.text-xl.capitalize.md\\:text-4xl")
            if await season_heading.count():
                season_label = _clean(await season_heading.first.inner_text())

            previous_link = page.locator("a:has-text('Previous Year')")
            if await previous_link.count():
                previous_year_url = _absolute_url(
                    await previous_link.first.get_attribute("href") or "",
                    page.url,
                )

            next_link = page.locator("a:has-text('Next Year')")
            if await next_link.count():
                next_year_url = _absolute_url(
                    await next_link.first.get_attribute("href") or "",
                    page.url,
                )

            event_cards = page.locator("div.container.flex.flex-col.gap-4 > div.w-full.font-body")
            card_count = await event_cards.count()
            for index in range(card_count):
                card = event_cards.nth(index)
                card_text = await card.inner_text(timeout=10000)
                watch_urls = await card.locator("a[href*='nfhsnetwork.com']").evaluate_all(
                    "els => els.map(el => el.href || '').filter(Boolean)",
                )
                parsed = _parse_event_card(card_text, [str(url) for url in watch_urls if isinstance(url, str)])
                if parsed:
                    games.append(parsed)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"schedule_page_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    if not games:
        errors.append("no_public_football_games_found")

    extracted_items: dict[str, Any] = {
        "program_url": PROGRAM_URL,
        "schedule_url": SCHEDULE_URL,
        "schedule_title": schedule_title,
        "season_label": season_label,
        "previous_year_url": previous_year_url,
        "next_year_url": next_year_url,
        "games": games,
        "game_count": len(games),
        "home_game_count": sum(1 for game in games if game.get("game_side") == "home"),
        "away_game_count": sum(1 for game in games if game.get("game_side") == "away"),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": PROXY_PROFILE,
            "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "navigation_steps": [
                "goto_current_schedule_page",
                "read_season_heading",
                "extract_event_cards",
            ],
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
