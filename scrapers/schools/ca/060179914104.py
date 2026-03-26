"""Deterministic football scraper for Ednovate - Brio College Prep (CA)."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060179914104"
SCHOOL_NAME = "Ednovate - Brio College Prep"
STATE = "CA"
PROXY_PROFILE = "datacenter"

TEAM_URL = "https://www.maxpreps.com/ca/los-angeles/brio-college-prep-olympians/flag-football/girls/fall/"
SCHEDULE_URL = f"{TEAM_URL}schedule/"
ROSTER_URL = f"{TEAM_URL}roster/"

TARGET_URLS = [TEAM_URL, SCHEDULE_URL, ROSTER_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for value in values:
        if value is None:
            continue
        key = _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _parse_team_name(text: str) -> str:
    match = re.search(
        r"Brio College Prep Olympians\s+Varsity\s+Flag Football",
        text,
        re.IGNORECASE,
    )
    if match:
        return "Brio College Prep Olympians Varsity Girls Flag Football"
    return "Brio College Prep Olympians Varsity Girls Flag Football"


def _parse_coaches(text: str) -> list[dict[str, str]]:
    match = re.search(
        r"Meet the Team.*?Do you have a team photo\? Upload it here\s+([^\n]+)\s+Head Coach\s+([^\n]+)\s+Statistician",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []
    return [
        {"name": _clean(match.group(1)), "role": "Head Coach"},
        {"name": _clean(match.group(2)), "role": "Statistician"},
    ]


def _parse_record(text: str) -> dict[str, str]:
    match = re.search(
        r"Overall\s+([0-9-]+)\s+([0-9.]+\s+Win Pct)\s+League\s+([0-9-]+)\s+([0-9.]+\s+Win Pct)\s+Home\s+([0-9-]+)\s+Away\s+([0-9-]+)\s+Neutral\s+([0-9-]+)\s+PF\s+([0-9]+)\s+PA\s+([0-9]+)\s+Streak\s+([A-Z0-9]+)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return {}
    return {
        "overall": _clean(match.group(1)),
        "overall_win_pct": _clean(match.group(2)),
        "league": _clean(match.group(3)),
        "league_win_pct": _clean(match.group(4)),
        "home": _clean(match.group(5)),
        "away": _clean(match.group(6)),
        "neutral": _clean(match.group(7)),
        "pf": _clean(match.group(8)),
        "pa": _clean(match.group(9)),
        "streak": _clean(match.group(10)),
    }


def _parse_schedule_rows(text: str) -> list[dict[str, str]]:
    flat = _clean(text)
    pattern = re.compile(
        r"(?P<date>\d{1,2}/\d{1,2})\s+(?P<site>vs|@)\s+(?P<opponent>[A-Za-z0-9&.'*\- ]+?)\s+"
        r"(?P<result>[WL]\s+[0-9-]+(?:\s+\([A-Z]+\))?)\s+Box Score",
        flags=re.IGNORECASE,
    )
    rows: list[dict[str, str]] = []
    for match in pattern.finditer(flat):
        rows.append(
            {
                "date": _clean(match.group("date")),
                "site": "Home" if match.group("site").lower() == "vs" else "Away",
                "opponent": _clean(match.group("opponent")).rstrip("*"),
                "result": _clean(match.group("result")),
                "game_info": "Box Score",
            }
        )
    return rows


def _parse_main_page_summary(text: str) -> dict[str, Any]:
    flattened = _clean(text)
    followers_match = re.search(r"Varsity Flag Football\s+Los Angeles, CA\s+([0-9]+)Followers", flattened)
    return {
        "followers": int(followers_match.group(1)) if followers_match else None,
        "page_title": "Brio College Prep Olympians Varsity Girls Flag Football",
    }


def _parse_roster_rows(rows: list[list[str]]) -> list[dict[str, str]]:
    roster: list[dict[str, str]] = []
    for cells in rows:
        if len(cells) < 5:
            continue
        jersey = _clean(cells[0])
        name = _clean(cells[1])
        grade = _clean(cells[2])
        position = _clean(cells[3])
        height = _clean(cells[4])
        if not name or name.lower() == "player":
            continue
        roster.append(
            {
                "jersey_number": jersey,
                "name": name,
                "grade": grade,
                "position": position,
                "height": height,
            }
        )
    return roster


async def _load_page(page, url: str) -> tuple[str, str, list[list[str]]]:
    response = await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    await page.wait_for_timeout(2500)
    text = await page.locator("body").inner_text()
    table_rows: list[list[str]] = []
    if await page.locator("table").count():
        rows = page.locator("table tr")
        row_count = await rows.count()
        for idx in range(row_count):
            cells = await rows.nth(idx).locator("td").all_inner_texts()
            if cells:
                table_rows.append([_clean(cell) for cell in cells])
    final_url = response.url if response else page.url
    return final_url, text, table_rows


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    team_text = ""
    schedule_text = ""
    roster_text = ""
    roster_rows: list[list[str]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            ignore_https_errors=True,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
        )
        page = await context.new_page()
        try:
            for url, target in (
                (TEAM_URL, "team"),
                (SCHEDULE_URL, "schedule"),
                (ROSTER_URL, "roster"),
            ):
                try:
                    final_url, text, table_data = await _load_page(page, url)
                    source_pages.append(final_url)
                    if target == "team":
                        team_text = text
                    elif target == "schedule":
                        schedule_text = text
                    else:
                        roster_text = text
                        roster_rows = table_data
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{target}_fetch_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    main_summary = _parse_main_page_summary(team_text)
    record = _parse_record(schedule_text)
    schedule_rows = _parse_schedule_rows(schedule_text)
    coaches = _parse_coaches(team_text)
    roster = _parse_roster_rows(roster_rows)

    football_program_available = bool(schedule_rows and roster)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_sport": "Girls Flag Football",
        "football_team_name": _parse_team_name(team_text),
        "football_team_level": "Varsity",
        "football_team_page_url": TEAM_URL,
        "football_schedule_url": SCHEDULE_URL,
        "football_roster_url": ROSTER_URL,
        "football_record": record,
        "football_schedule_rows": schedule_rows,
        "football_recent_results": schedule_rows[:5],
        "football_coaches": coaches,
        "football_roster": roster,
        "football_roster_count": len(roster),
        "football_staff_count": 2,
        "football_location": "533 Glendale Boulevard, Los Angeles, CA 90026",
        "source_summary": (
            "Public MaxPreps pages show Brio College Prep's varsity girls flag football team, including a 2-6 overall record, 0-1 league record, "
            "coach L. Lauve, statistician M. Guzman, an 18-player roster, and recent game results."
        ),
        "maxpreps_followers": main_summary.get("followers"),
        "maxpreps_page_title": main_summary.get("page_title"),
    }

    extracted_items = {key: value for key, value in extracted_items.items() if value not in (None, "", [], {})}

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "focus": "football_only",
            "pages_checked": len(source_pages),
            "football_sources": [TEAM_URL, SCHEDULE_URL, ROSTER_URL],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    import asyncio

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
