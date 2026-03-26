"""Deterministic football scraper for El Camino High (CA)."""

from __future__ import annotations

import asyncio
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062825004360"
SCHOOL_NAME = "El Camino High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://echs.oside.us/"
ATHLETICS_URL = "https://echs.oside.us/athletics"
COACH_DOC_URL = (
    "https://docs.google.com/document/d/1AtuvvdiEqozbOYKVxMcyZ_aOESOeF6lX2HGsxD6ffIU/export?format=txt"
)
MAXPREPS_SCHEDULE_URL = "https://www.maxpreps.com/ca/oceanside/el-camino-wildcats/football/schedule/"
MAXPREPS_STAFF_URL = "https://www.maxpreps.com/ca/oceanside/el-camino-wildcats/football/staff/"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    COACH_DOC_URL,
    MAXPREPS_SCHEDULE_URL,
    MAXPREPS_STAFF_URL,
]

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


def _preview_lines(text: str, limit: int = 80) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines[:limit]


def _collect_keyword_lines(text: str, keywords: tuple[str, ...], limit: int = 30) -> list[str]:
    matches: list[str] = []
    for line in _preview_lines(text, limit=200):
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _lines_between(lines: list[str], start_marker: str, end_markers: tuple[str, ...]) -> list[str]:
    start_idx = -1
    for idx, line in enumerate(lines):
        if line.lower() == start_marker.lower():
            start_idx = idx
            break
    if start_idx < 0:
        return []

    block: list[str] = []
    for line in lines[start_idx + 1 :]:
        if line in end_markers or any(line.lower() == marker.lower() for marker in end_markers):
            break
        block.append(line)
    return block


def _extract_football_coach(doc_text: str) -> dict[str, str]:
    lines = _preview_lines(doc_text, limit=200)
    for idx, line in enumerate(lines):
        if line.lower() != "football":
            continue

        coach_name = ""
        coach_email = ""
        for candidate in lines[idx + 1 :]:
            if not coach_name and candidate and "@" not in candidate:
                coach_name = candidate
                continue
            if "@" in candidate:
                coach_email = candidate
                break
        if coach_name:
            return {
                "name": coach_name,
                "role": "Head Coach",
                "email": coach_email,
                "source": "official_coach_doc",
            }
    return {}


def _extract_maxpreps_head_coach(page_text: str) -> dict[str, str]:
    lines = _preview_lines(page_text, limit=200)
    for idx, line in enumerate(lines):
        if line.lower() != "staff position":
            continue
        # The staff table is simple enough that the first Head Coach entry is stable.
        for j in range(idx + 1, len(lines)):
            if lines[j].lower() == "head coach" and j > 0:
                name = lines[j - 1]
                return {
                    "name": name,
                    "role": "Head Coach",
                    "source": "maxpreps_staff_page",
                }
    return {}


def _parse_schedule_rows(page_text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    text = _clean(page_text)
    pattern = re.compile(
        r"(?P<date>\d+/\d+)\s+(?P<venue>vs|@)\s+"
        r"(?P<opponent>[A-Za-z0-9 .,'&()-]+?)\s+"
        r"(?P<result>[WLT]\s+\d+-\d+(?:\s+\([A-Z]+\))?)\s+(?:Box Score|Watch Replay|$)",
    )
    for match in pattern.finditer(text):
        opponent = _clean(match.group("opponent")).rstrip("*").strip()
        rows.append(
            {
                "date": match.group("date"),
                "opponent": opponent,
                "match_type": match.group("venue"),
                "is_home_game": "true" if match.group("venue") == "vs" else "false",
                "result": _clean(match.group("result")),
            }
        )
    return rows


def _parse_schedule_summary(page_text: str) -> dict[str, str]:
    text = _clean(page_text)
    overall = re.search(r"Overall\s+([0-9-]+)\s+([0-9.]+)\s+Win Pct", text)
    league = re.search(r"League\s+([0-9-]+)\s+([^)]+?)\s+Avocado - East", text)
    home_away = re.search(r"Home\s+([0-9-]+)\s+Away\s+([0-9-]+)\s+Neutral\s+([0-9-]+)", text)
    record = re.search(r"PF\s+([0-9-]+)\s+PA\s+([0-9-]+)\s+Streak\s+([A-Z0-9-]+)", text)

    return {
        "overall_record": overall.group(1) if overall else "",
        "overall_win_pct": overall.group(2) if overall else "",
        "league_record": league.group(1) if league else "",
        "league_place": league.group(2).strip() if league else "",
        "home_record": home_away.group(1) if home_away else "",
        "away_record": home_away.group(2) if home_away else "",
        "neutral_record": home_away.group(3) if home_away else "",
        "points_for": record.group(1) if record else "",
        "points_against": record.group(2) if record else "",
        "streak": record.group(3) if record else "",
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape El Camino High football signals from the public school and MaxPreps pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    home_text = ""
    athletics_text = ""
    coach_doc_text = ""
    staff_text = ""
    schedule_text = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in [HOME_URL, ATHLETICS_URL, COACH_DOC_URL, MAXPREPS_STAFF_URL, MAXPREPS_SCHEDULE_URL]:
                try:
                    wait_until = "commit" if url == COACH_DOC_URL else "domcontentloaded"
                    await page.goto(url, wait_until=wait_until, timeout=90000)
                    await page.wait_for_timeout(1500)
                    source_pages.append(page.url)
                    body_text = await page.locator("body").inner_text(timeout=15000)
                    if url == HOME_URL:
                        home_text = body_text
                    elif url == ATHLETICS_URL:
                        athletics_text = body_text
                    elif url == COACH_DOC_URL:
                        coach_doc_text = body_text
                    elif url == MAXPREPS_STAFF_URL:
                        staff_text = body_text
                    elif url == MAXPREPS_SCHEDULE_URL:
                        schedule_text = body_text
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_lines = _collect_keyword_lines(
        home_text,
        keywords=("football", "coach", "athletics", "wildcat", "schedule", "roster"),
    )
    athletics_lines = _collect_keyword_lines(
        athletics_text,
        keywords=("football", "fall sports", "coach", "schedule", "athletic", "physical"),
    )
    coach_doc_lines = _collect_keyword_lines(
        coach_doc_text,
        keywords=("football", "head coach", "stefan", "mcclure"),
    )
    staff_lines = _collect_keyword_lines(
        staff_text,
        keywords=("football", "coach", "head coach", "varsity"),
    )
    schedule_lines = _collect_keyword_lines(
        schedule_text,
        keywords=("football", "win", "loss", "box score", "schedule", "o", "vs", "@"),
    )

    fall_sports = _lines_between(
        _preview_lines(athletics_text, limit=200),
        "Fall Sports",
        ("Winter Sports", "Spring Sports", "Athletic Resources"),
    )
    football_program_available = "Football" in fall_sports or bool(schedule_lines or coach_doc_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    official_coach = _extract_football_coach(coach_doc_text)
    maxpreps_head_coach = _extract_maxpreps_head_coach(staff_text)
    schedule_rows = _parse_schedule_rows(schedule_text)
    schedule_summary = _parse_schedule_summary(schedule_text)

    football_signals = _dedupe_keep_order(
        [
            *home_lines,
            *athletics_lines,
            *coach_doc_lines,
            *staff_lines,
            *schedule_lines,
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_profile": {
            "name": SCHOOL_NAME,
            "website": HOME_URL,
            "athletics_url": ATHLETICS_URL,
            "city": "Oceanside",
            "state": STATE,
        },
        "fall_sports": fall_sports,
        "football_head_coach": official_coach or maxpreps_head_coach,
        "football_head_coach_current_source": official_coach.get("source") or maxpreps_head_coach.get("source", ""),
        "maxpreps_head_coach": maxpreps_head_coach,
        "official_coach_doc_url": COACH_DOC_URL,
        "athletics_page_url": ATHLETICS_URL,
        "home_page_url": HOME_URL,
        "maxpreps_staff_url": MAXPREPS_STAFF_URL,
        "maxpreps_schedule_url": MAXPREPS_SCHEDULE_URL,
        "schedule_summary": schedule_summary,
        "schedule_rows": schedule_rows,
        "football_signals": football_signals,
        "summary": (
            "El Camino High publishes football on its official athletics page, lists Stefan McClure on the 2025-26 coach document, and has a public 2025-26 MaxPreps varsity schedule."
            if football_program_available
            else ""
        ),
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
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
            "pages_checked": len(source_pages),
            "focus": "football_only",
            "manual_navigation_steps": [
                "official_home_page",
                "official_athletics_page",
                "official_coach_document",
                "maxpreps_staff_page",
                "maxpreps_schedule_page",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
