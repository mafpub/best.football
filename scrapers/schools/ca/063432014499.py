"""Deterministic football scraper for Logan Memorial Educational Campus (CA)."""

from __future__ import annotations

import asyncio
import json
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

NCES_ID = "063432014499"
SCHOOL_NAME = "Logan Memorial Educational Campus"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_HOME_URL = "https://lmec.sandiegounified.org/"
SCHOOL_ATHLETICS_URL = "https://lmec.sandiegounified.org/school_site_council_ssc/Athletics"
ATHLETICS_HOME_URL = "https://www.lmeceagles.com/"
FOOTBALL_TEAM_URL = "https://www.lmeceagles.com/varsity/flag-football-girls/"
FOOTBALL_SCHEDULE_URL = "https://www.lmeceagles.com/varsity/flag-football-girls/schedule-results"
FOOTBALL_ROSTER_URL = "https://www.lmeceagles.com/varsity/flag-football-girls/roster"
FOOTBALL_COACHES_URL = "https://www.lmeceagles.com/varsity/flag-football-girls/coaches"

TARGET_URLS = [
    SCHOOL_HOME_URL,
    SCHOOL_ATHLETICS_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_TEAM_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_COACHES_URL,
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
    ordered: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _normalize_href(href: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    return value


async def _collect_team_page(page) -> dict[str, Any]:
    await page.goto(FOOTBALL_TEAM_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1500)

    links = await page.locator("a[href]").evaluate_all(
        """els => els.map((a) => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || '',
        }))"""
    )
    recent_results = await page.locator("li.schedule-and-results-list-item").evaluate_all(
        """els => els.slice(0, 5).map((el) => ({
            raw_text: (el.innerText || '').replace(/\\s+/g, ' ').trim(),
        }))"""
    )

    football_links: list[dict[str, str]] = []
    if isinstance(links, list):
        for item in links:
            if not isinstance(item, dict):
                continue
            href = _normalize_href(str(item.get("href") or ""))
            text = _clean(str(item.get("text") or ""))
            if not href:
                continue
            if "flag-football-girls" not in href and "lmecgirlsflagfootball" not in href:
                continue
            football_links.append({"text": text, "href": href})

    return {
        "football_team_url": page.url,
        "football_team_title": _clean(await page.title()),
        "football_team_links": football_links,
        "football_recent_results": recent_results if isinstance(recent_results, list) else [],
    }


async def _collect_schedule(page) -> tuple[dict[str, Any], list[dict[str, str]]]:
    await page.goto(FOOTBALL_SCHEDULE_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector("#load_schedule-results-team .schedule-results ul > li", timeout=60000)
    await page.wait_for_timeout(1500)

    record_text = ""
    record_node = page.locator("#load_schedule-results-team .schedule-results h3.record")
    if await record_node.count():
        record_text = _clean(await record_node.inner_text())

    seasons: list[str] = []
    season_options = page.locator("select option")
    if await season_options.count():
        raw_options = await season_options.evaluate_all(
            "els => els.map((opt) => (opt.textContent || '').replace(/\\s+/g, ' ').trim())"
        )
        if isinstance(raw_options, list):
            seasons = [
                _clean(str(value))
                for value in raw_options
                if _clean(str(value)).startswith("20")
            ]

    rows = await page.locator(
        "#load_schedule-results-team .schedule-results ul > li.schedule-and-results-list-item"
    ).evaluate_all(
        """els => els.map((el) => {
            const schoolLines = Array.from(el.querySelectorAll('.school > p'))
                .map((node) => (node.textContent || '').replace(/\\s+/g, ' ').trim())
                .filter(Boolean);
            return {
                event_id: el.className.match(/event-id-(\\d+)/)?.[1] || '',
                sport: (el.querySelector('.sport')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                date: (el.querySelector('.date')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                home_away: (el.querySelector('.vs')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                opponent_label: schoolLines[0] || '',
                opponent_school: schoolLines[1] || '',
                location: (el.querySelector('.location2')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                location_url: el.querySelector('.location-link')?.href || '',
                time: (el.querySelector('.time strong')?.innerText || el.querySelector('.time')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                score: (el.querySelector('.outcome .score')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                result: (el.querySelector('.outcome strong')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                outcome: (el.querySelector('.outcome')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                raw_text: (el.innerText || '').replace(/\\s+/g, ' ').trim(),
            };
        })"""
    )

    parsed_rows: list[dict[str, str]] = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            opponent_label = _clean(str(row.get("opponent_label") or ""))
            opponent_school = _clean(str(row.get("opponent_school") or ""))
            parsed_rows.append(
                {
                    "event_id": _clean(str(row.get("event_id") or "")),
                    "sport": _clean(str(row.get("sport") or "")),
                    "date": _clean(str(row.get("date") or "")),
                    "home_away": _clean(str(row.get("home_away") or "")),
                    "opponent": opponent_school or opponent_label,
                    "opponent_label": opponent_label,
                    "opponent_school": opponent_school,
                    "location": _clean(str(row.get("location") or "")),
                    "location_url": _normalize_href(str(row.get("location_url") or "")),
                    "time": _clean(str(row.get("time") or "")),
                    "score": _clean(str(row.get("score") or "")),
                    "result": _clean(str(row.get("result") or "")),
                    "outcome": _clean(str(row.get("outcome") or "")),
                    "raw_text": _clean(str(row.get("raw_text") or "")),
                }
            )

    return (
        {
            "football_schedule_url": page.url,
            "football_record": record_text,
            "available_seasons": _dedupe_keep_order(seasons),
        },
        parsed_rows,
    )


async def _collect_roster(page) -> tuple[dict[str, Any], list[dict[str, str]]]:
    await page.goto(FOOTBALL_ROSTER_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector("#coach_div_2025 table.rostertable tr", timeout=60000)
    await page.wait_for_timeout(1500)

    roster_rows = await page.locator("#coach_div_2025 table.rostertable tr").evaluate_all(
        """els => els.map((el) => ({
            cells: Array.from(el.querySelectorAll('th,td')).map((td) => (td.innerText || '').replace(/\\s+/g, ' ').trim()),
            profile_url: el.querySelector('td:nth-child(3) a[href]')?.href || '',
        }))"""
    )

    seasons: list[str] = []
    season_options = page.locator("select option")
    if await season_options.count():
        raw_options = await season_options.evaluate_all(
            "els => els.map((opt) => (opt.textContent || '').replace(/\\s+/g, ' ').trim())"
        )
        if isinstance(raw_options, list):
            seasons = [
                _clean(str(value))
                for value in raw_options
                if _clean(str(value)).startswith("20")
            ]

    parsed_rows: list[dict[str, str]] = []
    if isinstance(roster_rows, list):
        for index, row in enumerate(roster_rows):
            if not isinstance(row, dict) or index == 0:
                continue
            cells = row.get("cells")
            if not isinstance(cells, list):
                continue
            normalized = [_clean(str(cell)) for cell in cells]
            if len(normalized) < 4:
                normalized.extend([""] * (4 - len(normalized)))
            name = normalized[2]
            if not name:
                continue
            parsed_rows.append(
                {
                    "number": normalized[1],
                    "name": name,
                    "position": normalized[3],
                    "profile_url": _normalize_href(str(row.get("profile_url") or "")),
                }
            )

    return (
        {
            "football_roster_url": page.url,
            "available_seasons": _dedupe_keep_order(seasons),
        },
        parsed_rows,
    )


async def _collect_coaches(page) -> tuple[dict[str, Any], list[dict[str, str]]]:
    await page.goto(FOOTBALL_COACHES_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1500)

    seasons: list[str] = []
    season_options = page.locator("select option")
    if await season_options.count():
        raw_options = await season_options.evaluate_all(
            "els => els.map((opt) => (opt.textContent || '').replace(/\\s+/g, ' ').trim())"
        )
        if isinstance(raw_options, list):
            seasons = [
                _clean(str(value))
                for value in raw_options
                if _clean(str(value)).startswith("20")
            ]

    links = await page.locator("a[href*='/coaching-staff/']").evaluate_all(
        """els => els.map((a) => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || '',
        }))"""
    )

    coaches: list[dict[str, str]] = []
    seen_profiles: set[str] = set()
    if isinstance(links, list):
        for item in links:
            if not isinstance(item, dict):
                continue
            href = _normalize_href(str(item.get("href") or ""))
            text = _clean(str(item.get("text") or ""))
            if not href or not text or "/athletic-department/coaching-staff/" in href:
                continue
            if href in seen_profiles:
                continue
            seen_profiles.add(href)
            role = "Head Coach" if "head coach" in text.lower() else "Coach"
            name = _clean(re.sub(r"\bHead Coach\b", "", text, flags=re.I))
            coaches.append(
                {
                    "name": name,
                    "role": role,
                    "profile_url": href,
                }
            )

    coach_assignments: list[dict[str, Any]] = []
    for coach in coaches:
        profile_url = coach.get("profile_url", "")
        if not profile_url:
            continue
        await page.goto(profile_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(1200)
        teams_text = await page.locator("body").inner_text(timeout=15000)
        assignment_lines: list[str] = []
        for raw_line in teams_text.splitlines():
            line = _clean(raw_line)
            if not line or "coach" not in line.lower():
                continue
            if coach["name"].lower() in line.lower():
                continue
            if "flag football" in line.lower() or "basketball" in line.lower():
                assignment_lines.append(line)
        coach_assignments.append(
            {
                "name": coach["name"],
                "profile_url": profile_url,
                "assignments": _dedupe_keep_order(assignment_lines),
            }
        )

    await page.goto(FOOTBALL_COACHES_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(500)

    return (
        {
            "football_coaches_url": page.url,
            "available_seasons": _dedupe_keep_order(seasons),
        },
        coaches,
        coach_assignments,
    )


async def scrape_school() -> dict[str, Any]:
    """Scrape Logan Memorial's public girls flag football pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    errors: list[str] = []

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
            await page.goto(SCHOOL_HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)

            await page.goto(SCHOOL_ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)

            team_meta = await _collect_team_page(page)
            source_pages.append(page.url)

            schedule_meta, schedule_rows = await _collect_schedule(page)
            source_pages.append(page.url)

            roster_meta, roster_rows = await _collect_roster(page)
            source_pages.append(page.url)

            coaches_meta, coaches, coach_assignments = await _collect_coaches(page)
            source_pages.append(page.url)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_flow_failed:{type(exc).__name__}")
            team_meta = {}
            schedule_meta = {"football_schedule_url": FOOTBALL_SCHEDULE_URL, "football_record": ""}
            roster_meta = {"football_roster_url": FOOTBALL_ROSTER_URL}
            coaches_meta = {"football_coaches_url": FOOTBALL_COACHES_URL}
            schedule_rows = []
            roster_rows = []
            coaches = []
            coach_assignments = []
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    coach_names = _dedupe_keep_order([coach["name"] for coach in coaches if coach.get("name")])
    roster_names = _dedupe_keep_order([player["name"] for player in roster_rows if player.get("name")])
    available_seasons = _dedupe_keep_order(
        list(schedule_meta.get("available_seasons", []))
        + list(roster_meta.get("available_seasons", []))
        + list(coaches_meta.get("available_seasons", []))
    )

    football_program_available = bool(schedule_rows or roster_rows or coaches)
    if not football_program_available:
        errors.append("blocked:no_public_flag_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_home_url": SCHOOL_HOME_URL,
        "school_athletics_url": SCHOOL_ATHLETICS_URL,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "football_team_url": team_meta.get("football_team_url", FOOTBALL_TEAM_URL),
        "football_schedule_url": schedule_meta.get("football_schedule_url", FOOTBALL_SCHEDULE_URL),
        "football_roster_url": roster_meta.get("football_roster_url", FOOTBALL_ROSTER_URL),
        "football_coaches_url": coaches_meta.get("football_coaches_url", FOOTBALL_COACHES_URL),
        "football_team_title": team_meta.get("football_team_title", ""),
        "football_record": schedule_meta.get("football_record", ""),
        "football_available_seasons": available_seasons,
        "football_recent_results": team_meta.get("football_recent_results", []),
        "football_team_links": team_meta.get("football_team_links", []),
        "football_schedule_rows": schedule_rows,
        "football_roster_players": roster_rows,
        "football_roster_player_names": roster_names,
        "football_coaches": coaches,
        "football_coach_names": coach_names,
        "football_coach_assignments": coach_assignments,
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
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "football_schedule_row_count": len(schedule_rows),
            "football_roster_player_count": len(roster_rows),
            "football_coach_count": len(coaches),
            "focus": "football_only",
            "football_variant": "girls_flag_football",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
