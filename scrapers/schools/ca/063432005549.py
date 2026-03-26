"""Deterministic football scraper for Canyon Hills High (CA)."""

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

NCES_ID = "063432005549"
SCHOOL_NAME = "Canyon Hills High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.chrattlers.com/"
FOOTBALL_TEAM_URL = "https://www.chrattlers.com/varsity/football/"
FOOTBALL_SCHEDULE_URL = (
    "https://www.chrattlers.com/varsity/football/schedule-results?selected_year=2025-26"
)
FOOTBALL_ROSTER_URL = "https://www.chrattlers.com/varsity/football/roster"
COACHING_STAFF_URL = "https://www.chrattlers.com/athletic-department/coaching-staff/"

TARGET_URLS = [
    HOME_URL,
    FOOTBALL_TEAM_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
    COACHING_STAFF_URL,
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


def _extract_schedule_row(li: Any) -> dict[str, str]:
    sport = li.query_selector(".sport small, .sport")
    date = li.query_selector(".date")
    vs = li.query_selector(".vs")
    school = li.query_selector(".school")
    time_node = li.query_selector(".time")
    outcome = li.query_selector(".outcome")
    score = li.query_selector(".outcome .score")
    result = li.query_selector(".outcome strong")
    location_link = li.query_selector(".location-link")
    location_text = li.query_selector(".location2")
    school_name = li.query_selector(".school p:first-child")

    score_text = ""
    if score:
        score_text = _clean(score.inner_text()).replace(" ", " ")
    result_text = _clean(result.inner_text()) if result else ""
    outcome_text = _clean(outcome.inner_text()) if outcome else ""
    time_text = _clean(time_node.inner_text()) if time_node else ""
    opponent_text = _clean(school_name.inner_text()) if school_name else _clean(
        school.inner_text() if school else ""
    )

    location_href = ""
    if location_link:
        location_href = _normalize_href(location_link.get_attribute("href") or "")

    row = {
        "event_id": "",
        "sport": _clean(sport.inner_text()) if sport else "",
        "date": _clean(date.inner_text()) if date else "",
        "home_away": _clean(vs.inner_text()) if vs else "",
        "opponent": opponent_text,
        "location": _clean(location_text.inner_text()) if location_text else "",
        "location_url": location_href,
        "time": time_text,
        "score": score_text,
        "result": result_text,
        "outcome": outcome_text,
        "raw_text": _clean(li.inner_text()),
    }

    event_id = ""
    if school:
        event_id = _clean(school.get_attribute("data-event-id") or "")
    if not event_id:
        classes = _clean(li.get_attribute("class") or "")
        match = re.search(r"event-id-(\d+)", classes)
        if match:
            event_id = match.group(1)
    row["event_id"] = event_id
    return row


def _extract_roster_row(tr: Any) -> dict[str, str]:
    cells = tr.query_selector_all("th,td")
    values = [_clean(cell.inner_text()) for cell in cells]
    if len(values) < 4:
        values.extend([""] * (4 - len(values)))

    name_link = tr.query_selector("td:nth-child(3) a[href]")
    profile_url = _normalize_href(name_link.get_attribute("href") or "") if name_link else ""
    name_text = _clean(name_link.inner_text()) if name_link else values[2]

    return {
        "number": values[1] if len(values) > 1 else "",
        "name": name_text,
        "position": values[3] if len(values) > 3 else "",
        "profile_url": profile_url,
    }


def _extract_coach_card(card: Any) -> dict[str, str]:
    name_link = card.query_selector("h3 a[href]")
    role_node = card.query_selector("h4")
    contact_button = card.query_selector('a[onclick^="openContactModal("]')

    name = _clean(name_link.inner_text()) if name_link else ""
    role = _clean(role_node.inner_text()) if role_node else ""
    profile_url = _normalize_href(name_link.get_attribute("href") or "") if name_link else ""
    contact_id = ""
    if contact_button:
        onclick = contact_button.get_attribute("onclick") or ""
        match = re.search(r"openContactModal\('([^']+)'\)", onclick)
        if match:
            contact_id = match.group(1)

    return {
        "name": name,
        "role": role,
        "profile_url": profile_url,
        "contact_modal_id": contact_id,
    }


async def _collect_schedule(page) -> tuple[dict[str, Any], list[dict[str, str]]]:
    await page.goto(FOOTBALL_SCHEDULE_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector("#load_schedule-results-team .schedule-results ul > li", timeout=60000)
    await page.wait_for_timeout(1500)

    record_text = ""
    record_node = page.locator("#load_schedule-results-team .schedule-results h3.record")
    if await record_node.count():
        record_text = _clean(await record_node.inner_text())

    rows = await page.locator("#load_schedule-results-team .schedule-results ul > li.schedule-and-results-list-item").evaluate_all(
        """els => els.map((el) => ({
            event_id: el.className.match(/event-id-(\\d+)/)?.[1] || '',
            sport: (el.querySelector('.sport')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            date: (el.querySelector('.date')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            home_away: (el.querySelector('.vs')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            opponent: (el.querySelector('.school p:first-child')?.innerText || el.querySelector('.school')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            location: (el.querySelector('.location2')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            location_url: el.querySelector('.location-link')?.href || '',
            time: (el.querySelector('.time')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            score: (el.querySelector('.outcome .score')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            result: (el.querySelector('.outcome strong')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            outcome: (el.querySelector('.outcome')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            raw_text: (el.innerText || '').replace(/\\s+/g, ' ').trim(),
        }))"""
    )
    parsed_rows: list[dict[str, str]] = []
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                parsed_rows.append(
                    {
                        "event_id": _clean(str(row.get("event_id") or "")),
                        "sport": _clean(str(row.get("sport") or "")),
                        "date": _clean(str(row.get("date") or "")),
                        "home_away": _clean(str(row.get("home_away") or "")),
                        "opponent": _clean(str(row.get("opponent") or "")),
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
            "football_schedule_url": FOOTBALL_SCHEDULE_URL,
            "football_team_url": FOOTBALL_TEAM_URL,
            "football_record": record_text,
        },
        parsed_rows,
    )


async def _collect_roster(page) -> list[dict[str, str]]:
    await page.goto(FOOTBALL_ROSTER_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector("#coach_div_2025 table.rostertable tr", timeout=60000)
    await page.wait_for_timeout(1500)

    rows = await page.locator("#coach_div_2025 table.rostertable tr").evaluate_all(
        """els => els.map((el) => Array.from(el.querySelectorAll('th,td')).map(td => (td.innerText || '').replace(/\\s+/g, ' ').trim()))"""
    )
    parsed: list[dict[str, str]] = []
    if not isinstance(rows, list):
        return parsed

    for idx, cells in enumerate(rows):
        if not isinstance(cells, list) or idx == 0:
            continue
        normalized = [_clean(str(cell or "")) for cell in cells]
        if len(normalized) < 3:
            continue
        name = normalized[2]
        if not name:
            continue
        parsed.append(
            {
                "number": normalized[1] if len(normalized) > 1 else "",
                "name": name,
                "position": normalized[3] if len(normalized) > 3 else "",
            }
        )

    return parsed


async def _collect_coaches(page) -> list[dict[str, str]]:
    await page.goto(COACHING_STAFF_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector("main#main .info", timeout=60000)
    await page.wait_for_timeout(1500)

    cards = await page.locator("main#main .info").evaluate_all(
        """els => els.map((el) => ({
            name: (el.querySelector('h3 a')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            role: (el.querySelector('h4')?.innerText || '').replace(/\\s+/g, ' ').trim(),
            profile_url: el.querySelector('h3 a[href]')?.href || '',
            contact_modal_id: (() => {
                const button = el.querySelector('a[onclick^="openContactModal("]');
                if (!button) return '';
                const onclick = button.getAttribute('onclick') || '';
                const match = onclick.match(/openContactModal\\('([^']+)'\\)/);
                return match ? match[1] : '';
            })(),
            text: (el.innerText || '').replace(/\\s+/g, ' ').trim(),
        }))"""
    )

    parsed: list[dict[str, str]] = []
    if not isinstance(cards, list):
        return parsed

    for card in cards:
        if not isinstance(card, dict):
            continue
        role = _clean(str(card.get("role") or ""))
        text = _clean(str(card.get("text") or ""))
        if "football" not in role.lower() and "football" not in text.lower():
            continue
        parsed.append(
            {
                "name": _clean(str(card.get("name") or "")),
                "role": role,
                "profile_url": _normalize_href(str(card.get("profile_url") or "")),
                "contact_modal_id": _clean(str(card.get("contact_modal_id") or "")),
            }
        )

    return parsed


async def scrape_school() -> dict[str, Any]:
    """Scrape Canyon Hills High football's public schedule, roster, and coaching staff."""
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
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)

            schedule_meta, schedule_rows = await _collect_schedule(page)
            source_pages.append(page.url)

            roster_rows = await _collect_roster(page)
            source_pages.append(page.url)

            coaches = await _collect_coaches(page)
            source_pages.append(page.url)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_flow_failed:{type(exc).__name__}")
            schedule_meta = {
                "football_schedule_url": FOOTBALL_SCHEDULE_URL,
                "football_team_url": FOOTBALL_TEAM_URL,
                "football_record": "",
            }
            schedule_rows = []
            roster_rows = []
            coaches = []
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    coach_names = _dedupe_keep_order([coach["name"] for coach in coaches if coach.get("name")])
    roster_names = _dedupe_keep_order([player["name"] for player in roster_rows if player.get("name")])
    football_related_programs = _dedupe_keep_order(
        [
            coach["role"]
            for coach in coaches
            if coach.get("role") and "football" in coach["role"].lower()
        ]
    )

    football_program_available = bool(schedule_rows or roster_rows or coaches)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_home_url": HOME_URL,
        "football_team_url": FOOTBALL_TEAM_URL,
        "football_schedule_url": schedule_meta["football_schedule_url"],
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "football_coaching_staff_url": COACHING_STAFF_URL,
        "football_record": schedule_meta.get("football_record", ""),
        "football_schedule_rows": schedule_rows,
        "football_roster_players": roster_rows,
        "football_roster_player_names": roster_names,
        "football_coaches": coaches,
        "football_coach_names": coach_names,
        "football_related_programs": football_related_programs,
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "football_schedule_row_count": len(schedule_rows),
            "football_roster_player_count": len(roster_rows),
            "football_coach_count": len(coaches),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
