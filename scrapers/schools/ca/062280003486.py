"""Deterministic football scraper for Los Gatos High (CA)."""

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

NCES_ID = "062280003486"
SCHOOL_NAME = "Los Gatos High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_HOME_URL = "https://www.lghs.net/"
ATHLETICS_HOME_URL = "https://www.losgatosathletics.org/"
DIRECTORY_URL = "https://www.losgatosathletics.org/directory"
FOOTBALL_HOME_URL = "https://www.losgatosathletics.org/sport/football/boys/"
FOOTBALL_SCHEDULE_URL = "https://www.losgatosathletics.org/sport/football/boys/?tab=schedule"
FOOTBALL_ROSTER_URL = "https://www.losgatosathletics.org/sport/football/boys/?tab=roster"
FOOTBALL_STAFF_URL = "https://www.losgatosathletics.org/sport/football/boys/?tab=staff"

TARGET_URLS = [
    SCHOOL_HOME_URL,
    ATHLETICS_HOME_URL,
    DIRECTORY_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_STAFF_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _clean_lines(text: str) -> list[str]:
    return [_clean(line) for line in (text or "").splitlines() if _clean(line)]


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


def _extract_contact_fields(text: str) -> dict[str, str]:
    phone_match = re.search(r"CONTACT US\s+([0-9(). -]+)\s+\|", text, re.IGNORECASE)
    address_match = re.search(r"CONTACT US\s+[0-9(). -]+\s+\|\s+(.+?)\s+THANK YOU TO ALL OF OUR SPONSORS!", text, re.DOTALL)
    return {
        "phone": _clean(phone_match.group(1)) if phone_match else "",
        "address": _clean(address_match.group(1)) if address_match else "",
    }


def _parse_coach_lines(lines: list[str]) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    if "ANNOUNCEMENTS" in lines:
        lines = lines[: lines.index("ANNOUNCEMENTS")]

    start = -1
    for idx, line in enumerate(lines):
        if line == "MARK KRAIL":
            start = idx
            break
    if start == -1:
        return coaches

    current_name = ""
    for line in lines[start:]:
        if not line:
            continue
        if line.startswith("ROLE:"):
            role = _clean(line.split(":", 1)[1])
            if current_name:
                coaches.append({"name": current_name, "role": role})
                current_name = ""
            continue
        if line in {"EMAIL", "CALL FULL BIO"}:
            continue
        if line == line.upper() and any(char.isalpha() for char in line):
            current_name = line.title()

    return coaches


def _parse_player_cell(text: str) -> dict[str, str]:
    raw = _clean(text)
    match = re.match(
        r"(?P<name>[A-Z' .-]+?)\s+(?P<position>[A-Z/]+)\s+(?P<height>\d+'\s+\d+\")\s+(?P<weight>\d+\s+LBS)$",
        raw,
    )
    if not match:
        return {
            "name": raw,
            "position": "",
            "height": "",
            "weight": "",
        }
    return {key: _clean(value).title() if key == "name" else _clean(value) for key, value in match.groupdict().items()}


async def _collect_school_home(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    athletics_links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || ''
        })).filter(item => /athletics/i.test(item.text + ' ' + item.href))""",
    )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "athletics_links": athletics_links if isinstance(athletics_links, list) else [],
    }


async def _collect_athletics_home(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    football_links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || ''
        })).filter(item => /football|schedule|roster|news/i.test(item.text + ' ' + item.href))""",
    )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_links": football_links if isinstance(football_links, list) else [],
    }


async def _collect_directory(page) -> dict[str, Any]:
    football_contacts = await page.eval_on_selector_all(
        ".staff-member",
        """cards => cards.map(card => ({
            text: (card.innerText || '').replace(/\\s+/g, ' ').trim(),
            email: card.querySelector('a[href^="mailto:"]')?.getAttribute('href') || '',
            phone: card.querySelector('a[href^="tel:"]')?.getAttribute('href') || '',
            bio_url: card.querySelector('a[href*="/Staff/Bio/"]')?.href || ''
        })).filter(card => /(^|\\s)football(\\s|$)/i.test(card.text) && !/flag football/i.test(card.text))""",
    )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "football_contacts": football_contacts if isinstance(football_contacts, list) else [],
    }


async def _collect_schedule(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    team_levels = await page.locator(".schedule-submenu option").all_inner_texts()
    school_years = await page.locator(".schedule-school-year option").all_inner_texts()
    rows = await page.eval_on_selector_all(
        "#schedule-table tbody tr",
        """rows => rows.map(row => ({
            text: (row.innerText || '').replace(/\\s+/g, ' ').trim(),
            cells: Array.from(row.querySelectorAll('td')).map(td => (td.innerText || '').replace(/\\s+/g, ' ').trim())
        }))""",
    )
    schedule_entries: list[dict[str, str]] = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            cells = row.get("cells") or []
            if len(cells) < 4:
                continue
            game_info = _clean(str(cells[1]))
            location = _clean(str(cells[2]))
            result = _clean(str(cells[3]))
            if not game_info or not location:
                continue
            match = re.match(r"(?P<date>[A-Z]{3}\s+\d{2}\s*/\s+\d{1,2}:\d{2}\s+[AP]M)\s+(?P<matchup>.+)$", game_info)
            schedule_entries.append(
                {
                    "date_time": _clean(match.group("date")) if match else "",
                    "matchup": _clean(match.group("matchup")) if match else game_info,
                    "location": location,
                    "result": result,
                }
            )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "team_levels": _dedupe_keep_order(team_levels),
        "school_years": _dedupe_keep_order(school_years),
        "schedule_entries": schedule_entries,
    }


async def _collect_roster(page) -> dict[str, Any]:
    rows = await page.eval_on_selector_all(
        "#athlete-table tbody tr",
        """rows => rows.map(row => ({
            cells: Array.from(row.querySelectorAll('td')).map(td => (td.innerText || '').replace(/\\s+/g, ' ').trim())
        }))""",
    )
    players: list[dict[str, str]] = []
    grade_counts: dict[str, int] = {}
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            cells = row.get("cells") or []
            if len(cells) < 4:
                continue
            jersey_number = _clean(str(cells[1]))
            player_info = _parse_player_cell(str(cells[2]))
            grade = _clean(str(cells[3])).title()
            if not jersey_number or not player_info["name"]:
                continue
            grade_counts[grade] = grade_counts.get(grade, 0) + 1
            players.append(
                {
                    "jersey_number": jersey_number,
                    "name": player_info["name"],
                    "position": player_info["position"],
                    "height": player_info["height"],
                    "weight": player_info["weight"],
                    "grade": grade,
                }
            )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "player_count": len(players),
        "grade_breakdown": grade_counts,
        "players": players,
    }


async def _collect_staff_page(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    lines = _clean_lines(body_text)
    coaches = _parse_coach_lines(lines)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || ''
        })).filter(item => /photo|gofan|donate|physical|standings|records|teamsnap|social media/i.test(item.text + ' ' + item.href))""",
    )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "coaches": coaches,
        "resource_links": links if isinstance(links, list) else [],
        "contact": _extract_contact_fields(body_text),
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)

    school_home: dict[str, Any] = {}
    athletics_home: dict[str, Any] = {}
    directory: dict[str, Any] = {}
    schedule: dict[str, Any] = {}
    roster: dict[str, Any] = {}
    staff: dict[str, Any] = {}

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
            for url, collector in [
                (SCHOOL_HOME_URL, _collect_school_home),
                (ATHLETICS_HOME_URL, _collect_athletics_home),
                (DIRECTORY_URL, _collect_directory),
                (FOOTBALL_SCHEDULE_URL, _collect_schedule),
                (FOOTBALL_ROSTER_URL, _collect_roster),
                (FOOTBALL_STAFF_URL, _collect_staff_page),
            ]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(2500)
                    payload = await collector(page)
                    source_pages.append(page.url)
                    if url == SCHOOL_HOME_URL:
                        school_home = payload
                    elif url == ATHLETICS_HOME_URL:
                        athletics_home = payload
                    elif url == DIRECTORY_URL:
                        directory = payload
                    elif url == FOOTBALL_SCHEDULE_URL:
                        schedule = payload
                    elif url == FOOTBALL_ROSTER_URL:
                        roster = payload
                    elif url == FOOTBALL_STAFF_URL:
                        staff = payload
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await context.close()
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_home_links = [
        link
        for link in (athletics_home.get("football_links") or [])
        if isinstance(link, dict)
        and "/sport/football/boys/" in str(link.get("href", "")).lower()
    ]
    football_directory_contacts = directory.get("football_contacts") or []
    coach_roles = staff.get("coaches") or []
    schedule_entries = schedule.get("schedule_entries") or []
    roster_players = roster.get("players") or []
    resource_links = staff.get("resource_links") or []

    extracted_items: dict[str, Any] = {
        "school_site": {
            "school_home_url": SCHOOL_HOME_URL,
            "school_home_title": school_home.get("title", ""),
            "athletics_links": school_home.get("athletics_links", []),
        },
        "football_program": {
            "athletics_home_url": ATHLETICS_HOME_URL,
            "football_home_url": FOOTBALL_HOME_URL,
            "schedule_url": FOOTBALL_SCHEDULE_URL,
            "roster_url": FOOTBALL_ROSTER_URL,
            "staff_url": FOOTBALL_STAFF_URL,
            "team_levels": schedule.get("team_levels", []),
            "school_years": schedule.get("school_years", [])[:5],
            "contact_phone": staff.get("contact", {}).get("phone", ""),
            "contact_address": staff.get("contact", {}).get("address", ""),
            "football_links": football_home_links,
        },
        "football_directory_contact": football_directory_contacts,
        "football_staff": {
            "coach_roles": coach_roles,
            "resource_links": resource_links,
        },
        "varsity_schedule": schedule_entries,
        "varsity_roster": {
            "player_count": roster.get("player_count", 0),
            "grade_breakdown": roster.get("grade_breakdown", {}),
            "players_sample": roster_players[:20],
        },
    }

    if not schedule_entries and not coach_roles and not football_directory_contacts and not roster_players:
        errors.append("blocked:no_public_football_content_extracted")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers"),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "target_urls": TARGET_URLS,
            "pages_checked": len(source_pages),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    import json

    print(json.dumps(asyncio.run(scrape_school()), indent=2, sort_keys=True))
