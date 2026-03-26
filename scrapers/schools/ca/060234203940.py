"""Deterministic football scraper for Clayton Valley Charter High (CA)."""

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

NCES_ID = "060234203940"
SCHOOL_NAME = "Clayton Valley Charter High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.claytonvalley.org/"
ATHLETICS_URL = "https://www.claytonvalley.org/athletics"
FOOTBALL_URL = "https://www.claytonvalley.org/athletics/fall-sports/football"
STADIUM_URL = "https://www.claytonvalley.org/athletics/stadium-event-information"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    FOOTBALL_URL,
    STADIUM_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

PHONE_PATTERN = re.compile(r"\(\d{3}\)\s*\d{3}-\d{4}")
EMAIL_PATTERN = re.compile(r"[\w.\-+]+@[\w.\-]+\.\w+")
ADDRESS_PATTERN = re.compile(r"\d{3,5}\s+[^,\n]+,\s*[A-Za-z .'-]+,\s*CA\s+\d{5}")


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        value = _clean(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _normalize_level(raw: str) -> str:
    value = _clean(raw).lower()
    if value == "varsity":
        return "Varsity"
    if value in {"jv", "junior varsity", "jv varsity"}:
        return "Junior Varsity"
    if value == "freshman":
        return "Freshman"
    return _clean(raw)


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 20) -> list[str]:
    matches: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _extract_contact_info(text: str) -> dict[str, str]:
    phone_match = PHONE_PATTERN.search(text)
    email_match = EMAIL_PATTERN.search(text)
    address_match = ADDRESS_PATTERN.search(text)
    return {
        "phone": phone_match.group(0) if phone_match else "",
        "email": email_match.group(0) if email_match else "",
        "address": _clean(address_match.group(0)) if address_match else "",
    }


def _extract_intro_lines(text: str) -> list[str]:
    lines = _extract_keyword_lines(
        text,
        keywords=("football", "varsity", "junior varsity", "freshman", "state champions"),
        limit=12,
    )
    return lines[:6]


async def _collect_text(page) -> str:
    for selector in ("main", "body"):
        try:
            return await page.locator(selector).inner_text(timeout=10000)
        except Exception:  # noqa: BLE001
            continue
    return ""


async def _collect_home_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    athletics_links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        })).filter(item => /athletics|football|fall sports/i.test(item.text + ' ' + item.href))""",
    )
    if not isinstance(athletics_links, list):
        athletics_links = []

    normalized_links = _dedupe_keep_order(
        [
            f"{_clean(str(item.get('text') or ''))}|{_clean(str(item.get('href') or ''))}"
            for item in athletics_links
            if isinstance(item, dict)
        ]
    )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "contact": _extract_contact_info(body_text),
        "athletics_links": normalized_links,
    }


async def _collect_athletics_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    football_links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        })).filter(item => /football|coaching staff|stadium|calendar/i.test(item.text + ' ' + item.href))""",
    )
    if not isinstance(football_links, list):
        football_links = []

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "football_links": _dedupe_keep_order(
            [
                f"{_clean(str(item.get('text') or ''))}|{_clean(str(item.get('href') or ''))}"
                for item in football_links
                if isinstance(item, dict)
            ]
        ),
        "athletics_keyword_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "fall sports", "coaching staff", "stadium"),
            limit=15,
        ),
    }


async def _collect_football_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)

    title = _clean(await page.title())
    intro_lines = _extract_intro_lines(body_text)
    championship_lines = _extract_keyword_lines(body_text, ("champion",), limit=5)

    level_tabs = await page.locator("main a[href^='#fs-panel-']").all_inner_texts()
    levels = _dedupe_keep_order([_normalize_level(item) for item in level_tabs])

    record_sections = await page.locator("section.fsAthleticsTeam.fsProfile").evaluate_all(
        """els => els.map(section => ({
            title: (section.querySelector('h2')?.textContent || '').replace(/\\s+/g, ' ').trim(),
            overall: (section.querySelector('.fsAthleticsTeamRecord')?.textContent || '').replace(/\\s+/g, ' ').trim(),
            league: (section.querySelector('.fsAthleticsTeamLeagueRecord')?.textContent || '').replace(/\\s+/g, ' ').trim()
        }))""",
    )
    if not isinstance(record_sections, list):
        record_sections = []

    coach_sections = await page.locator("section.fsAthleticsRoster.fsCoaches").evaluate_all(
        """els => els.map(section => ({
            coaches: Array.from(section.querySelectorAll('li')).map(li => ({
                name: (li.querySelector('.fsRosterName')?.textContent || '').replace(/\\s+/g, ' ').trim(),
                role: (li.querySelector('.fsRosterTitle')?.textContent || '').replace(/\\s+/g, ' ').trim()
            }))
        }))""",
    )
    if not isinstance(coach_sections, list):
        coach_sections = []

    roster_sections = await page.locator("section.fsAthleticsRoster.fsPlayers").evaluate_all(
        """els => els.map(section => {
            const headers = Array.from(section.querySelectorAll('thead th'))
                .map(th => (th.textContent || '').replace(/\\s+/g, ' ').trim());
            const rows = Array.from(section.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll('td')).map(td => (td.textContent || '').replace(/\\s+/g, ' ').trim())
            );
            return {
                title: (section.querySelector('h2')?.textContent || '').replace(/\\s+/g, ' ').trim(),
                headers,
                rows
            };
        })""",
    )
    if not isinstance(roster_sections, list):
        roster_sections = []

    schedule_sections = await page.locator("section.fsAthleticsEvent.fsTable").evaluate_all(
        """els => els.map(section => {
            const headers = Array.from(section.querySelectorAll('thead th'))
                .map(th => (th.textContent || '').replace(/\\s+/g, ' ').trim());
            const rows = Array.from(section.querySelectorAll('tbody tr')).map(tr =>
                Array.from(tr.querySelectorAll('td')).map(td => (td.textContent || '').replace(/\\s+/g, ' ').trim())
            );
            const season = section.querySelector('select option[selected]')?.textContent || '';
            return {
                title: (section.querySelector('h2')?.textContent || '').replace(/\\s+/g, ' ').trim(),
                season: season.replace(/\\s+/g, ' ').trim(),
                headers,
                rows
            };
        })""",
    )
    if not isinstance(schedule_sections, list):
        schedule_sections = []

    football_links = await page.eval_on_selector_all(
        "main a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        })).filter(item => /football|athletics|stadium|coaching/i.test(item.text + ' ' + item.href))""",
    )
    if not isinstance(football_links, list):
        football_links = []

    records_by_level: dict[str, dict[str, str]] = {}
    for section in record_sections:
        if not isinstance(section, dict):
            continue
        title_text = _clean(str(section.get("title") or ""))
        if title_text.startswith("Varsity"):
            level = "Varsity"
        elif title_text.startswith("JV"):
            level = "Junior Varsity"
        elif title_text.startswith("Freshman"):
            level = "Freshman"
        else:
            continue
        records_by_level[level] = {
            "overall_record": _clean(str(section.get("overall") or "")).replace("Record: ", "", 1),
            "league_record": _clean(str(section.get("league") or "")).replace("League Record: ", "", 1),
        }

    coaches_by_level: dict[str, list[dict[str, str]]] = {}
    for level, section in zip(("Varsity", "Junior Varsity", "Freshman"), coach_sections):
        if not isinstance(section, dict):
            continue
        coaches: list[dict[str, str]] = []
        for coach in section.get("coaches") or []:
            if not isinstance(coach, dict):
                continue
            name = _clean(str(coach.get("name") or ""))
            role = _clean(str(coach.get("role") or ""))
            if not name and not role:
                continue
            coaches.append({"name": name, "role": role})
        coaches_by_level[level] = coaches

    rosters_by_level: dict[str, list[dict[str, str]]] = {}
    for section in roster_sections:
        if not isinstance(section, dict):
            continue
        title_text = _clean(str(section.get("title") or ""))
        if title_text.startswith("Varsity"):
            level = "Varsity"
        elif title_text.startswith("JV"):
            level = "Junior Varsity"
        elif title_text.startswith("Freshman"):
            level = "Freshman"
        else:
            continue

        headers = [_clean(str(item)) for item in (section.get("headers") or [])]
        rows = section.get("rows") or []
        roster: list[dict[str, str]] = []
        for raw_row in rows:
            if not isinstance(raw_row, list):
                continue
            cells = [_clean(str(item)) for item in raw_row]
            if not any(cells):
                continue
            entry: dict[str, str] = {}
            if headers == ["#", "Name", "Grade"] and len(cells) >= 3:
                entry = {"number": cells[0], "name": cells[1], "grade": cells[2]}
            elif headers == ["Name", "Grade"] and len(cells) >= 2:
                entry = {"name": cells[0], "grade": cells[1]}
            else:
                for index, header in enumerate(headers):
                    key = re.sub(r"[^a-z0-9]+", "_", header.lower()).strip("_") or f"col_{index + 1}"
                    if index < len(cells):
                        entry[key] = cells[index]
            if entry:
                roster.append(entry)
        rosters_by_level[level] = roster

    schedules_by_level: dict[str, dict[str, Any]] = {}
    for section in schedule_sections:
        if not isinstance(section, dict):
            continue
        title_text = _clean(str(section.get("title") or ""))
        if title_text.startswith("Varsity"):
            level = "Varsity"
        elif title_text.startswith("JV"):
            level = "Junior Varsity"
        elif title_text.startswith("Freshman"):
            level = "Freshman"
        else:
            continue

        headers = [_clean(str(item)) for item in (section.get("headers") or [])]
        rows = section.get("rows") or []
        schedule_rows: list[dict[str, str]] = []
        for raw_row in rows:
            if not isinstance(raw_row, list):
                continue
            cells = [_clean(str(item)) for item in raw_row]
            if not any(cells):
                continue
            row_data = dict(zip(headers, cells, strict=False))
            matchup = row_data.get("Opponent", "")
            opponent = re.sub(r"^\s*vs\.\s*", "", matchup).strip()
            schedule_rows.append(
                {
                    "matchup": matchup,
                    "opponent": opponent,
                    "date": row_data.get("Date", ""),
                    "time": row_data.get("Time", ""),
                    "location": row_data.get("Location", ""),
                    "advantage": row_data.get("Advantage", ""),
                    "type": row_data.get("Type", ""),
                    "details": row_data.get("Details", ""),
                    "result": row_data.get("Result", ""),
                    "score": row_data.get("Score", ""),
                }
            )
        schedules_by_level[level] = {
            "season": _clean(str(section.get("season") or "")),
            "games": schedule_rows,
        }

    return {
        "url": page.url,
        "title": title,
        "intro_lines": intro_lines,
        "championship_lines": championship_lines,
        "football_levels": levels,
        "records_by_level": records_by_level,
        "coaches_by_level": coaches_by_level,
        "rosters_by_level": rosters_by_level,
        "schedules_by_level": schedules_by_level,
        "football_links": _dedupe_keep_order(
            [
                f"{_clean(str(item.get('text') or ''))}|{_clean(str(item.get('href') or ''))}"
                for item in football_links
                if isinstance(item, dict)
            ]
        ),
        "contact": _extract_contact_info(body_text),
    }


async def _collect_stadium_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "stadium_policy_lines": _extract_keyword_lines(
            body_text,
            keywords=("bag policy", "stadium", "gym rules", "footballs", "field"),
            limit=12,
        ),
        "contact": _extract_contact_info(body_text),
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_log: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 1200},
            locale="en-US",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            home_snapshot = await _collect_home_page(page)
            source_pages.append(home_snapshot["url"])
            navigation_log.append("visit_home")

            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            athletics_snapshot = await _collect_athletics_page(page)
            source_pages.append(athletics_snapshot["url"])
            navigation_log.append("visit_athletics")

            await page.goto(FOOTBALL_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(2000)
            football_snapshot = await _collect_football_page(page)
            source_pages.append(football_snapshot["url"])
            navigation_log.append("visit_football")

            await page.goto(STADIUM_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            stadium_snapshot = await _collect_stadium_page(page)
            source_pages.append(stadium_snapshot["url"])
            navigation_log.append("visit_stadium_information")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
            home_snapshot = locals().get("home_snapshot", {})
            athletics_snapshot = locals().get("athletics_snapshot", {})
            football_snapshot = locals().get("football_snapshot", {})
            stadium_snapshot = locals().get("stadium_snapshot", {})
        finally:
            await browser.close()

    football_levels = football_snapshot.get("football_levels") if isinstance(football_snapshot, dict) else []
    records_by_level = (
        football_snapshot.get("records_by_level") if isinstance(football_snapshot, dict) else {}
    )
    rosters_by_level = (
        football_snapshot.get("rosters_by_level") if isinstance(football_snapshot, dict) else {}
    )
    schedules_by_level = (
        football_snapshot.get("schedules_by_level") if isinstance(football_snapshot, dict) else {}
    )
    coaches_by_level = (
        football_snapshot.get("coaches_by_level") if isinstance(football_snapshot, dict) else {}
    )

    varsity_coaches = coaches_by_level.get("Varsity", []) if isinstance(coaches_by_level, dict) else []
    varsity_head_coach = varsity_coaches[0] if varsity_coaches else {}
    varsity_roster = rosters_by_level.get("Varsity", []) if isinstance(rosters_by_level, dict) else []
    varsity_schedule = schedules_by_level.get("Varsity", {}) if isinstance(schedules_by_level, dict) else {}

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_levels),
        "football_page_url": FOOTBALL_URL,
        "athletics_page_url": ATHLETICS_URL,
        "stadium_information_url": STADIUM_URL,
        "football_levels": football_levels,
        "football_intro_lines": football_snapshot.get("intro_lines", []) if isinstance(football_snapshot, dict) else [],
        "football_championship_lines": football_snapshot.get("championship_lines", [])
        if isinstance(football_snapshot, dict)
        else [],
        "varsity_head_coach": varsity_head_coach,
        "records_by_level": records_by_level,
        "rosters_by_level": rosters_by_level,
        "roster_counts_by_level": {
            level: len(players)
            for level, players in rosters_by_level.items()
            if isinstance(players, list)
        },
        "schedules_by_level": schedules_by_level,
        "schedule_counts_by_level": {
            level: len(data.get("games", []))
            for level, data in schedules_by_level.items()
            if isinstance(data, dict)
        },
        "varsity_schedule_season": varsity_schedule.get("season", "") if isinstance(varsity_schedule, dict) else "",
        "varsity_schedule_preview": (
            varsity_schedule.get("games", [])[:5] if isinstance(varsity_schedule, dict) else []
        ),
        "varsity_roster_preview": varsity_roster[:10] if isinstance(varsity_roster, list) else [],
        "school_contact": (
            home_snapshot.get("contact")
            or football_snapshot.get("contact")
            or stadium_snapshot.get("contact")
            or {}
        ),
        "athletics_navigation_links": athletics_snapshot.get("football_links", [])
        if isinstance(athletics_snapshot, dict)
        else [],
        "football_page_links": football_snapshot.get("football_links", [])
        if isinstance(football_snapshot, dict)
        else [],
        "stadium_policy_lines": stadium_snapshot.get("stadium_policy_lines", [])
        if isinstance(stadium_snapshot, dict)
        else [],
        "navigation_log": navigation_log,
    }

    if not extracted_items["football_program_available"]:
        errors.append("football_program_not_detected_on_public_pages")

    if not extracted_items["varsity_head_coach"]:
        errors.append("varsity_head_coach_not_found")

    if not extracted_items["roster_counts_by_level"]:
        errors.append("football_rosters_not_found")

    if not extracted_items["schedule_counts_by_level"]:
        errors.append("football_schedules_not_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0",
            "proxy": get_proxy_runtime_meta(PROXY_PROFILE),
        },
        "errors": errors,
    }
