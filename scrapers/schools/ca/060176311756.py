"""Deterministic football scraper for Classical Academy High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060176311756"
SCHOOL_NAME = "Classical Academy High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_URL = "https://www.classicalacademy.com/"
ATHLETICS_HOME_URL = "https://www.caimansports.com/"
FOOTBALL_HOME_URL = "https://www.caimansports.com/varsity/football/"
FOOTBALL_COACHES_URL = "https://www.caimansports.com/varsity/football/coaches"
FOOTBALL_SCHEDULE_URL = "https://www.caimansports.com/varsity/football/schedule-results"
FOOTBALL_ROSTER_URL = "https://www.caimansports.com/varsity/football/roster"
STAFF_DIRECTORY_URL = "https://www.caimansports.com/staff-directory/"

TARGET_URLS = [
    SCHOOL_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
    STAFF_DIRECTORY_URL,
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
    out: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_href(href: str, base: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("/"):
        return urljoin(base, raw)
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return ""


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 50) -> list[str]:
    lines: list[str] = []
    for raw in (text or "").splitlines():
        line = _clean(raw)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _parse_score(raw: str) -> dict[str, str]:
    text = _clean(raw)
    match = re.search(r"(?P<team>\d+)\s*-\s*(?P<opp>\d+)", text)
    result = ""
    if re.search(r"\sW\b", f" {text}") or text.endswith("W"):
        result = "W"
    elif re.search(r"\sL\b", f" {text}") or text.endswith("L"):
        result = "L"
    elif re.search(r"\sT\b", f" {text}") or text.endswith("T"):
        result = "T"
    return {
        "team_score": match.group("team") if match else "",
        "opponent_score": match.group("opp") if match else "",
        "result": result,
    }


async def _collect_text(page) -> str:
    try:
        return await page.locator("main").inner_text(timeout=10000)
    except Exception:  # noqa: BLE001
        try:
            return await page.locator("body").inner_text(timeout=10000)
        except Exception:  # noqa: BLE001
            return ""


async def _collect_school_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        })).filter(x => /caimansports|athletics|sports|football/i.test(x.text + ' ' + x.href))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "athletics_lines": _extract_keyword_lines(
            body_text,
            keywords=("athletics", "sports", "football", "caiman"),
        ),
        "athletics_links": _dedupe_keep_order(
            [
                f"{_clean(str(item.get('text') or ''))}|{_normalize_href(str(item.get('href') or ''), page.url)}"
                for item in links
                if isinstance(item, dict)
            ]
        ),
    }


async def _collect_athletics_home(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        })).filter(x => /football|schedule|roster|staff|coach/i.test(x.text + ' ' + x.href))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "staff directory", "coaches page", "schedule", "roster"),
        ),
        "football_links": _dedupe_keep_order(
            [
                f"{_clean(str(item.get('text') or ''))}|{_normalize_href(str(item.get('href') or ''), page.url)}"
                for item in links
                if isinstance(item, dict)
            ]
        ),
    }


async def _collect_football_home(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        })).filter(x => /football|schedule|roster|coach|news|instagram/i.test(x.text + ' ' + x.href))""",
    )
    if not isinstance(links, list):
        links = []

    levels = _dedupe_keep_order(
        re.findall(r"\b(Varsity|Junior Varsity|Novice)\b", body_text, flags=re.I)
    )
    normalized_levels = []
    for value in levels:
        lowered = value.lower()
        if lowered == "varsity":
            normalized_levels.append("Varsity")
        elif lowered == "junior varsity":
            normalized_levels.append("Junior Varsity")
        elif lowered == "novice":
            normalized_levels.append("Novice")

    news_titles = []
    for raw in body_text.splitlines():
        line = _clean(raw)
        if not line or "caiman football" not in line.lower():
            continue
        if "weekly spring passing update" not in line.lower():
            continue
        news_titles.append(line)

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "varsity", "junior varsity", "novice", "spring passing"),
        ),
        "football_links": _dedupe_keep_order(
            [
                f"{_clean(str(item.get('text') or ''))}|{_normalize_href(str(item.get('href') or ''), page.url)}"
                for item in links
                if isinstance(item, dict)
            ]
        ),
        "football_levels": normalized_levels,
        "football_news_titles": _dedupe_keep_order(news_titles),
    }


async def _collect_coaches_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    coach_cards = await page.locator("main .rostertable").evaluate_all(
        """els => els.map((card) => ({
            text: (card.innerText || '').replace(/\\s+/g, ' ').trim(),
            links: Array.from(card.querySelectorAll('a[href]')).map(a => ({
                text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: a.href || ''
            }))
        }))""",
    )
    if not isinstance(coach_cards, list):
        coach_cards = []

    head_coach: dict[str, str] = {}
    for card in coach_cards:
        if not isinstance(card, dict):
            continue
        text = _clean(str(card.get("text") or ""))
        if not text:
            continue
        if "head coach" not in text.lower():
            continue
        name = text.split("Head Coach", 1)[0].strip()
        if not name:
            continue
        profile_url = ""
        for link in card.get("links") or []:
            if not isinstance(link, dict):
                continue
            href = _normalize_href(str(link.get("href") or ""), page.url)
            if "/coaching-staff/" in href:
                profile_url = href
                break
        head_coach = {
            "name": _clean(name),
            "role": "Head Coach",
            "profile_url": profile_url,
            "source_text": text,
        }
        break

    years = await page.locator("#yearDropdown option").all_inner_texts()
    if not isinstance(years, list):
        years = []

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "head_coach": head_coach,
        "available_years": _dedupe_keep_order([_clean(str(year)) for year in years]),
        "coaches_page_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "head coach", "contact coach"),
        ),
    }


async def _collect_schedule_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    rows = await page.locator(".schedule-results li.schedule-and-results-list-item").evaluate_all(
        """els => els.map((li) => {
            const links = Array.from(li.querySelectorAll('a[href]')).map(a => ({
                text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: a.href || ''
            }));
            return {
                date: li.getAttribute('data-date') || '',
                sport: (li.querySelector('.sport')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                date_text: (li.querySelector('.date')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                vs: (li.querySelector('.vs')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                opponent: (li.querySelector('.school p')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                school_text: (li.querySelector('.school')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                outcome: (li.querySelector('.outcome')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                outcome_result: li.querySelector('.outcome')?.getAttribute('result') || '',
                time: (li.querySelector('.time')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                links
            };
        })""",
    )
    if not isinstance(rows, list):
        rows = []

    schedule_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        opponent = _clean(str(row.get("opponent") or ""))
        school_text = _clean(str(row.get("school_text") or ""))
        if not opponent and not school_text:
            continue
        score = _parse_score(str(row.get("outcome") or ""))
        if not score["result"]:
            score["result"] = _clean(str(row.get("outcome_result") or ""))
        schedule_rows.append(
            {
                "date": _clean(str(row.get("date") or "")),
                "date_text": _clean(str(row.get("date_text") or "")),
                "sport": _clean(str(row.get("sport") or "")),
                "match_type": _clean(str(row.get("vs") or "")).lower(),
                "home_game": _clean(str(row.get("vs") or "")).lower() == "vs",
                "opponent": opponent,
                "school_text": school_text,
                "time_text": _clean(str(row.get("time") or "")),
                "outcome_text": _clean(str(row.get("outcome") or "")),
                "team_score": score["team_score"],
                "opponent_score": score["opponent_score"],
                "result": score["result"],
            }
        )

    record_text = ""
    try:
        record_text = _clean(await page.locator(".schedule-results .record").first.inner_text())
    except Exception:  # noqa: BLE001
        record_text = ""
    record_match = re.search(
        r"Overall Record:\s*([0-9-]+),\s*League Record:\s*([0-9-]+)",
        record_text,
    )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "overall_record": record_match.group(1) if record_match else "",
        "league_record": record_match.group(2) if record_match else "",
        "schedule_rows": schedule_rows,
        "schedule_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "overall record", "league record", "vs", "at"),
        ),
    }


async def _collect_roster_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    years = await page.locator("#yearDropdown option").all_inner_texts()
    if not isinstance(years, list):
        years = []

    rows = await page.locator("table.rostertable tbody tr").evaluate_all(
        """rows => rows.map(row => Array.from(row.querySelectorAll('th,td')).map(cell => (
            (cell.innerText || '').replace(/\\s+/g, ' ').trim()
        )))""",
    )
    if not isinstance(rows, list):
        rows = []

    roster_players: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 4:
            continue
        number = _clean(str(row[1] or ""))
        name = _clean(str(row[2] or ""))
        position = _clean(str(row[3] or ""))
        if name.lower() == "name" or number.lower() == "number":
            continue
        if not name:
            continue
        if not number:
            continue
        roster_players.append(
            {
                "number": number,
                "name": name,
                "position": position,
            }
        )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "available_years": _dedupe_keep_order([_clean(str(year)) for year in years]),
        "roster_players": roster_players,
        "roster_count": len(roster_players),
        "roster_lines": _extract_keyword_lines(
            body_text,
            keywords=("roster", "number", "name", "position"),
        ),
    }


async def _collect_staff_directory(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    rows = await page.locator("table tr").evaluate_all(
        """rows => rows.map(row => Array.from(row.querySelectorAll('th,td')).map(cell => (
            (cell.innerText || '').replace(/\\s+/g, ' ').trim()
        )))""",
    )
    if not isinstance(rows, list):
        rows = []

    athletic_department_contacts: list[dict[str, str]] = []
    football_contacts: list[dict[str, str]] = []
    section = ""
    for row in rows:
        if not isinstance(row, list) or len(row) < 3:
            continue
        first = _clean(str(row[0] or ""))
        second = _clean(str(row[1] or ""))
        third = _clean(str(row[2] or ""))
        key = "|".join([first, second, third]).lower()

        if key == "name|title|email address":
            section = "department"
            continue
        if key == "head coach|sport|email address":
            if section == "department":
                section = "fall_sports"
            else:
                section = "other_sports"
            continue

        if section == "department":
            athletic_department_contacts.append(
                {
                    "name": first,
                    "title": second,
                    "email": third,
                }
            )
        elif section == "fall_sports" and "football" in second.lower():
            football_contacts.append(
                {
                    "name": first,
                    "sport": second,
                    "email": third,
                }
            )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "athletic_department_contacts": athletic_department_contacts,
        "football_contacts": football_contacts,
        "staff_directory_lines": _extract_keyword_lines(
            body_text,
            keywords=("athletic director", "football", "email address"),
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Classical Academy High's public football pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, dict[str, Any]] = {}

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
            for url, collector, key in [
                (SCHOOL_URL, _collect_school_page, "school"),
                (ATHLETICS_HOME_URL, _collect_athletics_home, "athletics_home"),
                (FOOTBALL_HOME_URL, _collect_football_home, "football_home"),
                (FOOTBALL_COACHES_URL, _collect_coaches_page, "coaches"),
                (FOOTBALL_SCHEDULE_URL, _collect_schedule_page, "schedule"),
                (FOOTBALL_ROSTER_URL, _collect_roster_page, "roster"),
                (STAFF_DIRECTORY_URL, _collect_staff_directory, "staff"),
            ]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1800)
                    page_data[key] = await collector(page)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_home = page_data.get("football_home", {})
    coaches_page = page_data.get("coaches", {})
    schedule_page = page_data.get("schedule", {})
    roster_page = page_data.get("roster", {})
    staff_page = page_data.get("staff", {})

    head_coach = coaches_page.get("head_coach", {})
    if not isinstance(head_coach, dict):
        head_coach = {}

    football_contacts = staff_page.get("football_contacts", [])
    if not isinstance(football_contacts, list):
        football_contacts = []

    athletic_department_contacts = staff_page.get("athletic_department_contacts", [])
    if not isinstance(athletic_department_contacts, list):
        athletic_department_contacts = []

    schedule_rows = schedule_page.get("schedule_rows", [])
    if not isinstance(schedule_rows, list):
        schedule_rows = []

    roster_players = roster_page.get("roster_players", [])
    if not isinstance(roster_players, list):
        roster_players = []

    football_levels = football_home.get("football_levels", [])
    if not isinstance(football_levels, list):
        football_levels = []

    football_program_available = bool(
        head_coach.get("name")
        or football_contacts
        or schedule_rows
        or roster_players
        or football_home.get("football_lines")
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_caimansports")

    football_contact_email = ""
    football_contact_name = ""
    if football_contacts:
        first = football_contacts[0]
        if isinstance(first, dict):
            football_contact_email = _clean(str(first.get("email") or ""))
            football_contact_name = _clean(str(first.get("name") or ""))

    athletic_director = {}
    for item in athletic_department_contacts:
        if not isinstance(item, dict):
            continue
        if "athletic director" in _clean(str(item.get("title") or "")).lower():
            athletic_director = {
                "name": _clean(str(item.get("name") or "")),
                "title": _clean(str(item.get("title") or "")),
                "email": _clean(str(item.get("email") or "")),
            }
            break

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_url": SCHOOL_URL,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "football_home_url": FOOTBALL_HOME_URL,
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "staff_directory_url": STAFF_DIRECTORY_URL,
        "football_levels": football_levels,
        "football_news_titles": football_home.get("football_news_titles", []),
        "head_coach": head_coach,
        "football_contact": {
            "name": football_contact_name,
            "email": football_contact_email,
        },
        "athletic_director": athletic_director,
        "football_contacts": football_contacts,
        "football_schedule_rows": schedule_rows,
        "football_schedule_count": len(schedule_rows),
        "football_overall_record": _clean(str(schedule_page.get("overall_record") or "")),
        "football_league_record": _clean(str(schedule_page.get("league_record") or "")),
        "football_roster_players": roster_players,
        "football_roster_count": len(roster_players),
        "football_roster_years": _dedupe_keep_order(
            (roster_page.get("available_years") or []) + (coaches_page.get("available_years") or [])
        ),
        "school_athletics_links": page_data.get("school", {}).get("athletics_links", []),
        "athletics_home_links": page_data.get("athletics_home", {}).get("football_links", []),
        "football_home_links": football_home.get("football_links", []),
        "football_program_evidence": _dedupe_keep_order(
            list(page_data.get("school", {}).get("athletics_lines", []) or [])
            + list(page_data.get("athletics_home", {}).get("football_lines", []) or [])
            + list(football_home.get("football_lines", []) or [])
            + [str(head_coach.get("name") or "")]
            + [str(contact.get("email") or "") for contact in football_contacts if isinstance(contact, dict)]
            + [str(row.get("opponent") or "") for row in schedule_rows if isinstance(row, dict)]
        ),
        "summary": (
            "Classical Academy High exposes a public Home Campus football portal with dedicated football, schedule, roster, coaches, and staff-directory pages."
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
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers"),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "target_urls": TARGET_URLS,
            "manual_navigation_steps": [
                "school_home",
                "athletics_home",
                "football_home",
                "football_coaches",
                "football_schedule",
                "football_roster",
                "staff_directory",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
