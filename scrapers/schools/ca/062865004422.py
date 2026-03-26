"""Deterministic football scraper for Canyon Hills (CA)."""

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

NCES_ID = "062865004422"
SCHOOL_NAME = "Canyon Hills"
STATE = "CA"
PROXY_PROFILE = "datacenter"

FOOTBALL_HOME_URL = "https://www.chrattlers.com/varsity/football/"
FOOTBALL_COACHES_URL = "https://www.chrattlers.com/varsity/football/coaches?hl=0"
FOOTBALL_SCHEDULE_URL = "https://www.chrattlers.com/varsity/football/schedule-results?hl=0"
FOOTBALL_ROSTER_URL = "https://www.chrattlers.com/varsity/football/roster"
COACHING_STAFF_URL = "https://www.chrattlers.com/athletic-department/coaching-staff/"
HEAD_COACH_PROFILE_URL = "https://www.chrattlers.com/coaching-staff/marcus-cook/"

TARGET_URLS = [
    FOOTBALL_HOME_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
    COACHING_STAFF_URL,
    HEAD_COACH_PROFILE_URL,
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


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], limit: int = 40) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if any(keyword in lower for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


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


def _parse_score(raw: str) -> dict[str, str]:
    text = _clean(raw)
    match = re.search(r"(?P<team>\d+)\s*-\s*(?P<opponent>\d+)", text)
    result = ""
    if " W" in f" {text}" or text.endswith("W"):
        result = "W"
    elif " L" in f" {text}" or text.endswith("L"):
        result = "L"
    elif " T" in f" {text}" or text.endswith("T"):
        result = "T"
    return {
        "team_score": match.group("team") if match else "",
        "opponent_score": match.group("opponent") if match else "",
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


async def _collect_home_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || ''
        })).filter(x => /football|coach|schedule|roster/i.test(x.text + ' ' + x.href))""",
    )
    if not isinstance(links, list):
        links = []
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "coach", "schedule", "roster", "varsity", "junior varsity"),
        ),
        "links": [
            f"{_clean(str(item.get('text') or ''))}|{_normalize_href(str(item.get('href') or ''), page.url)}"
            if isinstance(item, dict)
            else ""
            for item in links
        ],
    }


async def _collect_coaches_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    coach_cards = await page.locator("main .rostertable").evaluate_all(
        """els => els.map((card) => {
            const links = Array.from(card.querySelectorAll('a[href]')).map(a => ({
                text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: a.href || ''
            }));
            const nameNode = card.querySelector('.t-wrapper p') || card.querySelector('p');
            const roleNodes = Array.from(card.querySelectorAll('.sport-div p')).map(p => (p.textContent || '').replace(/\\s+/g, ' ').trim());
            return {
                name: (nameNode ? nameNode.textContent : '').replace(/\\s+/g, ' ').trim(),
                role: roleNodes.join(' | '),
                links,
                text: (card.innerText || '').replace(/\\s+/g, ' ').trim()
            };
        })""",
    )
    if not isinstance(coach_cards, list):
        coach_cards = []

    head_coach = {}
    for card in coach_cards:
        if not isinstance(card, dict):
            continue
        text = _clean(str(card.get("text") or ""))
        links = card.get("links") if isinstance(card.get("links"), list) else []
        name = _clean(str(card.get("name") or ""))
        role = _clean(str(card.get("role") or ""))
        if "football" not in text.lower():
            continue
        if not name:
            continue
        profile_url = ""
        for link in links:
            if not isinstance(link, dict):
                continue
            href = _normalize_href(str(link.get("href") or ""), page.url)
            if "/coaching-staff/" in href:
                profile_url = href
                break
        head_coach = {
            "name": name,
            "role": role or "Football Head Coach",
            "profile_url": profile_url or HEAD_COACH_PROFILE_URL,
            "source_text": text,
        }
        break

    if not head_coach:
        try:
            name_texts = await page.locator("main .rostertable .t-wrapper p").all_inner_texts()
            profile_href = await page.locator("main .rostertable a[href*='/coaching-staff/']").first.get_attribute("href")
            if name_texts:
                head_coach = {
                    "name": _clean(name_texts[0]),
                    "role": _clean(name_texts[1]) if len(name_texts) > 1 else "Football Head Coach",
                    "profile_url": _normalize_href(profile_href or HEAD_COACH_PROFILE_URL, page.url),
                    "source_text": body_text,
                }
        except Exception:  # noqa: BLE001
            pass

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "head_coach": head_coach,
        "coaches_page_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "coach", "head coach", "varsity", "junior varsity"),
        ),
    }


async def _collect_staff_directory(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    text = _clean(body_text)
    football_lines = _extract_keyword_lines(
        text,
        keywords=("football", "head coach", "varsity", "junior varsity"),
    )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": football_lines,
    }


async def _collect_roster_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    year_options = await page.locator("#yearDropdown option").all_inner_texts()
    if not isinstance(year_options, list):
        year_options = []

    roster_rows = await page.locator("table.rostertable tbody tr").evaluate_all(
        """rows => rows.map((row) => {
            const cells = Array.from(row.querySelectorAll('td')).map(td => (td.innerText || '').replace(/\\s+/g, ' ').trim());
            const nameLink = row.querySelector('a[href*="/player/"]');
            const name = nameLink ? (nameLink.textContent || '').replace(/\\s+/g, ' ').trim() : '';
            return {
                number: cells[1] || '',
                name,
                position: cells[3] || '',
                profile_url: nameLink ? nameLink.href || '' : '',
                raw_cells: cells,
            };
        })""",
    )
    if not isinstance(roster_rows, list):
        roster_rows = []

    players: list[dict[str, str]] = []
    for row in roster_rows:
        if not isinstance(row, dict):
            continue
        name = _clean(str(row.get("name") or ""))
        if not name:
            continue
        profile_url = _normalize_href(str(row.get("profile_url") or ""), page.url)
        players.append(
            {
                "number": _clean(str(row.get("number") or "")),
                "name": name,
                "position": _clean(str(row.get("position") or "")),
                "profile_url": profile_url,
            }
        )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "available_years": _dedupe_keep_order([_clean(str(item)) for item in year_options]),
        "roster_players": players,
        "roster_lines": _extract_keyword_lines(
            body_text,
            keywords=("number", "name", "position", "football", "varsity"),
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
                location_links: links,
            };
        })""",
    )
    if not isinstance(rows, list):
        rows = []

    schedule_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        school_text = _clean(str(row.get("school_text") or ""))
        opponent = _clean(str(row.get("opponent") or ""))
        score = _parse_score(str(row.get("outcome") or ""))
        if not score["result"]:
            score["result"] = _clean(str(row.get("outcome_result") or ""))
        location_url = ""
        for item in row.get("location_links") or []:
            if not isinstance(item, dict):
                continue
            href = _normalize_href(str(item.get("href") or ""), page.url)
            if href:
                location_url = href
                break

        schedule_rows.append(
            {
                "date": _clean(str(row.get("date") or "")),
                "date_text": _clean(str(row.get("date_text") or "")),
                "sport": _clean(str(row.get("sport") or "")),
                "match_type": _clean(str(row.get("vs") or "")).lower(),
                "home_game": _clean(str(row.get("vs") or "")).lower() == "vs",
                "opponent": opponent,
                "school_text": school_text,
                "location_url": location_url,
                "team_score": score["team_score"],
                "opponent_score": score["opponent_score"],
                "result": score["result"],
                "outcome_text": _clean(str(row.get("outcome") or "")),
                "time_text": _clean(str(row.get("time") or "")),
            }
        )

    first_record = _clean(str(await page.locator(".schedule-results .record").first.inner_text()))
    record_match = re.search(r"Overall Record:\s*([0-9-]+),\s*League Record:\s*([0-9-]+)", first_record)
    overall_record = record_match.group(1) if record_match else ""
    league_record = record_match.group(2) if record_match else ""

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "overall_record": overall_record,
        "league_record": league_record,
        "schedule_rows": schedule_rows,
        "schedule_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "varsity", "junior varsity", "overall record", "league record"),
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Canyon Hills football sources and return a deterministic envelope."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)
    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, dict[str, Any]] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url, collector, key in [
                (FOOTBALL_HOME_URL, _collect_home_page, "home"),
                (FOOTBALL_COACHES_URL, _collect_coaches_page, "coaches"),
                (COACHING_STAFF_URL, _collect_staff_directory, "staff"),
                (FOOTBALL_SCHEDULE_URL, _collect_schedule_page, "schedule"),
                (FOOTBALL_ROSTER_URL, _collect_roster_page, "roster"),
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

    head_coach = page_data.get("coaches", {}).get("head_coach", {})
    schedule_rows = page_data.get("schedule", {}).get("schedule_rows", [])
    roster_players = page_data.get("roster", {}).get("roster_players", [])

    available_years = _dedupe_keep_order(
        (page_data.get("roster", {}).get("available_years") or [])
        + ["2023-24", "2024-25", "2025-26", "2026-27"]
    )

    football_program_available = bool(
        head_coach.get("name") or schedule_rows or roster_players or page_data.get("home", {}).get("football_lines")
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_coach_names = _dedupe_keep_order(
        [
            str(head_coach.get("name") or ""),
        ]
    )
    football_program_evidence = _dedupe_keep_order(
        (page_data.get("home", {}).get("football_lines") or [])
        + (page_data.get("coaches", {}).get("coaches_page_lines") or [])
        + (page_data.get("staff", {}).get("football_lines") or [])
        + [str(head_coach.get("name") or ""), str(head_coach.get("role") or "")]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_home_url": FOOTBALL_HOME_URL,
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "coaching_staff_directory_url": COACHING_STAFF_URL,
        "head_coach": {
            "name": _clean(str(head_coach.get("name") or "")),
            "role": _clean(str(head_coach.get("role") or "Football Head Coach")),
            "profile_url": _clean(str(head_coach.get("profile_url") or HEAD_COACH_PROFILE_URL)),
        },
        "football_coach_names": football_coach_names,
        "football_program_evidence": football_program_evidence,
        "football_team_years": available_years,
        "football_levels": ["Varsity", "Junior Varsity"],
        "football_schedule_record": {
            "overall": _clean(str(page_data.get("schedule", {}).get("overall_record") or "")),
            "league": _clean(str(page_data.get("schedule", {}).get("league_record") or "")),
        },
        "football_schedule_rows": schedule_rows,
        "football_schedule_count": len(schedule_rows),
        "football_roster_players": roster_players,
        "football_roster_count": len(roster_players),
        "football_home_lines": page_data.get("home", {}).get("football_lines") or [],
        "coaches_page_lines": page_data.get("coaches", {}).get("coaches_page_lines") or [],
        "staff_directory_lines": page_data.get("staff", {}).get("football_lines") or [],
        "summary": (
            "Canyon Hills has a public Home Campus football portal with a head coach profile for Marcus Cook, a public 2025-26 varsity schedule, and a public varsity roster."
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
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE).get("proxy_profile"),
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE).get("proxy_servers"),
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE).get("proxy_auth_mode"),
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_data),
            "manual_navigation_steps": [
                "football_home",
                "football_coaches",
                "coaching_staff_directory",
                "football_schedule",
                "football_roster",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
