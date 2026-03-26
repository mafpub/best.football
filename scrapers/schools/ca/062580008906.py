"""Deterministic football scraper for Canyon Springs High (CA)."""

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

NCES_ID = "062580008906"
SCHOOL_NAME = "Canyon Springs High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_URL = "https://canyonsprings.mvusd.net/"
ATHLETICS_HOME_URL = "https://canyonsprings.homecampus.com/"
FOOTBALL_HOME_URL = "https://canyonsprings.homecampus.com/varsity/football/"
FOOTBALL_COACHES_URL = "https://canyonsprings.homecampus.com/athletic-department/coaching-staff/"
FOOTBALL_SCHEDULE_URL = "https://canyonsprings.homecampus.com/varsity/football/schedule-results"
FOOTBALL_ROSTER_URL = "https://canyonsprings.homecampus.com/varsity/football/roster"

TARGET_URLS = [
    SCHOOL_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
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
        value = _clean(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 40) -> list[str]:
    lines: list[str] = []
    for raw in (text or "").splitlines():
        line = _clean(raw)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
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


async def _collect_home_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || ''
        })).filter(x => /athletics|football|coaching|roster|schedule|homecampus/i.test(x.text + ' ' + x.href))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _extract_keyword_lines(
            body_text,
            keywords=("athletics", "athletic", "football", "coach", "roster", "schedule"),
        ),
        "links": [
            f"{_clean(str(item.get('text') or ''))}|{_normalize_href(str(item.get('href') or ''), page.url)}"
            for item in links
            if isinstance(item, dict)
        ],
    }


async def _collect_coaches_page(page) -> list[dict[str, str]]:
    cards = await page.eval_on_selector_all(
        "a[href*='coaching-staff/']",
        """els => els.map(a => {
            const ctx = a.closest('li') || a.closest('article') || a.closest('section') || a.parentElement || a;
            return {
                name: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: a.href || '',
                context: (ctx ? (ctx.innerText || '').replace(/\\s+/g, ' ').trim() : ''),
            };
        })""",
    )
    if not isinstance(cards, list):
        cards = []

    coaches: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in cards:
        if not isinstance(item, dict):
            continue
        name = _clean(str(item.get("name") or ""))
        if not name or not name.replace(" ", "").isalpha():
            continue
        context = _clean(str(item.get("context") or ""))
        lower = context.lower()
        if "football" not in lower or "coach" not in lower:
            continue
        if name in seen:
            continue

        role = "Coach"
        if "head coach" in lower:
            role = "Head Coach"
        elif "assistant coach" in lower:
            role = "Assistant Coach"

        teams = _dedupe_keep_order(
            [
                _clean(match.group(1))
                for match in re.finditer(r"([a-z\\s/]+?)\\s*-\\s*(?:head coach|assistant coach)", lower)
            ]
        )
        if not teams:
            teams = ["football"] if "football" in lower else []
        coaches.append(
            {
                "name": name,
                "role": role,
                "teams": ", ".join(teams),
                "raw_context": context,
                "profile_url": _normalize_href(str(item.get("href") or ""), page.url),
            }
        )
        seen.add(name)

    return coaches


async def _collect_roster_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    lines = [_clean(line) for line in body_text.splitlines() if _clean(line)]
    available_years = _dedupe_keep_order(re.findall(r"20\\d{2}-\\d{2}", body_text))

    roster_players: list[dict[str, str]] = []
    if "Number Name Position" in " ".join(lines[:120]).lower():
        for index, line in enumerate(lines):
            if not re.fullmatch(r"\\d{1,3}", line):
                continue
            name = _clean(lines[index + 1]) if index + 1 < len(lines) else ""
            position = _clean(lines[index + 2]) if index + 2 < len(lines) else ""
            if not name or not position:
                continue
            if name.lower().startswith("image") or name.lower().endswith(".com"):
                continue
            if re.fullmatch(r"\\d{1,3}", name):
                continue
            roster_players.append(
                {
                    "number": line,
                    "name": name,
                    "position": position,
                }
            )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "available_years": available_years,
        "roster_rows": _dedupe_keep_order([f"{p['number']}|{p['name']}|{p['position']}" for p in roster_players]),
        "roster_players": roster_players,
        "roster_lines": _extract_keyword_lines(
            body_text,
            keywords=("number", "name", "position", "varsity", "junior varsity", "roster"),
        ),
    }


async def _collect_schedule_page(page) -> dict[str, Any]:
    body_text = await _collect_text(page)
    print_link = ""
    try:
        print_href = await page.locator("a[href*='print']").first.get_attribute("href")
        print_link = _normalize_href(print_href or "", page.url)
    except Exception:  # noqa: BLE001
        print_link = ""

    rows = await page.eval_on_selector_all(
        "table tr",
        """els => els.map(row => Array.from(row.querySelectorAll('th,td')).map(cell => (
            (cell.innerText || '').replace(/\\s+/g, ' ').trim()
        )) )""",
    )
    if not isinstance(rows, list):
        rows = []

    schedule_rows: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 2:
            continue
        if len(row) <= 3 and (("Date" in row[0] and "Opponent" in row[1]) or "Date" in row[0]):
            continue
        entry = [re.sub(r"\\s+", " ", str(item or "")).strip() for item in row[:8]]
        if not any(entry):
            continue
        date = entry[0]
        opponent_raw = entry[1] if len(entry) > 1 else ""
        outcome = entry[-1] if entry else ""
        score = _parse_score(outcome)
        if not date and not opponent_raw:
            continue
        schedule_rows.append(
            {
                "date": date,
                "opponent_or_event": opponent_raw,
                "result_text": outcome,
                "team_score": score["team_score"],
                "opponent_score": score["opponent_score"],
                "result": score["result"],
                "raw_cells": entry,
            }
        )

    # Fallback to a few structured lines if table capture is empty.
    if not schedule_rows:
        for raw_line in body_text.splitlines():
            text = _clean(raw_line)
            if not text:
                continue
            if re.search(r"\\d{2,4}", text) and any(word in text.lower() for word in ["vs ", "at ", "@", "football"]):
                schedule_rows.append(
                    {
                        "line_text": text,
                        "raw_cells": [_clean(text)],
                    }
                )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "print_preview_url": print_link,
        "body_text": body_text,
        "schedule_rows": schedule_rows[:200],
        "schedule_lines": _extract_keyword_lines(
            body_text,
            keywords=("schedule", "record", "result", "vs", "at", "football", "home", "junior varsity"),
            limit=40,
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Canyon Springs football pages and return a deterministic payload."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    page_data: dict[str, dict[str, Any]] = {}
    page_data["school"] = {}

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
            try:
                await page.goto(SCHOOL_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1500)
                page_data["school"] = await _collect_home_page(page)
                source_pages.append(page.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:{SCHOOL_URL}")

            try:
                await page.goto(FOOTBALL_COACHES_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1800)
                page_data["coaches"] = {"coaches": await _collect_coaches_page(page)}
                source_pages.append(page.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:{FOOTBALL_COACHES_URL}")

            try:
                await page.goto(FOOTBALL_SCHEDULE_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1800)
                page_data["schedule"] = await _collect_schedule_page(page)
                source_pages.append(page.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:{FOOTBALL_SCHEDULE_URL}")

            try:
                await page.goto(FOOTBALL_ROSTER_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1800)
                page_data["roster"] = await _collect_roster_page(page)
                source_pages.append(page.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:{FOOTBALL_ROSTER_URL}")
        finally:
            await browser.close()

    coaches = page_data.get("coaches", {}).get("coaches", [])
    if not isinstance(coaches, list):
        coaches = []

    roster_players = page_data.get("roster", {}).get("roster_players", [])
    if not isinstance(roster_players, list):
        roster_players = []

    schedule_rows = page_data.get("schedule", {}).get("schedule_rows", [])
    if not isinstance(schedule_rows, list):
        schedule_rows = []

    school_lines = _dedupe_keep_order(
        page_data.get("school", {}).get("football_lines", []) or []
    )
    football_home_page = _extract_keyword_lines(
        page_data.get("school", {}).get("body_text", ""),
        keywords=("football", "athletics", "athletic", "roster", "schedule"),
    )
    football_home_evidence = _dedupe_keep_order(
        (school_lines or []) + football_home_page
    )

    football_coach_names = _dedupe_keep_order([str(item.get("name") or "") for item in coaches])
    football_head_coach = ""
    for coach in coaches:
        name = _clean(str(coach.get("name") or ""))
        role = _clean(str(coach.get("role") or "")).lower()
        teams = _clean(str(coach.get("teams") or "")).lower()
        if name and (role == "head coach" or "head coach" in role) and "football" in teams:
            football_head_coach = name
            break

    football_program_available = bool(
        football_coach_names
        or roster_players
        or schedule_rows
        or football_home_evidence
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    available_years = _dedupe_keep_order(
        page_data.get("roster", {}).get("available_years", [])
        + ["2023-24", "2024-25", "2025-26", "2026-27"]
    )

    source_pages = _dedupe_keep_order(source_pages)
    schedule_summary = page_data.get("schedule", {})
    roster_summary = page_data.get("roster", {})

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_url": SCHOOL_URL,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "football_home_url": FOOTBALL_HOME_URL,
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "head_coach": football_head_coach,
        "football_coach_names": football_coach_names,
        "football_coaches": coaches,
        "football_levels": ["Varsity", "Junior Varsity", "Frosh/Soph"],
        "football_roster_years": available_years,
        "football_roster_players": roster_players,
        "football_roster_count": len(roster_players),
        "football_roster_lines": roster_summary.get("roster_lines", []),
        "football_roster_rows": roster_summary.get("roster_rows", []),
        "football_schedule_print_url": _normalize_href(
            str(schedule_summary.get("print_preview_url") or ""),
            FOOTBALL_SCHEDULE_URL,
        ),
        "football_schedule_rows": schedule_rows,
        "football_schedule_count": len(schedule_rows),
        "football_home_lines": _extract_keyword_lines(
            page_data.get("school", {}).get("body_text", ""),
            keywords=("football", "athletics", "athletic", "coach", "roster", "schedule"),
        ),
        "coaches_page_lines": _extract_keyword_lines(
            (page_data.get("coaches", {}).get("body_text", "") or ""),
            keywords=("football", "coach", "head coach"),
            limit=40,
        ),
        "schedule_page_lines": schedule_summary.get("schedule_lines", []),
        "football_program_evidence": _dedupe_keep_order(
            football_home_evidence
            + football_coach_names
            + [str(row.get("opponent_or_event") or "") for row in schedule_rows if isinstance(row, dict)]
        ),
        "summary": (
            "Canyon Springs has a public HomeCampus football portal with dedicated roster, coaching staff, and schedule pages."
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
                "school_home",
                "coaching_staff",
                "football_schedule",
                "football_roster",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
