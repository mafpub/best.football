"""Deterministic football scraper for Desert Hot Springs High (CA)."""

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

NCES_ID = "062955008267"
SCHOOL_NAME = "Desert Hot Springs High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_HOME_URL = "https://www.talonsdown.com/"
FOOTBALL_SCHEDULE_URL = "https://www.talonsdown.com/sport/football/boys/?tab=schedule"
FOOTBALL_ROSTER_URL = "https://www.talonsdown.com/sport/football/boys/?tab=roster"
FOOTBALL_STAFF_URL = "https://www.talonsdown.com/sport/football/boys/?tab=staff"
FOOTBALL_NEWS_URL = "https://www.talonsdown.com/sport/football/boys/?tab=news"

TARGET_URLS = [
    ATHLETICS_HOME_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_STAFF_URL,
    FOOTBALL_NEWS_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_TERMS = (
    "football",
    "boys varsity football",
    "varsity football",
    "head coach",
    "running backs",
    "quarterbacks",
    "defensive backs",
    "defensive line",
    "linebackers",
    "season signups",
    "home campus",
    "clearance",
)

DATE_TIME_RE = re.compile(r"^[A-Z][a-z]{2}\s+\d{1,2}\s*/\s*\d{1,2}:\d{2}\s+PM$")
ROSTER_ENTRY_RE = re.compile(r"^(?P<number>\d+)\s+(?P<name>.+)$")
ROLE_RE = re.compile(r"^Role:\s*(?P<role>.+)$", re.IGNORECASE)
GRADE_WORDS = {"FRESHMAN", "SOPHOMORE", "JUNIOR", "SENIOR"}
IGNORED_NAME_LINES = {
    "#  FOOTBALL",
    "SCHEDULE",
    "ROSTER",
    "COACHES",
    "NEWS",
    "ANNOUNCEMENTS",
    "FORMS",
    "LINKS",
    "NUMBER ATHLETES GRADE",
    "Varsity",
    "Football",
    "THE OFFICIAL SITE OF",
    "Desert Hot Springs High School Athletics",
}


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _body_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines


def _extract_links(link_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    kept: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in link_rows:
        text = _clean(str(item.get("text") or ""))
        href = str(item.get("href") or "").strip()
        if not text or not href:
            continue
        combo = f"{text} {href}".lower()
        if not any(term in combo for term in FOOTBALL_TERMS):
            continue
        key = f"{text}|{href}"
        if key in seen:
            continue
        seen.add(key)
        kept.append({"text": text, "href": href})
    return kept


def _extract_page_signal(page_text: str, links: list[dict[str, str]], requested_url: str, final_url: str) -> dict[str, Any]:
    lines = _body_lines(page_text)
    football_lines = [
        line
        for line in lines
        if any(term in line.lower() for term in FOOTBALL_TERMS)
    ]
    return {
        "requested_url": requested_url,
        "final_url": final_url,
        "lines": _dedupe_keep_order(football_lines),
        "links": _extract_links([item for item in links if isinstance(item, dict)]),
    }


def _parse_schedule_entries(lines: list[str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if not DATE_TIME_RE.match(line):
            i += 1
            continue

        entry: dict[str, str] = {"date_time": line}
        block: list[str] = []
        j = i + 1
        while j < len(lines):
            candidate = lines[j]
            if DATE_TIME_RE.match(candidate) or candidate in {"ANNOUNCEMENTS", "FORMS", "LINKS"}:
                break
            block.append(candidate)
            j += 1

        for candidate in block:
            if candidate.startswith("AT ") or candidate.startswith("VS "):
                entry["matchup_line"] = candidate
            elif re.match(r"^[WL]\s+\d+\s-\s+\d+$", candidate):
                entry["result"] = candidate
            elif candidate.startswith("No practice schedule"):
                entry["practice_note"] = candidate

        if block:
            entry["details"] = " | ".join(block[:5])
        entries.append(entry)
        i = j
    return entries


def _parse_roster_entries(lines: list[str]) -> list[dict[str, str]]:
    players: list[dict[str, str]] = []
    i = 0
    while i < len(lines):
        match = ROSTER_ENTRY_RE.match(lines[i])
        if not match or lines[i] in IGNORED_NAME_LINES:
            i += 1
            continue

        number = match.group("number").strip()
        name = _clean(match.group("name"))
        if not name or name in IGNORED_NAME_LINES:
            i += 1
            continue

        position = ""
        grade = ""
        j = i + 1
        while j < len(lines):
            candidate = lines[j]
            if ROSTER_ENTRY_RE.match(candidate) and candidate != lines[i]:
                break
            if candidate.startswith("####"):
                position = _clean(candidate.lstrip("#").strip())
            elif candidate.upper() in GRADE_WORDS:
                grade = candidate.title()
            elif candidate in {"ANNOUNCEMENTS", "FORMS", "LINKS"}:
                break
            j += 1

        players.append(
            {
                "number": number,
                "name": name,
                "position": position,
                "grade": grade,
            }
        )
        i = j
    return players


def _parse_staff_entries(lines: list[str]) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for idx, line in enumerate(lines):
        role_match = ROLE_RE.match(line)
        if not role_match:
            continue

        role = _clean(role_match.group("role"))
        name = ""
        for back in range(idx - 1, max(-1, idx - 5), -1):
            candidate = lines[back]
            if candidate.startswith("Image:") or candidate in IGNORED_NAME_LINES:
                continue
            if candidate.startswith("Role:"):
                continue
            name = candidate
            break
        if not name:
            continue

        key = (name, role)
        if key in seen:
            continue
        seen.add(key)
        coaches.append({"name": name, "role": role})
    return coaches


def _football_signals(all_lines: list[str]) -> list[str]:
    return _dedupe_keep_order(
        [line for line in all_lines if any(term in line.lower() for term in FOOTBALL_TERMS)]
    )


async def scrape_school() -> dict[str, Any]:
    """Scrape the public Desert Hot Springs football program pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=proxy,
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    body_text = await page.locator("body").inner_text()
                    link_rows = await page.evaluate(
                        """() => Array.from(document.querySelectorAll('a[href]')).map((anchor) => ({
                            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
                            href: anchor.href || '',
                        }))"""
                    )
                    if not isinstance(link_rows, list):
                        link_rows = []
                    page_signals.append(
                        _extract_page_signal(body_text, link_rows, url, page.url)
                    )
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_lines: list[str] = []
    all_links: list[dict[str, str]] = []
    schedule_lines: list[str] = []
    roster_lines: list[str] = []
    staff_lines: list[str] = []

    for signal in page_signals:
        lines = [line for line in signal.get("lines", []) if isinstance(line, str)]
        links = [
            item
            for item in signal.get("links", [])
            if isinstance(item, dict)
        ]
        all_lines.extend(lines)
        all_links.extend(links)
        if signal.get("final_url") == FOOTBALL_SCHEDULE_URL:
            schedule_lines = lines
        elif signal.get("final_url") == FOOTBALL_ROSTER_URL:
            roster_lines = lines
        elif signal.get("final_url") == FOOTBALL_STAFF_URL:
            staff_lines = lines

    all_lines = _dedupe_keep_order(all_lines)
    all_links = _extract_links(all_links)

    football_schedule_entries = _parse_schedule_entries(schedule_lines)
    football_roster = _parse_roster_entries(roster_lines)
    football_staff = _parse_staff_entries(staff_lines)
    football_team_names = _dedupe_keep_order(
        [
            "Football",
            "Boys Varsity Football" if any("boys varsity football" in line.lower() for line in all_lines) else "",
            "Varsity Football" if any(line == "Varsity" for line in schedule_lines) else "",
        ]
    )

    football_program_available = any(
        [
            football_schedule_entries,
            football_roster,
            football_staff,
            any("football" in line.lower() for line in all_lines),
        ]
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_talonsdown_pages")

    football_links = _dedupe_keep_order(
        [f"{item['text']}|{item['href']}" for item in all_links]
    )
    football_links = _dedupe_keep_order(football_links)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "football_staff_url": FOOTBALL_STAFF_URL,
        "football_news_url": FOOTBALL_NEWS_URL,
        "football_team_names": football_team_names,
        "football_schedule_entries": football_schedule_entries,
        "football_roster": football_roster,
        "football_staff": football_staff,
        "football_links": football_links,
        "home_page_signals": _football_signals(
            [line for signal in page_signals if signal.get("final_url") == ATHLETICS_HOME_URL for line in signal.get("lines", [])]
        ),
        "schedule_page_signals": _football_signals(schedule_lines),
        "roster_page_signals": _football_signals(roster_lines),
        "staff_page_signals": _football_signals(staff_lines),
        "summary": (
            "Desert Hot Springs High publishes a public football program site on Talons Down with a football homepage, schedule tab, roster tab, and staff tab including coach names and roles."
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
            "script_version": "1.0.0",
            "proxy_profile": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_auth_mode"],
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "athletics_home",
                "football_schedule_tab",
                "football_roster_tab",
                "football_staff_tab",
                "football_news_tab",
            ],
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import json
    import asyncio

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True))
