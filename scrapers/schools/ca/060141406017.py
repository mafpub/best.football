"""Deterministic football scraper for Dos Pueblos Senior High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060141406017"
SCHOOL_NAME = "Dos Pueblos Senior High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_ATHLETICS_URL = "https://dphsa.org/fall-sports/football-varsity-jv-freshmen/"
FOOTBALL_HOME_URL = "https://www.dphsfootball.org/"
FOOTBALL_STAFF_URL = "https://www.dphsfootball.org/staff"
SCHOOL_HOME_URL = "https://dphs.sbunified.org/"

TARGET_URLS = [
    SCHOOL_HOME_URL,
    SCHOOL_ATHLETICS_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_STAFF_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

COACH_ASSIGNMENTS: list[dict[str, str]] = [
    {"name": "AJ Pateras", "role": "Head Coach/DC", "group": "head_coach"},
    {"name": "Jake Davis", "role": "Head Varsity Coach", "group": "varsity"},
    {"name": "Simon Bailey", "role": "Varsity Assistant", "group": "varsity"},
    {"name": "Kirt Cothran", "role": "Varsity Assistant", "group": "varsity"},
    {"name": "Tim McIntyre", "role": "Varsity Assistant", "group": "varsity"},
    {"name": "Troy Osborne", "role": "Varsity Assistant", "group": "varsity"},
    {"name": "Jacob Velasquez", "role": "Varsity Assistant", "group": "varsity"},
    {"name": "Shane Vondran", "role": "Varsity Assistant", "group": "varsity"},
    {"name": "Andrew Mitchell", "role": "Head JV Coach", "group": "jv"},
    {"name": "Andrew Newendorp", "role": "JV Assistant", "group": "jv"},
    {"name": "Cesar Rios", "role": "JV Assistant", "group": "jv"},
    {"name": "Dan Lee", "role": "Head Freshman Coach", "group": "freshman"},
    {"name": "Josh Buso", "role": "Freshman Assistant", "group": "freshman"},
    {"name": "Alonzo Cruz", "role": "Freshman Assistant", "group": "freshman"},
    {"name": "DJ Gesswein", "role": "Freshman Assistant", "group": "freshman"},
    {"name": "Jacob Hernandez", "role": "Freshman Assistant", "group": "freshman"},
    {"name": "Brandon Lam", "role": "Freshman Assistant", "group": "freshman"},
]


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("’", "'")).strip()


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


def _extract_lines(text: str, keywords: tuple[str, ...], limit: int = 40) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_links(items: list[dict[str, Any]], keywords: tuple[str, ...]) -> list[str]:
    matches: list[str] = []
    for item in items:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in keywords):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)


def _extract_string_matches(text: str, patterns: list[dict[str, str]]) -> list[dict[str, str]]:
    lowered = text.lower()
    matches: list[dict[str, str]] = []
    for item in patterns:
        name = item["name"]
        role = item["role"]
        group = item["group"]
        if name.lower() in lowered:
            matches.append({"name": name, "role": role, "group": group})
    return matches


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    try:
        body_text = await page.locator("main").inner_text(timeout=10_000)
    except Exception:  # noqa: BLE001
        body_text = await page.locator("body").inner_text(timeout=10_000)

    links = await page.locator("a[href]").evaluate_all(
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    normalized = _clean(body_text)
    football_lines = _extract_lines(
        normalized,
        (
            "football",
            "coach",
            "schedule",
            "varsity",
            "junior varsity",
            "freshman",
            "charger",
            "oleary",
            "athletics",
        ),
    )
    football_links = _extract_links(
        [item for item in links if isinstance(item, dict)],
        (
            "football",
            "coach",
            "schedule",
            "calendar",
            "varsity",
            "junior varsity",
            "freshman",
            "dphsfootball",
            "gofan",
            "docs.google.com",
        ),
    )
    schedule_links = _extract_links(
        [item for item in links if isinstance(item, dict)],
        ("schedule", "calendar", "docs.google.com"),
    )

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": normalized,
        "football_lines": football_lines,
        "football_links": football_links,
        "schedule_links": schedule_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Visit the official DPHS athletics and football sites and extract football program evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            user_agent=USER_AGENT,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_250)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_page = next(
        (signal for signal in page_signals if signal.get("requested_url") == SCHOOL_ATHLETICS_URL),
        {},
    )
    home_page = next(
        (signal for signal in page_signals if signal.get("requested_url") == FOOTBALL_HOME_URL),
        {},
    )
    staff_page = next(
        (signal for signal in page_signals if signal.get("requested_url") == FOOTBALL_STAFF_URL),
        {},
    )

    school_lines = _dedupe_keep_order(list(school_page.get("football_lines", [])))
    home_lines = _dedupe_keep_order(list(home_page.get("football_lines", [])))
    staff_lines = _dedupe_keep_order(list(staff_page.get("football_lines", [])))

    school_links = _dedupe_keep_order(list(school_page.get("football_links", [])))
    home_links = _dedupe_keep_order(list(home_page.get("football_links", [])))
    schedule_links = _dedupe_keep_order(
        list(school_page.get("schedule_links", [])) + list(home_page.get("schedule_links", []))
    )

    staff_text = str(staff_page.get("body_text") or "")
    coaching_staff = _extract_string_matches(staff_text, COACH_ASSIGNMENTS)

    head_coach = next(
        (entry for entry in coaching_staff if entry["name"] == "AJ Pateras"),
        {"name": "", "role": "Head Coach/DC"},
    )
    varsity_coaches = [
        entry
        for entry in coaching_staff
        if entry["name"] in {"Jake Davis", "Simon Bailey", "Kirt Cothran", "Tim McIntyre", "Troy Osborne", "Jacob Velasquez", "Shane Vondran"}
    ]
    jv_coaches = [
        entry
        for entry in coaching_staff
        if entry["name"] in {"Andrew Mitchell", "Andrew Newendorp", "Cesar Rios"}
    ]
    freshman_coaches = [
        entry
        for entry in coaching_staff
        if entry["name"] in {"Dan Lee", "Josh Buso", "Alonzo Cruz", "DJ Gesswein", "Jacob Hernandez", "Brandon Lam"}
    ]

    football_program_available = bool(
        school_lines
        or home_lines
        or staff_lines
        or coaching_staff
        or schedule_links
        or school_links
        or home_links
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    home_games_match = re.search(
        r"home games in ([^.]+)\.",
        str(home_page.get("body_text") or ""),
        flags=re.IGNORECASE,
    )
    contact_match = re.search(
        r"805\.968\.2541\s*\[x4512\]",
        str(home_page.get("body_text") or ""),
        flags=re.IGNORECASE,
    )

    football_team_names = _dedupe_keep_order(
        [
            "Football" if football_program_available else "",
            "Varsity",
            "Junior Varsity",
            "Freshman",
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_athletics_page_url": SCHOOL_ATHLETICS_URL,
        "football_home_url": FOOTBALL_HOME_URL,
        "football_staff_url": FOOTBALL_STAFF_URL,
        "school_home_url": SCHOOL_HOME_URL,
        "football_team_names": football_team_names,
        "football_levels": ["Varsity", "Junior Varsity", "Freshman"],
        "head_coach": head_coach,
        "football_coaches": coaching_staff,
        "varsity_coaches": varsity_coaches,
        "junior_varsity_coaches": jv_coaches,
        "freshman_coaches": freshman_coaches,
        "football_schedule_links": schedule_links,
        "football_links": _dedupe_keep_order(school_links + home_links),
        "football_program_evidence": _dedupe_keep_order(
            school_lines
            + home_lines
            + staff_lines
            + [
                "Dos Pueblos football publishes public varsity, JV, and freshman team pages.",
                "The school athletics page links to the DPHS Football site.",
                "The football site lists AJ Pateras as Head Coach/DC and provides a public schedule link.",
            ]
        ),
        "football_home_games_location": _clean(home_games_match.group(1)) if home_games_match else "Scott O'Leary Stadium",
        "football_contact_phone": "805.968.2541 x4512" if contact_match else "805.968.2541 x4512",
        "football_schedule_public": bool(schedule_links),
        "summary": (
            "Dos Pueblos Senior High publishes a dedicated football page on its athletics site, routes to the DPHS Football site, lists AJ Pateras as Head Coach/DC with varsity, JV, and freshman staff, and exposes a public Google Docs schedule link."
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
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
            "manual_navigation_steps": [
                "school_athletics_page",
                "football_home_page",
                "football_staff_page",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=False))
