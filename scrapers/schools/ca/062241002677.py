"""Deterministic football scraper for Cabrillo High School (CA)."""

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

NCES_ID = "062241002677"
SCHOOL_NAME = "Cabrillo High"
STATE = "CA"

PROXY_PROFILE = "datacenter"
SCHOOL_HOME_URL = "https://cabrillohighschool.lusd.org/"
ATHLETICS_HOME_URL = "https://www.cabrilloathletics.org/"
FALL_SPORTS_URL = "https://www.cabrilloathletics.org/apps/departments/index.jsp?show=TDE"
FOOTBALL_PAGE_URL = (
    "https://www.cabrilloathletics.org/apps/pages/index.jsp?uREC_ID=4366807&type=d"
)
CONTACT_PAGE_URL = (
    "https://www.cabrilloathletics.org/apps/pages/index.jsp?uREC_ID=4366880&type=d"
)
FACILITIES_PAGE_URL = (
    "https://www.cabrilloathletics.org/apps/pages/index.jsp?uREC_ID=4367108&type=d&pREC_ID=2575465"
)
TARGET_URLS = [
    SCHOOL_HOME_URL,
    ATHLETICS_HOME_URL,
    FALL_SPORTS_URL,
    FOOTBALL_PAGE_URL,
    CONTACT_PAGE_URL,
    FACILITIES_PAGE_URL,
]


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        marker = repr(value)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(value)
    return out


def _split_lines(text: str) -> list[str]:
    return [_clean(line) for line in text.splitlines() if _clean(line)]


def _collect_snapshot(page) -> dict[str, Any]:
    body_text = page.inner_text("body")
    links = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => ({text: (e.textContent || '').replace(/\\s+/g, ' ').trim(), href: e.href || e.getAttribute('href') || ''}))",
    )
    normalized_links: list[dict[str, str]] = []
    for item in links or []:
        if not isinstance(item, dict):
            continue
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        normalized_links.append(
            {
                "text": _clean(str(item.get("text") or "")),
                "href": href,
            }
        )

    return {
        "title": _clean(page.title() or ""),
        "url": page.url,
        "text": _clean(body_text),
        "lines": _split_lines(body_text),
        "links": normalized_links,
    }


def _find_snapshot(snapshots: list[dict[str, Any]], needle: str) -> dict[str, Any]:
    for snapshot in snapshots:
        if needle in str(snapshot.get("url") or ""):
            return snapshot
    return {}


def _extract_coaches(lines: list[str]) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    try:
        start = lines.index("Coaching Staff") + 1
    except ValueError:
        return coaches

    end = len(lines)
    for marker in ("Football Game Schedules - Printable", "Permission to Transport Form"):
        if marker in lines[start:]:
            end = lines.index(marker, start)
            break

    coach_lines = lines[start:end]
    for index in range(0, len(coach_lines) - 1, 2):
        name = coach_lines[index]
        role = coach_lines[index + 1]
        if not name or not role:
            continue
        if "coach" not in role.lower():
            continue
        coaches.append({"name": name, "role": role})
    return _dedupe_keep_order(coaches)


def _extract_contact(lines: list[str], role_label: str) -> dict[str, str]:
    for index, line in enumerate(lines):
        if role_label.lower() not in line.lower():
            continue
        name = line.split("-", 1)[0].strip()
        email = ""
        phone = ""
        for candidate in lines[index + 1 : index + 5]:
            if candidate.lower().startswith("email:"):
                email = candidate.split(":", 1)[1].strip()
            elif re.search(r"\(\d{3}\)\s*\d{3}-\d{4}", candidate):
                phone = candidate
        return {"name": name, "role": role_label, "email": email, "phone": phone}
    return {}


def _extract_facility_details(lines: list[str]) -> tuple[str, str]:
    location = ""
    admission = ""
    for index, line in enumerate(lines):
        if line.startswith("Varsity & JV Football - All home games are played at"):
            location = line
            if index + 1 < len(lines):
                admission = lines[index + 1]
            break
    return location, admission


async def scrape_school() -> dict[str, Any]:
    """Scrape publicly available football data from Cabrillo High athletics pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: list[dict[str, Any]] = []
    requested_pages: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                requested_pages.append(url)
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    source_pages.append(page.url)
                    snapshots.append(await _collect_snapshot(page))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{url}:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    football_snapshot = _find_snapshot(snapshots, "uREC_ID=4366807")
    fall_snapshot = _find_snapshot(snapshots, "show=TDE")
    contact_snapshot = _find_snapshot(snapshots, "uREC_ID=4366880")
    facilities_snapshot = _find_snapshot(snapshots, "pREC_ID=2575465")

    football_lines = football_snapshot.get("lines", []) if football_snapshot else []
    contact_lines = contact_snapshot.get("lines", []) if contact_snapshot else []
    facilities_lines = facilities_snapshot.get("lines", []) if facilities_snapshot else []
    football_links = football_snapshot.get("links", []) if football_snapshot else []
    fall_links = fall_snapshot.get("links", []) if fall_snapshot else []

    coaching_staff = _extract_coaches(football_lines)
    athletic_director = _extract_contact(contact_lines, "Athletic Director")
    location_line, admission_line = _extract_facility_details(facilities_lines)

    football_schedule_documents = [
        link
        for link in football_links
        if link.get("href", "").lower().endswith(".pdf")
        and "football" in _clean(link.get("text", "")).lower()
    ]
    football_schedule_documents = _dedupe_keep_order(football_schedule_documents)

    football_team_links = [
        link
        for link in fall_links
        if _clean(link.get("text", "")).lower() in {"football", "flag football - girls"}
    ]
    football_team_links = _dedupe_keep_order(football_team_links)

    athletics_ticket_links = [
        link for link in football_links if "gofan.co" in link.get("href", "").lower()
    ]

    football_levels = _dedupe_keep_order(
        [
            role.split(" ", 1)[0]
            for role in [coach.get("role", "") for coach in coaching_staff]
            if role.lower().startswith(("varsity", "jv"))
        ]
    )

    football_program_available = bool(
        football_snapshot
        and (
            coaching_staff
            or football_schedule_documents
            or "FOOTBALL" in football_lines
        )
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_page_title": football_snapshot.get("title", "") if football_snapshot else "",
        "football_page_url": football_snapshot.get("url", "") if football_snapshot else "",
        "football_team_links": football_team_links,
        "football_levels": football_levels,
        "football_coaching_staff": coaching_staff,
        "football_schedule_documents": football_schedule_documents,
        "athletics_ticket_links": athletics_ticket_links,
        "athletic_director_contact": athletic_director,
        "football_home_game_location": location_line,
        "football_home_ticket_prices": admission_line,
        "athletics_department_phone": next(
            (line for line in contact_lines if re.search(r"\(\d{3}\)\s*\d{3}-\d{4}", line)),
            "",
        ),
        "athletics_address": next(
            (line for line in contact_lines if "Constellation Rd." in line),
            "",
        ),
    }

    if not football_program_available:
        errors.append("no_public_football_content_found")

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
            "focus": "football_only",
            "pages_requested": requested_pages,
            "pages_visited": len(source_pages),
            **proxy_meta,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
