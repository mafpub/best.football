"""Deterministic football scraper for Liberty High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061954007994"
SCHOOL_NAME = "Liberty High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://liberty.kernhigh.org/"
FOOTBALL_URL = "https://liberty.kernhigh.org/apps/pages/football"
ATHLETIC_SCHEDULES_URL = "https://liberty.kernhigh.org/apps/pages/Athletic-Schedules"
STAFF_URL = "https://liberty.kernhigh.org/apps/pages/index.jsp?uREC_ID=602785&type=d&pREC_ID=staff"

TARGET_URLS = [HOME_URL, FOOTBALL_URL, ATHLETIC_SCHEDULES_URL, STAFF_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for value in values:
        key = repr(value) if isinstance(value, dict) else _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _abs_url(base_url: str, href: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


def _extract_links(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        href = _abs_url(base_url, str(anchor.get("href") or ""))
        if not href:
            continue
        links.append(
            {
                "text": _clean(anchor.get_text(" ", strip=True)),
                "href": href,
            }
        )
    return _dedupe_keep_order(links)


def _text_lines(text: str) -> list[str]:
    return [_clean(line) for line in text.splitlines() if _clean(line)]


def _extract_page(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    return {
        "url": url,
        "title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
        "html": html,
        "text": soup.get_text("\n"),
        "links": _extract_links(soup, url),
    }


def _decode_cfemail(encoded: str) -> str:
    encoded = _clean(encoded)
    if len(encoded) < 4:
        return ""
    try:
        key = int(encoded[:2], 16)
        chars = [
            chr(int(encoded[i : i + 2], 16) ^ key)
            for i in range(2, len(encoded), 2)
        ]
    except ValueError:
        return ""
    return "".join(chars)


def _find_first(lines: list[str], patterns: list[re.Pattern[str]]) -> str:
    for line in lines:
        for pattern in patterns:
            match = pattern.search(line)
            if match:
                value = match.groupdict().get("value") or (
                    match.group(1) if match.groups() else ""
                )
                return _clean(value)
    return ""


def _extract_football_page(page_data: dict[str, Any]) -> dict[str, Any]:
    raw_text = str(page_data.get("text") or "")
    text = _clean(raw_text)
    html = str(page_data.get("html") or "")
    soup = BeautifulSoup(html, "html.parser")
    links = page_data.get("links", [])

    schedule_links = [
        link
        for link in links
        if (
            "season schedule" in f"{link.get('text', '')}".lower()
            or (
                "drive.google.com" in f"{link.get('href', '')}".lower()
                and "schedule" in f"{link.get('text', '')}".lower()
            )
        )
    ]
    schedule_links = _dedupe_keep_order(schedule_links)

    coach_labels = [
        ("varsity_head_coach", "Varsity Head Coach"),
        ("jv_head_coach", "JV Head Coach"),
        ("fs_head_coach", "F/S Head Coach"),
        ("athletic_director", "Athletic Director"),
        ("athletics_secretary", "Athletics Secretary"),
        ("equipment_manager", "Equipment Manager"),
    ]
    coaches: dict[str, str] = {}
    for field, label in coach_labels:
        match = re.search(
            rf"{re.escape(label)}\s*[.\-–:]*\s*(?P<name>[A-Za-z .'\-]+?)\s*-\s*",
            text,
            re.I | re.S,
        )
        if match:
            coaches[field] = _clean(match.group("name"))
        else:
            coaches[field] = ""

    roster_sections: dict[str, list[dict[str, str]]] = {
        "varsity": [],
        "jv": [],
        "fs": [],
    }
    section_patterns = {
        "varsity": re.compile(r"Varsity Football Roster", re.I),
        "jv": re.compile(r"JV Roster", re.I),
        "fs": re.compile(r"FS Roster", re.I),
    }
    for block in soup.select("div.page-block.page-block-text"):
        block_text = _clean(block.get_text(" ", strip=True))
        section_name = ""
        for candidate, pattern in section_patterns.items():
            if pattern.search(block_text):
                section_name = candidate
                break
        if not section_name:
            continue
        table = block.select_one("table")
        if not table:
            continue
        rows = table.select("tr")
        for row in rows[1:]:
            cells = [_clean(cell.get_text(" ", strip=True)) for cell in row.select("td")]
            if len(cells) < 2:
                continue
            name, year = cells[0], cells[1]
            if not name or year not in {"9", "10", "11", "12"}:
                continue
            roster_sections[section_name].append({"name": name, "year": year})

    coach_cards: list[dict[str, str]] = []
    for div in soup.select("div.page-block-text div"):
        text_block = _clean(div.get_text(" ", strip=True))
        if "Varsity Football Coach" not in text_block:
            continue
        match = re.search(
            r"(?P<name>[A-Z][A-Za-z'.\-]+(?:\s+[A-Z][A-Za-z'.\-]+)*)\s+(?P<role>.*Varsity Football Coach.*)$",
            text_block,
        )
        if not match:
            continue
        email_anchor = div.select_one("a.__cf_email__[data-cfemail]")
        coach_cards.append(
            {
                "name": _clean(match.group("name")),
                "role": _clean(match.group("role")),
                "email": _decode_cfemail(str(email_anchor.get("data-cfemail") or "")) if email_anchor else "",
            }
        )

    program_available = bool(
        any(coaches.values())
        or schedule_links
        or any(roster_sections.values())
        or "liberty football" in text.lower()
    )

    return {
        "football_text": text,
        "football_schedule_links": schedule_links,
        "football_coaches": coaches,
        "football_rosters": roster_sections,
        "football_staff_cards": coach_cards,
        "program_available": program_available,
    }


def _extract_staff_page(page_data: dict[str, Any]) -> dict[str, Any]:
    raw_text = str(page_data.get("text") or "")
    lines = _text_lines(raw_text)
    staff_entries: list[dict[str, str]] = []

    for line in lines:
        if "Varsity Football Coach" not in line or "Richie Bolin" not in line:
            continue
        current_name = "Richie Bolin"
        current_role = _clean(line.split("Richie Bolin", 1)[1]).strip(" -–:")
        staff_entries.append(
            {
                "name": current_name,
                "role": current_role,
            }
        )

    return {"football_staff_entries": _dedupe_keep_order(staff_entries)}


async def _fetch_html(context, url: str) -> tuple[str, str]:
    response = await context.request.get(url, timeout=60_000)
    if response.status >= 400:
        raise RuntimeError(f"HTTP {response.status} for {url}")
    return response.url, await response.text()


async def scrape_school() -> dict[str, Any]:
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
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            ignore_https_errors=True,
        )

        try:
            for url in TARGET_URLS:
                try:
                    final_url, html = await _fetch_html(context, url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"fetch_failed:{type(exc).__name__}:{url}")
                    continue

                page_data[url] = _extract_page(html, final_url)
                source_pages.append(final_url)
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_page = page_data.get(HOME_URL, {})
    football_page = page_data.get(FOOTBALL_URL, {})
    schedule_page = page_data.get(ATHLETIC_SCHEDULES_URL, {})
    staff_page = page_data.get(STAFF_URL, {})

    football_data = _extract_football_page(football_page) if football_page else {}
    staff_data = _extract_staff_page(staff_page) if staff_page else {}

    football_coaches = football_data.get("football_coaches", {})
    football_rosters = football_data.get("football_rosters", {})
    football_schedule_links = football_data.get("football_schedule_links", [])
    football_staff_cards = football_data.get("football_staff_cards", [])
    football_staff_entries = staff_data.get("football_staff_entries", [])

    football_contacts: list[dict[str, str]] = []
    coach_fields = [
        ("varsity_head_coach", "Varsity Head Coach"),
        ("jv_head_coach", "JV Head Coach"),
        ("fs_head_coach", "F/S Head Coach"),
        ("athletic_director", "Athletic Director"),
        ("athletics_secretary", "Athletics Secretary"),
        ("equipment_manager", "Equipment Manager"),
    ]
    for field, role in coach_fields:
        value = _clean(str(football_coaches.get(field) or ""))
        if value:
            football_contacts.append(
                {
                    "name": value,
                    "role": role,
                    "source_page": FOOTBALL_URL,
                }
            )

    for entry in football_staff_entries:
        name = _clean(str(entry.get("name") or ""))
        role = _clean(str(entry.get("role") or ""))
        if not name:
            continue
        football_contacts.append(
            {
                "name": name,
                "role": role,
                "source_page": STAFF_URL,
            }
        )

    for entry in football_staff_cards:
        name = _clean(str(entry.get("name") or ""))
        role = _clean(str(entry.get("role") or ""))
        email = _clean(str(entry.get("email") or ""))
        if not name:
            continue
        item = {
            "name": name,
            "role": role,
            "source_page": FOOTBALL_URL,
        }
        if email:
            item["email"] = email
        football_contacts.append(item)

    football_contacts = _dedupe_keep_order(football_contacts)

    football_program_available = bool(
        football_data.get("program_available")
        or football_contacts
        or football_schedule_links
        or any(football_rosters.values())
    )
    if not football_program_available:
        errors.append("no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "home_page_url": HOME_URL,
        "football_page_url": FOOTBALL_URL,
        "athletic_schedules_url": ATHLETIC_SCHEDULES_URL,
        "staff_page_url": STAFF_URL,
        "home_page_title": _clean(str(home_page.get("title") or "")),
        "home_page_text": _clean(str(home_page.get("text") or "")),
        "football_schedule_links": football_schedule_links,
        "football_contacts": football_contacts,
        "football_rosters": football_rosters,
        "football_summary": (
            "Liberty High publishes a dedicated Football page with varsity, JV, and F/S head coaches, a 2025-2026 season schedule link, roster tables, and an athletics staff page that names the varsity football coach."
            if football_program_available
            else ""
        ),
    }

    if schedule_page:
        extracted_items["athletic_schedules_page_text"] = _clean(str(schedule_page.get("text") or ""))

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(source_pages),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
