"""Deterministic football scraper for Davis Senior High (CA)."""

from __future__ import annotations

import csv
import io
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061062001176"
SCHOOL_NAME = "Davis Senior High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://dshs.djusd.net/"
ATHLETICS_URL = "https://dshs.djusd.net/athletics"
PARTICIPATE_IN_A_SPORT_URL = "https://dshs.djusd.net/athletics/participate_in_a_sport"
COACHING_STAFF_DIRECTORY_URL = "https://dshs.djusd.net/athletics/coaching_staff_directory"
COACHING_STAFF_EXPORT_URL = (
    "https://docs.google.com/spreadsheets/d/13yl7KjWq0kD5aFeUTqdsZ7jI7B14qjqOFvSadvDGSTM/export?format=csv&gid=0"
)

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    PARTICIPATE_IN_A_SPORT_URL,
    COACHING_STAFF_DIRECTORY_URL,
    COACHING_STAFF_EXPORT_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 20) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_links(html: str, base_url: str, *, keywords: tuple[str, ...]) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = urljoin(base_url, anchor.get("href", "").strip())
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if keywords and not any(keyword in blob for keyword in keywords):
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        links.append({"text": text, "href": href})

    return links


def _parse_coaching_csv(csv_text: str) -> dict[str, Any]:
    rows = list(csv.reader(io.StringIO(csv_text)))
    football_rows: list[dict[str, str]] = []
    source_lines: list[str] = []
    current_season = ""

    for raw_row in rows:
        row = [_clean(cell) for cell in raw_row]
        if not any(row):
            continue

        first_cell = row[0].strip()
        first_lower = first_cell.lower()
        if first_lower in {"fall sports", "winter sports", "spring sports"}:
            current_season = first_cell.title()
            continue

        if not first_lower.startswith("football head coach"):
            continue

        coach_name = row[1] if len(row) > 1 else ""
        coach_email = row[2] if len(row) > 2 else ""
        level_match = re.search(r"\(([^)]+)\)", first_cell)
        level_code = level_match.group(1).upper() if level_match else ""
        level = {"V": "Varsity", "JV": "Junior Varsity", "F": "Freshman"}.get(level_code, level_code)

        football_rows.append(
            {
                "season": current_season,
                "sport": "Football",
                "level": level,
                "designation": first_cell,
                "coach_name": coach_name,
                "coach_email": coach_email,
            }
        )
        summary_bits = [bit for bit in (current_season, first_cell, coach_name, coach_email) if bit]
        source_lines.append(" | ".join(summary_bits))

    football_rows = football_rows[:10]
    source_lines = _dedupe_keep_order(source_lines)

    return {
        "football_program_available": bool(football_rows),
        "football_head_coach_rows": football_rows,
        "football_head_coach_names": _dedupe_keep_order([row["coach_name"] for row in football_rows if row.get("coach_name")]),
        "football_head_coach_emails": _dedupe_keep_order([row["coach_email"] for row in football_rows if row.get("coach_email")]),
        "football_levels": _dedupe_keep_order([row["level"] for row in football_rows if row.get("level")]),
        "football_summary_lines": source_lines,
    }


async def _fetch_text(context, url: str, *, timeout: int = 90000) -> tuple[str, str]:
    response = await context.request.get(url, timeout=timeout)
    text = await response.text()
    return str(response.url or url), text


async def scrape_school() -> dict[str, Any]:
    """Scrape public football signals for Davis Senior High."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    home_html = ""
    athletics_html = ""
    participate_html = ""
    coaching_html = ""
    coaching_csv = ""

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
        try:
            for url in (HOME_URL, ATHLETICS_URL, PARTICIPATE_IN_A_SPORT_URL, COACHING_STAFF_DIRECTORY_URL):
                try:
                    final_url, text = await _fetch_text(context, url)
                    source_pages.append(final_url)
                    if url == HOME_URL:
                        home_html = text
                    elif url == ATHLETICS_URL:
                        athletics_html = text
                    elif url == PARTICIPATE_IN_A_SPORT_URL:
                        participate_html = text
                    elif url == COACHING_STAFF_DIRECTORY_URL:
                        coaching_html = text
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"page_fetch_failed:{type(exc).__name__}:{url}")

            try:
                final_url, text = await _fetch_text(context, COACHING_STAFF_EXPORT_URL)
                source_pages.append(final_url)
                coaching_csv = text
            except Exception as exc:  # noqa: BLE001
                errors.append(f"csv_fetch_failed:{type(exc).__name__}:{COACHING_STAFF_EXPORT_URL}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_lines = _extract_lines(home_html, keywords=("athletics", "blue devil", "football", "coach"), limit=20)
    athletics_lines = _extract_lines(
        athletics_html + "\n" + participate_html + "\n" + coaching_html,
        keywords=("athletics", "football", "coach", "blue devil", "schedule"),
        limit=25,
    )
    athletics_links = _extract_links(
        home_html + "\n" + athletics_html,
        HOME_URL,
        keywords=("athletics", "football", "coach", "sports", "blue devil"),
    )
    football_sheet = _parse_coaching_csv(coaching_csv)

    football_program_available = bool(football_sheet["football_program_available"])
    if not football_program_available:
        errors.append("football_program_not_detected_on_public_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_home_url": HOME_URL,
        "athletics_url": ATHLETICS_URL,
        "participate_in_a_sport_url": PARTICIPATE_IN_A_SPORT_URL,
        "coaching_staff_directory_url": COACHING_STAFF_DIRECTORY_URL,
        "coaching_staff_export_url": COACHING_STAFF_EXPORT_URL,
        "home_summary_lines": home_lines,
        "athletics_summary_lines": athletics_lines,
        "school_athletics_links": athletics_links,
        "football_head_coach_rows": football_sheet["football_head_coach_rows"],
        "football_head_coach_names": football_sheet["football_head_coach_names"],
        "football_head_coach_emails": football_sheet["football_head_coach_emails"],
        "football_levels": football_sheet["football_levels"],
        "football_summary_lines": football_sheet["football_summary_lines"],
        "football_program_summary": (
            "Davis Senior High publicly lists varsity and junior varsity football head coaches in its athletics coaching staff directory."
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
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "target_urls": TARGET_URLS,
            "manual_navigation_steps": [
                "home",
                "athletics",
                "participate_in_a_sport",
                "coaching_staff_directory",
                "coaching_staff_export",
            ],
            "focus": "football_only",
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
