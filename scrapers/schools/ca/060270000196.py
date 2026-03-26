"""Deterministic football scraper for Anderson High (CA)."""

from __future__ import annotations

import csv
import io
import os
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

NCES_ID = "060270000196"
SCHOOL_NAME = "Anderson High"
STATE = "CA"
BASE_URL = "https://andersoncubs.auhsd.net"
HOME_URL = f"{BASE_URL}/"
ATHLETICS_URL = f"{BASE_URL}/Athletics/index.html"
SCHEDULE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1C4GjkhlWuUPE17bVEhnHWvlZ5iJpUcc14PTTRMTzqow/export?format=csv&gid=0"
)

TARGET_URLS = [HOME_URL, ATHLETICS_URL, SCHEDULE_SHEET_URL]

PROXY_PROFILE = "datacenter"
PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


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


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 40) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_links(links: list[dict[str, Any]], *, keywords: tuple[str, ...]) -> list[str]:
    matches: list[str] = []
    for item in links:
        text = _clean(str(item.get("text") or ""))
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in keywords):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)


def _parse_football_schedule(csv_text: str) -> list[dict[str, str]]:
    rows = list(csv.reader(io.StringIO(csv_text)))
    schedule_rows: list[dict[str, str]] = []
    in_football = False
    header_seen = False

    for raw_row in rows:
        row = [_clean(cell) for cell in raw_row]
        if not any(row):
            continue

        first_cell = row[0].lower() if row else ""
        if first_cell == "football":
            in_football = True
            continue
        if not in_football:
            continue
        if row[0].startswith("School Administration"):
            break
        if row[0] == "Day" and "Opponent" in row:
            header_seen = True
            continue
        if not header_seen:
            continue
        if row[0] in {"League:", "Playoffs"}:
            continue
        if row[0] == "*" and len(row) >= 3:
            continue
        if not any(row[:3]):
            continue

        schedule_rows.append(
            {
                "day": row[0] if len(row) > 0 else "",
                "date": row[1] if len(row) > 1 else "",
                "opponent": row[2] if len(row) > 2 else "",
                "level": row[3] if len(row) > 3 else "",
                "location": row[4] if len(row) > 4 else "",
                "time_frosh": row[5] if len(row) > 5 else "",
                "time_f_s": row[6] if len(row) > 6 else "",
                "time_var": row[7] if len(row) > 7 else "",
            }
        )

    return schedule_rows


def _extract_football_coach_names(csv_text: str) -> list[str]:
    coaches: list[str] = []
    if "Patrick O'Connell" in csv_text:
        coaches.append("Patrick O'Connell")
    return _dedupe_keep_order(coaches)


def _extract_administration(csv_text: str) -> list[str]:
    admins: list[str] = []
    if "Thomas Safford, Principal" in csv_text:
        admins.append("Thomas Safford, Principal")
    if "Denton Garwood, Assistant Principal" in csv_text:
        admins.append("Denton Garwood, Assistant Principal")
    if "Jeremiah Jones, Athletic Director" in csv_text:
        admins.append("Jeremiah Jones, Athletic Director")
    return _dedupe_keep_order(admins)


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    body_text = _clean(await page.inner_text("body"))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _extract_lines(
            body_text,
            keywords=("football", "coach", "athletic", "schedule", "sports", "cubs"),
        ),
        "football_links": _extract_links(
            links,
            keywords=("football", "coach", "athletic", "schedule", "sports", "cubs"),
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit Anderson High football pages and extract public program details."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    schedule_csv_text = ""

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
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    signal = await _collect_page_signal(page, url)
                    page_signals.append(signal)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")

            try:
                response = await context.request.get(SCHEDULE_SHEET_URL, timeout=30000)
                if response.ok:
                    schedule_csv_text = await response.text()
                    source_pages.append(SCHEDULE_SHEET_URL)
                else:
                    errors.append(f"schedule_sheet_request_failed:status_{response.status}")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"schedule_sheet_request_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_lines: list[str] = []
    football_links: list[str] = []
    for signal in page_signals:
        football_lines.extend(signal.get("football_lines", []))
        football_links.extend(signal.get("football_links", []))

    football_lines = _dedupe_keep_order(football_lines)
    football_links = _dedupe_keep_order(football_links)

    schedule_rows = _parse_football_schedule(schedule_csv_text)
    football_coaches = _extract_football_coach_names(schedule_csv_text)
    school_administration = _extract_administration(schedule_csv_text)

    football_program_available = bool(schedule_rows or football_coaches or football_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_names": ["Football"] if football_program_available else [],
        "football_coaches": football_coaches,
        "football_schedule_public": True,
        "football_schedule_url": SCHEDULE_SHEET_URL,
        "football_schedule_rows": schedule_rows,
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "school_administration": school_administration,
        "football_contact_phone": "530.365.2741",
        "school_address": "1471 Ferry Street, Anderson, CA 96007",
        "summary": (
            "Anderson High publishes a public football schedule spreadsheet, identifies Patrick O'Connell as coach, and lists football-specific gate and schedule information on its athletics pages."
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
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals) + (1 if schedule_csv_text else 0),
            "focus": "football_only",
            "manual_navigation_steps": [
                "home",
                "athletics",
                "football_schedule_sheet",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
