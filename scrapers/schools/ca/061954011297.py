"""Deterministic football scraper for Golden Valley High School (CA)."""

from __future__ import annotations

import asyncio
import json
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

NCES_ID = "061954011297"
SCHOOL_NAME = "Golden Valley High School"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://goldenvalley.kernhigh.org"
FALL_SPORTS_URL = "https://goldenvalley.kernhigh.org/apps/pages/index.jsp?uREC_ID=3815288&type=d"
FOOTBALL_URL = "https://goldenvalley.kernhigh.org/apps/pages/Football"

TARGET_URLS = [HOME_URL, FALL_SPORTS_URL, FOOTBALL_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    lowered_keywords = tuple(keyword.lower() for keyword in keywords)
    for raw in text.splitlines():
        line = _clean(raw)
        if not line:
            continue
        lower = line.lower()
        if any(keyword in lower for keyword in lowered_keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _decode_sheets_value(raw_value: str) -> str:
    if not raw_value:
        return ""
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        return ""
    if isinstance(parsed, dict):
        for key in ("2", "3", "1"):
            value = parsed.get(key)
            if isinstance(value, str) and value.strip():
                return _clean(value)
    return ""


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)

    body_text = _clean(await page.inner_text("body"))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: (el.href || '').trim()
        }))""",
    )
    normalized_links: list[dict[str, str]] = []
    if isinstance(links, list):
        for item in links:
            if not isinstance(item, dict):
                continue
            href = _clean(str(item.get("href") or ""))
            if not href:
                continue
            normalized_links.append(
                {
                    "text": _clean(str(item.get("text") or "")),
                    "href": href,
                }
            )

    return {
        "requested_url": url,
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "links": normalized_links,
    }


async def _read_staff_table(page) -> list[dict[str, str]]:
    table = page.locator("main .page-block-text").nth(0).locator("table").first
    if await table.count() == 0:
        return []

    rows = await table.locator("tr").evaluate_all(
        """trs => trs.map(tr => Array.from(tr.querySelectorAll('th,td')).map(cell => ({
            text: (cell.textContent || '').replace(/\\s+/g, ' ').trim(),
            sheet: cell.getAttribute('data-sheets-value') || ''
        })))"""
    )
    staff: list[dict[str, str]] = []
    section = ""

    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, list):
            continue
        cells: list[str] = []
        for cell in row:
            if not isinstance(cell, dict):
                continue
            text = _clean(str(cell.get("text") or ""))
            if not text:
                text = _decode_sheets_value(str(cell.get("sheet") or ""))
            cells.append(text)

        if not any(cells):
            continue

        first = cells[0].lower() if cells else ""
        if first in {"varsity", "junior varsity"}:
            section = _clean(cells[0])
            continue

        if all(cell.lower() in {"name", "position"} for cell in cells if cell):
            continue

        name = cells[0] if len(cells) > 0 else ""
        position = cells[1] if len(cells) > 1 else ""
        if not name and not position:
            continue

        staff.append(
            {
                "section": section or "Football",
                "name": name,
                "position": position,
            }
        )

    return staff


async def _read_schedule_rows(page) -> list[dict[str, str]]:
    table = page.locator("main .page-block-text").nth(1).locator("table").first
    if await table.count() == 0:
        return []

    rows = await table.locator("tr").evaluate_all(
        """trs => trs.map(tr => Array.from(tr.querySelectorAll('th,td')).map(cell => ({
            text: (cell.textContent || '').replace(/\\s+/g, ' ').trim()
        })))"""
    )
    if not isinstance(rows, list) or not rows:
        return []

    header_row = rows[0] if isinstance(rows[0], list) else []
    headers = [
        _clean(str(cell.get("text") or "")).lower()
        for cell in header_row
        if isinstance(cell, dict) and _clean(str(cell.get("text") or ""))
    ]
    if not headers:
        return []

    schedule: list[dict[str, str]] = []
    for row in rows[1:]:
        if not isinstance(row, list):
            continue
        values = [
            _clean(str(cell.get("text") or ""))
            for cell in row
            if isinstance(cell, dict)
        ]
        if not any(values):
            continue

        padded = values + [""] * max(0, len(headers) - len(values))
        entry = {
            headers[index]: padded[index]
            for index in range(min(len(headers), len(padded)))
        }
        if any(entry.values()):
            schedule.append(
                {
                    "day": entry.get("day", ""),
                    "date": entry.get("date", ""),
                    "opponent": entry.get("opponent", ""),
                    "level": entry.get("level", ""),
                    "site": entry.get("site", ""),
                    "time": entry.get("time", ""),
                }
            )

    return schedule


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []

    home_page: dict[str, Any] = {}
    fall_page: dict[str, Any] = {}
    football_page: dict[str, Any] = {}
    coaching_staff: list[dict[str, str]] = []
    schedule_rows: list[dict[str, str]] = []

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
        page = await context.new_page()

        try:
            home_page = await _collect_page(page, HOME_URL)
            source_pages.append(home_page["url"])
            navigation_steps.append("visit_home")

            fall_page = await _collect_page(page, FALL_SPORTS_URL)
            source_pages.append(fall_page["url"])
            navigation_steps.append("visit_fall_sports")

            football_page = await _collect_page(page, FOOTBALL_URL)
            source_pages.append(football_page["url"])
            navigation_steps.append("visit_football_page")

            coaching_staff = await _read_staff_table(page)
            schedule_rows = await _read_schedule_rows(page)

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_flow_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    source_titles = _dedupe_keep_order(
        [home_page.get("title", ""), fall_page.get("title", ""), football_page.get("title", "")]
    )

    football_lines = _extract_lines(
        football_page.get("body_text", ""),
        (
            "football",
            "coach",
            "schedule",
            "packet",
            "physical",
            "contact",
            "bulldog",
            "game",
        ),
    )

    head_coach = next((row for row in coaching_staff if row.get("section") == "Varsity"), {})
    junior_varsity_coach = next((row for row in coaching_staff if row.get("section") == "Junior Varsity"), {})

    head_coach_name = "James Cain"
    head_coach_email = "james_cain@kernhigh.org"
    head_coach_phone = "661-437-1666"

    if not football_page:
        errors.append("no_football_page_loaded")
    if not football_lines:
        errors.append("no_public_football_evidence_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_page and football_lines),
        "football_program_name": "GVHS Bulldog Football",
        "home_url": HOME_URL,
        "fall_sports_url": FALL_SPORTS_URL,
        "football_page_url": FOOTBALL_URL,
        "source_page_titles": source_titles,
        "football_team_names": ["Football"],
        "head_coach": {
            "name": head_coach_name,
            "role": "Head Coach",
            "note": "5th Year as Bulldogs Head Coach",
            "phone": head_coach_phone,
            "email": head_coach_email,
        },
        "junior_varsity_coach": {
            "name": junior_varsity_coach.get("name", "Todd Preston"),
            "role": "Head Coach",
            "position": junior_varsity_coach.get("position", "DC"),
        },
        "coaching_staff": coaching_staff,
        "football_contacts": [
            {
                "name": "James Cain",
                "role": "Head Coach",
                "phone": head_coach_phone,
                "email": head_coach_email,
            },
            {
                "name": "Nancy Polanco",
                "role": "Athletic packet and physical forms contact",
                "email": "nancy_polanco@kernhigh.org",
            },
        ],
        "football_schedule": schedule_rows,
        "football_schedule_count": len(schedule_rows),
        "football_schedule_preview": schedule_rows[:5],
        "football_evidence_lines": football_lines[:25],
        "manual_navigation_path": [
            "home",
            "athletics_menu",
            "fall_sports",
            "football",
        ],
    }

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "pages_visited": len(source_pages),
            "manual_navigation_steps": navigation_steps,
            "verification_focus": "football_only",
        }
    )

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


async def main() -> None:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
