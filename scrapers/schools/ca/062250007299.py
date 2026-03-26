"""Deterministic football scraper for Avalon K-12 (CA)."""

from __future__ import annotations

import csv
import io
import re
import urllib.request
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062250007299"
SCHOOL_NAME = "Avalon K-12"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://avalon.lbschools.net"
HOME_URL = f"{BASE_URL}/"
TITLE_IX_URL = f"{BASE_URL}/title-ix-parent-notification"
ATHLETICS_DATA_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1k7bnErMGKCatQxUCFX2A8hI3sUB0rYZ7vs4ZYgq2ee0/export?format=csv&gid=0"
)

TARGET_URLS = [HOME_URL, TITLE_IX_URL, ATHLETICS_DATA_SHEET_URL]
PLAYWRIGHT_URLS = [HOME_URL, TITLE_IX_URL]

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


def _preview_lines(text: str, *, keywords: tuple[str, ...], limit: int = 20) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _parse_csv_rows(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        cleaned = [_clean(value) for value in row]
        if any(cleaned):
            rows.append(cleaned)
    return rows


def _find_row(rows: list[list[str]], label: str) -> list[str]:
    for row in rows:
        if row and _clean(row[0]).lower() == label.lower():
            return row
    return []


def _parse_gender_row(rows: list[list[str]], label: str) -> dict[str, int]:
    row = _find_row(rows, label)
    if len(row) < 4:
        return {"female": 0, "male": 0, "non_binary": 0}
    try:
        return {
            "female": int(row[1] or 0),
            "male": int(row[2] or 0),
            "non_binary": int(row[3] or 0),
        }
    except ValueError:
        return {"female": 0, "male": 0, "non_binary": 0}


def _row_preview(row: list[str]) -> str:
    return " | ".join(value for value in row if value)


def _collect_links(page_links: list[dict[str, Any]], *, domain: str) -> list[str]:
    collected: list[str] = []
    for item in page_links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        text = _clean(str(item.get("text") or ""))
        if href and domain in href:
            collected.append(f"{text}|{href}")
    return _dedupe_keep_order(collected)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.inner_text("body")
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
        "football_lines": _preview_lines(
            body_text,
            keywords=("football", "athletics", "champions", "field"),
            limit=30,
        ),
        "docs_links": _collect_links(links, domain="docs.google.com"),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Avalon K-12's public football signals and athletics enrollment data."""
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
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in PLAYWRIGHT_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(2000)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_lines: list[str] = []
    title_ix_lines: list[str] = []
    sheet_lines: list[str] = []
    sheet_docs_links: list[str] = []
    sheet_body_text = ""

    for signal in page_signals:
        url = str(signal.get("final_url") or "")
        lines = [str(item) for item in signal.get("football_lines", []) if isinstance(item, str)]
        docs_links = [str(item) for item in signal.get("docs_links", []) if isinstance(item, str)]
        body_text = str(signal.get("body_text") or "")

        if url.startswith(BASE_URL):
            if url.rstrip("/") == HOME_URL.rstrip("/"):
                home_lines.extend(lines)
            else:
                title_ix_lines.extend(lines)
                sheet_docs_links.extend(docs_links)
    home_lines = _dedupe_keep_order(home_lines)
    title_ix_lines = _dedupe_keep_order(title_ix_lines)
    sheet_lines = _dedupe_keep_order(sheet_lines)
    sheet_docs_links = _dedupe_keep_order(sheet_docs_links)

    try:
        with urllib.request.urlopen(ATHLETICS_DATA_SHEET_URL, timeout=30) as response:
            sheet_body_text = response.read().decode("utf-8", "replace")
        source_pages.append(ATHLETICS_DATA_SHEET_URL)
        sheet_lines = _preview_lines(
            sheet_body_text,
            keywords=("football", "athletics", "champions", "field"),
            limit=30,
        )
    except Exception as exc:  # noqa: BLE001
        errors.append(f"csv_fetch_failed:{type(exc).__name__}:{ATHLETICS_DATA_SHEET_URL}")

    sheet_rows = _parse_csv_rows(sheet_body_text)

    flag_football = _parse_gender_row(sheet_rows, "Flag Football")
    tackle_football = _parse_gender_row(sheet_rows, "Tackle Football")
    school_athletic_enrollment = _parse_gender_row(
        sheet_rows,
        "Total School Athletic Enrollment",
    )
    flag_row = _find_row(sheet_rows, "Flag Football")
    tackle_row = _find_row(sheet_rows, "Tackle Football")
    school_total_row = _find_row(sheet_rows, "Total School Athletic Enrollment")

    total_flag_football = sum(flag_football.values())
    total_tackle_football = sum(tackle_football.values())
    total_football_athletes = total_flag_football + total_tackle_football

    football_program_available = bool(
        home_lines
        or title_ix_lines
        or sheet_lines
        or total_football_athletes
        or sheet_docs_links
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_names": [
            "Flag Football" if total_flag_football else "",
            "Tackle Football" if total_tackle_football else "",
        ],
        "football_participation": {
            "flag_football": flag_football,
            "tackle_football": tackle_football,
            "total_football_athletes": total_football_athletes,
        },
        "school_athletic_enrollment": school_athletic_enrollment,
        "sheet_row_previews": {
            "flag_football": _row_preview(flag_row),
            "tackle_football": _row_preview(tackle_row),
            "total_school_athletic_enrollment": _row_preview(school_total_row),
        },
        "home_page_football_lines": home_lines,
        "title_ix_football_lines": title_ix_lines,
        "athletics_data_sheet_lines": sheet_lines,
        "athletics_data_sheet_links": sheet_docs_links,
        "summary": (
            "Avalon K-12 publishes a public athletics enrollment sheet that includes flag football and tackle football rows, and the home page highlights the 2025 CIF Football Champions."
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
            "proxy_profile": PROXY_PROFILE,
            "pages_checked": len(page_signals),
            "manual_navigation_steps": [
                "home_page",
                "title_ix_parent_notification_page",
                "public_athletics_data_sheet",
            ],
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
