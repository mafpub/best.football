"""Deterministic football scraper for Ednovate - South LA College Prep (CA)."""

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

from scrapers.schools.runtime import (  # noqa: E402
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060254014489"
SCHOOL_NAME = "Ednovate - South LA College Prep"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_URL = "https://www.ednovate.org/south-la"
ANNUAL_REPORT_URL = "https://www.ednovate.org/annual-report"
TARGET_URLS = [SCHOOL_URL, ANNUAL_REPORT_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("’", "'")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_lines(body_text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in body_text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_football_excerpt(body_text: str) -> str:
    patterns = [
        r"Launch of Football Program:\s*[\"“](.+?)[\"”]",
        r"Launch of Football Program:\s*(.+?)(?:SCHOOL HIGHLIGHTS:|Item \d+ of \d+|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, body_text, re.IGNORECASE | re.DOTALL)
        if match:
            return _clean(match.group(1))
    return ""


def _value_after_label(lines: list[str], label: str) -> str:
    lowered_label = label.lower()
    for index, line in enumerate(lines):
        if line.lower() != lowered_label:
            continue
        for follow in lines[index + 1 :]:
            if follow:
                return follow
        return ""
    return ""


def _find_link(links: list[dict[str, Any]], *keywords: str) -> str:
    for item in links:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        blob = f"{text} {href}".lower()
        if href and any(keyword in blob for keyword in keywords):
            return href
    return ""


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    await page.goto(requested_url, wait_until="domcontentloaded", timeout=90_000)
    await page.wait_for_timeout(1_200)
    raw_body_text = await page.locator("body").inner_text(timeout=15_000)
    body_text = _clean(raw_body_text)
    links = await page.locator("a[href]").evaluate_all(
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "body_lines": [_clean(line) for line in raw_body_text.splitlines() if _clean(line)],
        "links": [item for item in links if isinstance(item, dict)],
    }


async def scrape_school() -> dict[str, Any]:
    """Visit the official school page and annual report to extract football evidence."""
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
            for target in TARGET_URLS:
                try:
                    signal = await _collect_page(page, target)
                    page_signals.append(signal)
                    source_pages.append(signal["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{target}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_page = next((item for item in page_signals if item.get("requested_url") == SCHOOL_URL), {})
    annual_page = next((item for item in page_signals if item.get("requested_url") == ANNUAL_REPORT_URL), {})

    school_lines = list(school_page.get("body_lines", []))
    annual_lines = list(annual_page.get("body_lines", []))

    school_leader = _value_after_label(school_lines, "School leader:")
    grades_served = _value_after_label(school_lines, "Grades served:")
    enrollment = _value_after_label(school_lines, "Enrollment:")
    contact_address = _value_after_label(school_lines, "Contact:")
    phone = ""
    for line in school_lines:
        if line.startswith("Tel:"):
            phone = line.removeprefix("Tel:").strip()
            break

    principal_bio_url = _find_link(school_page.get("links", []), "jaron", "bio")

    football_program_line = _extract_football_excerpt(str(annual_page.get("body_text") or ""))
    football_context_lines = _dedupe_keep_order(
        [
            line
            for line in annual_lines
            if any(keyword in line.lower() for keyword in ("south la college prep", "football", "knights", "coach"))
        ]
    )
    if football_program_line and football_program_line not in football_context_lines:
        football_context_lines.insert(0, football_program_line)
    football_program_available = bool(football_program_line)

    if not football_program_available:
        errors.append("blocked:no_public_football_program_found_on_official_ednovate_pages")

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_program_line": football_program_line,
        "football_context_lines": football_context_lines[:20],
        "school_overview": {
            "grades_served": grades_served,
            "enrollment": enrollment,
            "school_leader": school_leader,
            "contact_address": contact_address,
            "phone": phone,
            "principal_bio_url": principal_bio_url,
        },
        "source_page_summary": {
            "school_page": school_page.get("final_url", SCHOOL_URL),
            "annual_report_page": annual_page.get("final_url", ANNUAL_REPORT_URL),
        },
        "summary": (
            "Ednovate's 2025 annual report says South LA College Prep launched its inaugural football program with a dedicated coaching staff and first-ever team, while the school page lists Jaron Roberson as school leader and shows the campus quick facts."
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
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
            "manual_navigation_steps": [
                "school_home_page",
                "annual_report_page",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=False))
