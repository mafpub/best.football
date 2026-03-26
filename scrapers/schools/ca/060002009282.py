"""Deterministic football scraper for Amador Valley High (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright, Page

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060002009282"
SCHOOL_NAME = "Amador Valley High"
STATE = "CA"
BASE_URL = "https://amador.pleasantonusd.net"
ATHLETICS_URL = f"{BASE_URL}/activities-athletics/athletics"
COACHES_URL = f"{BASE_URL}/activities-athletics/athletics/coaches-directory"

TARGET_URLS = [
    ATHLETICS_URL,
    COACHES_URL,
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_TERMS = (
    "football",
    "flag football",
    "boys football",
    "girls flag football",
    "varsity head coach",
    "jv head coach",
    "frosh head coach",
    "home campus",
    "contract submission",
    "baseline testing",
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


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if line and any(term in line.lower() for term in FOOTBALL_TERMS):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_links(links: list[dict[str, str]]) -> list[str]:
    kept: list[str] = []
    seen: set[str] = set()
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        if not text or not href:
            continue
        combo = f"{text} {href}".lower()
        if not any(term in combo for term in FOOTBALL_TERMS):
            continue
        value = f"{text}|{href}"
        if value in seen:
            continue
        seen.add(value)
        kept.append(value)
    return _dedupe_keep_order(kept)


def _extract_football_section(text: str) -> list[str]:
    match = re.search(
        r"Football\s+(Varsity Head Coach:.*?)(?:Girls Flag Football|Amador Valley High School|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if match:
        block = match.group(1)
        lines = [_clean(line) for line in block.splitlines() if _clean(line)]
        return _dedupe_keep_order(lines)

    lines = [_clean(line) for line in text.splitlines() if _clean(line)]
    section: list[str] = []
    in_section = False
    for line in lines:
        lowered = line.lower()
        if lowered == "football":
            in_section = True
            section.append(line)
            continue
        if in_section and lowered in {"girls flag football", "boys flag football"}:
            break
        if in_section:
            section.append(line)
    return _dedupe_keep_order(section)


def _parse_coaches(section_lines: list[str]) -> list[str]:
    coaches: list[str] = []
    for line in section_lines:
        if "head coach" not in line.lower():
            continue
        coaches.append(line)
    return _dedupe_keep_order(coaches)


def _extract_football_coach_lines(text: str) -> list[str]:
    structured_match = re.search(
        r"Football\s+Varsity Head Coach:\s*(.+?)\s+JV Head Coach:\s*(.+?)\s+Frosh Head Coach:\s*(.+?)\s+Girls Flag Football",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if structured_match:
        varsity, jv, frosh = (
            _clean(structured_match.group(1)),
            _clean(structured_match.group(2)),
            _clean(structured_match.group(3)),
        )
        coach_lines = [
            f"Varsity Head Coach: {varsity}",
            f"JV Head Coach: {jv}",
            f"Frosh Head Coach: {frosh}",
        ]
        girls_match = re.search(
            r"Girls Flag Football\s+Head Coach:\s*([A-Za-z .'\-]+)",
            text,
            re.IGNORECASE,
        )
        if girls_match:
            coach_lines.append(f"Girls Flag Football Head Coach: {_clean(girls_match.group(1))}")
        return _dedupe_keep_order(coach_lines)

    patterns = [
        ("Varsity Head Coach", r"Varsity Head Coach:\s*([A-Za-z .'\-]+)"),
        ("JV Head Coach", r"JV Head Coach:\s*([A-Za-z .'\-]+)"),
        ("Frosh Head Coach", r"Frosh Head Coach:\s*([A-Za-z .'\-]+)"),
        ("Girls Flag Football Head Coach", r"Girls Flag Football\s+Head Coach:\s*([A-Za-z .'\-]+)"),
    ]
    coach_lines: list[str] = []
    for label, pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            coach_lines.append(f"{label}: {_clean(match.group(1))}")
    return _dedupe_keep_order(coach_lines)


async def _collect_page(page: Page, requested_url: str) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    links = await page.evaluate(
        """() => Array.from(document.querySelectorAll('a[href]')).map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.href || '',
        }))"""
    )
    if not isinstance(links, list):
        links = []
    normalized = _clean(body_text)
    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": normalized,
        "keyword_lines": _extract_lines(normalized),
        "football_links": _extract_links([item for item in links if isinstance(item, dict)]),
        "football_section": _extract_football_section(normalized),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit Amador Valley football-related public pages and extract program details."""
    require_proxy_credentials()
    assert_not_blocklisted(TARGET_URLS)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USERNAME,
                "password": PROXY_PASSWORD,
            },
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
                    await page.wait_for_timeout(1500)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_lines: list[str] = []
    athletics_links: list[str] = []
    football_section_lines: list[str] = []
    coaches_page_text = ""

    for signal in page_signals:
        if signal.get("requested_url") == COACHES_URL:
            coaches_page_text = str(signal.get("body_text") or "")
        athletics_lines.extend(signal.get("keyword_lines", []))
        athletics_links.extend(signal.get("football_links", []))
        football_section_lines.extend(signal.get("football_section", []))

    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_links = _dedupe_keep_order(athletics_links)
    football_section_lines = _dedupe_keep_order(football_section_lines)

    football_program_available = any(
        term in " | ".join(athletics_lines + football_section_lines).lower()
        for term in ("football", "flag football")
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_coaches = _extract_football_coach_lines(coaches_page_text)
    football_links = athletics_links
    football_team_names = _dedupe_keep_order(
        [
            "Football" if any("football" in line.lower() for line in athletics_lines + football_section_lines) else "",
            "Varsity Football" if any("varsity head coach" in line.lower() for line in football_coaches) else "",
            "JV Football" if any("jv head coach" in line.lower() for line in football_coaches) else "",
            "Freshman Football" if any("frosh head coach" in line.lower() for line in football_coaches) else "",
            "Girls Flag Football" if any("girls flag football" in line.lower() for line in athletics_lines + football_section_lines) else "",
        ]
    )

    fall_season_lines = [
        line
        for line in athletics_lines
        if "fall sports" in line.lower()
        or "football" in line.lower()
        or "contract submission" in line.lower()
        or "baseline testing" in line.lower()
        or "season has ended" in line.lower()
    ]

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_hub_url": ATHLETICS_URL,
        "football_coaches_url": COACHES_URL,
        "football_team_names": football_team_names,
        "football_coaches": football_coaches,
        "football_links": football_links,
        "football_fall_season_lines": fall_season_lines,
        "football_schedule_public": False,
        "football_schedule_note": "No public football game schedule link was exposed on the school athletics pages.",
        "summary": (
            "Amador Valley High publishes football in the fall sports registration and coaches directory, with varsity, JV, and freshman football coaches listed."
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
            "proxy_server": PROXY_SERVER,
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
