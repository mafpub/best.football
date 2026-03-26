"""Deterministic football scraper for Centerville High (Continuation), CA."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "063140007333"
SCHOOL_NAME = "Centerville High (Continuation)"
STATE = "CA"
BASE_URL = "https://www.pottervalleyschools.us"
FOOTBALL_PAGE = f"{BASE_URL}/copy-of-football"
ATHLETICS_PAGE = f"{BASE_URL}/athletics"
TARGET_URLS = [ATHLETICS_PAGE, FOOTBALL_PAGE]


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = _clean(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _extract_keyword_lines(text: str) -> list[str]:
    lines: list[str] = []
    keywords = ("football", "roster", "schedule", "coach", "team", "win")
    for raw in text.splitlines():
        line = _clean(raw)
        if not line:
            continue
        lower = line.lower()
        if any(token in lower for token in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_coaches(text: str) -> list[str]:
    coaches: list[str] = []
    for raw in text.splitlines():
        line = _clean(raw)
        if not line:
            continue
        if re.search(r"\bcoach\b", line, re.I):
            coaches.append(line)
    return _dedupe_keep_order(coaches)


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)

    body_text = await page.inner_text("body")
    normalized = _clean(body_text)

    all_links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(el => ({text: (el.textContent || '').replace(/\\s+/g, ' ').trim(), href: (el.href || '').trim()}))",
    )
    links = []
    if isinstance(all_links, list):
        for item in all_links:
            if not isinstance(item, dict):
                continue
            href = str(item.get("href") or "").strip()
            if href:
                links.append({"text": _clean(str(item.get("text") or "")), "href": href})

    football_lines = _extract_keyword_lines(normalized)

    return {
        "requested_url": url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": normalized,
        "football_lines": football_lines,
        "coach_lines": _extract_coaches(normalized),
        "links": links,
    }


async def scrape_athletics() -> dict[str, Any]:
    require_proxy_credentials()
    assert_not_blocklisted(TARGET_URLS)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile="datacenter"),
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 920},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        page_signals: list[dict[str, Any]] = []
        source_pages: list[str] = []
        errors: list[str] = []

        try:
            for url in TARGET_URLS:
                try:
                    signal = await _collect_page(page, url)
                    page_signals.append(signal)
                    source_pages.append(signal["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    football_lines: list[str] = []
    coach_lines: list[str] = []
    links: list[dict[str, str]] = []

    for signal in page_signals:
        football_lines.extend(signal.get("football_lines", []))
        coach_lines.extend(signal.get("coach_lines", []))
        links.extend(signal.get("links", []))

    football_lines = _dedupe_keep_order(football_lines)
    coach_lines = _dedupe_keep_order(coach_lines)

    football_links = [
        link
        for link in _dedupe_keep_order([
            f"{item.get('text')}|{item.get('href')}" for item in links if isinstance(item, dict)
            and isinstance(item.get("href"), str)
            and ("football" in item.get("href", "").lower() or "copy-of-football" in item.get("href", "").lower())
        ])
    ]

    football_schedule_links = [
        item for item in football_links if "schedule" in item.lower()
    ]

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_lines or coach_lines),
        "athletics_page": ATHLETICS_PAGE,
        "football_page": FOOTBALL_PAGE,
        "football_team_names": ["Football"] if any("football" in text.lower() for text in football_lines) else [],
        "football_keyword_lines": football_lines[:80],
        "football_coach_lines": coach_lines[:25],
        "football_links": football_links[:40],
        "football_schedule_links": football_schedule_links[:20],
        "summary": "Potter Valley publishes a football page listing team, schedule, and roster style content from its athletics section."
        if any("football" in text.lower() for text in football_lines)
        else "",
    }

    if not extracted_items["football_program_available"]:
        errors.append("blocked:no_public_football_content_found_on_page")

    source_pages = _dedupe_keep_order(source_pages)

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages or [FOOTBALL_PAGE],
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }
