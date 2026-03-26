"""Deterministic football scraper for Central High (CA)."""

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

NCES_ID = "060797014556"
SCHOOL_NAME = "Central High"
STATE = "CA"
BASE_URL = "https://chs.centralunified.org"
ATHLETICS_HOME = f"{BASE_URL}/athletics-home"
FOOTBALL_PAGE = f"{BASE_URL}/athletics-home/fall-sports/football"
TARGET_URLS = [ATHLETICS_HOME, FOOTBALL_PAGE]


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = _clean(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _extract_lines(body: str) -> list[str]:
    lines: list[str] = []
    for raw in body.splitlines():
        line = _clean(raw)
        if not line:
            continue
        lower = line.lower()
        if "football" in lower or "coach" in lower or "schedule" in lower or "roster" in lower or "team" in lower:
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_nameish(value: str) -> str:
    return _clean(re.sub(r"\s+", " ", value))


async def _collect(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)

    body = _clean(await page.inner_text("body"))
    title = _clean(await page.title())
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(el => ({ text: (el.textContent || '').replace(/\\s+/g, ' ').trim(), href: el.href || '' }))",
    )
    link_pairs = [
        f"{_extract_nameish(item.get('text') or '')}|{str(item.get('href') or '').strip()}"
        for item in links
        if isinstance(item, dict) and item.get("href")
    ]
    return {
        "url": page.url,
        "title": title,
        "body": body,
        "lines": _extract_lines(body),
        "links": _dedupe_keep_order(link_pairs),
    }


async def scrape_athletics() -> dict[str, Any]:
    require_proxy_credentials()
    assert_not_blocklisted(TARGET_URLS)

    football_text: list[str] = []
    all_links: list[str] = []
    source_pages: list[str] = []
    errors: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile="datacenter"),
        )
        context = await browser.new_context(viewport={"width": 1400, "height": 900})
        page = await context.new_page()
        try:
            for target in TARGET_URLS:
                try:
                    signal = await _collect(page, target)
                    page_signals.append(signal)
                    source_pages.append(signal["url"])
                    football_text.extend(signal["lines"])
                    all_links.extend(signal["links"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{target}")
        finally:
            await browser.close()

    football_lines = _dedupe_keep_order(football_text)
    links = _dedupe_keep_order(all_links)
    football_links = [item for item in links if "football" in item.lower()]

    program_found = any("football" in line.lower() for line in football_lines)
    if not program_found:
        errors.append("blocked:no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": program_found,
        "athletics_home": ATHLETICS_HOME,
        "football_page": FOOTBALL_PAGE,
        "football_team_names": ["Football"] if program_found else [],
        "football_lines": football_lines[:120],
        "football_links": football_links,
        "summary": "Central High athletics provides a football section under fall sports with schedule/roster-style content in crawlable HTML." if program_found else "",
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
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
