"""Deterministic athletics scraper for ALBA (CA).

This script manually traverses ALBA's public navigation pages and checks for
school-specific athletics program content.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "063432007682"
SCHOOL_NAME = "ALBA"
STATE = "CA"

HOME_URL = "https://www.alba.sandiegounified.org/"
FALLBACK_URLS = {
    "School Information": "https://www.alba.sandiegounified.org/school_information",
    "Students & Parents": "https://www.alba.sandiegounified.org/students_parents",
    "ALBA Pathways": "https://www.alba.sandiegounified.org/alba_pathways",
    "Community Schools Information": "https://www.alba.sandiegounified.org/student_activities",
    "News": "https://www.alba.sandiegounified.org/news",
    "Calendar": "https://www.alba.sandiegounified.org/calendar",
}
MANUAL_NAV_ORDER = [
    "School Information",
    "Students & Parents",
    "ALBA Pathways",
    "Community Schools Information",
    "News",
    "Calendar",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "sport",
    "football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
    "volleyball",
    "wrestling",
    "track",
    "cross country",
    "cheer",
)


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_keyword_lines(text: str, limit: int = 25) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _extract_athletics_links(page) -> list[str]:
    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').trim(),
            href: e.href || ''
        }))""",
    )

    out: list[str] = []
    for anchor in anchors:
        text = " ".join(str(anchor.get("text") or "").split()).strip()
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        combo = f"{text} {href}".lower()
        if any(keyword in combo for keyword in ATHLETICS_KEYWORDS):
            out.append(href)
    return _dedupe_keep_order(out)


async def _visit_and_collect(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)

    body_text = await page.inner_text("body")
    keyword_lines = _extract_keyword_lines(body_text)
    athletics_links = await _extract_athletics_links(page)

    return {
        "url": page.url,
        "title": await page.title(),
        "athletics_keyword_lines": keyword_lines,
        "athletics_links": athletics_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape ALBA pages to determine whether public athletics content exists."""
    require_proxy_credentials()

    planned_urls = [HOME_URL, *FALLBACK_URLS.values()]
    assert_not_blocklisted(planned_urls)

    errors: list[str] = []
    source_pages: list[str] = []
    collected: list[dict[str, Any]] = []

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
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            home_data = await _visit_and_collect(page, HOME_URL)
            source_pages.append(home_data["url"])
            collected.append(home_data)

            for nav_label in MANUAL_NAV_ORDER:
                clicked = False
                locator = page.get_by_role("link", name=nav_label).first
                if await locator.count() > 0:
                    try:
                        await locator.click()
                        await page.wait_for_load_state("domcontentloaded")
                        await page.wait_for_timeout(1200)
                        clicked = True
                    except Exception:  # noqa: BLE001
                        clicked = False

                if not clicked:
                    fallback_url = FALLBACK_URLS[nav_label]
                    await page.goto(fallback_url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)

                body_text = await page.inner_text("body")
                source_pages.append(page.url)
                collected.append(
                    {
                        "url": page.url,
                        "title": await page.title(),
                        "athletics_keyword_lines": _extract_keyword_lines(body_text),
                        "athletics_links": await _extract_athletics_links(page),
                    }
                )

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_keyword_lines: list[str] = []
    all_athletics_links: list[str] = []
    for item in collected:
        all_keyword_lines.extend(item.get("athletics_keyword_lines", []))
        all_athletics_links.extend(item.get("athletics_links", []))

    all_keyword_lines = _dedupe_keep_order(all_keyword_lines)
    all_athletics_links = _dedupe_keep_order(all_athletics_links)

    # School-specific athletics content is only true when athletics terms appear on ALBA pages.
    alba_keyword_lines = [
        line
        for line in all_keyword_lines
        if "alba" in line.lower() or "community day school" in line.lower()
    ]
    athletics_program_available = bool(alba_keyword_lines)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_alba_athletics_program_content_found_on_school_navigation_pages"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "manual_navigation_pages": [item.get("url", "") for item in collected],
        "athletics_keyword_mentions": alba_keyword_lines,
        "athletics_links_found": all_athletics_links,
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
            "pages_checked": len(source_pages),
            "manual_navigation_order": MANUAL_NAV_ORDER,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
