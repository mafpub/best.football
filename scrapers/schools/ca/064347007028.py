"""Deterministic athletics availability scraper for Albert Powell Continuation (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from urllib.parse import quote_plus
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials


NCES_ID = "064347007028"
SCHOOL_NAME = "Albert Powell Continuation"
STATE = "CA"
BASE_URL = "https://aphs.ycusd.org"
WEBSITE_URL = "https://aphs.ycusd.k12.ca.us"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/Our-School/index.html",
    f"{BASE_URL}/Counseling/index.html",
    f"{BASE_URL}/Activities/Bonus-Friday/index.html",
    f"{BASE_URL}/Activities/On-Site-Events/index.html",
    f"{BASE_URL}/Calendar/index.html",
    f"{BASE_URL}/Contact-Us/index.html",
    f"{BASE_URL}/A-Z/",
]

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
]

PROGRAM_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "sport",
    "football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
    "team",
    "roster",
    "schedule",
    "cross country",
    "cheer",
    "cif",
)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _keyword_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        lowered = line.lower()
        if not line:
            continue
        if any(keyword in lowered for keyword in PROGRAM_KEYWORDS):
            lines.append(line)
    return _dedupe(lines)[:24]


async def _collect_signal(page) -> dict[str, Any]:
    body = ""
    try:
        body = await page.get_by_role("main").inner_text()
    except Exception:
        body = await page.locator("body").inner_text()
    body = body.replace("\u00a0", " ")

    lines = _keyword_lines(body)
    links = await page.eval_on_selector_all(
        "a[href]",
        """
        (els) => els.map((e) => ({
          text: (e.textContent || "").replace(/\s+/g, ' ').trim(),
          href: e.href || '',
        }))
        """,
    )

    keyword_links = _dedupe(
        [
            f"{(item.get('text') or '').strip()}|{(item.get('href') or '').strip()}"
            for item in links
            if any(
                keyword in f"{(item.get('text') or '').lower()} {(item.get('href') or '').lower()}"
                for keyword in PROGRAM_KEYWORDS
            )
        ]
    )[:24]

    return {
        "url": page.url,
        "title": await page.title(),
        "keyword_lines": lines,
        "keyword_links": keyword_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate Albert Powell Continuation pages and detect athletics program links/content."""
    require_proxy_credentials()

    planned_urls = [
        *MANUAL_PAGES,
        *[f"{BASE_URL}/Site-Search/index.html?search_string={quote_plus(query)}" for query in SEARCH_QUERIES],
    ]
    assert_not_blocklisted(planned_urls + [WEBSITE_URL])

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
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            for url in planned_urls:
                await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                await page.wait_for_timeout(1_000)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe(source_pages)

    keyword_lines: list[str] = []
    keyword_links: list[str] = []
    for signal in page_signals:
        keyword_lines.extend([item for item in signal.get("keyword_lines", []) if isinstance(item, str)])
        keyword_links.extend([item for item in signal.get("keyword_links", []) if isinstance(item, str)])

    keyword_lines = _dedupe(keyword_lines)
    keyword_links = _dedupe(keyword_links)

    athletics_program_available = bool(keyword_lines or keyword_links)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_athletics_or_sports_content_found_within_homepage_nav_search_or_activity_pages"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "No public athletics page, sports section, or team-related content was found after navigating "
            "Albert Powell Continuation home, menus, activities, calendar, and site-search pages."
            if not athletics_program_available
            else ""
        ),
        "manual_pages_checked": MANUAL_PAGES,
        "search_queries_checked": SEARCH_QUERIES,
        "keyword_lines": keyword_lines,
        "keyword_links": keyword_links,
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
            "manual_navigation_steps": [
                "home_page",
                "our_school_page",
                "counseling_page",
                "activities_bonus_friday_page",
                "activities_onsite_events_page",
                "calendar_page",
                "contact_us_page",
                "a-z_page_map",
                "site_search_athletics",
                "site_search_sports",
                "site_search_football",
                "site_search_basketball",
                "site_search_baseball",
                "site_search_softball",
                "site_search_soccer",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
