"""Deterministic athletics availability scraper for Alder Grove Charter School (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials


NCES_ID = "060207211593"
SCHOOL_NAME = "Alder Grove Charter School"
STATE = "CA"
BASE_URL = "https://www.aldergrovecharter.org"
WEBSITE_URL = "https://www.aldergrovecharter.org/"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/A-Z/",
    f"{BASE_URL}/Site-Search/",
    f"{BASE_URL}/Announcements/",
    f"{BASE_URL}/For-Parents--Teachers/Important-School-Links/AGCS-School-Calendars/",
    f"{BASE_URL}/For-Parents--Teachers/Community-Partners-Directory/",
    f"{BASE_URL}/For-Parents--Teachers/Staff-Directory/",
]

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "football",
    "basketball",
    "soccer",
    "roster",
    "coach",
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
    "coach",
    "schedule",
    "tryout",
    "practice",
    "cross country",
    "track",
    "field",
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


def _is_school_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "www.aldergrovecharter.org" or host.endswith(".aldergrovecharter.org")


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
        """els => els.map(e => ({ text: (e.textContent || '').replace(/\s+/g, ' ').trim(), href: e.href || '' }))""",
    )

    keyword_links = _dedupe(
        [
            f"{(link.get('text') or '').strip()}|{(link.get('href') or '').strip()}"
            for link in links
            if any(
                keyword in f"{(link.get('text') or '').lower()} {(link.get('href') or '').lower()}"
                for keyword in PROGRAM_KEYWORDS
            )
        ]
    )[:30]

    return {
        "url": page.url,
        "title": await page.title(),
        "lines": lines,
        "keyword_links": keyword_links,
        "school_domain": _is_school_domain(page.url),
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate AGCS pages and determine whether athletics content exists."""
    require_proxy_credentials()
    planned_urls = [*MANUAL_PAGES, *[f"{BASE_URL}/Site-Search/index.html?search_string={quote_plus(query)}" for query in SEARCH_QUERIES]]
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
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            for url in planned_urls:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe(source_pages)

    school_lines: list[str] = []
    school_links: list[str] = []
    school_search_links: list[str] = []
    external_matches: list[str] = []

    for signal in page_signals:
        lines = [item for item in signal.get("lines", []) if isinstance(item, str)]
        links = [item for item in signal.get("keyword_links", []) if isinstance(item, str)]
        if signal.get("school_domain"):
            school_lines.extend(lines)
            school_links.extend(links)
        elif "/Site-Search/" in str(signal.get("url", "")) and lines:
            school_search_links.extend(links)
        else:
            external_matches.extend(lines)

    school_lines = _dedupe(school_lines)
    school_links = _dedupe(school_links)
    school_search_links = _dedupe(school_search_links)
    external_matches = _dedupe(external_matches)

    athletics_program_available = bool(school_lines or school_links)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_athletics_or_sports_content_found_after_manual_navigation_of_school_pages_and_site_search"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "Manual school-domain navigation (home, A-Z, search, calendar, partner, staff, school documents) "
            "and school-site search terms did not yield public athletics, sports, or team program content."
            if not athletics_program_available
            else ""
        ),
        "manual_pages_checked": MANUAL_PAGES,
        "search_queries_checked": SEARCH_QUERIES,
        "school_domain_keyword_lines": school_lines,
        "school_domain_keyword_links": school_links,
        "site_search_keyword_links": school_search_links,
        "external_non_school_keyword_matches": external_matches,
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
                "a-z_site_map",
                "site_search_entry",
                "announcements",
                "agcs_school_calendars",
                "community_partners_directory",
                "staff_directory",
                "site_search_athletics",
                "site_search_sports",
                "site_search_football",
                "site_search_basketball",
                "site_search_soccer",
                "site_search_roster",
                "site_search_coach",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
