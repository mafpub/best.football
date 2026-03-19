"""Deterministic athletics availability scraper for ASA Charter (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060171111105"
SCHOOL_NAME = "ASA Charter"
STATE = "CA"
BASE_URL = "https://www.asacharterschool.com"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1280496&type=d&pREC_ID=1492454",  # High School - Academics
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1280392&type=d&pREC_ID=1492396",  # High School - Calendars
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1320800&type=d&pREC_ID=1518990",  # Students
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1320803&type=d&pREC_ID=1518997",  # Parents
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1733618&type=d&pREC_ID=2181370",  # ASES/ELOP
    f"{BASE_URL}/apps/news/",
    f"{BASE_URL}/apps/search/",
]

RESOURCE_LINKS = [
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1320803&type=d&pREC_ID=1574744",  # 100 Mile Club
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1320803&type=d&pREC_ID=1574736",  # Book It!
]

SEARCH_QUERIES = [
    "athletics",
    "athletic",
    "sports",
    "football",
    "basketball",
    "soccer",
    "volleyball",
    "baseball",
    "softball",
    "wrestling",
    "track",
]

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "football",
    "basketball",
    "soccer",
    "volleyball",
    "baseball",
    "softball",
    "wrestling",
    "cross country",
    "track and field",
    "track",
    "roster",
    "coach",
    "tryout",
    "league",
    "schedule",
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


def _is_school_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "www.asacharterschool.com" or host.endswith(".asacharterschool.com")


def _keyword_lines(text: str, *, limit: int = 30) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _collect_signal(page) -> dict[str, Any]:
    body = await page.inner_text("body")
    lines = _keyword_lines(body)

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    athletics_links: list[str] = []
    for link in links:
        label = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        parsed_href = urlparse(href)
        href_without_query = f"{parsed_href.netloc}{parsed_href.path}".lower()
        combo = f"{label} {href_without_query}".lower()
        if any(keyword in combo for keyword in ATHLETICS_KEYWORDS):
            athletics_links.append(f"{label}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_domain(page.url),
        "keyword_lines": lines,
        "athletics_links": _dedupe_keep_order(athletics_links)[:30],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate ASA Charter pages and determine public athletics program availability."""
    require_proxy_credentials()

    planned_urls = [
        *MANUAL_PAGES,
        *RESOURCE_LINKS,
        *[f"{BASE_URL}/?s={query}" for query in SEARCH_QUERIES],
    ]
    assert_not_blocklisted(planned_urls)

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
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            await page.goto(MANUAL_PAGES[0], wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            page_signals.append(await _collect_signal(page))

            menu_toggle = page.get_by_role("link", name="Main Menu Toggle").first
            if await menu_toggle.count() > 0:
                await menu_toggle.click()
                await page.wait_for_timeout(800)

            for url in MANUAL_PAGES[1:]:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))

            for url in RESOURCE_LINKS:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))

            for query in SEARCH_QUERIES:
                search_url = f"{BASE_URL}/?s={query}"
                await page.goto(search_url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_domain_lines: list[str] = []
    school_domain_links: list[str] = []
    external_athletics_mentions: list[str] = []

    for signal in page_signals:
        lines = [item for item in signal.get("keyword_lines", []) if isinstance(item, str)]
        links = [item for item in signal.get("athletics_links", []) if isinstance(item, str)]

        if signal.get("school_domain"):
            school_domain_lines.extend(lines)
            school_domain_links.extend(links)
        elif lines or links:
            url = str(signal.get("url") or "")
            for item in lines[:10]:
                external_athletics_mentions.append(f"{url}|{item}")
            for item in links[:10]:
                external_athletics_mentions.append(f"{url}|{item}")

    school_domain_lines = _dedupe_keep_order(school_domain_lines)
    school_domain_links = _dedupe_keep_order(school_domain_links)
    external_athletics_mentions = _dedupe_keep_order(external_athletics_mentions)

    athletics_program_available = bool(school_domain_lines or school_domain_links)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_athletics_program_content_found_on_school_domain_manual_pages_or_site_search"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "No school-hosted public athletics program content found. Checked home/menu pages, "
            "students/parents/ASES pages, resource links, and site-search result URLs."
            if not athletics_program_available
            else ""
        ),
        "manual_pages_checked": MANUAL_PAGES,
        "resource_links_checked": RESOURCE_LINKS,
        "search_queries_checked": SEARCH_QUERIES,
        "school_domain_athletics_keyword_lines": school_domain_lines,
        "school_domain_athletics_links": school_domain_links,
        "external_athletics_mentions": external_athletics_mentions,
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
                "main_menu_toggle",
                "students_and_parents_pages",
                "ases_elop_page",
                "resource_redirect_pages",
                "site_search_query_pages",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
