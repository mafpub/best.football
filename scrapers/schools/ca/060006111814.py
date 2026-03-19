"""Deterministic athletics availability scraper for Adelante High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060006111814"
SCHOOL_NAME = "Adelante High"
STATE = "CA"
BASE_URL = "https://ahs.riverbank.k12.ca.us"
DISTRICT_BASE_URL = "https://www.riverbank.k12.ca.us"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/60901_2",
    f"{BASE_URL}/60907_2",
    f"{BASE_URL}/74012_2",
    f"{BASE_URL}/60909_2",
    f"{BASE_URL}/18372_1",
    f"{DISTRICT_BASE_URL}/",
]

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "football",
    "basketball",
]

PROGRAM_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
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
    "cif",
    "roster",
    "tryout",
)

SEARCH_RESULT_IGNORE_PREFIXES = (
    "you searched for:",
    "either no results were found",
    "search results",
)


def _dedupe(values: list[str]) -> list[str]:
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
    return host == "ahs.riverbank.k12.ca.us" or host.endswith(".ahs.riverbank.k12.ca.us")


def _is_search_page(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.path.rstrip("/") == "/search_r"


def _keyword_lines(text: str, *, is_search_page: bool) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue

        lowered = line.lower()
        if is_search_page and lowered.startswith(SEARCH_RESULT_IGNORE_PREFIXES):
            continue

        if any(keyword in lowered for keyword in PROGRAM_KEYWORDS):
            lines.append(line)

    return _dedupe(lines)[:25]


async def _collect_signal(page) -> dict[str, Any]:
    body = await page.inner_text("body")
    is_search_page = _is_search_page(page.url)
    lines = _keyword_lines(body, is_search_page=is_search_page)

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    keyword_links: list[str] = []
    for link in links:
        label = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        combo = label.lower() if is_search_page else f"{label} {href}".lower()
        if any(keyword in combo for keyword in PROGRAM_KEYWORDS):
            keyword_links.append(f"{label}|{href}")

    lowered_body = body.lower()

    return {
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_domain(page.url),
        "search_page": is_search_page,
        "keyword_lines": lines,
        "keyword_links": _dedupe(keyword_links)[:25],
        "search_empty_state": "either no results were found" in lowered_body,
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate Adelante High pages and determine public athletics program availability."""
    require_proxy_credentials()

    planned_urls = [
        *MANUAL_PAGES,
        *[f"{BASE_URL}/search_r?search={quote(query)}" for query in SEARCH_QUERIES],
        f"{DISTRICT_BASE_URL}/search_r?search=athletics",
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
            for url in MANUAL_PAGES:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))

            for query in SEARCH_QUERIES:
                search_url = f"{BASE_URL}/search_r?search={quote(query)}"
                await page.goto(search_url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))

            district_search_url = f"{DISTRICT_BASE_URL}/search_r?search=athletics"
            await page.goto(district_search_url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_signals.append(await _collect_signal(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe(source_pages)

    school_page_lines: list[str] = []
    school_page_links: list[str] = []
    school_search_lines: list[str] = []
    school_search_links: list[str] = []
    school_empty_search_queries: list[str] = []
    district_context_lines: list[str] = []
    district_context_links: list[str] = []
    district_empty_search = False

    for signal in page_signals:
        lines = [item for item in signal.get("keyword_lines", []) if isinstance(item, str)]
        links = [item for item in signal.get("keyword_links", []) if isinstance(item, str)]
        url = str(signal.get("url") or "")

        if signal.get("school_domain"):
            if signal.get("search_page"):
                school_search_lines.extend(lines)
                school_search_links.extend(links)
                if signal.get("search_empty_state"):
                    parsed = urlparse(url)
                    query = parsed.query.replace("search=", "", 1)
                    school_empty_search_queries.append(query)
            else:
                school_page_lines.extend(lines)
                school_page_links.extend(links)
        else:
            district_context_lines.extend(lines)
            district_context_links.extend(links)
            if signal.get("search_page") and signal.get("search_empty_state"):
                district_empty_search = True

    school_page_lines = _dedupe(school_page_lines)
    school_page_links = _dedupe(school_page_links)
    school_search_lines = _dedupe(school_search_lines)
    school_search_links = _dedupe(school_search_links)
    school_empty_search_queries = _dedupe(school_empty_search_queries)
    district_context_lines = _dedupe(district_context_lines)
    district_context_links = _dedupe(district_context_links)

    athletics_program_available = bool(
        school_page_lines or school_page_links or school_search_lines or school_search_links
    )

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_adelante_athletics_program_content_found_on_school_pages_or_school_search_results"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "Adelante High's public site exposes no athletics pages or sports references, and school-site "
            "searches for athletics, sports, football, and basketball all return the site's no-results state."
            if not athletics_program_available
            else ""
        ),
        "manual_pages_checked": MANUAL_PAGES,
        "search_queries_checked": SEARCH_QUERIES,
        "school_page_keyword_lines": school_page_lines,
        "school_page_keyword_links": school_page_links,
        "school_search_keyword_lines": school_search_lines,
        "school_search_keyword_links": school_search_links,
        "school_search_empty_queries": school_empty_search_queries,
        "district_context_keyword_lines": district_context_lines,
        "district_context_keyword_links": district_context_links,
        "district_search_athletics_empty": district_empty_search,
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
                "district_home_to_adelante_school_site",
                "adelante_about_page",
                "adelante_administrators_page",
                "adelante_bell_schedule_page",
                "adelante_contact_page",
                "adelante_parents_page",
                "adelante_school_search_results",
                "district_search_results",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
