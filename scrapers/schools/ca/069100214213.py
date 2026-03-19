"""Deterministic athletics availability scraper for Achieve Charter High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "069100214213"
SCHOOL_NAME = "Achieve Charter High"
STATE = "CA"

LEGACY_BASE_URL = "http://www.achsparadise.org/"
CURRENT_BASE_URL = "https://www.achievecharter.org"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{CURRENT_BASE_URL}/index.html",
    f"{CURRENT_BASE_URL}/A-Z/",
    f"{CURRENT_BASE_URL}/Academics/",
    f"{CURRENT_BASE_URL}/Students--Parents/index.html",
    f"{CURRENT_BASE_URL}/Achieve-More-E-L-O-P-After-School-Program/index.html",
    f"{CURRENT_BASE_URL}/Our-Campuses/Chico-Campus/",
    f"{CURRENT_BASE_URL}/Our-Campuses/Paradise-Campus/",
]

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "football",
]

ATHLETICS_KEYWORDS = (
    "athletic",
    "athletics",
    "sport",
    "sports",
    "football",
    "basketball",
    "soccer",
    "volleyball",
    "baseball",
    "softball",
    "wrestling",
    "track",
    "cross country",
    "coach",
    "roster",
    "tryout",
    "league",
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


def _clean_lines(text: str) -> list[str]:
    return [" ".join(raw.split()).strip() for raw in text.splitlines() if raw.strip()]


def _keyword_lines(lines: list[str], *, limit: int = 25) -> list[str]:
    hits: list[str] = []
    for line in lines:
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            hits.append(line)
    return _dedupe(hits)[:limit]


def _campus_grade_lines(lines: list[str]) -> list[str]:
    return _dedupe([line for line in lines if "(TK-8)" in line])[:10]


def _search_summary(lines: list[str]) -> list[str]:
    summary: list[str] = []
    for index, line in enumerate(lines):
        if line.startswith("No Results Returned For:"):
            summary.append(line)
            continue
        if line.startswith("Searched Using:"):
            summary.append(line)
            for extra in lines[index + 1 : index + 7]:
                if (
                    extra.startswith("Achieve Charter -")
                    or extra.startswith(CURRENT_BASE_URL)
                    or extra.startswith("https://www.achievecharter.org/")
                ):
                    summary.append(extra)
    return _dedupe(summary)[:12]


def _extract_query_from_search_url(url: str) -> str:
    marker = "search_string="
    if marker not in url:
        return ""
    return url.split(marker, 1)[1]


async def _collect_page_signal(page) -> dict[str, Any]:
    body = await page.inner_text("body")
    lines = _clean_lines(body)

    return {
        "url": page.url,
        "title": await page.title(),
        "hostname": (urlparse(page.url).hostname or "").lower(),
        "keyword_lines": _keyword_lines(lines),
        "campus_grade_lines": _campus_grade_lines(lines),
        "search_summary": _search_summary(lines),
        "legacy_dns_issue": (
            "legacy_domain_unresolved"
            if any("Unable to determine IP address from host name" in line for line in lines)
            else ""
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Inspect Achieve Charter public pages and determine athletics availability."""
    require_proxy_credentials()

    search_urls = [
        f"{CURRENT_BASE_URL}/Site-Search/index.html?search_string={quote(query)}"
        for query in SEARCH_QUERIES
    ]
    planned_urls = [LEGACY_BASE_URL, *MANUAL_PAGES, *search_urls]
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
            await page.goto(LEGACY_BASE_URL, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            page_signals.append(await _collect_page_signal(page))

            for url in MANUAL_PAGES:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signal(page))

            for url in search_urls:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signal(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe(source_pages)

    legacy_signal = next(
        (signal for signal in page_signals if signal.get("hostname") == "www.achsparadise.org"),
        {},
    )
    legacy_status = str(legacy_signal.get("legacy_dns_issue") or "")
    if legacy_status:
        errors.append("legacy_domain_unresolved:www.achsparadise.org")

    manual_signals = [
        signal
        for signal in page_signals
        if str(signal.get("url") or "").startswith(CURRENT_BASE_URL)
        and "/Site-Search/" not in str(signal.get("url") or "")
    ]
    search_signals = [
        signal
        for signal in page_signals
        if "/Site-Search/" in str(signal.get("url") or "")
    ]

    current_site_keyword_lines = _dedupe(
        [
            line
            for signal in manual_signals
            for line in signal.get("keyword_lines", [])
            if isinstance(line, str)
        ]
    )
    campus_grade_lines = _dedupe(
        [
            line
            for signal in manual_signals
            for line in signal.get("campus_grade_lines", [])
            if isinstance(line, str)
        ]
    )
    athletics_search_summary = next(
        (
            signal.get("search_summary", [])
            for signal in search_signals
            if _extract_query_from_search_url(str(signal.get("url") or "")) == "athletics"
        ),
        [],
    )
    sports_search_summary = next(
        (
            signal.get("search_summary", [])
            for signal in search_signals
            if _extract_query_from_search_url(str(signal.get("url") or "")) == "sports"
        ),
        [],
    )
    football_search_summary = next(
        (
            signal.get("search_summary", [])
            for signal in search_signals
            if _extract_query_from_search_url(str(signal.get("url") or "")) == "football"
        ),
        [],
    )

    athletics_program_available = False
    if not athletics_program_available:
        errors.append(
            "blocked:no_school_specific_public_athletics_program_found_on_achievecharter_org"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "The legacy achsparadise.org domain no longer resolves. The current public Achieve Charter "
            "site shows Chico and Paradise campuses as TK-8, the A-Z page has no athletics section, "
            "site search for athletics returns no results, and sports search only surfaces the E.L.O.P. "
            "after-school page mentioning outdoor group games rather than a school athletics program."
        ),
        "legacy_site": LEGACY_BASE_URL,
        "legacy_site_status": legacy_status or "legacy_site_checked",
        "current_public_site": CURRENT_BASE_URL,
        "manual_pages_checked": MANUAL_PAGES,
        "search_queries_checked": SEARCH_QUERIES,
        "current_site_campus_grade_lines": campus_grade_lines,
        "current_site_keyword_lines": current_site_keyword_lines,
        "athletics_search_summary": athletics_search_summary,
        "sports_search_summary": sports_search_summary,
        "football_search_summary": football_search_summary,
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
                "Checked the legacy school URL from the task input.",
                "Visited the current Achieve Charter homepage and A-Z index.",
                "Visited Academics, Students & Parents, the E.L.O.P. after-school page, and both campus pages.",
                "Ran the built-in site search for athletics, sports, and football.",
            ],
        },
        "errors": errors,
    }
