"""Deterministic athletics availability scraper for Acalanes Center For Independent Study (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060165010751"
SCHOOL_NAME = "Acalanes Center For Independent Study"
STATE = "CA"
BASE_URL = "https://acis.acalanes.k12.ca.us"
DISTRICT_BASE_URL = "https://www.acalanes.k12.ca.us"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/student-life",
    f"{BASE_URL}/student-life/student-activities",
    f"{BASE_URL}/student-life/clubs",
    f"{BASE_URL}/about-acis/acis-information1/student-handbook",
    f"{DISTRICT_BASE_URL}/schools/acalanes-center-for-independent-study",
    f"{DISTRICT_BASE_URL}/departments/educational-services/auhsd-independent-study-program",
    f"{DISTRICT_BASE_URL}/departments/athletics",
    f"{DISTRICT_BASE_URL}/departments/educational-services/course-information/physical-education-athletic-pe",
]

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "football",
    "basketball",
    "soccer",
    "volleyball",
]

PROGRAM_KEYWORDS = (
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
    "track",
    "coach",
    "roster",
    "tryout",
    "cif",
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
    return host == "acis.acalanes.k12.ca.us" or host.endswith(".acis.acalanes.k12.ca.us")


def _is_search_page(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.path.rstrip("/") == "/search-results"


def _keyword_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in PROGRAM_KEYWORDS):
            lines.append(line)
    return _dedupe(lines)[:25]


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

    keyword_links: list[str] = []
    merchandise_links: list[str] = []
    for link in links:
        label = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        combo = f"{label} {href}".lower()
        parsed_href = urlparse(href)

        if "sideline.bsnsports.com" in href.lower():
            merchandise_links.append(f"{label}|{href}")
            continue

        if parsed_href.path.rstrip("/") == "/search-results":
            continue

        if any(keyword in combo for keyword in PROGRAM_KEYWORDS):
            keyword_links.append(f"{label}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_domain(page.url),
        "search_page": _is_search_page(page.url),
        "keyword_lines": lines,
        "keyword_links": _dedupe(keyword_links)[:25],
        "merchandise_links": _dedupe(merchandise_links)[:25],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate ACIS pages and determine whether the school hosts a public athletics program."""
    require_proxy_credentials()

    planned_urls = [
        *MANUAL_PAGES,
        *[f"{BASE_URL}/search-results?q={quote(query)}" for query in SEARCH_QUERIES],
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
                search_url = f"{BASE_URL}/search-results?q={quote(query)}"
                await page.goto(search_url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe(source_pages)

    school_domain_lines: list[str] = []
    school_domain_links: list[str] = []
    search_result_links: list[str] = []
    district_context_lines: list[str] = []
    district_context_links: list[str] = []
    merchandise_links: list[str] = []

    for signal in page_signals:
        lines = [item for item in signal.get("keyword_lines", []) if isinstance(item, str)]
        links = [item for item in signal.get("keyword_links", []) if isinstance(item, str)]
        merch = [item for item in signal.get("merchandise_links", []) if isinstance(item, str)]

        merchandise_links.extend(merch)

        if signal.get("school_domain"):
            if signal.get("search_page"):
                search_result_links.extend(links)
            else:
                school_domain_lines.extend(lines)
                school_domain_links.extend(links)
        else:
            district_context_lines.extend(lines)
            district_context_links.extend(links)

    school_domain_lines = _dedupe(school_domain_lines)
    school_domain_links = _dedupe(school_domain_links)
    search_result_links = _dedupe(search_result_links)
    district_context_lines = _dedupe(district_context_lines)
    district_context_links = _dedupe(district_context_links)
    merchandise_links = _dedupe(merchandise_links)

    athletics_program_available = bool(school_domain_lines or school_domain_links)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_acis_hosted_athletics_program_content_found_on_school_pages_or_school_search_results"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "ACIS-hosted pages and school-site search results did not show a public athletics program. "
            "District-wide athletics and Athletic PE pages exist, but they do not establish an ACIS-specific "
            "school athletics program."
            if not athletics_program_available
            else ""
        ),
        "manual_pages_checked": MANUAL_PAGES,
        "search_queries_checked": SEARCH_QUERIES,
        "school_domain_keyword_lines": school_domain_lines,
        "school_domain_keyword_links": school_domain_links,
        "school_search_result_keyword_links": search_result_links,
        "district_context_keyword_lines": district_context_lines,
        "district_context_keyword_links": district_context_links,
        "external_merchandise_links": merchandise_links,
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
                "school_home_page",
                "student_life_page",
                "student_activities_page",
                "clubs_page",
                "student_handbook_page",
                "district_school_landing_page",
                "district_independent_study_page",
                "district_athletics_page",
                "district_athletic_pe_page",
                "school_site_search_results",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
