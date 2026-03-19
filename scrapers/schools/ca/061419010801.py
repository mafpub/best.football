"""Deterministic athletics availability scraper for Academy of the Redwoods (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "061419010801"
SCHOOL_NAME = "Academy of the Redwoods"
STATE = "CA"

BASE_URL = "https://ar.fuhsdistrict.org"
HOME_URL = f"{BASE_URL}/"
ABOUT_URL = f"{BASE_URL}/about-us"
PROGRAM_OVERVIEW_URL = f"{BASE_URL}/program-overview"
ACADEMICS_URL = f"{BASE_URL}/academics"
STUDENTS_URL = f"{BASE_URL}/95749_1"
REGISTRATION_GUIDE_URL = f"{BASE_URL}/336387_2"
REDWOOD_REVIEW_HOME_URL = f"{BASE_URL}/336396_2"
SITE_MAP_URL = f"{BASE_URL}/site_map"
SEARCH_URL = f"{BASE_URL}/search_e"

MANUAL_NAVIGATION = [
    ("Home", HOME_URL),
    ("About Us", ABOUT_URL),
    ("Program Overview", PROGRAM_OVERVIEW_URL),
    ("Academics", ACADEMICS_URL),
    ("Students", STUDENTS_URL),
    ("Registration Guide", REGISTRATION_GUIDE_URL),
    ("Redwood Review Home", REDWOOD_REVIEW_HOME_URL),
    ("Site Map", SITE_MAP_URL),
]

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "football",
    "basketball",
    "soccer",
    "volleyball",
]

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
    "track",
    "cross country",
    "wrestling",
)

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")


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
    return host == "ar.fuhsdistrict.org" or host.endswith(".ar.fuhsdistrict.org")


def _extract_keyword_lines(text: str, *, limit: int = 30) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _extract_keyword_links(page, *, limit: int = 30) -> list[str]:
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    matches: list[str] = []
    for link in links:
        text = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in ATHLETICS_KEYWORDS):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)[:limit]


async def _collect_page_signal(page, *, label: str, search_query: str = "") -> dict[str, Any]:
    body_text = await page.inner_text("body")
    return {
        "label": label,
        "search_query": search_query,
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_domain(page.url),
        "keyword_lines": _extract_keyword_lines(body_text),
        "keyword_links": await _extract_keyword_links(page),
        "no_results": "No results have been found." in body_text,
    }


async def _open_via_link_or_fallback(page, *, link_name: str, fallback_url: str) -> None:
    locator = page.get_by_role("link", name=link_name, exact=False).first
    if await locator.count() > 0:
        try:
            await locator.click(timeout=15000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(1500)
            return
        except Exception:  # noqa: BLE001
            pass

    await page.goto(fallback_url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1500)


async def _run_site_search(page, query: str) -> dict[str, Any]:
    await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)

    search_input = page.locator('input[placeholder="Search"]').first
    await search_input.fill(query)
    await page.get_by_role("button", name="Search").last.click()
    await page.wait_for_timeout(2200)

    signal = await _collect_page_signal(page, label="Search", search_query=query)
    return {
        "query": query,
        "url": signal["url"],
        "title": signal["title"],
        "no_results": signal["no_results"],
        "keyword_lines": signal["keyword_lines"],
        "keyword_links": signal["keyword_links"],
    }


async def scrape_school() -> dict[str, Any]:
    """Inspect visible school pages and search results for public athletics content."""
    require_proxy_credentials()

    planned_urls = [url for _, url in MANUAL_NAVIGATION] + [SEARCH_URL]
    assert_not_blocklisted(planned_urls)

    source_pages: list[str] = []
    manual_page_signals: list[dict[str, Any]] = []
    search_signals: list[dict[str, Any]] = []
    errors: list[str] = []

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
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1800)
            source_pages.append(page.url)
            manual_page_signals.append(await _collect_page_signal(page, label="Home"))

            for link_name, fallback_url in MANUAL_NAVIGATION[1:]:
                await _open_via_link_or_fallback(
                    page,
                    link_name=link_name,
                    fallback_url=fallback_url,
                )
                source_pages.append(page.url)
                manual_page_signals.append(
                    await _collect_page_signal(page, label=link_name)
                )

            for query in SEARCH_QUERIES:
                try:
                    signal = await _run_site_search(page, query)
                    source_pages.append(signal["url"])
                    search_signals.append(signal)
                except PlaywrightTimeoutError:
                    errors.append(f"site_search_timeout:{query}")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"site_search_failed:{query}:{type(exc).__name__}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_domain_keyword_lines: list[str] = []
    school_domain_keyword_links: list[str] = []
    for signal in manual_page_signals:
        if not signal.get("school_domain"):
            continue
        school_domain_keyword_lines.extend(
            value
            for value in signal.get("keyword_lines", [])
            if isinstance(value, str)
        )
        school_domain_keyword_links.extend(
            value
            for value in signal.get("keyword_links", [])
            if isinstance(value, str)
        )

    school_domain_keyword_lines = _dedupe_keep_order(school_domain_keyword_lines)
    school_domain_keyword_links = _dedupe_keep_order(school_domain_keyword_links)

    search_keyword_lines: list[str] = []
    search_keyword_links: list[str] = []
    search_observations: list[str] = []
    for signal in search_signals:
        query = str(signal.get("query") or "")
        lines = [
            value for value in signal.get("keyword_lines", []) if isinstance(value, str)
        ]
        links = [
            value for value in signal.get("keyword_links", []) if isinstance(value, str)
        ]
        search_keyword_lines.extend(lines)
        search_keyword_links.extend(links)

        if signal.get("no_results"):
            search_observations.append(f"{query}:no_results")
        elif lines or links:
            search_observations.append(f"{query}:keyword_match_in_search_page")
        else:
            search_observations.append(f"{query:}:results_without_athletics_keywords")

    search_keyword_lines = _dedupe_keep_order(search_keyword_lines)
    search_keyword_links = _dedupe_keep_order(search_keyword_links)
    search_observations = _dedupe_keep_order(search_observations)

    athletics_program_available = bool(
        school_domain_keyword_lines
        or school_domain_keyword_links
        or search_keyword_lines
        or search_keyword_links
    )

    blocked_reason = ""
    if not athletics_program_available:
        blocked_reason = (
            "No public school-hosted athletics program content was found on Academy of "
            "the Redwoods navigation pages or in the school's own site search results."
        )
        errors.append(
            "blocked:no_public_school_hosted_athletics_program_content_found"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "manual_navigation_pages_checked": [url for _, url in MANUAL_NAVIGATION],
        "site_search_queries_checked": SEARCH_QUERIES,
        "school_domain_athletics_keyword_mentions": school_domain_keyword_lines,
        "school_domain_athletics_links": school_domain_keyword_links,
        "site_search_athletics_keyword_mentions": search_keyword_lines,
        "site_search_athletics_links": search_keyword_links,
        "site_search_observations": search_observations,
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
            "manual_navigation_labels": [label for label, _ in MANUAL_NAVIGATION],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
