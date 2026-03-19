"""Deterministic athletics scraper for ACCESS Juvenile Hall (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "069102409236"
SCHOOL_NAME = "ACCESS Juvenile Hall"
STATE = "CA"

HOME_URL = "https://directory.ocde.us/"
ACCESS_URL = "https://directory.ocde.us/access/"
COUNTY_OPERATED_URL = "https://directory.ocde.us/county-operated-schools/"
SEARCH_QUERIES = [
    "ACCESS Juvenile Hall",
    "ACCESS Juvenile Hall athletics",
    "ACCESS Juvenile Hall sports",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
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
TARGET_KEYWORDS = (
    "access juvenile hall",
    "juvenile hall",
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


def _keyword_lines(text: str, limit: int = 20) -> list[str]:
    results: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue

        lowered = line.lower()
        if (
            lowered.startswith("no results for")
            or lowered.startswith("search results for")
            or "\"access juvenile hall" in lowered
            or "«access juvenile hall" in lowered
        ):
            continue

        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            results.append(line)

    return _dedupe_keep_order(results)[:limit]


async def _collect_page_signals(page) -> dict[str, Any]:
    text = await page.inner_text("body")
    text_lower = text.lower()

    athletics_lines = _keyword_lines(text)
    has_no_results = "no results" in text_lower
    mentions_target_school = any(keyword in text_lower for keyword in TARGET_KEYWORDS)

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").trim(),
            href: e.href || ""
        }))""",
    )

    matching_links: list[dict[str, str]] = []
    for link in links:
        href = str(link.get("href") or "").strip()
        label = " ".join(str(link.get("text") or "").split()).strip()
        combo = f"{label} {href}".lower()
        if any(keyword in combo for keyword in TARGET_KEYWORDS):
            matching_links.append({"title": label or href, "url": href})

    return {
        "url": page.url,
        "mentions_target_school": mentions_target_school,
        "has_no_results": has_no_results,
        "athletics_keyword_lines": athletics_lines,
        "matching_links": _dedupe_keep_order(
            [f"{item['title']}|{item['url']}" for item in matching_links]
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate OCDE directory pages to detect public athletics content for this school."""
    require_proxy_credentials()

    planned_urls = [
        HOME_URL,
        ACCESS_URL,
        COUNTY_OPERATED_URL,
        *[f"{HOME_URL}?s={query.replace(' ', '+')}" for query in SEARCH_QUERIES],
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
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_signals.append(await _collect_page_signals(page))

            access_link = page.get_by_role("link", name="ACCESS").first
            if await access_link.count() > 0:
                await access_link.click()
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signals(page))
            else:
                errors.append("navigation_missing_access_link")
                await page.goto(ACCESS_URL, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signals(page))

            juvenile_link = page.get_by_role("link", name="ACCESS Juvenile Hall").first
            if await juvenile_link.count() > 0:
                await juvenile_link.click()
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signals(page))

            county_link = page.get_by_role("link", name="County-Operated Schools").first
            if await county_link.count() > 0:
                await county_link.click()
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signals(page))
            else:
                await page.goto(COUNTY_OPERATED_URL, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signals(page))

            for query in SEARCH_QUERIES:
                await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(800)

                search_input = page.locator(
                    "input[type='search'], input[name='phrase'][type='text']"
                ).first
                if await search_input.count() > 0:
                    await search_input.fill(query)
                    await search_input.press("Enter")
                else:
                    search_url = f"{HOME_URL}?s={query.replace(' ', '+')}"
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)

                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signals(page))

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_athletics_lines: list[str] = []
    school_page_links: list[str] = []
    no_result_queries = 0

    for signal in page_signals:
        school_page_links.extend(signal.get("matching_links", []))
        if signal.get("has_no_results"):
            no_result_queries += 1

        if signal.get("mentions_target_school"):
            all_athletics_lines.extend(signal.get("athletics_keyword_lines", []))

    all_athletics_lines = _dedupe_keep_order(all_athletics_lines)
    school_page_links = _dedupe_keep_order(school_page_links)

    athletics_program_available = bool(all_athletics_lines)
    school_page_found = bool(school_page_links)

    if not school_page_found:
        errors.append("school_page_not_found_in_public_ocde_directory")

    if not athletics_program_available:
        errors.append("no_public_athletics_content_detected")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "school_page_found": school_page_found,
        "school_page_matches": school_page_links,
        "athletics_keyword_mentions": all_athletics_lines,
        "search_queries_no_results_count": no_result_queries,
        "search_queries_checked": SEARCH_QUERIES,
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
                "home",
                "access_menu",
                "access_juvenile_hall_subpage",
                "county_operated_schools",
                "site_search_queries",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
