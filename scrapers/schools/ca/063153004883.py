"""Deterministic athletics availability scraper for Abraxas Continuation High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError, async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "063153004883"
SCHOOL_NAME = "Abraxas Continuation High"
STATE = "CA"
BASE_URL = "https://abraxas.powayusd.com/"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_NAV_STEPS: list[dict[str, str]] = [
    {"label": "About Us", "url": "https://abraxas.powayusd.com/apps/pages/welcome"},
    {"label": "Academics", "url": "https://abraxas.powayusd.com/apps/pages/academics"},
    {"label": "Counseling", "url": "https://abraxas.powayusd.com/apps/pages/counseling"},
    {"label": "Student Handbook", "url": "https://abraxas.powayusd.com/apps/pages/student-handbook"},
    {"label": "Flyers", "url": "https://abraxas.powayusd.com/apps/pages/flyers"},
    {"label": "Frequently Asked Questions", "url": "https://abraxas.powayusd.com/apps/pages/faqs"},
    {"label": "Events Calendar", "url": "https://abraxas.powayusd.com/apps/events/"},
    {"label": "Announcements", "url": "https://abraxas.powayusd.com/apps/news/"},
]

SEARCH_TERMS = [
    "athletics",
    "athletic",
    "sports",
    "football",
    "basketball",
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
    "track",
    "track and field",
    "cif",
    "coach",
    "roster",
    "tryout",
    "season",
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
    return host in {"abraxas.powayusd.com", "powayusd-abraxas.edlioschool.com"}


def _keyword_lines(text: str, limit: int = 30) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _collect_signal(page: Page) -> dict[str, Any]:
    body = await page.inner_text("body")
    lines = _keyword_lines(body)

    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    athletics_links: list[str] = []
    for anchor in anchors:
        text = str(anchor.get("text") or "").strip()
        href = str(anchor.get("href") or "").strip()
        parsed = urlparse(href)
        compact_href = f"{parsed.netloc}{parsed.path}".lower()
        combined = f"{text} {compact_href}".lower()
        if any(keyword in combined for keyword in ATHLETICS_KEYWORDS):
            athletics_links.append(f"{text}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_domain(page.url),
        "keyword_lines": lines,
        "athletics_links": _dedupe_keep_order(athletics_links)[:30],
        "no_results": "no results" in body.lower(),
    }


async def _open_home(page: Page) -> None:
    await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)


async def _click_menu_or_fallback(page: Page, label: str, fallback_url: str) -> str:
    await _open_home(page)

    menu_toggle = page.get_by_text("Main Menu Toggle").first
    if await menu_toggle.count() > 0:
        await menu_toggle.click()
        await page.wait_for_timeout(500)

    link = page.get_by_role("link", name=label).first
    if await link.count() > 0:
        try:
            await link.click(timeout=6000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(1200)
            return "clicked_menu"
        except PlaywrightTimeoutError:
            pass
        except Exception:  # noqa: BLE001
            pass

    await page.goto(fallback_url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)
    return "fallback_url"


async def _run_site_search(page: Page, query: str) -> str:
    await _open_home(page)

    search_input = page.locator("input[type='search'], input[name='q'], input[name='query']").first
    if await search_input.count() > 0:
        await search_input.fill(query)
        await search_input.press("Enter")
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(1200)
        return "site_search_input"

    fallback_url = f"https://abraxas.powayusd.com/apps/search/?q={query}"
    await page.goto(fallback_url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)
    return "search_fallback_url"


async def scrape_school() -> dict[str, Any]:
    """Navigate Abraxas High pages and determine public athletics program availability."""
    require_proxy_credentials()

    planned_urls = [
        BASE_URL,
        *[step["url"] for step in MANUAL_NAV_STEPS],
        *[f"https://abraxas.powayusd.com/apps/search/?q={term}" for term in SEARCH_TERMS],
    ]
    assert_not_blocklisted(planned_urls)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    navigation_log: list[str] = []

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
            await _open_home(page)
            source_pages.append(page.url)
            page_signals.append(await _collect_signal(page))
            navigation_log.append("home")

            for step in MANUAL_NAV_STEPS:
                method = await _click_menu_or_fallback(page, step["label"], step["url"])
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))
                navigation_log.append(f"{step['label']}:{method}")

            for term in SEARCH_TERMS:
                method = await _run_site_search(page, term)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))
                navigation_log.append(f"search:{term}:{method}")

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_domain_lines: list[str] = []
    school_domain_links: list[str] = []
    external_athletics_mentions: list[str] = []
    no_result_pages = 0

    for signal in page_signals:
        lines = [item for item in signal.get("keyword_lines", []) if isinstance(item, str)]
        links = [item for item in signal.get("athletics_links", []) if isinstance(item, str)]

        if signal.get("no_results"):
            no_result_pages += 1

        if signal.get("school_domain"):
            school_domain_lines.extend(lines)
            school_domain_links.extend(links)
        elif lines or links:
            source_url = str(signal.get("url") or "")
            for item in lines[:10]:
                external_athletics_mentions.append(f"{source_url}|{item}")
            for item in links[:10]:
                external_athletics_mentions.append(f"{source_url}|{item}")

    school_domain_lines = _dedupe_keep_order(school_domain_lines)
    school_domain_links = _dedupe_keep_order(school_domain_links)
    external_athletics_mentions = _dedupe_keep_order(external_athletics_mentions)

    athletics_program_available = bool(school_domain_lines or school_domain_links)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_athletics_program_content_found_on_abraxas_school_pages_after_manual_navigation"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "No school-hosted public athletics program content found on Abraxas High pages "
            "after manual menu navigation and site search checks."
            if not athletics_program_available
            else ""
        ),
        "manual_navigation_steps": MANUAL_NAV_STEPS,
        "search_terms_checked": SEARCH_TERMS,
        "navigation_log": navigation_log,
        "school_domain_athletics_keyword_lines": school_domain_lines,
        "school_domain_athletics_links": school_domain_links,
        "external_athletics_mentions": external_athletics_mentions,
        "search_pages_with_no_results": no_result_pages,
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
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
