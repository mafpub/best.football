"""Deterministic athletics scraper for Academia Avance Charter (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060229110895"
SCHOOL_NAME = "Academia Avance Charter"
STATE = "CA"

BASE_URL = "https://www.academiaavance.org"
HOME_URL = f"{BASE_URL}/"
STUDENTS_URL = f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3786128&type=d"
ATHLETICS_URL = f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3786426&type=d"
BASKETBALL_URL = f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3786426&type=d&pREC_ID=2449494"
SOCCER_URL = f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3786426&type=d&pREC_ID=2449516"
EXL_URL = f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3787764&type=d&pREC_ID=2449566"

MANUAL_NAV_STEPS = [
    "home_page",
    "students_menu",
    "athletics_page",
    "basketball_subpage",
    "soccer_subpage",
    "expanded_learning_program_page",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "sport",
    "basketball",
    "soccer",
    "volleyball",
    "cross country",
    "cheer",
    "team",
    "league",
    "scrimmage",
    "conditioning",
    "coach",
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
    return host == "academiaavance.org" or host == "www.academiaavance.org"


def _extract_keyword_lines(text: str, *, limit: int = 35) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lower = line.lower()
        if any(keyword in lower for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _extract_keyword_links(page) -> list[str]:
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
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
    return _dedupe_keep_order(matches)[:35]


async def _collect_page_data(page) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    return {
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_domain(page.url),
        "keyword_lines": _extract_keyword_lines(body_text),
        "keyword_links": await _extract_keyword_links(page),
    }


async def _click_if_present(page, names: list[str]) -> bool:
    for name in names:
        locator = page.get_by_role("link", name=name, exact=False).first
        if await locator.count() == 0:
            continue
        try:
            await locator.click(timeout=12000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(1200)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def scrape_school() -> dict[str, Any]:
    """Navigate school pages and confirm public athletics program content."""
    require_proxy_credentials()

    planned_urls = [
        HOME_URL,
        STUDENTS_URL,
        ATHLETICS_URL,
        BASKETBALL_URL,
        SOCCER_URL,
        EXL_URL,
    ]
    assert_not_blocklisted(planned_urls)

    source_pages: list[str] = []
    errors: list[str] = []
    page_data: list[dict[str, Any]] = []

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
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            clicked_students = await _click_if_present(page, ["Students", "STUDENTS"])
            if not clicked_students:
                await page.goto(STUDENTS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            clicked_athletics = await _click_if_present(page, ["Athletics", "ATHLETICS"])
            if not clicked_athletics:
                await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            for url in [BASKETBALL_URL, SOCCER_URL, EXL_URL]:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_data.append(await _collect_page_data(page))

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_lines: list[str] = []
    school_links: list[str] = []
    athletics_lines: list[str] = []
    athletics_links: list[str] = []

    for item in page_data:
        if not item.get("school_domain"):
            continue

        lines = [value for value in item.get("keyword_lines", []) if isinstance(value, str)]
        links = [value for value in item.get("keyword_links", []) if isinstance(value, str)]
        school_lines.extend(lines)
        school_links.extend(links)

        url = str(item.get("url") or "")
        if "uREC_ID=3786426" in url:
            athletics_lines.extend(lines)
            athletics_links.extend(links)

    school_lines = _dedupe_keep_order(school_lines)
    school_links = _dedupe_keep_order(school_links)
    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_links = _dedupe_keep_order(athletics_links)

    athletics_program_available = bool(athletics_lines or athletics_links)
    blocked_reason = ""
    if not athletics_program_available:
        blocked_reason = (
            "No school-hosted public athletics program content found while navigating "
            "home, students, athletics, and related subpages."
        )
        errors.append(
            "blocked:no_public_athletics_program_content_found_on_manual_navigation_pages"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "manual_navigation_steps": MANUAL_NAV_STEPS,
        "athletics_page_url": ATHLETICS_URL,
        "athletics_subpages_checked": [BASKETBALL_URL, SOCCER_URL],
        "related_program_page_checked": EXL_URL,
        "athletics_keyword_mentions": athletics_lines,
        "athletics_related_links": athletics_links,
        "all_school_domain_athletics_keyword_mentions": school_lines,
        "all_school_domain_athletics_links": school_links,
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
            "manual_navigation_steps": MANUAL_NAV_STEPS,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
