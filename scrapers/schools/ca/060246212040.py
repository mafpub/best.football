"""Deterministic athletics scraper for ARISE High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060246212040"
SCHOOL_NAME = "ARISE High"
STATE = "CA"

BASE_URL = "https://arisehighschool.org"
HOME_URL = f"{BASE_URL}/"
STUDENT_LIFE_URL = f"{BASE_URL}/student-life"
ATHLETICS_URL = f"{BASE_URL}/athletics"

MANUAL_NAV_STEPS = [
    "home",
    "student_life_menu",
    "scholar_athletes_menu",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "sport",
    "scholar athletes",
    "football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
    "volleyball",
    "cross country",
    "track",
    "wrestling",
    "coach",
    "athletic director",
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


def _extract_keyword_lines(text: str, *, limit: int = 30) -> list[str]:
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
            text: (e.textContent || '').trim(),
            href: e.href || ''
        }))""",
    )

    matches: list[str] = []
    for link in links:
        text = " ".join(str(link.get("text") or "").split()).strip()
        href = str(link.get("href") or "").strip()
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in ATHLETICS_KEYWORDS):
            matches.append(f"{text}|{href}" if text else href)

    return _dedupe_keep_order(matches)[:30]


async def _collect_page_data(page) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    return {
        "url": page.url,
        "title": await page.title(),
        "keyword_lines": _extract_keyword_lines(body_text),
        "keyword_links": await _extract_keyword_links(page),
    }


async def _click_if_present(page, names: list[str]) -> bool:
    for name in names:
        locator = page.get_by_role("link", name=name).first
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
    """Navigate ARISE pages and confirm public athletics program content."""
    require_proxy_credentials()

    planned_urls = [HOME_URL, STUDENT_LIFE_URL, ATHLETICS_URL]
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

            clicked_student_life = await _click_if_present(
                page,
                ["Student Life", "Beyond the Classroom"],
            )
            if not clicked_student_life:
                await page.goto(STUDENT_LIFE_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            clicked_athletics = await _click_if_present(
                page,
                ["Scholar Athletes", "Athletics", "Athletics Count"],
            )
            if not clicked_athletics:
                await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            if page.url.rstrip("/") != ATHLETICS_URL:
                await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_data.append(await _collect_page_data(page))

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_lines: list[str] = []
    all_links: list[str] = []
    athletics_page_lines: list[str] = []

    for item in page_data:
        all_lines.extend([line for line in item.get("keyword_lines", []) if isinstance(line, str)])
        all_links.extend([line for line in item.get("keyword_links", []) if isinstance(line, str)])

        url = str(item.get("url") or "").lower()
        if "/athletics" in url:
            athletics_page_lines.extend(
                [line for line in item.get("keyword_lines", []) if isinstance(line, str)]
            )

    all_lines = _dedupe_keep_order(all_lines)
    all_links = _dedupe_keep_order(all_links)
    athletics_page_lines = _dedupe_keep_order(athletics_page_lines)

    athletics_program_available = bool(athletics_page_lines)

    if not athletics_program_available:
        errors.append("blocked:no_public_athletics_program_content_found_on_arise_pages")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "manual_navigation_steps": MANUAL_NAV_STEPS,
        "athletics_page_url": ATHLETICS_URL,
        "athletics_keyword_mentions": athletics_page_lines,
        "all_athletics_keyword_mentions": all_lines,
        "athletics_related_links": all_links,
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
