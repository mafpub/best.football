"""Deterministic athletics availability scraper for Agnes J. Johnson Charter (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060254306363"
SCHOOL_NAME = "Agnes J. Johnson Charter"
STATE = "CA"

BASE_URL = "https://ajjcharter.com"
HOME_URL = f"{BASE_URL}/"
ACADEMICS_URL = f"{BASE_URL}/academics"
PARENTS_URL = f"{BASE_URL}/parents"
AG_PROGRAM_URL = f"{BASE_URL}/ajjcs-ag-program"

MANUAL_NAVIGATION_STEPS = [
    "home_page",
    "home_menu_to_academics",
    "parents_page",
    "ag_program_page",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

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
    "coach",
    "roster",
    "schedule",
)

CLOSURE_KEYWORDS = (
    "permanently closing our doors",
    "permanently closed",
    "goodbye weotters",
)


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = " ".join(value.split()).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_matching_lines(text: str, keywords: tuple[str, ...], *, limit: int = 20) -> list[str]:
    matches: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


async def _collect_page_data(page) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    return {
        "url": page.url,
        "title": await page.title(),
        "athletics_lines": _extract_matching_lines(body_text, ATHLETICS_KEYWORDS),
        "closure_lines": _extract_matching_lines(body_text, CLOSURE_KEYWORDS),
    }


async def _click_nav_link(page, label: str) -> bool:
    locator = page.get_by_role("link", name=label).first
    if await locator.count() == 0:
        return False

    try:
        await locator.click(timeout=12000)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(1500)
        return True
    except Exception:  # noqa: BLE001
        return False


async def scrape_school() -> dict[str, Any]:
    """Navigate public AJJCS pages and capture athletics availability evidence."""
    require_proxy_credentials()

    planned_urls = [HOME_URL, ACADEMICS_URL, PARENTS_URL, AG_PROGRAM_URL]
    assert_not_blocklisted(planned_urls)

    errors: list[str] = []
    source_pages: list[str] = []
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

            clicked_academics = await _click_nav_link(page, "Academics")
            if not clicked_academics:
                await page.goto(ACADEMICS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            await page.goto(PARENTS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            await page.goto(AG_PROGRAM_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_lines: list[str] = []
    closure_lines: list[str] = []
    athletics_evidence_pages: list[str] = []

    for item in page_data:
        item_athletics_lines = [
            value for value in item.get("athletics_lines", []) if isinstance(value, str)
        ]
        item_closure_lines = [
            value for value in item.get("closure_lines", []) if isinstance(value, str)
        ]
        if item_athletics_lines:
            athletics_evidence_pages.append(str(item.get("url") or ""))
        athletics_lines.extend(item_athletics_lines)
        closure_lines.extend(item_closure_lines)

    athletics_lines = _dedupe_keep_order(athletics_lines)
    closure_lines = _dedupe_keep_order(closure_lines)
    athletics_evidence_pages = _dedupe_keep_order(athletics_evidence_pages)

    athletics_program_available = bool(athletics_lines)
    dedicated_athletics_page_found = any("/athletics" in url.lower() for url in source_pages)

    if not athletics_program_available:
        errors.append("blocked:no_public_athletics_content_found_on_manual_school_pages")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "dedicated_athletics_page_found": dedicated_athletics_page_found,
        "closure_notice_present": bool(closure_lines),
        "closure_notice_lines": closure_lines,
        "manual_navigation_steps": MANUAL_NAVIGATION_STEPS,
        "athletics_evidence_pages": athletics_evidence_pages,
        "athletics_keyword_mentions": athletics_lines,
        "observed_context": (
            "The homepage announces the school permanently closed on 2026-02-28. The public "
            "athletics signal observed on the school domain was a statement on the Academics page "
            "that AJJCS would provide opportunities for students to play sports."
            if athletics_program_available
            else "No public athletics-specific content was found on the manually checked school pages."
        ),
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
            "manual_navigation_steps": MANUAL_NAVIGATION_STEPS,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
