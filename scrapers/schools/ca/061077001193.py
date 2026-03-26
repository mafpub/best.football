"""Deterministic football scraper for Del Norte High (CA)."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061077001193"
SCHOOL_NAME = "Del Norte High"
STATE = "CA"

HOME_URL = "https://www.dnhigh.org/"
ATHLETICS_URL = "https://www.dnhigh.org/athletics"
FOOTBALL_URL = "https://www.dnhigh.org/athletics/football"

MANUAL_PAGES = [HOME_URL, ATHLETICS_URL, FOOTBALL_URL]


def _clean(value: str) -> str:
    return " ".join(value.replace("\u00a0", " ").split()).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


async def _extract_schedule(page) -> dict[str, Any]:
    section = page.locator("section#fsEl_34277")
    title = ""
    if await section.count():
        title = _clean(await section.locator("h2.fsElementTitle").first.inner_text())

    rows: list[dict[str, str]] = []
    table = section.locator("table")
    if await table.count():
        body_rows = table.locator("tbody tr")
        row_count = await body_rows.count()
        for index in range(1, row_count):
            cells = body_rows.nth(index).locator("td")
            cell_count = await cells.count()
            if cell_count < 5:
                continue
            rows.append(
                {
                    "date": _clean(await cells.nth(0).inner_text()),
                    "location": _clean(await cells.nth(1).inner_text()),
                    "opponent": _clean(await cells.nth(2).inner_text()),
                    "jv": _clean(await cells.nth(3).inner_text()),
                    "varsity": _clean(await cells.nth(4).inner_text()),
                }
            )

    return {
        "season_label": title,
        "rows": rows,
        "row_count": len(rows),
    }


async def _extract_coaches(page) -> list[dict[str, str]]:
    section = page.locator("section#fsEl_30154")
    if not await section.count():
        return []

    items = section.locator(".fsConstituentItem")
    coaches: list[dict[str, str]] = []
    for index in range(await items.count()):
        item = items.nth(index)
        name = ""
        title = ""
        roles = ""

        full_name = item.locator(".fsFullName")
        if await full_name.count():
            name = _clean(await full_name.first.inner_text())

        titles = item.locator(".fsTitles")
        if await titles.count():
            title = _clean(await titles.first.inner_text()).replace("Titles:", "").strip()

        role_nodes = item.locator(".fsRoles")
        if await role_nodes.count():
            roles = _clean(await role_nodes.first.inner_text()).replace("Roles:", "").strip()

        if name:
            coaches.append({"name": name, "title": title, "roles": roles})

    return coaches


async def _extract_announcements(page) -> list[dict[str, str]]:
    section = page.locator("section#fsEl_36003")
    if not await section.count():
        return []

    articles = section.locator("article")
    announcements: list[dict[str, str]] = []
    for index in range(await articles.count()):
        article = articles.nth(index)
        title = ""
        summary = ""
        published = ""
        slug = ""

        title_link = article.locator(".fsTitle a.fsPostLink")
        if await title_link.count():
            title = _clean(await title_link.first.inner_text())
            slug = _clean(await title_link.first.get_attribute("data-slug") or "")

        summary_node = article.locator(".fsSummary")
        if await summary_node.count():
            summary = _clean(await summary_node.first.inner_text())

        date_node = article.locator("time.fsDate")
        if await date_node.count():
            published = _clean(await date_node.first.get_attribute("datetime") or "")

        if title:
            announcements.append(
                {
                    "title": title,
                    "summary": summary,
                    "published": published,
                    "slug": slug,
                }
            )

    return announcements


async def scrape_school() -> dict[str, Any]:
    """Scrape Del Norte High's public football page."""
    require_proxy_credentials(profile="datacenter")
    assert_not_blocklisted(MANUAL_PAGES, profile="datacenter")

    source_pages: list[str] = []
    errors: list[str] = []
    schedule: dict[str, Any] = {"season_label": "", "rows": [], "row_count": 0}
    coaches: list[dict[str, str]] = []
    announcements: list[dict[str, str]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile="datacenter"),
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await context.new_page()

        try:
            for url in MANUAL_PAGES:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)

                if url == FOOTBALL_URL:
                    schedule = await _extract_schedule(page)
                    coaches = await _extract_coaches(page)
                    announcements = await _extract_announcements(page)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    if not schedule["rows"]:
        errors.append("football_schedule_table_empty")
    if not coaches:
        errors.append("football_coaches_section_empty")
    if not announcements:
        errors.append("football_announcements_section_empty")

    extracted_items: dict[str, Any] = {
        "home_url": HOME_URL,
        "athletics_url": ATHLETICS_URL,
        "football_url": FOOTBALL_URL,
        "schedule": schedule,
        "coaches": coaches,
        "announcements": announcements[:5],
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
            "proxy": get_proxy_runtime_meta(profile="datacenter"),
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "home",
                "athletics",
                "football",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


async def main() -> None:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
