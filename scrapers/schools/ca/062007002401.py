"""Deterministic football scraper for Lower Lake High (CA)."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import Page, async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062007002401"
SCHOOL_NAME = "Lower Lake High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_URL = "https://www.konoctiusd.org/schools/llhs/programs-services/athletics/index"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _absolute_url(base_url: str, href: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    return urljoin(base_url, raw)


async def _goto(page: Page, url: str) -> None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except Exception as exc:  # noqa: BLE001
        if "ERR_ABORTED" not in str(exc):
            raise
    await page.wait_for_load_state("domcontentloaded", timeout=60000)
    await page.wait_for_selector("h1", timeout=60000)


async def _extract_schedule_rows(page: Page) -> list[dict[str, Any]]:
    rows = await page.locator("table").nth(0).locator("tbody tr").evaluate_all(
        """rows => rows.map((row) => {
            const seasonHeading = row.querySelector('th[colspan]');
            if (seasonHeading) {
                return {
                    row_type: 'season',
                    season: (seasonHeading.textContent || '').replace(/\\s+/g, ' ').trim(),
                    sport: '',
                    links: []
                };
            }
            const cells = Array.from(row.querySelectorAll('td, th')).map((cell) =>
                (cell.textContent || '').replace(/\\s+/g, ' ').trim()
            );
            const links = Array.from(row.querySelectorAll('a[href]')).map((a) => ({
                text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: a.getAttribute('href') || ''
            }));
            return {
                row_type: 'sport',
                season: '',
                sport: cells[0] || '',
                links
            };
        })"""
    )
    return rows if isinstance(rows, list) else []


async def _extract_resource_rows(page: Page) -> list[dict[str, str]]:
    rows = await page.locator("table").nth(1).locator("tbody tr").evaluate_all(
        """rows => rows.map((row) => {
            const cells = Array.from(row.querySelectorAll('td')).map((cell) =>
                (cell.textContent || '').replace(/\\s+/g, ' ').trim()
            );
            const link = row.querySelector('a[href]');
            return {
                resource: cells[0] || '',
                label: link ? (link.textContent || '').replace(/\\s+/g, ' ').trim() : '',
                href: link ? (link.getAttribute('href') || '') : ''
            };
        })"""
    )
    cleaned: list[dict[str, str]] = []
    if not isinstance(rows, list):
        return cleaned
    for row in rows:
        if not isinstance(row, dict):
            continue
        cleaned.append(
            {
                "resource": _clean(str(row.get("resource") or "")),
                "label": _clean(str(row.get("label") or "")),
                "href": _clean(str(row.get("href") or "")),
            }
        )
    return cleaned


async def _extract_eligibility_section(page: Page) -> list[str]:
    text = await page.evaluate(
        """() => {
            const heading = Array.from(document.querySelectorAll('h3'))
                .find((node) => (node.textContent || '').trim() === 'Eligibility & Participation');
            if (!heading) return '';
            const lines = [];
            let node = heading.nextElementSibling;
            while (node && node.tagName !== 'H3') {
                const text = (node.textContent || '').replace(/\\s+/g, ' ').trim();
                if (text) lines.push(text);
                node = node.nextElementSibling;
            }
            return lines.join('\\n');
        }"""
    )
    if not isinstance(text, str):
        return []
    return _dedupe_keep_order(text.splitlines())


async def _extract_intro_paragraph(page: Page) -> str:
    text = await page.evaluate(
        """() => {
            const heading = document.querySelector('h1');
            if (!heading) return '';
            let node = heading.nextElementSibling;
            while (node) {
                const text = (node.textContent || '').replace(/\\s+/g, ' ').trim();
                if (node.tagName === 'P' && text) return text;
                if (node.tagName === 'H2' || node.tagName === 'H3') break;
                node = node.nextElementSibling;
            }
            return '';
        }"""
    )
    return _clean(text) if isinstance(text, str) else ""


async def scrape_school() -> dict[str, Any]:
    """Scrape public Lower Lake High football signals from the athletics page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted([ATHLETICS_URL], profile=PROXY_PROFILE)
    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    source_pages: list[str] = []
    errors: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            await _goto(page, ATHLETICS_URL)
            await page.wait_for_selector("h1", timeout=60000)
            await page.wait_for_timeout(750)
            source_pages.append(page.url)

            page_title = _clean(await page.title())
            heading = _clean(await page.locator("h1").first.inner_text())
            intro_paragraph = await _extract_intro_paragraph(page)
            schedule_rows = await _extract_schedule_rows(page)
            resource_rows = await _extract_resource_rows(page)
            eligibility_lines = await _extract_eligibility_section(page)
            athletic_director_email = _clean(
                await page.locator("a[href^='mailto:']").first.inner_text()
            )
            school_phone = _clean(
                await page.locator("footer").inner_text(timeout=10000)
            )
        finally:
            await context.close()
            await browser.close()

    football_row: dict[str, Any] = {}
    current_season = ""
    for row in schedule_rows:
        if not isinstance(row, dict):
            continue
        row_type = row.get("row_type")
        if row_type == "season":
            current_season = _clean(str(row.get("season") or ""))
            continue
        sport = _clean(str(row.get("sport") or ""))
        if sport.lower() != "football":
            continue
        links = row.get("links") if isinstance(row.get("links"), list) else []
        football_row = {
            "season_group": current_season,
            "sport": sport,
            "links": [
                {
                    "label": _clean(str(link.get("text") or "")),
                    "url": _absolute_url(ATHLETICS_URL, str(link.get("href") or "")),
                }
                for link in links
                if isinstance(link, dict) and _clean(str(link.get("href") or ""))
            ],
        }
        break

    football_schedule_url = ""
    football_schedule_label = ""
    if football_row.get("links"):
        first_link = football_row["links"][0]
        football_schedule_url = _clean(str(first_link.get("url") or ""))
        football_schedule_label = _clean(str(first_link.get("label") or ""))
        if football_schedule_url:
            source_pages.append(football_schedule_url)

    resource_map = {
        row["resource"]: {
            "label": row["label"],
            "url": _absolute_url(ATHLETICS_URL, row["href"]),
        }
        for row in resource_rows
        if row.get("resource") and row.get("href")
    }

    football_resources = {
        "scores_stats_standings": resource_map.get("Scores, Stats & Standings", {}),
        "league_information": resource_map.get("League Information", {}),
        "playoff_section_info": resource_map.get("Playoff & Section Info", {}),
        "game_tickets": resource_map.get("Game Tickets", {}),
        "live_streaming": resource_map.get("Live Streaming", {}),
        "news_updates": resource_map.get("News & Updates", {}),
        "participation_forms": resource_map.get("Athletic Participation & Forms", {}),
    }
    football_resources = {
        key: value for key, value in football_resources.items() if any(value.values())
    }

    if not football_schedule_url:
        errors.append("football_schedule_link_not_found_on_public_llhs_athletics_page")

    school_phone_match = re.search(r"P:\s*([^(]*\(\d{3}\)\s*\d{3}-\d{4})", school_phone)
    school_phone_value = _clean(school_phone_match.group(1)) if school_phone_match else ""

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_schedule_url),
        "athletics_page": {
            "url": ATHLETICS_URL,
            "title": page_title,
            "heading": heading,
            "intro": intro_paragraph,
        },
        "football_schedule": {
            "season_group": _clean(str(football_row.get("season_group") or "")),
            "sport": _clean(str(football_row.get("sport") or "Football")),
            "label": football_schedule_label,
            "url": football_schedule_url,
        },
        "football_contact": {
            "athletic_director_email": athletic_director_email,
            "school_phone": school_phone_value,
        },
        "football_support_links": football_resources,
        "eligibility_notes": eligibility_lines,
        "football_signals": _dedupe_keep_order(
            [
                "Lower Lake High School publishes a football schedule PDF on its public athletics page.",
                (
                    f"Football schedule link: {football_schedule_url}"
                    if football_schedule_url
                    else ""
                ),
                (
                    f"Athletic director contact: {athletic_director_email}"
                    if athletic_director_email
                    else ""
                ),
                (
                    "The athletics page states that participation requires a current physical "
                    "and required paperwork through Home Campus."
                    if eligibility_lines
                    else ""
                ),
            ]
        ),
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": [ATHLETICS_URL],
            "pages_checked": 1,
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
