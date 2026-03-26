"""Deterministic football scraper for Alternatives in Action (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060163508674"
SCHOOL_NAME = "Alternatives in Action"
STATE = "CA"
BASE_URL = "https://www.alternativesinaction.org"
ATHLETICS_URL = f"{BASE_URL}/athletics-at-aiahs"
SCHEDULE_URL = "https://scheduler.leaguelobster.com/2537450/cffl/2026/"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

TARGET_URLS = [
    ATHLETICS_URL,
    SCHEDULE_URL,
]

FOOTBALL_TERMS = (
    "football",
    "flag football",
    "boys' flag football",
    "boys flag football",
    "girls flag football",
)

SPORT_TERMS = (
    "baseball",
    "basketball",
    "flag football",
    "football",
    "girls' soccer",
    "girls soccer",
    "soccer",
    "volleyball",
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


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _is_public_sports_url(url: str) -> bool:
    lowered = url.lower()
    return lowered.startswith(BASE_URL.lower()) or lowered.startswith("https://alternativesinaction.smugmug.com/")


def _extract_sports(lines: list[str]) -> list[str]:
    lowered = " | ".join(lines).lower()
    found: list[str] = []
    for sport in SPORT_TERMS:
        if sport in lowered:
            found.append(sport)
    return _dedupe_keep_order(found)


def _extract_football_links(links: list[dict[str, str]]) -> list[str]:
    kept: list[str] = []
    seen: set[str] = set()
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        if not text or not href:
            continue
        combo = f"{text} {href}".lower()
        if not any(term in combo for term in FOOTBALL_TERMS):
            continue
        value = f"{text}|{href}"
        if value in seen:
            continue
        seen.add(value)
        kept.append(value)
    return _dedupe_keep_order(kept)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )
    if not isinstance(links, list):
        links = []

    lines = []
    for raw_line in body.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        if any(term in line.lower() for term in FOOTBALL_TERMS + SPORT_TERMS):
            lines.append(line)

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "keyword_lines": _dedupe_keep_order(lines)[:40],
        "football_links": _extract_football_links([item for item in links if isinstance(item, dict)]),
        "is_public_sports_url": _is_public_sports_url(page.url),
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate public AIAHS athletics pages and extract football program signals."""
    require_proxy_credentials()
    assert_not_blocklisted(TARGET_URLS)

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
            ),
            locale="en-US",
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_lines: list[str] = []
    athletics_links: list[str] = []
    schedule_public = False
    schedule_status = ""
    teams: list[str] = []
    sports_list: list[str] = []

    for signal in page_signals:
        lines = [line for line in signal.get("keyword_lines", []) if isinstance(line, str)]
        links = [link for link in signal.get("football_links", []) if isinstance(link, str)]
        athletics_lines.extend(lines)
        athletics_links.extend(links)

        if signal.get("final_url") == ATHLETICS_URL:
            sports_list.extend(_extract_sports(lines))
            if any("boys' flag football" in line.lower() or "boys flag football" in line.lower() for line in lines):
                teams.append("Boys' Flag Football")
            if any("girls' soccer" in line.lower() or "girls soccer" in line.lower() for line in lines):
                teams.append("Girls' Soccer")
        elif signal.get("final_url") == SCHEDULE_URL:
            schedule_status = "This schedule is not public"

    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_links = _dedupe_keep_order(athletics_links)
    teams = _dedupe_keep_order(teams)
    sports_list = _dedupe_keep_order(sports_list)

    football_program_available = any(
        term in " | ".join(athletics_lines).lower() for term in FOOTBALL_TERMS
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_program_found_on_aiahs_athletics_page")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": ATHLETICS_URL,
        "football_schedule_url": SCHEDULE_URL,
        "football_schedule_public": schedule_public,
        "football_schedule_note": schedule_status or "schedule page is not public",
        "football_team_names": teams,
        "athletics_sports_list": sports_list,
        "athletics_keyword_lines": athletics_lines[:40],
        "athletics_links": athletics_links[:20],
        "summary": (
            "Alternatives in Action publishes an athletics hub that includes boys' flag football, a league-lobster schedule link, and girls' soccer."
            if football_program_available
            else ""
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
            "manual_navigation_steps": [
                "athletics_hub",
                "league_lobster_schedule",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
