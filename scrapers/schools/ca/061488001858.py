"""Deterministic football scraper for Los Amigos High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061488001858"
SCHOOL_NAME = "Los Amigos High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://www.losamigoshs.com"
SEARCH_URL_TEMPLATE = f"{BASE_URL}/apps/search/?q={{query}}"
SEARCH_QUERIES = (
    "football",
    "varsity football",
    "football club",
)
TARGET_URLS = [SEARCH_URL_TEMPLATE.format(query=quote_plus(query)) for query in SEARCH_QUERIES]

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


def _parse_result_count(value: str) -> int | None:
    match = re.search(r"(\d+)\s+results", _clean(value), re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _extract_participation_counts(preview: str) -> list[dict[str, int | str]]:
    text = _clean(preview)
    matches = re.findall(
        r"(Football\s*-\s*(?:Varsity|JV|Freshman))\s+(\d+)\s+(\d+)",
        text,
        flags=re.IGNORECASE,
    )
    counts: list[dict[str, int | str]] = []
    for label, male_count, female_count in matches:
        counts.append(
            {
                "team_level": _clean(label),
                "male_count": int(male_count),
                "female_count": int(female_count),
            }
        )
    return counts


def _extract_named_contact(preview: str, label: str) -> dict[str, str]:
    text = _clean(preview)
    match = re.search(
        rf"{re.escape(label)}:\s*(Mr\.|Mrs\.|Ms\.|Coach)\s+([A-Za-z'-]+)",
        text,
    )
    if not match:
        return {}
    return {
        "label": label,
        "name": _clean(f"{match.group(1)} {match.group(2)}"),
    }


def _find_first_result(results: list[dict[str, str]], pattern: str) -> dict[str, str]:
    regex = re.compile(pattern, re.IGNORECASE)
    for item in results:
        haystack = " ".join(
            [
                _clean(str(item.get("title") or "")),
                _clean(str(item.get("preview") or "")),
                _clean(str(item.get("href") or "")),
            ]
        )
        if regex.search(haystack):
            return item
    return {}


async def _load_search_results(page, query: str) -> dict[str, Any]:
    search_url = SEARCH_URL_TEMPLATE.format(query=quote_plus(query))
    await page.goto(search_url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector("#search_results_count_text", timeout=90000)
    await page.wait_for_selector("#results .result-item", timeout=90000)
    await page.wait_for_timeout(1500)

    result_count_text = _clean(
        await page.locator("#search_results_count_text").inner_text(timeout=10000)
    )
    items = await page.locator("#results .result-item").evaluate_all(
        """nodes => nodes.map((node) => ({
            title: (node.querySelector('.result-title a')?.textContent || '')
                .replace(/\\s+/g, ' ')
                .trim(),
            href: node.querySelector('.result-title a')?.href || '',
            preview: (node.querySelector('.result-preview')?.textContent || '')
                .replace(/\\s+/g, ' ')
                .trim(),
        }))""",
    )
    if not isinstance(items, list):
        items = []

    normalized_items: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = _clean(str(item.get("title") or ""))
        href = _clean(str(item.get("href") or ""))
        preview = _clean(str(item.get("preview") or ""))
        if not title:
            continue
        normalized_items.append({"title": title, "href": href, "preview": preview})

    return {
        "query": query,
        "search_url": search_url,
        "result_count_text": result_count_text,
        "result_count": _parse_result_count(result_count_text),
        "items": normalized_items,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Los Amigos football signals from public site-search results."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)
    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    query_results: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for query in SEARCH_QUERIES:
                result = await _load_search_results(page, query)
                query_results.append(result)
                source_pages.append(result["search_url"])
        finally:
            await context.close()
            await browser.close()

    all_items: list[dict[str, str]] = []
    search_result_counts: dict[str, int | None] = {}
    for result in query_results:
        search_result_counts[result["query"]] = result.get("result_count")
        items = result.get("items")
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    all_items.append(item)

    canonical_items: list[dict[str, str]] = []
    seen_keys: set[tuple[str, str]] = set()
    for item in all_items:
        title = _clean(str(item.get("title") or ""))
        href = _clean(str(item.get("href") or ""))
        key = (title, href)
        if not title or key in seen_keys:
            continue
        seen_keys.add(key)
        canonical_items.append(
            {
                "title": title,
                "href": href,
                "preview": _clean(str(item.get("preview") or "")),
            }
        )

    football_counts_result = _find_first_result(canonical_items, r"SB 1349|Football - Varsity")
    football_club_result = _find_first_result(canonical_items, r"FOOTBALL CLUB")
    football_event_result = _find_first_result(canonical_items, r"VAR Football vs")
    football_game_result = _find_first_result(canonical_items, r"Support Lobo Football|Garden Grove stadium")
    football_team_result = _find_first_result(canonical_items, r"JV and Varsity Football Teams|Rams training")

    participation_counts = _extract_participation_counts(
        football_counts_result.get("preview", "") if football_counts_result else ""
    )
    football_club_contact = _extract_named_contact(
        football_club_result.get("preview", "") if football_club_result else "",
        "FOOTBALL CLUB",
    )

    football_program_available = any(
        [
            bool(participation_counts),
            bool(football_event_result),
            bool(football_game_result),
            bool(football_team_result),
        ]
    )
    if not football_program_available:
        errors.append("no_public_football_search_results_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "search_result_counts": search_result_counts,
        "football_search_results": canonical_items[:10],
        "football_participation_counts": participation_counts,
        "football_club_contact": {
            **football_club_contact,
            "source_url": football_club_result.get("href", "") if football_club_result else "",
            "source_preview": football_club_result.get("preview", "") if football_club_result else "",
        }
        if football_club_contact
        else {},
        "featured_varsity_event": football_event_result,
        "featured_game_announcement": football_game_result,
        "featured_team_activity": football_team_result,
        "data_origin_note": (
            "Football signals were extracted from Los Amigos High School's public site-search index "
            "because the athletics section itself is sparse and some legacy result URLs are inconsistent."
        ),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            **get_proxy_runtime_meta(PROXY_PROFILE),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0",
            "strategy": "public-site-search-results",
        },
        "errors": errors,
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
