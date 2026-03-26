"""Deterministic football scraper for Firebaugh High (CA)."""

from __future__ import annotations

import asyncio
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.schools.runtime import (  # noqa: E402
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061384001561"
SCHOOL_NAME = "Firebaugh High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://fhs.fldusd.org/"
SEARCH_URL = "https://fhs.fldusd.org/apps/search/"
SEARCH_QUERIES = [
    "football",
    "football coach",
    "football field",
    "football team",
    "Saldana",
]

NAME_PREFIX_RE = re.compile(r"(?i)\bcoach(?:es)?\s*:\s*([A-Za-z0-9/ ,.'&-]+)")
TITLE_CASE_NAME_RE = re.compile(r"^[A-Z][A-Za-z.'-]*(?:\s+[A-Z][A-Za-z.'-]*){0,2}$")
COACH_STOPWORDS = {
    "JV",
    "V",
    "Varsity",
    "release",
    "Depart",
    "depart",
    "game",
    "practice",
    "camp",
    "dinner",
    "team",
    "weigh",
    "photo",
    "pictures",
    "stadium",
    "field",
}


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


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


def _is_football_relevant(title: str, preview: str, href: str, query: str) -> bool:
    text = f"{title} {preview} {href} {query}".lower()
    return "football" in text


def _extract_locations(title: str, preview: str) -> list[str]:
    hints: list[str] = []
    for source in (title, preview):
        for match in re.finditer(r"@\s*([A-Za-z0-9.' \-]+)", source):
            hint = _clean(match.group(1)).rstrip(".,")
            if hint and re.search(r"[A-Za-z]", hint) and not re.fullmatch(r"\d+(?::\d+)?\s*(?:am|pm)?", hint, re.I):
                hints.append(hint)
        for match in re.finditer(r"\(([^)]+)\)", source):
            hint = _clean(match.group(1)).rstrip(".,")
            if hint and re.search(r"[A-Za-z]", hint) and any(
                word in hint.lower() for word in ["football field", "football stadium", "cafeteria", "gym", "lounge"]
            ):
                hints.append(hint)
    return _dedupe_keep_order(hints)


def _extract_coaches(title: str, preview: str) -> list[str]:
    candidates: list[str] = []
    preview_text = _clean(preview)
    title_text = _clean(title).lower()

    for match in NAME_PREFIX_RE.finditer(preview_text):
        raw = match.group(1)
        for token in re.split(r"[\\/,&;]", raw):
            value = _clean(token)
            if not value:
                continue
            value = re.split(
                r"\b(?:JV|Varsity|release|Depart|depart|game|practice|camp|dinner|team|weigh|photo|pictures)\b",
                value,
                maxsplit=1,
            )[0]
            value = _clean(value)
            if not value:
                continue
            if TITLE_CASE_NAME_RE.fullmatch(value):
                candidates.append(value)

    if "football" in title_text and "/" in preview_text:
        for token in re.split(r"[\\/,&;]", preview_text):
            value = _clean(token)
            if not value:
                continue
            if any(
                stop.lower() in value.lower()
                for stop in ["football", "jv", "varsity", "release", "depart", "game", "practice", "camp", "dinner", "team", "weigh", "photo", "pictures"]
            ):
                continue
            if TITLE_CASE_NAME_RE.fullmatch(value):
                candidates.append(value)

    return _dedupe_keep_order(candidates)


async def _collect_homepage(page) -> dict[str, Any]:
    await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1800)
    body = _clean(await page.locator("body").inner_text())
    title = _clean(await page.title())

    address_match = re.search(r"1976 Morris Kyle Drive, Firebaugh, CA 93622", body)
    phone_match = re.search(r"\(559\) 659-1415", body)

    return {
        "url": page.url,
        "title": title,
        "school_contact": {
            "name": SCHOOL_NAME,
            "address": address_match.group(0) if address_match else "",
            "phone": phone_match.group(0) if phone_match else "",
        },
        "home_summary": body[:500],
    }


async def _collect_search_results(page, query: str) -> dict[str, Any]:
    await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_selector("#site_search", timeout=30000)
    await page.fill("#site_search", query)
    await page.press("#site_search", "Enter")
    await page.wait_for_function(
        "() => document.querySelectorAll('#results .result-item').length > 0",
        timeout=30000,
    )
    await page.wait_for_timeout(1000)

    body = _clean(await page.locator("body").inner_text())
    result_count_match = re.search(r"(\d+)\s+results", body)
    result_count = int(result_count_match.group(1)) if result_count_match else 0

    cards = page.locator("#results .result-item")
    card_count = await cards.count()
    items: list[dict[str, Any]] = []
    for index in range(card_count):
        card = cards.nth(index)
        title_link = card.locator(".result-title a")
        preview_locator = card.locator(".result-preview")
        link_href = ""
        try:
            link_href = _clean(await title_link.get_attribute("href") or "")
        except Exception:  # noqa: BLE001
            link_href = ""
        title = _clean(await title_link.inner_text()) if await title_link.count() else ""
        preview = _clean(await preview_locator.inner_text()) if await preview_locator.count() else ""
        if not title and not link_href:
            continue
        items.append(
            {
                "query": query,
                "title": title,
                "url": urljoin(page.url, link_href) if link_href else "",
                "preview": preview,
                "rank": index + 1,
            }
        )

    relevant_items: list[dict[str, Any]] = []
    for item in items:
        if _is_football_relevant(item["title"], item["preview"], item["url"], query):
            relevant_items.append(item)

    return {
        "query": query,
        "page_url": page.url,
        "result_count": result_count,
        "items": relevant_items,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football evidence from the Firebaugh High site search index."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted([HOME_URL, SEARCH_URL], profile=PROXY_PROFILE)

    source_pages: list[str] = []
    errors: list[str] = []

    homepage: dict[str, Any] = {}
    search_runs: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 1200},
            ignore_https_errors=True,
        )
        page = await context.new_page()
        try:
            homepage = await _collect_homepage(page)
            source_pages.append(homepage["url"])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"homepage_failed:{type(exc).__name__}")

        for query in SEARCH_QUERIES:
            try:
                search_run = await _collect_search_results(page, query)
                search_runs.append(search_run)
                source_pages.append(search_run["page_url"])
            except Exception as exc:  # noqa: BLE001
                errors.append(f"search_failed:{query}:{type(exc).__name__}")
        await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_results: list[dict[str, Any]] = []
    seen_result_keys: set[tuple[str, str]] = set()
    search_summaries: list[dict[str, Any]] = []

    for run in search_runs:
        search_summaries.append(
            {
                "query": run["query"],
                "result_count": run["result_count"],
                "football_results": len(run["items"]),
            }
        )
        for item in run["items"]:
            key = (item["title"], item["url"])
            if key in seen_result_keys:
                continue
            seen_result_keys.add(key)
            football_results.append(
                {
                    "query": item["query"],
                    "title": item["title"],
                    "url": item["url"],
                    "preview": item["preview"],
                    "coach_mentions": _extract_coaches(item["title"], item["preview"]),
                    "location_hints": _extract_locations(item["title"], item["preview"]),
                }
            )

    coach_mentions = _dedupe_keep_order(
        coach
        for item in football_results
        for coach in item.get("coach_mentions", [])
    )
    location_hints = _dedupe_keep_order(
        hint
        for item in football_results
        for hint in item.get("location_hints", [])
    )

    extracted_items: dict[str, Any] = {
        "school_contact": homepage.get("school_contact", {}),
        "football_search_queries": SEARCH_QUERIES,
        "search_summaries": search_summaries,
        "football_results": football_results,
        "coach_mentions": coach_mentions,
        "location_hints": location_hints,
        "homepage_summary": homepage.get("home_summary", ""),
    }

    if not football_results:
        errors.append("no_public_football_content_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": PROXY_PROFILE,
            "queries": SEARCH_QUERIES,
            "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def _main() -> None:
    import json

    payload = await scrape_school()
    print(json.dumps(payload, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(_main())
