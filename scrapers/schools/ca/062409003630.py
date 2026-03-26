"""Deterministic football scraper for Lindhurst High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062409003630"
SCHOOL_NAME = "Lindhurst High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_HOME_URL = "https://www.blazersathletics.org/page/5"
SCHEDULE_YEARS = [
    "2025-2026",
    "2024-2025",
    "2023-2024",
    "2022-2023",
]

TARGET_URLS = [
    ATHLETICS_HOME_URL,
    *[f"https://www.blazersathletics.org/schedule?year={year}" for year in SCHEDULE_YEARS],
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for value in values:
        key = repr(value) if isinstance(value, dict) else _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _extract_links(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        href = _clean(str(anchor.get("href") or ""))
        if not href:
            continue
        if href.startswith("/"):
            href = f"https://www.blazersathletics.org{href}"
        elif not href.startswith("http"):
            continue
        links.append(
            {
                "text": _clean(anchor.get_text(" ", strip=True)),
                "href": href,
            }
        )
    return _dedupe_keep_order(links)


def _extract_page(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    return {
        "url": url,
        "title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
        "html": html,
        "text": soup.get_text("\n"),
        "links": _extract_links(soup, url),
    }


def _first_text(card: BeautifulSoup, suffix: str) -> str:
    node = card.select_one(f"[data-testid$='{suffix}']")
    return _clean(node.get_text(" ", strip=True)) if node else ""


def _card_has_football(card: BeautifulSoup) -> bool:
    activity = _first_text(card, "activity-name")
    return activity.upper() == "FOOTBALL"


def _extract_event(card: BeautifulSoup, source_url: str) -> dict[str, str]:
    event_links = []
    for anchor in card.select("a[href]"):
        href = _clean(str(anchor.get("href") or ""))
        if not href:
            continue
        if href.startswith("/"):
            href = f"https://www.blazersathletics.org{href}"
        text = _clean(anchor.get_text(" ", strip=True))
        event_links.append({"text": text, "href": href})

    event_links = _dedupe_keep_order(event_links)
    buy_tickets = next(
        (link["href"] for link in event_links if "gofan.co/app/events/" in link["href"]),
        "",
    )
    watch_live = next(
        (link["href"] for link in event_links if "nfhsnetwork.com/events/" in link["href"]),
        "",
    )
    home_away = ""
    badge = card.select_one("div.rounded-full span")
    if badge:
        home_away = _clean(badge.get_text(" ", strip=True))

    return {
        "date": _first_text(card, "month-and-day"),
        "day_of_week": _first_text(card, "day-of-week"),
        "time": _first_text(card, "time"),
        "activity": _first_text(card, "activity-name"),
        "gender_level": _first_text(card, "gender-level"),
        "event_name": _first_text(card, "event-name"),
        "venue": _first_text(card, "venue"),
        "home_away": home_away,
        "buy_tickets_url": buy_tickets,
        "watch_live_url": watch_live,
        "event_links": event_links,
        "source_page": source_url,
    }


def _extract_football_events(html: str, url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    events: list[dict[str, str]] = []
    seen: set[tuple[str, str, str, str, str]] = set()

    for card in soup.select("div.w-full.font-body"):
        if not _card_has_football(card):
            continue
        event = _extract_event(card, url)
        key = (
            event["date"],
            event["time"],
            event["gender_level"],
            event["event_name"],
            event["venue"],
        )
        if key in seen:
            continue
        seen.add(key)
        events.append(event)

    return events


async def _fetch_page(context, url: str) -> tuple[str, str]:
    page = await context.new_page()
    try:
        response = await page.goto(url, wait_until="networkidle", timeout=60_000)
        if response is not None and response.status >= 400:
            raise RuntimeError(f"HTTP {response.status} for {url}")
        return page.url, await page.content()
    finally:
        await page.close()


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, dict[str, Any]] = {}
    football_year: str = ""
    football_events: list[dict[str, str]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            ignore_https_errors=True,
        )

        try:
            for url in TARGET_URLS:
                try:
                    final_url, html = await _fetch_page(context, url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"fetch_failed:{type(exc).__name__}:{url}")
                    continue

                page_data[url] = _extract_page(html, final_url)
                source_pages.append(final_url)

                if "/schedule?year=" in url and not football_events:
                    candidate_events = _extract_football_events(html, final_url)
                    if candidate_events:
                        football_year = url.rsplit("=", 1)[-1]
                        football_events = candidate_events

        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    if not football_events:
        errors.append("no_public_football_content_found_on_schedule_pages")
        return {
            "nces_id": NCES_ID,
            "school_name": SCHOOL_NAME,
            "state": STATE,
            "source_pages": source_pages,
            "extracted_items": {
                "football_program_available": False,
                "athletics_home_url": ATHLETICS_HOME_URL,
                "schedule_years_checked": SCHEDULE_YEARS,
            },
            "scrape_meta": {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "script_version": "1.0.0",
                "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)[
                    "proxy_profile"
                ],
                "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)[
                    "proxy_servers"
                ],
                "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)[
                    "proxy_auth_mode"
                ],
                "target_urls": TARGET_URLS,
                "focus": "football_only",
            },
            "errors": errors,
        }

    home_page = page_data.get(ATHLETICS_HOME_URL, {})
    sample_events = football_events[:30]

    extracted_items: dict[str, Any] = {
        "football_program_available": True,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "football_schedule_year": football_year,
        "football_schedule_url": f"https://www.blazersathletics.org/schedule?year={football_year}",
        "football_event_count": len(football_events),
        "sample_football_events": sample_events,
        "home_page_title": _clean(str(home_page.get("title") or "")),
        "home_page_text": _clean(str(home_page.get("text") or "")),
        "summary": (
            f"Lindhurst High publishes football events in the {football_year} schedule archive on the Blazers athletics site."
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
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)[
                "proxy_profile"
            ],
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)[
                "proxy_servers"
            ],
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)[
                "proxy_auth_mode"
            ],
            "target_urls": TARGET_URLS,
            "football_year": football_year,
            "football_event_count": len(football_events),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
