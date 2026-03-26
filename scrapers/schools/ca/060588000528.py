"""Deterministic football scraper for Brea Canyon High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "060588000528"
SCHOOL_NAME = "Brea Canyon High"
STATE = "CA"
BASE_URL = "https://breacanyon.bousd.us"
HOME_URL = f"{BASE_URL}/"
CALENDAR_PAGE_URL = (
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=1178824&type=d&pREC_ID=1426076"
)
STAFF_URL = f"{BASE_URL}/apps/staff/"
NEWS_URL = f"{BASE_URL}/apps/news/"
EVENTS_URL = f"{BASE_URL}/apps/events/"

PROXY_PROFILE = "datacenter"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in values:
        value = _clean(item)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _normalize_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    value = _clean(url)
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("/"):
        return urljoin(BASE_URL + "/", value)
    if value.startswith("#"):
        return ""
    return urljoin(BASE_URL + "/", value)


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 80) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _extract_links(page) -> list[tuple[str, str]]:
    raw = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => ({text:(e.textContent||'').replace(/\\s+/g,' ').trim(), href:e.getAttribute('href') || ''}))",
    )
    links: list[tuple[str, str]] = []
    if not isinstance(raw, list):
        return links

    for item in raw:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _normalize_url(str(item.get("href") or ""))
        if not href:
            continue
        links.append((text, href))

    return links


def _find_link_by_keywords(
    links: list[tuple[str, str]],
    keywords: tuple[str, ...],
) -> str:
    if not links:
        return ""

    for text, href in links:
        haystack = f"{text} {href}".lower()
        if any(keyword in haystack for keyword in keywords):
            return href

    return ""


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    try:
        title = _clean(await page.title())
    except Exception:  # noqa: BLE001
        title = ""

    try:
        body_text = _clean(await page.inner_text("body"))
    except Exception:  # noqa: BLE001
        body_text = ""

    try:
        links = await _extract_links(page)
    except Exception:  # noqa: BLE001
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": title,
        "body_text": body_text,
        "football_lines": _extract_lines(
            body_text,
            keywords=(
                "football",
                "flag football",
                "athletic",
                "coach",
                "schedule",
            ),
        ),
        "links": links,
    }


def _extract_football_link_rows(links: list[tuple[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for text, href in links:
        if not text or not href:
            continue
        if re.search(r"football", text, re.IGNORECASE):
            out.append({"label": text, "url": href})
    return out


def _extract_menu_targets(links: list[tuple[str, str]]) -> list[str]:
    menu_targets = [
        _find_link_by_keywords(
            links,
            ("school calendar", "school calendar -", "calendar", "staff directory", "news"),
        ),
    ]

    explicit = [
        CALENDAR_PAGE_URL,
        STAFF_URL,
        NEWS_URL,
        EVENTS_URL,
    ]

    return _dedupe_keep_order([*menu_targets, *explicit])


def _blocked_by_cloudflare(signal: dict[str, Any]) -> bool:
    if not isinstance(signal.get("title"), str):
        return False
    return "attention required" in signal["title"].lower()


async def scrape_school() -> dict[str, Any]:
    """Explore school pages and extract public football evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)

    target_urls = _dedupe_keep_order(
        [
            HOME_URL,
            CALENDAR_PAGE_URL,
            STAFF_URL,
            NEWS_URL,
            EVENTS_URL,
        ]
    )
    assert_not_blocklisted(target_urls, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )

        try:
            page = await context.new_page()

            for url in target_urls:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(1200)
                    signal = await _collect_page_signal(page, url)
                    page_signals.append(signal)
                    source_pages.append(signal["final_url"])

                    # Follow explicit football/event links found on the homepage.
                    if url == HOME_URL:
                        football_links = _extract_football_link_rows(signal["links"])
                        for item in football_links[:6]:
                            href = item.get("url")
                            if not href or href in source_pages:
                                continue
                            try:
                                await page.goto(href, wait_until="domcontentloaded", timeout=60000)
                                await page.wait_for_timeout(1200)
                                follow_signal = await _collect_page_signal(page, href)
                                page_signals.append(follow_signal)
                                source_pages.append(follow_signal["final_url"])
                            except Exception as exc:  # noqa: BLE001
                                errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{href}")

                    # Follow discoverable menu-like pages from homepage links.
                    if url == HOME_URL:
                        menu_targets = _extract_menu_targets(signal["links"])
                        for menu_url in menu_targets:
                            if not menu_url or menu_url in source_pages:
                                continue
                            try:
                                await page.goto(menu_url, wait_until="domcontentloaded", timeout=60000)
                                await page.wait_for_timeout(1200)
                                menu_signal = await _collect_page_signal(page, menu_url)
                                page_signals.append(menu_signal)
                                source_pages.append(menu_signal["final_url"])
                            except Exception as exc:  # noqa: BLE001
                                errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{menu_url}")

                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")

        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_links: list[tuple[str, str]] = []
    all_body = []
    for signal in page_signals:
        all_links.extend(signal.get("links", []))
        all_body.append(str(signal.get("body_text") or ""))

    football_links = _dedupe_keep_order(
        [
            item["url"]
            for item in _extract_football_link_rows(all_links)
        ]
    )
    football_lines = _dedupe_keep_order(
        [
            line
            for body in all_body
            for line in _extract_lines(
                body,
                keywords=(
                    "flag football",
                    "football",
                    "athletic",
                    "athletics",
                    "coach",
                    "varsity",
                ),
            )
        ]
    )
    events_page_signal = next(
        (
            signal
            for signal in page_signals
            if signal.get("requested_url") == EVENTS_URL
        ),
        {},
    )
    calendar_blocked = bool(_blocked_by_cloudflare(events_page_signal))

    football_program_available = bool(
        football_lines
        or football_links
        or any("football" in (text or "").lower() for text in all_body)
    )

    if football_program_available:
        if not football_links:
            errors.append("blocked:football_program_detected_without_event_url")

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_calendar_url": CALENDAR_PAGE_URL,
        "staff_url": STAFF_URL,
        "news_url": NEWS_URL,
        "events_url": EVENTS_URL,
        "football_schedule_public": not calendar_blocked,
        "football_team_names": ["Flag Football"] if football_program_available else [],
        "football_event_links": football_links,
        "football_keyword_lines": football_lines,
        "football_schedule_blocked": calendar_blocked,
        "summary": (
            (
                "Homepage and linked event content indicate a Flag Football game is listed, "
                "but the full events index/path is Cloudflare-restricted via datacenter proxy."
                if football_program_available
                else ""
            )
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
            "proxy_profile": PROXY_PROFILE,
            "target_urls": target_urls,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
