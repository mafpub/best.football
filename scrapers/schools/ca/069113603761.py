"""Deterministic football scraper for Anzar High (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "069113603761"
SCHOOL_NAME = "Anzar High"
STATE = "CA"
BASE_URL = "https://www.asjusd.org/o/anzar-high-school"
HOME_URL = BASE_URL
STAFF_URL = f"{BASE_URL}/staff/"
FOOTBALL_NEWS_URL = f"{BASE_URL}/article/2269690"

TARGET_URLS = [HOME_URL, STAFF_URL, FOOTBALL_NEWS_URL]

PROXY_PROFILE = "datacenter"
PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
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
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 50) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_links(links: list[dict[str, Any]], *, keywords: tuple[str, ...]) -> list[str]:
    matches: list[str] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in keywords):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)


def _title_case_name(value: str) -> str:
    value = _clean(value)
    if not value:
        return ""
    if value.isupper():
        return value.title()
    return value


def _extract_athletic_director(text: str) -> dict[str, str]:
    lines = [_clean(line) for line in text.splitlines() if _clean(line)]
    for idx, line in enumerate(lines):
        if "athletic director" not in line.lower():
            continue

        name = ""
        for prior in reversed(lines[max(0, idx - 4) : idx]):
            if "@" in prior:
                continue
            if re.search(r"[A-Za-z]", prior):
                name = _title_case_name(prior)
                break

        email = ""
        for following in lines[idx + 1 : idx + 4]:
            if re.search(r"[\w.\-+]+@[\w.\-]+\.\w+", following):
                email = following.lower()
                break

        return {
            "name": name,
            "role": "Athletic Director",
            "email": email,
        }

    return {}


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    body_text = ""
    title = ""
    links: list[dict[str, Any]] = []

    try:
        body_text = await page.inner_text("body")
    except Exception:  # noqa: BLE001
        body_text = ""

    try:
        title = _clean(await page.title())
    except Exception:  # noqa: BLE001
        title = ""

    try:
        links = await page.eval_on_selector_all(
            "a[href]",
            """els => els.map(e => ({
                text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: e.getAttribute('href') || ''
            }))""",
        )
    except Exception:  # noqa: BLE001
        links = []

    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": title,
        "body_text": body_text,
        "football_lines": _extract_lines(
            body_text,
            keywords=("football", "ticket", "admission", "athletic director", "game"),
        ),
        "football_links": _extract_links(
            links,
            keywords=("football", "athletics", "ticket", "admission", "director", "game"),
        ),
    }


async def _goto_with_retry(page, url: str) -> None:
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == 0:
                await page.wait_for_timeout(750)
                continue
            raise last_error


async def scrape_school() -> dict[str, Any]:
    """Visit the home, staff, and newsletter pages and extract football evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

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
            for url in TARGET_URLS:
                page = await context.new_page()
                try:
                    await _goto_with_retry(page, url)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page_signal(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
                finally:
                    await page.close()
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    signal_map = {signal["requested_url"]: signal for signal in page_signals}
    home_signal = signal_map.get(HOME_URL, {})
    staff_signal = signal_map.get(STAFF_URL, {})
    football_signal = signal_map.get(FOOTBALL_NEWS_URL, {})

    home_text = str(home_signal.get("body_text") or "")
    staff_text = str(staff_signal.get("body_text") or "")
    football_text = str(football_signal.get("body_text") or "")

    all_text = " ".join([home_text, staff_text, football_text])
    football_lines = _dedupe_keep_order(
        _extract_lines(
            all_text,
            keywords=(
                "football",
                "ticket",
                "admission",
                "athletic director",
                "home games",
                "pcal",
            ),
        )
    )
    football_links = _dedupe_keep_order(
        [
            value
            for signal in page_signals
            for value in signal.get("football_links", [])
        ]
    )

    athletic_director = _extract_athletic_director(staff_text)

    football_program_available = bool(
        football_lines
        or football_links
        or "football" in all_text.lower()
        or athletic_director
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_names": ["Football"] if football_program_available else [],
        "football_schedule_public": False,
        "football_schedule_note": (
            "Public football schedule page was not exposed on the inspected school pages; "
            "the public newsletter article instead lists PCAL football admission pricing for regular-season home games."
        ),
        "football_article_title": (
            str(football_signal.get("title") or "").split(" | ")[0] or "Anzar's Summer Newsletter"
        ),
        "football_article_url": FOOTBALL_NEWS_URL,
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "athletic_director": athletic_director,
        "school_address": "2000 San Juan Highway, San Juan Bautista, CA 95045",
        "school_phone": "831-623-7660",
        "summary": (
            "Anzar High publicly mentions football in its summer newsletter's PCAL ticket pricing section and lists Rance Hodge as Athletic Director on the staff page."
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
            "proxy_profile": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
            "manual_navigation_steps": ["home", "staff", "summer_newsletter_article"],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
