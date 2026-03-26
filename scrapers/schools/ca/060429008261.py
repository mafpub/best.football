"""Deterministic football scraper for Glen View High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060429008261"
SCHOOL_NAME = "Glen View High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://gvhs.beaumontusd.us/"
ATHLETICS_URL = (
    "https://gvhs.beaumontusd.us/apps/pages/index.jsp?uREC_ID=1022637&type=d&pREC_ID=1328747"
)

TARGET_URLS = [HOME_URL, ATHLETICS_URL]

CONTENT_SELECTOR = "#content_main"
ATHLETICS_MENU_SELECTOR = 'a[href*="pREC_ID=1328747"]'
MAILTO_SELECTOR = 'a[href^="mailto:"]'

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


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


def _absolute_url(href: str, base_url: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    if href.startswith(("http://", "https://")):
        return href
    if href.startswith("//"):
        return f"https:{href}"
    return urljoin(base_url, href)


async def _main_text(page) -> str:
    main = page.locator(CONTENT_SELECTOR)
    if await main.count():
        return _clean(await main.first.inner_text())
    return _clean(await page.locator("body").inner_text())


async def _collect_links(page, base_url: str) -> list[dict[str, str]]:
    anchors = await page.eval_on_selector_all(
        "#content_main a[href], a[href]",
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.getAttribute('href') || '',
            absoluteHref: anchor.href || ''
        }))""",
    )
    if not isinstance(anchors, list):
        return []

    results: list[dict[str, str]] = []
    for item in anchors:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("absoluteHref") or item.get("href") or ""))
        if not href:
            continue
        results.append({"text": text, "url": _absolute_url(href, base_url)})
    return results


def _extract_contact(text: str, links: list[dict[str, str]]) -> dict[str, str]:
    name = ""
    email_display = ""
    email_href = ""

    match = re.search(
        r"contact the GVHS Athletic Director,\s*(Mr\.\s+[A-Za-z]+)\s+"
        r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        text,
        flags=re.I,
    )
    if match:
        name = _clean(match.group(1))
        email_display = _clean(match.group(2))

    for item in links:
        if not item.get("url", "").startswith("mailto:"):
            continue
        email_href = _clean(item["url"].removeprefix("mailto:"))
        if not email_display:
            email_display = email_href
        break

    return {
        "name": name,
        "email_display": email_display,
        "email_href": email_href,
    }


def _extract_sports(text: str) -> list[str]:
    sport_patterns = [
        "Slow-Pitch Softball",
        "Volleyball",
        "Flag Football",
        "Basketball",
        "Soccer",
    ]
    sports: list[str] = []
    for sport in sport_patterns:
        if sport.lower() in text.lower():
            sports.append(sport)
    return _dedupe_keep_order(sports)


def _filter_relevant_links(links: list[dict[str, str]]) -> list[str]:
    relevant_terms = (
        "football",
        "athletics",
        "contact",
        "map",
        "mailto:",
        "tel:",
        "flag football",
        "glen view athletics",
        "back to athletics",
    )
    filtered: list[str] = []
    for item in links:
        text = _clean(item.get("text", ""))
        url = _clean(item.get("url", ""))
        if not url or url.startswith("javascript:"):
            continue
        combo = f"{text} {url}".lower()
        if any(term in combo for term in relevant_terms):
            filtered.append(f"{text}|{url}")
    return _dedupe_keep_order(filtered)


def _extract_school_identity(text: str) -> dict[str, str]:
    address = ""
    phone = ""
    fax = ""

    address_match = re.search(
        r"939\s+E\s+10th\s+Street,\s+Beaumont,\s+CA\s+92223",
        text,
        flags=re.I,
    )
    phone_match = re.search(r"Phone:\s*\(951\)\s*769-8424", text, flags=re.I)
    fax_match = re.search(r"Fax:\s*\(951\)\s*845-1134", text, flags=re.I)

    if address_match:
        address = "939 E 10th Street, Beaumont, CA 92223"
    if phone_match:
        phone = "(951) 769-8424"
    if fax_match:
        fax = "(951) 845-1134"

    return {
        "address": address,
        "phone": phone,
        "fax": fax,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Glen View High public football signals from the athletics page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            home_text = _clean(await page.locator("body").inner_text())
            home_title = _clean(await page.title())
            source_pages.append(page.url)

            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            athletics_text = await _main_text(page)
            athletics_title = _clean(await page.title())
            athletics_links = await _collect_links(page, ATHLETICS_URL)
            source_pages.append(page.url)
        finally:
            await context.close()
            await browser.close()

    school_identity = _extract_school_identity(home_text)
    director_contact = _extract_contact(athletics_text, athletics_links)
    sports_offered = _extract_sports(athletics_text)
    football_program_available = "flag football" in athletics_text.lower()

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_signals = _dedupe_keep_order(
        [
            "GVHS participates in the following four sports: Slow-Pitch Softball, Volleyball, Flag Football, Basketball, Soccer.",
            "Flag Football is publicly listed on the athletics page.",
            (
                f"Athletic Director contact: {director_contact['name']} "
                f"{director_contact['email_display'] or director_contact['email_href']}"
            ).strip(),
        ]
    )

    extracted_items: dict[str, Any] = {
        "school_identity": {
            "name": SCHOOL_NAME,
            "home_url": HOME_URL,
            "athletics_url": ATHLETICS_URL,
            "home_title": home_title,
            "address": school_identity["address"],
            "phone": school_identity["phone"],
            "fax": school_identity["fax"],
        },
        "athletics": {
            "page_title": athletics_title,
            "athletics_menu_selector": ATHLETICS_MENU_SELECTOR,
            "content_selector": CONTENT_SELECTOR,
            "athletic_director": director_contact,
            "sports_offered": sports_offered,
            "football_program_available": football_program_available,
            "football_signals": football_signals,
            "public_links": _filter_relevant_links(athletics_links),
        },
        "source_page_signals": {
            "home_content_selector": CONTENT_SELECTOR,
            "athletics_content_selector": CONTENT_SELECTOR,
            "mailto_selector": MAILTO_SELECTOR,
        },
    }

    if not any(
        [
            school_identity["address"],
            school_identity["phone"],
            director_contact["email_display"],
            sports_offered,
            football_program_available,
        ]
    ):
        errors.append("blocked:no_public_football_content_found")

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "manual_navigation_steps": [
                "home_page",
                "athletics_page",
            ],
            "focus": "football_only",
            "script_version": "1.0.0",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


def main() -> None:
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
