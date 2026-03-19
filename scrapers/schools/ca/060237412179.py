"""Deterministic football scraper for Health Sciences High and Middle College (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright, Page

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060237412179"
SCHOOL_NAME = "Health Sciences High and Middle College"
STATE = "CA"
BASE_URL = "https://hshmc.org"
SPORTS_URL = f"{BASE_URL}/sports/"
GIRLS_FLAG_FOOTBALL_URL = f"{BASE_URL}/flag-football/"
BOYS_FLAG_FOOTBALL_URL = f"{BASE_URL}/boys-flag-football/"

TARGET_URLS = [
    SPORTS_URL,
    GIRLS_FLAG_FOOTBALL_URL,
    BOYS_FLAG_FOOTBALL_URL,
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _extract_email(text: str) -> str | None:
    match = re.search(r"[\w.\-+]+@[\w.\-]+\.\w+", text)
    return match.group(0) if match else None


def _extract_coach(text: str) -> str | None:
    match = re.search(r"COACH:\s*([^|]+?)(?:Game Schedule|$)", text, re.IGNORECASE)
    if not match:
        return None
    return _clean_text(match.group(1))


def _extract_practice_schedule(text: str) -> str | None:
    match = re.search(
        r"Practice Schedule\s+(.+?)(?:COACH:|Game Schedule|$)",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    return _clean_text(match.group(1))


def _extract_football_summary(text: str) -> str | None:
    match = re.search(
        r"About Flag Football\s+(.+?)(?:Practice Schedule|COACH:|Game Schedule|$)",
        text,
        re.IGNORECASE,
    )
    if not match:
        return None
    return _clean_text(match.group(1))


def _extract_football_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    football_terms = ("flag football", "football", "game schedule")
    kept: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        text = _clean_text(link.get("text", ""))
        href = (link.get("href") or "").strip()
        if not text or not href:
            continue
        lowered = text.lower()
        if not any(term in lowered for term in football_terms):
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        kept.append({"text": text, "href": href})
    return kept


async def _extract_page(page: Page, requested_url: str) -> dict[str, Any]:
    h1 = await page.locator("h1").first.text_content()
    post_content_locator = page.locator("div.post-content")
    if await post_content_locator.count():
        body_text = await post_content_locator.first.text_content() or ""
    else:
        body_text = await page.locator("body").inner_text()

    links = await page.evaluate(
        """() => Array.from(document.querySelectorAll('a[href]')).map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.href || '',
        }))"""
    )
    if not isinstance(links, list):
        links = []

    normalized_text = _clean_text(body_text)
    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean_text(h1 or ""),
        "body_text": normalized_text,
        "football_links": _extract_football_links(
            [item for item in links if isinstance(item, dict)]
        ),
        "contact_email": _extract_email(normalized_text),
        "coach": _extract_coach(normalized_text),
        "practice_schedule": _extract_practice_schedule(normalized_text),
        "summary": _extract_football_summary(normalized_text),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit HSHMC football pages and extract public football details."""
    require_proxy_credentials()
    assert_not_blocklisted(TARGET_URLS)

    errors: list[str] = []
    source_pages: list[str] = []
    page_results: list[dict[str, Any]] = []

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
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    result = await _extract_page(page, url)
                    page_results.append(result)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_pages = []
    for item in page_results:
        title = str(item.get("title") or "")
        if "football" not in title.lower():
            continue
        football_pages.append(
            {
                "team_name": title,
                "url": item["final_url"],
                "coach": item["coach"],
                "practice_schedule": item["practice_schedule"],
                "summary": item["summary"],
            }
        )

    football_links: list[str] = []
    for item in page_results:
        for link in item.get("football_links", []):
            if not isinstance(link, dict):
                continue
            text = _clean_text(str(link.get("text") or ""))
            href = str(link.get("href") or "").strip()
            if text and href:
                football_links.append(f"{text}|{href}")
    football_links = _dedupe_keep_order(football_links)

    game_schedule_links = [
        value
        for value in football_links
        if value.lower().startswith("game schedule|")
        or "maxpreps" in value.lower()
    ]

    football_hub_links = [
        value
        for value in football_links
        if value.lower().startswith("girls flag football|")
        or value.lower().startswith("boys flag football|")
    ]

    contact_emails = _dedupe_keep_order(
        [
            str(item.get("contact_email") or "")
            for item in page_results
            if item.get("contact_email")
        ]
    )

    football_program_available = bool(football_pages)
    if not football_program_available:
        errors.append("blocked:no_public_football_pages_found_on_school_domain")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_hub_url": SPORTS_URL,
        "football_hub_links": football_hub_links,
        "teams": football_pages,
        "game_schedule_links": game_schedule_links,
        "program_contact_email": contact_emails[0] if contact_emails else None,
        "all_contact_emails": contact_emails,
        "notes": (
            "Public football team pages expose coach and practice details, but no explicit public "
            "game schedule link was present in the football page body content."
            if football_program_available and not game_schedule_links
            else None
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
            "target_urls": TARGET_URLS,
            "page_count": len(page_results),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Compatibility entrypoint for runtime discovery."""
    return await scrape_school()
