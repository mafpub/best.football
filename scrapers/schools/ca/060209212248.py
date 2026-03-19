"""Deterministic athletics availability scraper for Alain LeRoy Locke College Preparatory Academy (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060209212248"
SCHOOL_NAME = "Alain Leroy Locke College Preparatory Academy"
STATE = "CA"
BASE_URL = "https://greendot.org/locke"
SCHOOL_URL = f"{BASE_URL}/"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    SCHOOL_URL,
    f"{BASE_URL}/about-us/",
    f"{BASE_URL}/about-us/?section=section-5",
    f"{BASE_URL}/students-families/",
    f"{BASE_URL}/calendar-events/",
]

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "sport",
    "football",
    "flag football",
    "volleyball",
    "cross country",
    "basketball",
    "competitive cheer",
    "soccer",
    "baseball",
    "softball",
    "track and field",
    "track",
    "intramural",
    "cif",
    "conditioning",
)

SPORT_KEYWORDS = (
    "football",
    "flag football",
    "volleyball",
    "cross country",
    "basketball",
    "competitive cheer",
    "soccer",
    "baseball",
    "softball",
    "track and field",
    "intramural sports",
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


def _is_school_url(url: str) -> bool:
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() != "greendot.org":
        return False
    path = parsed.path.rstrip("/")
    return path.startswith("/locke") or path.startswith("/wp-content/uploads/sites/16")


def _keyword_lines(text: str, *, limit: int = 30) -> list[str]:
    matches: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _extract_reported_sports(lines: list[str]) -> list[str]:
    found: list[str] = []
    haystack = " | ".join(lines).lower()
    for sport in SPORT_KEYWORDS:
        if sport in haystack:
            found.append(sport)
    return _dedupe_keep_order(found)


async def _collect_signal(page) -> dict[str, Any]:
    body = await page.inner_text("body")
    keyword_lines = _keyword_lines(body)

    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            href: e.href || "",
            text: (e.textContent || "").replace(/\\s+/g, " ").trim()
        }))""",
    )

    athletics_links: list[str] = []
    for anchor in anchors:
        href = str(anchor.get("href") or "").strip()
        text = str(anchor.get("text") or "").strip()
        if not href or not _is_school_url(href):
            continue
        combo = f"{text} {href}".lower()
        if any(keyword in combo for keyword in ATHLETICS_KEYWORDS):
            athletics_links.append(f"{text}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "keyword_lines": keyword_lines,
        "athletics_links": _dedupe_keep_order(athletics_links)[:25],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate the live Locke school pages and confirm public athletics content."""
    require_proxy_credentials()
    assert_not_blocklisted(MANUAL_PAGES)

    source_pages: list[str] = []
    errors: list[str] = []
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
            for url in MANUAL_PAGES:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1500)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_keyword_lines: list[str] = []
    all_athletics_links: list[str] = []
    cif_notes: list[str] = []
    page_titles: list[str] = []

    for signal in page_signals:
        page_titles.append(str(signal.get("title") or ""))
        lines = [line for line in signal.get("keyword_lines", []) if isinstance(line, str)]
        links = [link for link in signal.get("athletics_links", []) if isinstance(link, str)]
        all_keyword_lines.extend(lines)
        all_athletics_links.extend(links)
        for line in lines:
            lowered = line.lower()
            if "cif athletics" in lowered or "probation year" in lowered:
                cif_notes.append(line)

    all_keyword_lines = _dedupe_keep_order(all_keyword_lines)
    all_athletics_links = _dedupe_keep_order(all_athletics_links)
    cif_notes = _dedupe_keep_order(cif_notes)
    reported_sports = _extract_reported_sports(all_keyword_lines)

    athletics_program_available = bool(all_keyword_lines or all_athletics_links)
    if not athletics_program_available:
        errors.append("no_public_athletics_content_detected_on_live_locke_pages")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "manual_pages_checked": MANUAL_PAGES,
        "page_titles_checked": _dedupe_keep_order(page_titles),
        "athletics_keyword_lines": all_keyword_lines[:30],
        "athletics_navigation_links": all_athletics_links[:20],
        "reported_sports": reported_sports,
        "cif_status_notes": cif_notes[:10],
        "athletics_summary": (
            "Locke publishes athletics program details on its live Green Dot school pages, "
            "including an athletics overview and a students/families clubs and athletics section."
            if athletics_program_available
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
                "home_page",
                "about_us_page",
                "athletics_section_on_about_us",
                "students_and_families_page",
                "calendar_events_page",
            ],
            "canonical_school_url": SCHOOL_URL,
            "nces_website_redirect_note": "NCES host ca.greendot.org/locke resolves to live school content on greendot.org/locke.",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
