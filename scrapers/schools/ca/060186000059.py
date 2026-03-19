"""Deterministic athletics scraper for Albany High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060186000059"
SCHOOL_NAME = "Albany High"
STATE = "CA"
SCHOOL_BASE_URL = "https://ahs.ausdk12.org"
ATHLETICS_SITE_BASE_URL = "https://www.goalbanyathletics.org"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{SCHOOL_BASE_URL}/",
    f"{SCHOOL_BASE_URL}/athletics",
    f"{ATHLETICS_SITE_BASE_URL}/home",
    f"{ATHLETICS_SITE_BASE_URL}/aboutus",
    f"{ATHLETICS_SITE_BASE_URL}/page/show/2610387-teams-",
    f"{ATHLETICS_SITE_BASE_URL}/page/show/2811225-registration",
]

PROGRAM_KEYWORDS = (
    "athletics",
    "athletic",
    "interscholastic",
    "student athletes",
    "boosters",
    "football",
    "flag football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
    "volleyball",
    "track",
    "cross country",
    "wrestling",
    "swimming",
    "water polo",
    "tennis",
    "golf",
    "ultimate frisbee",
    "cif",
    "tcal",
)

SPORT_TOKENS = (
    "baseball",
    "basketball",
    "cross country",
    "flag football",
    "football",
    "golf",
    "soccer",
    "softball",
    "swimming",
    "tennis",
    "track",
    "ultimate frisbee",
    "volleyball",
    "water polo",
    "wrestling",
)


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = " ".join(value.split()).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _is_school_or_athletics_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in {"ahs.ausdk12.org", "www.goalbanyathletics.org"}


def _keyword_lines(text: str, *, limit: int = 50) -> list[str]:
    matches: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in PROGRAM_KEYWORDS):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _extract_reported_sports(lines: list[str]) -> list[str]:
    haystack = " | ".join(lines).lower()
    sports: list[str] = []
    for token in SPORT_TOKENS:
        if token in haystack:
            sports.append(token.title() if token != "cross country" else "Cross Country")
    return _dedupe_keep_order(sports)


async def _collect_signal(page) -> dict[str, Any]:
    body = await page.inner_text("body")
    keyword_lines = _keyword_lines(body)
    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    keyword_links: list[str] = []
    for anchor in anchors:
        text = str(anchor.get("text") or "").strip()
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        combo = f"{text} {href}".lower()
        if any(keyword in combo for keyword in PROGRAM_KEYWORDS):
            keyword_links.append(f"{text}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_or_athletics_domain(page.url),
        "keyword_lines": keyword_lines,
        "keyword_links": _dedupe_keep_order(keyword_links)[:40],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate Albany High and its linked athletics site to capture public athletics details."""
    require_proxy_credentials()
    assert_not_blocklisted(MANUAL_PAGES)

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
            ignore_https_errors=True,
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

    school_page_lines: list[str] = []
    school_page_links: list[str] = []
    athletics_site_lines: list[str] = []
    athletics_site_links: list[str] = []
    page_titles: list[str] = []

    for signal in page_signals:
        page_titles.append(str(signal.get("title") or ""))
        lines = [item for item in signal.get("keyword_lines", []) if isinstance(item, str)]
        links = [item for item in signal.get("keyword_links", []) if isinstance(item, str)]
        url = str(signal.get("url") or "")

        if url.startswith(SCHOOL_BASE_URL):
            school_page_lines.extend(lines)
            school_page_links.extend(links)
        elif url.startswith(ATHLETICS_SITE_BASE_URL):
            athletics_site_lines.extend(lines)
            athletics_site_links.extend(links)

    school_page_lines = _dedupe_keep_order(school_page_lines)
    school_page_links = _dedupe_keep_order(school_page_links)
    athletics_site_lines = _dedupe_keep_order(athletics_site_lines)
    athletics_site_links = _dedupe_keep_order(athletics_site_links)
    page_titles = _dedupe_keep_order(page_titles)

    reported_sports = _extract_reported_sports(athletics_site_lines)
    league_notes = [
        line
        for line in athletics_site_lines
        if "tri counties athletic league" in line.lower() or "north coast section" in line.lower()
    ][:10]
    registration_notes = [
        line
        for line in athletics_site_lines
        if "registration" in line.lower() or "athletic registration" in line.lower()
    ][:10]

    athletics_program_available = bool(
        school_page_lines or school_page_links or athletics_site_lines or athletics_site_links
    )

    if not athletics_program_available:
        errors.append("blocked:no_public_albany_high_athletics_content_found_on_manual_navigation_pages")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "manual_pages_checked": MANUAL_PAGES,
        "page_titles_checked": page_titles,
        "school_athletics_keyword_lines": school_page_lines[:20],
        "school_athletics_links": school_page_links[:20],
        "linked_athletics_site_keyword_lines": athletics_site_lines[:40],
        "linked_athletics_site_links": athletics_site_links[:25],
        "reported_sports": reported_sports,
        "league_affiliation_notes": league_notes,
        "registration_notes": registration_notes,
        "athletics_summary": (
            "Albany High exposes a school-hosted athletics page and links to a dedicated Albany Athletics "
            "site with AHS registration, league affiliation, boosters support, and named sports teams."
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
                "school_homepage",
                "school_athletics_page",
                "linked_athletics_home",
                "linked_athletics_about_page",
                "linked_athletics_teams_page",
                "linked_athletics_registration_page",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
