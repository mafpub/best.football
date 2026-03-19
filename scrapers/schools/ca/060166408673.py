"""Deterministic athletics scraper for Alameda Community Learning Center (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060166408673"
SCHOOL_NAME = "Alameda Community Learning Center"
STATE = "CA"
BASE_URL = "https://www.alamedaclc.org"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/our-program/athletics",
    f"{BASE_URL}/our-program/athletics/basketball-champs",
    f"{BASE_URL}/our-program/clubs-and-activities",
]

PROGRAM_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "basketball",
    "soccer",
    "volleyball",
    "baseball",
    "softball",
    "football",
    "track",
    "cross country",
    "wrestling",
    "fitness",
    "mamba hawks",
)

SPORT_TOKENS = (
    "baseball",
    "basketball",
    "cheerleading",
    "cross country",
    "flag football",
    "football",
    "golf",
    "soccer",
    "softball",
    "swimming",
    "tennis",
    "track",
    "volleyball",
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


def _is_school_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "www.alamedaclc.org" or host.endswith(".alamedaclc.org")


def _keyword_lines(text: str, *, limit: int = 40) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in PROGRAM_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_sports(summary: str) -> list[str]:
    lowered = summary.lower()
    found: list[str] = []
    for token in SPORT_TOKENS:
        if token in lowered:
            found.append(token.title() if token != "cross country" else "Cross Country")
    return _dedupe_keep_order(found)


async def _collect_signal(page) -> dict[str, Any]:
    body = await page.inner_text("body")
    lines = _keyword_lines(body)

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    keyword_links: list[str] = []
    for link in links:
        label = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        combo = f"{label} {href}".lower()
        if any(keyword in combo for keyword in PROGRAM_KEYWORDS):
            keyword_links.append(f"{label}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "school_domain": _is_school_domain(page.url),
        "keyword_lines": lines,
        "keyword_links": _dedupe_keep_order(keyword_links)[:40],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate ACLC athletics pages and extract public program details."""
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
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
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

    school_domain_lines: list[str] = []
    school_domain_links: list[str] = []
    athletics_page_lines: list[str] = []
    championship_page_lines: list[str] = []
    clubs_page_lines: list[str] = []
    partner_program_links: list[str] = []

    for signal in page_signals:
        if not signal.get("school_domain"):
            continue

        lines = [item for item in signal.get("keyword_lines", []) if isinstance(item, str)]
        links = [item for item in signal.get("keyword_links", []) if isinstance(item, str)]
        url = str(signal.get("url") or "")

        school_domain_lines.extend(lines)
        school_domain_links.extend(links)

        if url.rstrip("/") == f"{BASE_URL}/our-program/athletics":
            athletics_page_lines.extend(lines)
            for link in links:
                if "Athletic Program|" in link:
                    partner_program_links.append(link)
        elif url.rstrip("/") == f"{BASE_URL}/our-program/athletics/basketball-champs":
            championship_page_lines.extend(lines)
        elif url.rstrip("/") == f"{BASE_URL}/our-program/clubs-and-activities":
            clubs_page_lines.extend(lines)

    school_domain_lines = _dedupe_keep_order(school_domain_lines)
    school_domain_links = _dedupe_keep_order(school_domain_links)
    athletics_page_lines = _dedupe_keep_order(athletics_page_lines)
    championship_page_lines = _dedupe_keep_order(championship_page_lines)
    clubs_page_lines = _dedupe_keep_order(clubs_page_lines)
    partner_program_links = _dedupe_keep_order(partner_program_links)

    middle_school_summary = next(
        (
            line
            for line in athletics_page_lines
            if "middle schoolers combine with their sister school" in line.lower()
        ),
        "",
    )
    middle_school_competition = next(
        (
            line
            for line in athletics_page_lines
            if "compete with other alameda middle school teams" in line.lower()
        ),
        "",
    )
    high_school_summary = next(
        (
            line
            for line in athletics_page_lines
            if "aclc high schoolers who are alameda residents" in line.lower()
        ),
        "",
    )
    basketball_fitness_line = next(
        (
            line
            for line in athletics_page_lines
            if "basketball fitness as an elective" in line.lower()
        ),
        "",
    )

    middle_school_sports = _extract_sports(middle_school_summary)
    high_school_sports = _extract_sports(high_school_summary)

    championship_mentions = [
        line
        for line in championship_page_lines
        if "citywide champs" in line.lower() or "girls basketball team" in line.lower()
    ]

    clubs_mentions = [
        line
        for line in clubs_page_lines
        if "bevy" in line.lower() or "fantasy soccer" in line.lower()
    ]

    athletics_program_available = bool(
        athletics_page_lines or championship_page_lines or partner_program_links
    )

    if not athletics_program_available:
        errors.append("blocked:no_public_athletics_program_content_found_on_school_domain")

    team_name = "Mamba Hawks" if any("mamba hawks" in line.lower() for line in school_domain_lines) else ""

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            ""
            if athletics_program_available
            else "No public athletics program content found on the ACLC school domain."
        ),
        "manual_pages_checked": MANUAL_PAGES,
        "team_name": team_name,
        "middle_school_sports_summary": middle_school_summary,
        "middle_school_competition_summary": middle_school_competition,
        "middle_school_sports": middle_school_sports,
        "high_school_sports_summary": high_school_summary,
        "high_school_sports": high_school_sports,
        "partner_athletics_program_links": partner_program_links,
        "basketball_fitness_elective": basketball_fitness_line,
        "basketball_championship_mentions": _dedupe_keep_order(championship_mentions),
        "sports_related_clubs": _dedupe_keep_order(clubs_mentions),
        "school_domain_keyword_lines": school_domain_lines,
        "school_domain_keyword_links": school_domain_links,
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
                "athletics_and_fitness_page",
                "basketball_championship_page",
                "clubs_and_activities_page",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
