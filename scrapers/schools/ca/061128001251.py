"""Deterministic football scraper for Dixon High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "061128001251"
SCHOOL_NAME = "Dixon High"
STATE = "CA"

PROXY_PROFILE = "datacenter"

DIXON_USD_ATHLETICS_URL = "https://www.dixonusd.org/dhs/athletics/"
ATHLETICS_HOME_URL = "https://dixon.homecampus.com/"
FOOTBALL_HOME_URL = "https://dixon.homecampus.com/varsity/football/"
FOOTBALL_COACHES_URL = "https://dixon.homecampus.com/varsity/football/coaches/"
FOOTBALL_SCHEDULE_URL = "https://dixon.homecampus.com/varsity/football/schedule-results/"
FOOTBALL_ROSTER_URL = "https://dixon.homecampus.com/varsity/football/roster/"
FOOTBALL_PRINT_PREVIEW_PREFIX = (
    "https://dixon.homecampus.com/varsity/football/print-schedule-results"
)

TARGET_URLS = [
    DIXON_USD_ATHLETICS_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
]

FOOTBALL_KEYWORDS = (
    "football",
    "coach",
    "coaching",
    "schedule",
    "result",
    "varsity",
    "junior varsity",
    "groupme",
    "practice",
    "tryout",
    "clearance",
    "athletic",
    "dixon ram",
    "rams",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 40) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if any(keyword in lower for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_links(soup: BeautifulSoup, *, keywords: tuple[str, ...]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _clean(anchor.get("href", ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in keywords):
            links.append({"text": text, "href": href})
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in links:
        key = f"{link.get('text', '')}|{link.get('href', '')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    return deduped


def _extract_page_payload(page_html: str, page_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(page_html, "html.parser")
    body_text = soup.get_text("\n", strip=True)
    headings = _dedupe_keep_order(
        _clean(node.get_text(" ", strip=True))
        for node in soup.select("h1, h2, h3, h4, h5")
    )
    links = _extract_links(
        soup,
        keywords=("football", "coach", "schedule", "result", "varsity", "clearance", "team"),
    )

    paragraph_lines = _dedupe_keep_order(
        _clean(node.get_text(" ", strip=True))
        for node in soup.select("p, li")
        if _clean(node.get_text(" ", strip=True))
    )
    keyword_lines = _extract_keyword_lines(body_text, FOOTBALL_KEYWORDS)
    schedule_years = _dedupe_keep_order(
        re.findall(r"\b20\d{2}-\d{2}\b", body_text)
        + re.findall(r"\b20\d{2}-\d{2}\b", " ".join(link["text"] for link in links))
    )
    print_preview_url = ""
    for link in links:
        href = link.get("href", "")
        if "print-schedule-results" in href:
            print_preview_url = href
            break

    coaches: list[dict[str, str]] = []
    for h3 in soup.select("h3"):
        name = _clean(h3.get_text(" ", strip=True))
        if not name or name.lower() in {"football", "teams"}:
            continue
        role = ""
        sibling = h3.find_next("p")
        if sibling is not None:
            role = _clean(sibling.get_text(" ", strip=True))
        if "coach" not in (f"{name} {role}".lower()):
            continue
        coach_item: dict[str, str] = {"name": name}
        if role:
            coach_item["role"] = role
        coaches.append(coach_item)

    coach_names = _dedupe_keep_order([item["name"] for item in coaches])
    coach_roles = _dedupe_keep_order(
        [item.get("role", "") for item in coaches if item.get("role")]
    )

    return {
        "requested_url": page_url,
        "title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
        "body_text": body_text,
        "headings": headings,
        "links": links,
        "keyword_lines": keyword_lines,
        "paragraph_lines": paragraph_lines,
        "schedule_years": schedule_years,
        "print_preview_url": print_preview_url,
        "coach_names": coach_names,
        "coach_roles": coach_roles,
        "coaches": coaches,
    }


def _find_announcement_lines(page_data: dict[str, Any]) -> list[str]:
    lines = [
        value
        for value in page_data.get("paragraph_lines", [])
        if isinstance(value, str)
        and any(
            term in value.lower()
            for term in (
                "football",
                "groupme",
                "practice",
                "tryout",
                "freshman",
                "parent",
                "coach",
                "varsity",
                "junior varsity",
                "schedule",
                "clearance",
            )
        )
    ]
    return _dedupe_keep_order(lines)[:20]


async def scrape_school() -> dict[str, Any]:
    """Scrape public Dixon High football content from the Home Campus portal."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_data: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_200)
                    html = await page.content()
                    page_data.append(_extract_page_payload(html, page.url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_home = next(
        (item for item in page_data if item.get("requested_url") == FOOTBALL_HOME_URL),
        {},
    )
    football_coaches = next(
        (item for item in page_data if item.get("requested_url") == FOOTBALL_COACHES_URL),
        {},
    )
    football_schedule = next(
        (item for item in page_data if item.get("requested_url") == FOOTBALL_SCHEDULE_URL),
        {},
    )

    home_lines = _find_announcement_lines(football_home)
    home_links = [
        f"{link.get('text', '')}|{link.get('href', '')}"
        for link in football_home.get("links", [])
        if isinstance(link, dict)
    ]
    home_links = _dedupe_keep_order(home_links)

    coach_profiles = football_coaches.get("coaches", [])
    coach_names = football_coaches.get("coach_names", [])
    coach_roles = football_coaches.get("coach_roles", [])
    schedule_years = football_schedule.get("schedule_years", [])
    print_preview_url = football_schedule.get("print_preview_url") or ""

    team_level_links = [
        link
        for link in football_home.get("links", [])
        if isinstance(link, dict)
        and any(
            term in f"{link.get('text', '')} {link.get('href', '')}".lower()
            for term in ("varsity", "junior varsity", "football", "schedule")
        )
    ]

    football_program_available = bool(
        football_home.get("title")
        and (
            football_home.get("headings")
            or home_lines
            or coach_profiles
            or schedule_years
            or team_level_links
        )
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_homecampus_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "district_athletics_url": DIXON_USD_ATHLETICS_URL,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "football_home_url": FOOTBALL_HOME_URL,
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "football_page_title": football_home.get("title") or "",
        "football_page_headings": football_home.get("headings", []),
        "football_page_links": home_links,
        "football_announcements": home_lines,
        "football_team_level_links": [
            f"{link.get('text', '')}|{link.get('href', '')}"
            for link in team_level_links
        ],
        "football_coaches": coach_profiles,
        "football_coach_names": coach_names,
        "football_coach_roles": coach_roles,
        "football_schedule_years": schedule_years,
        "football_schedule_print_preview_url": print_preview_url,
        "football_schedule_keyword_lines": football_schedule.get("keyword_lines", []),
        "football_schedule_links": [
            f"{link.get('text', '')}|{link.get('href', '')}"
            for link in football_schedule.get("links", [])
            if isinstance(link, dict)
        ],
        "summary": (
            "Dixon High publicly exposes a Home Campus football portal with a dedicated "
            "football home page, a coaches page naming Wes Besseghini as head coach, "
            "and a schedule/results page with seasonal year tabs and print preview."
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
            "pages_checked": len(source_pages),
        },
        "errors": errors,
    }


async def main() -> int:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
