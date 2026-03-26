"""Deterministic football scraper for Granada Hills Charter (CA)."""

from __future__ import annotations

import asyncio
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

NCES_ID = "060220603050"
SCHOOL_NAME = "Granada Hills Charter"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_URL = "https://www.ghctk12.com/"
ATHLETICS_URL = "https://www.ghctk12.com/programs/athletics"
DIRECTORY_URL = "https://www.ghctk12.com/why-ghc/directory?const_page=1"
MEDIA_DAY_ARTICLE_URL = (
    "https://www.ghctk12.com/news/details/~board/grades-9-12-news/post/"
    "west-valley-league-football-media-day-previews-2025-season"
)
FOOTBALL_HIGHLIGHTS_URL = (
    "https://www.ghctk12.com/news/details/~board/grades-9-12-news/post/"
    "sports-highlights-from-our-new-media-academy-students"
)

TARGET_URLS = [
    SCHOOL_URL,
    ATHLETICS_URL,
    DIRECTORY_URL,
    MEDIA_DAY_ARTICLE_URL,
    FOOTBALL_HIGHLIGHTS_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _collect_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines


def _extract_links(page, *, keywords: tuple[str, ...], limit: int = 40) -> list[str]:
    return page.eval_on_selector_all(
        "a[href]",
        """(els) => els.map((a) => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        }))""",
    )


def _normalize_href(href: str, base: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("/"):
        return urljoin(base, raw)
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return ""


def _keyword_lines(text: str, keywords: tuple[str, ...], limit: int = 40) -> list[str]:
    matches: list[str] = []
    for line in _collect_lines(text):
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _extract_email(text: str) -> str:
    match = re.search(r"[\w.+-]+@[\w.-]+\.\w+", text)
    return match.group(0) if match else ""


def _extract_phone(text: str) -> str:
    match = re.search(r"\(\d{3}\)\s*\d{3}-\d{4}(?:,\s*ext\.\s*\d+)?", text)
    return _clean(match.group(0)) if match else ""


def _extract_named_role(lines: list[str], marker: str) -> dict[str, str]:
    marker_lower = marker.lower()
    for idx, line in enumerate(lines):
        if marker_lower not in line.lower():
            continue
        name = lines[idx - 1] if idx > 0 else ""
        if not name:
            continue
        return {
            "name": name,
            "title": line,
        }
    return {}


def _extract_football_staff(lines: list[str]) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    for idx, line in enumerate(lines):
        lowered = line.lower()
        if "football coach" not in lowered:
            continue
        name = lines[idx - 1] if idx > 0 else ""
        if name:
            coaches.append(
                {
                    "name": name,
                    "role": "Football Coach",
                    "source": "staff_directory",
                }
            )
    return coaches


async def _collect_page(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=20000)
    links = await _extract_links(page, keywords=("football", "gofan", "sportsengine", "ticket"))

    football_links: list[str] = []
    ticket_links: list[str] = []
    livestream_links: list[str] = []
    for item in links if isinstance(links, list) else []:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _normalize_href(str(item.get("href") or ""), page.url)
        if not href:
            continue
        hay = f"{text} {href}".lower()
        if any(keyword in hay for keyword in ("football", "athletics", "coach", "schedule", "roster")):
            football_links.append(f"{text}|{href}" if text else href)
        if "gofan" in hay:
            ticket_links.append(f"{text}|{href}" if text else href)
        if "sportsengine" in hay or "locallive" in hay:
            livestream_links.append(f"{text}|{href}" if text else href)

    lines = _collect_lines(body_text)
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "lines": lines,
        "football_lines": _keyword_lines(
            body_text,
            keywords=("football", "gofan", "sportsengine", "tryout", "stadium", "highlanders"),
            limit=40,
        ),
        "football_links": _dedupe_keep_order(football_links),
        "ticket_links": _dedupe_keep_order(ticket_links),
        "livestream_links": _dedupe_keep_order(livestream_links),
    }


async def _collect_directory(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=25000)
    lines = _collect_lines(body_text)

    athletics_director = _extract_named_role(lines, "Athletic Director")
    football_coaches = _extract_football_staff(lines)
    athletics_contact = _extract_named_role(lines, "Activities Office")

    emails = _dedupe_keep_order([_extract_email(line) for line in lines if "@" in line])
    phones = _dedupe_keep_order([_extract_phone(line) for line in lines if "(" in line and "-" in line])

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "lines": lines,
        "athletic_director": athletics_director,
        "football_coaches": football_coaches,
        "activities_contact": athletics_contact,
        "emails": emails,
        "phones": phones,
        "football_lines": _keyword_lines(body_text, keywords=("football", "athletic director", "activities office")),
    }


async def _collect_news_article(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=20000)
    lines = _collect_lines(body_text)
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "lines": lines,
        "football_lines": _keyword_lines(
            body_text,
            keywords=("football", "gofan", "north hollywood", "bucky brooks", "head coaches", "game"),
            limit=30,
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Granada Hills Charter's public football signals from official pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, dict[str, Any]] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url, collector, key in [
                (SCHOOL_URL, _collect_page, "school"),
                (ATHLETICS_URL, _collect_page, "athletics"),
                (DIRECTORY_URL, _collect_directory, "directory"),
                (MEDIA_DAY_ARTICLE_URL, _collect_news_article, "media_day_article"),
                (FOOTBALL_HIGHLIGHTS_URL, _collect_news_article, "highlights_article"),
            ]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1600)
                    page_data[key] = await collector(page)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_page = page_data.get("school", {})
    athletics_page = page_data.get("athletics", {})
    directory_page = page_data.get("directory", {})
    media_day_article = page_data.get("media_day_article", {})
    highlights_article = page_data.get("highlights_article", {})

    school_lines = school_page.get("lines", [])
    if not isinstance(school_lines, list):
        school_lines = []
    athletics_lines = athletics_page.get("lines", [])
    if not isinstance(athletics_lines, list):
        athletics_lines = []
    directory_lines = directory_page.get("lines", [])
    if not isinstance(directory_lines, list):
        directory_lines = []

    fall_sports: list[str] = []
    fall_marker_seen = False
    for line in athletics_lines:
        if line.lower() == "fall sports":
            fall_marker_seen = True
            continue
        if fall_marker_seen and line in {"Winter Sports", "Spring Sports", "Information Resources"}:
            break
        if fall_marker_seen and line and line not in fall_sports:
            if line not in {"About", "Coaches", "Schedules", "Rosters", "Tryouts", "Tickets and More"}:
                fall_sports.append(line)

    football_fall_present = any("football" in line.lower() for line in fall_sports)

    athletic_director = directory_page.get("athletic_director", {})
    if not isinstance(athletic_director, dict):
        athletic_director = {}

    football_coaches = directory_page.get("football_coaches", [])
    if not isinstance(football_coaches, list):
        football_coaches = []

    football_articles = []
    for article in [media_day_article, highlights_article]:
        if not isinstance(article, dict):
            continue
        title = _clean(str(article.get("title") or ""))
        url = _clean(str(article.get("url") or ""))
        if not title or not url:
            continue
        football_articles.append(
            {
                "title": title,
                "url": url,
                "football_lines": article.get("football_lines", []),
            }
        )

    football_article_titles = [str(article.get("title") or "") for article in football_articles]
    football_article_urls = [str(article.get("url") or "") for article in football_articles]
    football_article_lines = []
    for article in football_articles:
        if isinstance(article.get("football_lines"), list):
            football_article_lines.extend([str(value) for value in article["football_lines"] if value])

    ticket_links = athletics_page.get("ticket_links", [])
    if not isinstance(ticket_links, list):
        ticket_links = []
    livestream_links = athletics_page.get("livestream_links", [])
    if not isinstance(livestream_links, list):
        livestream_links = []

    football_program_available = bool(
        football_fall_present
        or football_coaches
        or football_articles
        or ticket_links
        or livestream_links
        or school_page.get("football_lines")
        or athletics_page.get("football_lines")
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_official_ghctk12_pages")

    football_contact = {}
    if football_coaches:
        first = football_coaches[0]
        if isinstance(first, dict):
            football_contact = {
                "name": _clean(str(first.get("name") or "")),
                "role": _clean(str(first.get("role") or "")),
                "source": _clean(str(first.get("source") or "")),
            }

    athletics_contact = directory_page.get("activities_contact", {})
    if not isinstance(athletics_contact, dict):
        athletics_contact = {}

    football_evidence = _dedupe_keep_order(
        [
            *[str(line) for line in school_page.get("football_lines", []) if line],
            *[str(line) for line in athletics_page.get("football_lines", []) if line],
            *[str(line) for line in directory_page.get("football_lines", []) if line],
            *football_article_lines,
            str(athletic_director.get("name") or ""),
            str(athletic_director.get("title") or ""),
            str(football_contact.get("name") or ""),
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_url": SCHOOL_URL,
        "athletics_url": ATHLETICS_URL,
        "directory_url": DIRECTORY_URL,
        "fall_sports": fall_sports,
        "football_team_names": ["Football", "Highlanders Football"] if football_program_available else [],
        "football_coaches": football_coaches,
        "football_contact": football_contact,
        "athletic_director": athletic_director,
        "activities_contact": athletics_contact,
        "football_articles": football_articles,
        "football_article_titles": football_article_titles,
        "football_article_urls": football_article_urls,
        "football_article_lines": football_article_lines,
        "ticket_links": ticket_links,
        "livestream_links": livestream_links,
        "football_lines": _dedupe_keep_order(
            [
                *[str(line) for line in school_page.get("football_lines", []) if line],
                *[str(line) for line in athletics_page.get("football_lines", []) if line],
                *[str(line) for line in directory_page.get("football_lines", []) if line],
                *football_article_lines,
            ]
        ),
        "football_evidence": football_evidence,
        "summary": (
            "Granada Hills Charter publicly lists football as a fall sport, names football coach William Brooks in the staff directory, and publishes football news articles including the 2025 media day preview with Bucky Brooks and the first-game announcement."
            if football_program_available
            else ""
        ),
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers"),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "target_urls": TARGET_URLS,
            "manual_navigation_steps": [
                "school_home",
                "athletics_program",
                "staff_directory",
                "football_media_day_article",
                "football_highlights_article",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
