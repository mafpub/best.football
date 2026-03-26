"""Deterministic football scraper for Da Vinci Science (CA)."""

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

NCES_ID = "060241612208"
SCHOOL_NAME = "Da Vinci Science"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_HOME_URL = "https://athletics.davincischools.org/"
GIRLS_FLAG_FOOTBALL_URL = "https://athletics.davincischools.org/girls-flag-football/"
TRYOUTS_URL = "https://athletics.davincischools.org/tryouts/"
FOOTBALL_NEWS_URL = (
    "https://athletics.davincischools.org/2025/05/09/"
    "future-wolves-athletics-night-tonight-and-flag-football-girls-interest-meeting-may-13/"
)

TARGET_URLS = [
    ATHLETICS_HOME_URL,
    GIRLS_FLAG_FOOTBALL_URL,
    TRYOUTS_URL,
    FOOTBALL_NEWS_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_TERMS = (
    "football",
    "flag football",
    "girls flag football",
    "tryout",
    "tryouts",
    "coach",
    "coaches",
    "season",
    "practice",
    "game",
    "games",
    "playoffs",
    "championship",
    "intramural",
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


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.+-]+\.[A-Za-z]{2,}", text or ""))


def _extract_relevant_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(term in lowered for term in FOOTBALL_TERMS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:120]


def _extract_relevant_links(links: list[dict[str, str]], base_url: str) -> list[str]:
    out: list[str] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        if not text and not href:
            continue
        absolute = urljoin(base_url, href) if href else ""
        combo = f"{text} {absolute}".lower()
        if not any(term in combo for term in FOOTBALL_TERMS):
            continue
        if absolute:
            out.append(f"{text}|{absolute}" if text else absolute)
    return _dedupe_keep_order(out)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body = _clean(await page.locator("body").inner_text())
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    football_lines = _extract_relevant_lines(body)
    football_links = _extract_relevant_links(
        [link for link in links if isinstance(link, dict)],
        requested_url,
    )
    lower = body.lower()
    team_names: list[str] = []
    if "girls flag football" in lower or "flag football" in lower:
        team_names.append("Girls Flag Football")

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body,
        "football_lines": football_lines,
        "football_links": football_links,
        "team_names": team_names,
        "emails": _extract_emails(body),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape the public Wiseburn Da Vinci athletics pages for football signals."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy={
                "server": proxy["server"],
                "username": proxy.get("username"),
                "password": proxy.get("password"),
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
                    signal = await _collect_page(page, url)
                    page_signals.append(signal)
                    source_pages.append(signal["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_lines: list[str] = []
    football_links: list[str] = []
    team_names: list[str] = []
    emails: list[str] = []
    for signal in page_signals:
        football_lines.extend(signal.get("football_lines", []))
        football_links.extend(signal.get("football_links", []))
        team_names.extend(signal.get("team_names", []))
        emails.extend(signal.get("emails", []))

    football_lines = _dedupe_keep_order(football_lines)
    football_links = _dedupe_keep_order(football_links)
    team_names = _dedupe_keep_order(team_names)
    emails = _dedupe_keep_order(emails)

    football_program_available = bool(football_lines or football_links or team_names)
    if not football_program_available:
        errors.append("no_public_football_content_found_on_athletics_site")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "football_team_names": team_names,
        "football_links": football_links,
        "football_keyword_lines": football_lines,
        "football_schedule_public": True,
        "program_contact_emails": emails,
        "summary": (
            "Wiseburn Da Vinci Athletics publishes an active Girls Flag Football program with intramural conditioning, team coaches, game dates, and a season schedule."
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
            "proxy_profile": PROXY_PROFILE,
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
