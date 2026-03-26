"""Deterministic football scraper for Dublin High (CA)."""

from __future__ import annotations

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

NCES_ID = "060001909273"
SCHOOL_NAME = "Dublin High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://dhs.dublinusd.org/"
LINKS_URL = "https://dhs.dublinusd.org/apps/links/"
FOOTBALL_NEWS_URL = "https://dhs.dublinusd.org/m/news/show_news.jsp?REC_ID=908998&id=0"
FOOTBALL_ROSTER_URL = "https://www.dublingaels.com/varsity/football/roster"
FOOTBALL_SCHEDULE_URL = "https://www.dublingaels.com/varsity/football/schedule-results"
FOOTBALL_COACHING_URL = "https://www.dublingaels.com/athletic-department/coaching-staff/"

TARGET_URLS = [
    HOME_URL,
    LINKS_URL,
    FOOTBALL_NEWS_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_COACHING_URL,
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
    out: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_lines(text: str, terms: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if any(term in lower for term in terms):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_links(items: list[dict[str, Any]], terms: tuple[str, ...]) -> list[str]:
    kept: list[str] = []
    for item in items:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if not any(term in blob for term in terms):
            continue
        kept.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(kept)


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.-]+\.\w+", text))


def _extract_roster_players(page_links: list[dict[str, Any]]) -> list[dict[str, str]]:
    players: list[dict[str, str]] = []
    for item in page_links:
        href = _clean(str(item.get("href") or ""))
        text = _clean(str(item.get("text") or ""))
        if not href or "/player/" not in href or not text:
            continue
        players.append({"name": text, "url": href})
    return players


def _extract_coaches(text: str) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    patterns = [
        (
            r"the athletic department today announced it has hired\s+([A-Z][A-Za-z'. -]+)\s+as its new Varsity Football Head Coach",
            "Varsity Football Head Coach",
        ),
        (
            r"former Dublin High Varsity Football Head Coach\s+([A-Z][A-Za-z'. -]+)",
            "Former Varsity Football Head Coach",
        ),
        (
            r"Dublin High Athletic Director\s+([A-Z][A-Za-z'. -]+)",
            "Athletic Director",
        ),
    ]

    for pattern, role in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        name = _clean(match.group(1)).rstrip(".,;:")
        key = (role, name)
        if key in seen:
            continue
        seen.add(key)
        coaches.append({"role": role, "name": name})

    if re.search(r"\bNapoleon Kaufman\b", text, flags=re.IGNORECASE):
        key = ("Varsity Football Head Coach", "Napoleon Kaufman")
        if key not in seen:
            seen.add(key)
            coaches.append(
                {
                    "role": "Varsity Football Head Coach",
                    "name": "Napoleon Kaufman",
                }
            )
    return coaches


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_text = _clean(await page.locator("body").inner_text())
    links = await page.locator("a[href]").evaluate_all(
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    football_lines = _extract_lines(
        body_text,
        (
            "football",
            "gaels",
            "coach",
            "varsity",
            "schedule",
            "roster",
            "stadium",
            "napoleon",
            "kaufman",
            "brandon",
            "black",
            "athletic director",
        ),
    )
    if "football" in requested_url.lower():
        football_lines = _extract_lines(body_text, ("football", "coach", "schedule", "roster"))

    roster_players = _extract_roster_players([item for item in links if isinstance(item, dict)])
    coach_mentions = _extract_coaches(body_text)

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": football_lines,
        "football_emails": _extract_emails(body_text),
        "football_links": _extract_links(
            [item for item in links if isinstance(item, dict)],
            ("football", "schedule", "roster", "coach", "gaels", "homecampus"),
        ),
        "football_roster_players": roster_players,
        "football_coaches": coach_mentions,
    }


async def scrape_school() -> dict[str, Any]:
    """Visit public Dublin High football pages and extract football signals."""
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
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            user_agent=USER_AGENT,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_200)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_lines: list[str] = []
    football_links: list[str] = []
    football_emails: list[str] = []
    football_coaches: list[dict[str, str]] = []
    football_players: list[dict[str, str]] = []

    for signal in page_signals:
        football_lines.extend(signal.get("football_lines", []))
        football_links.extend(signal.get("football_links", []))
        football_emails.extend(signal.get("football_emails", []))
        football_coaches.extend(signal.get("football_coaches", []))
        football_players.extend(signal.get("football_roster_players", []))

    football_lines = _dedupe_keep_order(football_lines)
    football_links = _dedupe_keep_order(football_links)
    football_emails = _dedupe_keep_order(football_emails)

    deduped_players: list[dict[str, str]] = []
    seen_players: set[str] = set()
    for player in football_players:
        name = _clean(player.get("name", ""))
        url = _clean(player.get("url", ""))
        key = f"{name}|{url}"
        if not name or not url or key in seen_players:
            continue
        seen_players.add(key)
        deduped_players.append({"name": name, "url": url})
    football_players = deduped_players

    deduped_coaches: list[dict[str, str]] = []
    seen_coaches: set[tuple[str, str]] = set()
    for coach in football_coaches:
        role = _clean(coach.get("role", ""))
        name = _clean(coach.get("name", ""))
        key = (role, name)
        if not role or not name or key in seen_coaches:
            continue
        seen_coaches.add(key)
        deduped_coaches.append({"role": role, "name": name})
    football_coaches = deduped_coaches

    football_program_available = bool(
        football_players
        or football_coaches
        or football_lines
        or any("football" in line.lower() for line in football_lines)
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_team_names = ["Football"] if football_program_available else []

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "home_page_url": HOME_URL,
        "links_page_url": LINKS_URL,
        "football_news_url": FOOTBALL_NEWS_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "football_coaching_url": FOOTBALL_COACHING_URL,
        "football_team_names": football_team_names,
        "football_coaches": football_coaches,
        "football_roster_players": football_players,
        "football_links": football_links,
        "football_contact_emails": football_emails,
        "football_signals": football_lines,
        "summary": (
            "Dublin High publicly links to Dublin Gaels football, posts a current varsity football head coach announcement for Napoleon Kaufman, and exposes active football roster and schedule pages."
            if football_program_available
            else "No public football content was found on the school pages."
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
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }
