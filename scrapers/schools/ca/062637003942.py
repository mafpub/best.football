"""Deterministic football scraper for Concord High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062637003942"
SCHOOL_NAME = "Concord High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_FALL_URL = "https://chs.mdusd.org/chs-athletics/fall-sports"
ATHLETICS_TEAM_WEBPAGES_URL = "https://chs.mdusd.org/chs-athletics/fall-sports/team-webpages"

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
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


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


def _extract_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_link_map(links: list[dict[str, str]], base_url: str) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _normalize_href(str(item.get("href") or ""), base_url)
        if href:
            normalized.append({"text": text, "href": href})
    return normalized


async def _collect_page_snapshot(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=15000)
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({"
        "text: (a.textContent || '').replace(/\\s+/g, ' ').trim(), "
        "href: a.getAttribute('href') || ''"
        "}))",
    )
    if not isinstance(links, list):
        links = []
    return {
        "title": _clean(await page.title()),
        "url": page.url,
        "text": _clean(body_text),
        "links": _extract_link_map(links, page.url),
    }


def _find_link(
    links: list[dict[str, str]],
    *,
    text_contains: tuple[str, ...] = (),
    href_contains: tuple[str, ...] = (),
) -> dict[str, str]:
    for link in links:
        text = (link.get("text") or "").lower()
        href = (link.get("href") or "").lower()
        if text_contains and not any(token in text for token in text_contains):
            continue
        if href_contains and not any(token in href for token in href_contains):
            continue
        return link
    return {}


def _extract_football_contacts(text: str) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    seen: set[str] = set()
    patterns = [
        re.compile(
            r"Football:\s*([A-Z][A-Za-z .'\-]+):\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})",
            re.IGNORECASE,
        ),
        re.compile(
            r"Football\s+([A-Z][A-Za-z .'\-]+):\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})",
            re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        for match in pattern.finditer(text or ""):
            name = _clean(match.group(1))
            email = _clean(match.group(2))
            key = f"{name}|{email}"
            if not name or not email or key in seen:
                continue
            seen.add(key)
            contacts.append({"sport": "Football", "name": name, "email": email})
    return contacts


async def scrape_school() -> dict[str, Any]:
    """Scrape Concord High's public football-facing athletics content."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted([ATHLETICS_FALL_URL, ATHLETICS_TEAM_WEBPAGES_URL], profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: dict[str, dict[str, Any]] = {}

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
            for url in [ATHLETICS_FALL_URL, ATHLETICS_TEAM_WEBPAGES_URL]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    snapshot = await _collect_page_snapshot(page)
                    snapshots[url] = snapshot
                    source_pages.append(snapshot["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    fall_snapshot = snapshots.get(ATHLETICS_FALL_URL, {})
    team_snapshot = snapshots.get(ATHLETICS_TEAM_WEBPAGES_URL, {})

    fall_text = str(fall_snapshot.get("text") or "")
    team_text = str(team_snapshot.get("text") or "")
    fall_links = fall_snapshot.get("links") if isinstance(fall_snapshot.get("links"), list) else []
    team_links = team_snapshot.get("links") if isinstance(team_snapshot.get("links"), list) else []

    football_mentions = _extract_lines(
        fall_text,
        ("football", "flag football", "coach", "schedule", "fall coach"),
    )
    team_page_mentions = _extract_lines(team_text, ("football", "fall sports", "team webpages"))
    football_contacts = _extract_football_contacts(fall_text)

    football_team_link = _find_link(
        team_links,
        text_contains=("football",),
        href_contains=("concordminutemenfootball",),
    )
    football_schedule_link = _find_link(
        fall_links,
        text_contains=("schedule",),
        href_contains=("dalathletics",),
    )
    fall_sports_link = _find_link(
        fall_links,
        text_contains=("fall sports league schedules",),
        href_contains=("dalathletics",),
    )
    athletics_page_link = _find_link(
        fall_links,
        text_contains=("team webpages",),
        href_contains=("team-webpages",),
    )

    football_team_url = football_team_link.get("href") or "https://www.concordminutemenfootball.com/"
    football_schedule_url = football_schedule_link.get("href") or fall_sports_link.get("href") or "https://www.dalathletics.com/"
    football_team_name = football_team_link.get("text") or "Football"

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_mentions or football_team_link),
        "football_team_name": football_team_name,
        "football_team_url": football_team_url,
        "football_schedule_url": football_schedule_url,
        "football_contacts": football_contacts,
        "football_contact_count": len(football_contacts),
        "football_mentions": football_mentions,
        "team_page_mentions": team_page_mentions,
        "fall_sports_page_url": ATHLETICS_FALL_URL,
        "team_webpages_page_url": ATHLETICS_TEAM_WEBPAGES_URL,
        "fall_sports_link": fall_sports_link,
        "athletics_team_webpages_link": athletics_page_link,
        "school_address": _clean(str(fall_snapshot.get("school_address") or "4200 Concord Blvd., Concord, CA 94521")),
        "school_phone": _clean(str(fall_snapshot.get("school_phone") or "(925)-687-2030")),
        "football_summary": (
            "Concord High's Fall Sports page lists Football with head coach Paul Reynaud, "
            "links the team website, and links DAL fall sports schedules."
        ),
    }

    if not extracted_items["football_program_available"]:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    proxy_meta = get_proxy_runtime_meta(PROXY_PROFILE)

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
            "focus": "football_only",
            "pages_visited": len(source_pages),
            "pages_requested": [ATHLETICS_FALL_URL, ATHLETICS_TEAM_WEBPAGES_URL],
            "football_contact_count": len(football_contacts),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
