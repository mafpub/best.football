"""Deterministic football scraper for Le Grand High (CA)."""

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

NCES_ID = "062127002548"
SCHOOL_NAME = "Le Grand High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.lghs.k12.ca.us/"
ATHLETICS_HOME_URL = "https://www.lghs.k12.ca.us/apps/pages/athleticdepartmenthome"
ATHLETIC_TEAMS_URL = "https://www.lghs.k12.ca.us/apps/pages/teamsandschedules"

TARGET_URLS = [HOME_URL, ATHLETICS_HOME_URL, ATHLETIC_TEAMS_URL]

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
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _dedupe_links(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    output: list[dict[str, str]] = []
    for value in values:
        text = _clean(str(value.get("text") or ""))
        href = _clean(str(value.get("href") or ""))
        if not href:
            continue
        key = f"{text}|{href}"
        if key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


def _keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 40) -> list[str]:
    matches: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _extract_football_coach_names(lines: list[str]) -> list[str]:
    names: list[str] = []
    patterns = [
        re.compile(r"Varsity Football Coach\s+([A-Z][A-Za-z .'\-]+)", re.IGNORECASE),
        re.compile(r"J\.?V\.? Football Coach\s+([A-Z][A-Za-z .'\-]+)", re.IGNORECASE),
    ]
    for line in lines:
        for pattern in patterns:
            match = pattern.search(line)
            if match:
                names.append(_clean(match.group(1)))
    return _dedupe_keep_order(names)


def _football_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    matched: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        combo = f"{text} {href}".lower()
        if "football" in combo:
            matched.append({"text": text, "href": href})
    return _dedupe_links(matched)


async def _snapshot_page(page) -> dict[str, Any]:
    body = await page.locator("body").inner_text(timeout=20000)
    links = await page.eval_on_selector_all(
        "a[href]",
        "elements => elements.map(a => ({"
        "text: (a.textContent || '').replace(/\\s+/g, ' ').trim(), "
        "href: a.href || ''"
        "}))",
    )
    if not isinstance(links, list):
        links = []
    normalized_links = _dedupe_links(
        [
            {
                "text": str(item.get("text") or ""),
                "href": str(item.get("href") or ""),
            }
            for item in links
            if isinstance(item, dict)
        ]
    )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body": body,
        "links": normalized_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Le Grand High football signals from public school-hosted pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    errors: list[str] = []
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
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    snapshots[url] = await _snapshot_page(page)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_snapshot = snapshots.get(HOME_URL, {})
    athletics_snapshot = snapshots.get(ATHLETICS_HOME_URL, {})
    teams_snapshot = snapshots.get(ATHLETIC_TEAMS_URL, {})

    home_links = home_snapshot.get("links") if isinstance(home_snapshot.get("links"), list) else []
    athletics_links = (
        athletics_snapshot.get("links") if isinstance(athletics_snapshot.get("links"), list) else []
    )
    teams_links = teams_snapshot.get("links") if isinstance(teams_snapshot.get("links"), list) else []

    football_relevant_lines = _keyword_lines(
        str(teams_snapshot.get("body") or ""),
        (
            "varsity football",
            "j.v. football",
            "jv football",
            "football coach",
            "maxpreps",
            "teams & schedules",
        ),
    )
    football_coach_names = _extract_football_coach_names(football_relevant_lines)

    football_team_links = _football_links(teams_links)
    football_schedule_links = [
        link
        for link in football_team_links
        if "maxpreps.com" in str(link.get("href") or "").lower()
    ]

    athletics_page_links = _dedupe_links(
        [
            link
            for link in home_links + athletics_links
            if "athletic" in str(link.get("text") or "").lower()
            or "athletic" in str(link.get("href") or "").lower()
            or "sport" in str(link.get("text") or "").lower()
        ]
    )

    football_program_available = bool(football_team_links or football_coach_names or football_relevant_lines)
    if not football_program_available:
        errors.append("no_football_signals_found_on_school_athletics_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_links": football_team_links,
        "football_schedule_links": football_schedule_links,
        "football_coach_names": football_coach_names,
        "football_keyword_lines": football_relevant_lines,
        "athletics_page_links": athletics_page_links,
        "athletics_main_page": athletics_snapshot.get("url") or ATHLETICS_HOME_URL,
        "athletics_teams_page": teams_snapshot.get("url") or ATHLETIC_TEAMS_URL,
        "football_summary": (
            "Le Grand High athletics teams page lists Varsity and J.V. Football, "
            "coach names, and MaxPreps football team links."
        ),
    }

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
            "pages_requested": TARGET_URLS,
            "pages_visited": len(source_pages),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
