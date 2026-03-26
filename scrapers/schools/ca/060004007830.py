"""Deterministic football scraper for Horizon High (CA)."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060004007830"
SCHOOL_NAME = "Horizon High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_SPORTS_TEAMS_URL = "https://rhs.rjusd.org/departments/athletics/sports-teams"
SCHOOL_ATHLETICS_CALENDAR_URL = "https://rhs.rjusd.org/departments/athletics/athletics-calendar"
SCHOOL_ATHLETICS_SCORES_URL = "https://rhs.rjusd.org/departments/athletics/athletics-scores"
RIVERDALE_ATHLETICS_HOME = "https://riverdaleathletics.com"
RIVERDALE_FOOTBALL_WEBSITE = "https://riverdalefootball.weebly.com"

DISCOVERY_URLS = [
    SCHOOL_SPORTS_TEAMS_URL,
    SCHOOL_ATHLETICS_CALENDAR_URL,
    SCHOOL_ATHLETICS_SCORES_URL,
    RIVERDALE_ATHLETICS_HOME,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "football",
    "cowboys",
    "cowboy",
    "roster",
    "schedule",
    "coach",
    "head coach",
    "team",
    "score",
)


def _clean(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split()).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in values:
        item = _clean(item)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_keyword_lines(text: str, *, keywords: tuple[str, ...], limit: int = 40) -> list[str]:
    lowered = [keyword.lower() for keyword in keywords]
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if any(keyword in lower for keyword in lowered):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _collect_links(
    soup: BeautifulSoup,
    *,
    base_url: str,
    keywords: tuple[str, ...],
) -> list[dict[str, str]]:
    lowered = [keyword.lower() for keyword in keywords]
    links: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href_raw = _clean(str(anchor.get("href") or ""))
        if not href_raw:
            continue

        alt = _clean(str(anchor.get("alt") or ""))
        title = _clean(str(anchor.get("title") or ""))
        resolved = urljoin(base_url.rstrip("/") + "/", href_raw)

        blob = f"{text} {alt} {title} {resolved}".lower()
        if not any(keyword in blob for keyword in lowered):
            continue

        key = (text.lower(), resolved.lower())
        if key in seen:
            continue
        seen.add(key)

        links.append({
            "text": text,
            "href": resolved,
            "alt": alt,
            "title": title,
        })

    return links


async def _snapshot(page) -> dict[str, Any]:
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    try:
        body_text = await page.inner_text("body")
    except Exception:  # noqa: BLE001
        body_text = soup.get_text(" ", strip=True)

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": _clean(body_text),
        "links": _collect_links(soup, base_url=page.url, keywords=FOOTBALL_KEYWORDS),
    }


def _build_weebly_pages(weebly_base: str) -> list[str]:
    base = weebly_base.rstrip("/")
    return _dedupe_keep_order(
        [
            f"{base}/",
            f"{base}/roster.html",
            f"{base}/coaches.html",
            f"{base}/schedule.html",
            f"{base}/team-information.html",
            f"{base}/contact.html",
        ]
    )


async def scrape_school() -> dict[str, Any]:
    """Scrape Horizon High football-related public pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(DISCOVERY_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    school_snapshots: list[dict[str, Any]] = []
    football_snapshots: list[dict[str, Any]] = []

    discovered_facebook_url: str | None = None
    discovered_team_url: str | None = None

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in DISCOVERY_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_200)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"team_discovery_failed:{url}:{type(exc).__name__}")
                    continue

                snapshot = await _snapshot(page)
                school_snapshots.append(snapshot)
                source_pages.append(snapshot["url"])

                for link in snapshot["links"]:
                    href = _clean(link.get("href"))
                    text = _clean(link.get("text")).lower()
                    alt = _clean(link.get("alt")).lower()
                    blob = f"{text} {alt} {href}".lower()

                    if "riverdalefootball.weebly.com" in href and "football" in blob:
                        discovered_facebook_url = href

                    if "/main/team/" in href and "football" in blob:
                        discovered_team_url = href
        finally:
            await context.close()
            await browser.close()

    if discovered_facebook_url is None:
        discovered_facebook_url = RIVERDALE_FOOTBALL_WEBSITE

    football_targets: deque[str] = deque()
    football_targets.extend(_build_weebly_pages(discovered_facebook_url))
    if discovered_team_url:
        football_targets.append(discovered_team_url)
    football_targets.append(RIVERDALE_ATHLETICS_HOME)

    visited_pages: set[str] = set()

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            while football_targets:
                url = football_targets.popleft()
                if not url or url in visited_pages:
                    continue

                try:
                    assert_not_blocklisted([url], profile=PROXY_PROFILE)
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_200)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"football_navigation_failed:{url}:{type(exc).__name__}")
                    continue

                visited_pages.add(url)
                snapshot = await _snapshot(page)
                football_snapshots.append(snapshot)
                source_pages.append(snapshot["url"])

                if "riverdaleathletics.com/main/team/" in snapshot["url"]:
                    for link in snapshot["links"]:
                        href = _clean(link.get("href"))
                        low = href.lower()
                        if (
                            "/main/teamschedule/" in low
                            or "/main/teamroster/" in low
                            or "/main/teamstaff/" in low
                        ) and href not in visited_pages:
                            football_targets.append(href)
        finally:
            await context.close()
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_keyword_lines = _extract_keyword_lines(
        "\n".join(snapshot.get("body_text", "") for snapshot in school_snapshots),
        keywords=FOOTBALL_KEYWORDS,
        limit=80,
    )
    football_keyword_lines = _extract_keyword_lines(
        "\n".join(snapshot.get("body_text", "") for snapshot in football_snapshots),
        keywords=FOOTBALL_KEYWORDS,
        limit=120,
    )

    all_links: list[str] = []
    football_links: list[str] = []
    football_schedule_links: list[str] = []
    football_coach_links: list[str] = []
    football_roster_links: list[str] = []
    weebly_download_links: list[str] = []

    for snapshot in [*school_snapshots, *football_snapshots]:
        for link in snapshot.get("links", []):
            text = link.get("text", "")
            href = link.get("href", "")
            combined = f"{text} {href}".lower()
            if "football" in combined:
                football_links.append(f"{text}|{href}")
            if "schedule" in combined:
                football_schedule_links.append(f"{text}|{href}")
            if "coach" in combined:
                football_coach_links.append(f"{text}|{href}")
            if "roster" in combined:
                football_roster_links.append(f"{text}|{href}")
            if "riverdalefootball.weebly.com" in combined and any(
                href.lower().endswith(ext)
                for ext in [".xls", ".xlsx", ".pdf", ".doc", ".docx"]
            ):
                weebly_download_links.append(href)

            all_links.append(f"{text}|{href}")

    football_program_available = bool(
        school_keyword_lines
        or football_keyword_lines
        or visited_pages
        or football_links
    )

    if not football_program_available:
        errors.append("blocked:no_public_football_content_detected")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_keyword_lines": school_keyword_lines,
        "football_keyword_lines": football_keyword_lines,
        "school_football_links": _dedupe_keep_order(all_links),
        "football_links": _dedupe_keep_order(football_links),
        "football_schedule_links": _dedupe_keep_order(football_schedule_links),
        "football_coach_links": _dedupe_keep_order(football_coach_links),
        "football_roster_links": _dedupe_keep_order(football_roster_links),
        "weebly_download_links": _dedupe_keep_order(weebly_download_links),
        "football_pages_visited": sorted(visited_pages),
        "discovered_facebook_url": discovered_facebook_url,
        "discovered_team_url": discovered_team_url or "",
        "source_pages_count": len(source_pages),
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
            "school_sports_pages": DISCOVERY_URLS,
            "football_program_available": football_program_available,
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers"),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
        },
        "errors": errors,
    }
