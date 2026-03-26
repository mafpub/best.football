"""Deterministic football scraper for Golden West High (CA)."""

from __future__ import annotations

import asyncio
import json
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

NCES_ID = "064116006792"
SCHOOL_NAME = "Golden West High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_URL = "https://www.vusd.org/"
ATHLETICS_HOME_URL = "https://www.trailblazerathletics.com/"
TEAM_URL = "https://www.maxpreps.com/ca/visalia/golden-west-trailblazers/football/"
STAFF_URL = "https://www.maxpreps.com/ca/visalia/golden-west-trailblazers/football/staff/"
SCHEDULE_URL = "https://www.maxpreps.com/ca/visalia/golden-west-trailblazers/football/schedule/"
ROSTER_URL = "https://www.maxpreps.com/ca/visalia/golden-west-trailblazers/football/roster/"

TARGET_URLS = [
    SCHOOL_URL,
    ATHLETICS_HOME_URL,
    TEAM_URL,
    STAFF_URL,
    SCHEDULE_URL,
    ROSTER_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_lines(text: str) -> list[str]:
    return [_clean(line) for line in text.splitlines() if _clean(line)]


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 40) -> list[str]:
    lines: list[str] = []
    for line in _extract_lines(text):
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_score_lines(text: str) -> list[str]:
    patterns = [
        r"On [A-Za-z]+, [A-Za-z]+ \d{1,2}, \d{4}, the Golden West Varsity Boys Football team (?:won|lost|tied).*",
        r"Stats have been entered for the Golden West vs\. .+",
    ]
    lines: list[str] = []
    for line in _extract_lines(text):
        if any(re.match(pattern, line) for pattern in patterns):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_article_lines(text: str) -> list[str]:
    lines: list[str] = []
    for line in _extract_lines(text):
        lowered = line.lower()
        if "football recap:" in lowered or "football game preview:" in lowered:
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_player_lines(text: str) -> list[str]:
    lines: list[str] = []
    for line in _extract_lines(text):
        if "players of the game" in line.lower():
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_navigation_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    wanted = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in ("football", "schedule", "roster", "staff", "stats", "rankings", "standings", "news")):
            wanted.append({"text": text, "href": href})
    return wanted


def _extract_address(text: str) -> str:
    match = re.search(r"1717\s+N\s+Mcauliff\s+Rd\s+Visalia,\s*CA\s+93292", text, re.IGNORECASE)
    return _clean(match.group(0)) if match else ""


async def _collect_snapshot(page) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=20000)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || e.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    normalized_links: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        normalized_links.append(
            {
                "text": _clean(str(item.get("text") or "")),
                "href": href,
            }
        )

    return {
        "title": _clean(await page.title()),
        "url": page.url,
        "body_text": body_text,
        "links": normalized_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football signals for Golden West High from official and MaxPreps pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: list[dict[str, Any]] = []
    visit_log: list[dict[str, str]] = []

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
            for url, label in [
                (SCHOOL_URL, "school_home"),
                (ATHLETICS_HOME_URL, "athletics_home"),
                (TEAM_URL, "team"),
                (STAFF_URL, "staff"),
                (SCHEDULE_URL, "schedule"),
                (ROSTER_URL, "roster"),
            ]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    snapshots.append(await _collect_snapshot(page))
                    source_pages.append(page.url)
                    visit_log.append({"requested_url": url, "final_url": page.url, "status": "ok", "label": label})
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
                    visit_log.append({"requested_url": url, "final_url": "", "status": type(exc).__name__, "label": label})
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_snapshot = snapshots[0] if snapshots else {}
    athletics_snapshot = snapshots[1] if len(snapshots) > 1 else {}
    team_snapshot = snapshots[2] if len(snapshots) > 2 else {}
    staff_snapshot = snapshots[3] if len(snapshots) > 3 else {}
    schedule_snapshot = snapshots[4] if len(snapshots) > 4 else {}
    roster_snapshot = snapshots[5] if len(snapshots) > 5 else {}

    team_text = str(team_snapshot.get("body_text") or "")
    staff_text = str(staff_snapshot.get("body_text") or "")
    schedule_text = str(schedule_snapshot.get("body_text") or "")
    roster_text = str(roster_snapshot.get("body_text") or "")
    combined_text = "\n".join([team_text, staff_text, schedule_text, roster_text])

    extracted_links = []
    for snapshot in (home_snapshot, athletics_snapshot, team_snapshot, staff_snapshot, schedule_snapshot, roster_snapshot):
        links = snapshot.get("links")
        if isinstance(links, list):
            extracted_links.extend([link for link in links if isinstance(link, dict)])

    nav_links = _extract_navigation_links(extracted_links)
    team_title = _clean(str(team_snapshot.get("title") or "")) or "Golden West High Football"

    extracted_items: dict[str, Any] = {
        "team_title": team_title,
        "school_address": _extract_address(combined_text),
        "football_page_urls": {
            "school_home": SCHOOL_URL,
            "athletics_home": ATHLETICS_HOME_URL,
            "team": TEAM_URL,
            "staff": STAFF_URL,
            "schedule": SCHEDULE_URL,
            "roster": ROSTER_URL,
        },
        "navigation_links": _dedupe_keep_order([json.dumps(link, sort_keys=True) for link in nav_links]),
        "football_keyword_lines": _extract_keyword_lines(
            combined_text,
            (
                "football",
                "varsity",
                "schedule",
                "roster",
                "coach",
                "players of the game",
                "game results",
                "game preview",
                "team reports",
            ),
        ),
        "football_article_lines": _extract_article_lines(combined_text),
        "football_score_lines": _extract_score_lines(combined_text),
        "football_player_lines": _extract_player_lines(combined_text),
        "page_titles": _dedupe_keep_order(
            [
                _clean(str(snapshot.get("title") or ""))
                for snapshot in snapshots
                if isinstance(snapshot, dict)
            ]
        ),
        "raw_visit_log": visit_log,
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
            "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "pages_checked": len(visit_log),
            "navigation_steps": [
                "school_home",
                "athletics_home",
                "team",
                "staff",
                "schedule",
                "roster",
            ],
        },
        "errors": errors,
    }


def main() -> None:
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
