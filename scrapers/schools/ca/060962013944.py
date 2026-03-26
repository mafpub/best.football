"""Deterministic football scraper for Compton Early College High (CA)."""

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

NCES_ID = "060962013944"
SCHOOL_NAME = "Compton Early College High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_URL = "https://earlycollegehs.compton.k12.ca.us/athletics"
MAXPREPS_SCHOOL_URL = "https://www.maxpreps.com/ca/compton/compton-early-college-rising-phoenix/"
MAXPREPS_FOOTBALL_URL = f"{MAXPREPS_SCHOOL_URL}football/"

TARGET_URLS = [ATHLETICS_URL, MAXPREPS_SCHOOL_URL, MAXPREPS_FOOTBALL_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_links(page_links: list[dict[str, Any]]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for item in page_links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if href:
            links.append({"text": text, "href": href})
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for link in links:
        key = (link["text"], link["href"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    return deduped


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], limit: int = 20) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _section_lines(text: str, start: str, end: str | None = None) -> list[str]:
    pattern = rf"{re.escape(start)}\s+(.*?){re.escape(end)}" if end else rf"{re.escape(start)}\s+(.*)$"
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    block = match.group(1)
    lines = [
        _clean(raw_line)
        for raw_line in block.splitlines()
        if _clean(raw_line)
        and not _clean(raw_line).lower().startswith(("2023-2024 sports", "participation eligibility"))
    ]
    return _dedupe_keep_order(lines)


def _parse_maxpreps_record(text: str) -> dict[str, str]:
    match = re.search(
        r"Overall\s+([0-9-]+)\s+League\s+([0-9-]+(?:\s+\([^)]+\))?)\s+NAT Rank\s+([0-9]+)\s+CA Rank\s+([0-9]+)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return {}
    return {
        "overall_record": _clean(match.group(1)),
        "league_record": _clean(match.group(2)),
        "national_rank": _clean(match.group(3)),
        "california_rank": _clean(match.group(4)),
    }


def _parse_schedule_glance(text: str) -> list[str]:
    matches = re.findall(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s*\d{1,2}/\d{1,2}[^\n]*",
        text,
        flags=re.IGNORECASE,
    )
    cleaned = [_clean(item) for item in matches if _clean(item)]
    return _dedupe_keep_order(cleaned)


def _parse_recent_results(text: str) -> list[str]:
    patterns = [
        r"On (?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday), [^.]+?Football team [^.]+?\.",
        r"Football Recap: [^\n]+",
        r"Football Game Preview: [^\n]+",
        r"Congratulations to [^\n]+player of the game",
        r"Stats have been entered for [^\n]+",
    ]
    matches: list[str] = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    return _dedupe_keep_order([_clean(item) for item in matches if _clean(item)])


async def _capture_page(page, url: str) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    links = await page.locator("a[href]").evaluate_all(
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "links": _extract_links(links),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football signals from the school athletics site and MaxPreps."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    signals: dict[str, dict[str, Any]] = {}

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
                    await page.wait_for_timeout(2500 if "maxpreps" in url else 1500)
                    source_pages.append(page.url)
                    signals[url] = await _capture_page(page, url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"page_fetch_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_text = str(signals.get(ATHLETICS_URL, {}).get("body_text") or "")
    athletics_links = [
        link
        for link in signals.get(ATHLETICS_URL, {}).get("links", [])
        if isinstance(link, dict)
    ]
    maxpreps_school_text = str(signals.get(MAXPREPS_SCHOOL_URL, {}).get("body_text") or "")
    maxpreps_football_text = str(signals.get(MAXPREPS_FOOTBALL_URL, {}).get("body_text") or "")
    maxpreps_football_links = [
        link
        for link in signals.get(MAXPREPS_FOOTBALL_URL, {}).get("links", [])
        if isinstance(link, dict)
    ]

    fall_sports = _section_lines(athletics_text, "Fall", "Winter")
    winter_sports = _section_lines(athletics_text, "Winter", "Spring")
    spring_sports = _section_lines(athletics_text, "Spring", "RETURN TO PLAY SAFETY PLAN")

    athletics_summary_lines = _extract_keyword_lines(
        athletics_text,
        (
            "athletics",
            "football",
            "flag football",
            "maxpreps",
            "cif",
            "return to play",
            "jose perez",
        ),
        limit=25,
    )
    school_links = _dedupe_keep_order(
        [
            f"{link.get('text', '')}|{link.get('href', '')}"
            for link in athletics_links
            if any(
                keyword in f"{link.get('text', '')} {link.get('href', '')}".lower()
                for keyword in ("maxpreps", "football", "return to play", "cif", "athletics")
            )
        ]
    )

    maxpreps_school_lines = _extract_keyword_lines(
        maxpreps_school_text,
        ("football", "athletic director", "mascot", "rising phoenix", "school sports"),
        limit=20,
    )
    maxpreps_record = _parse_maxpreps_record(maxpreps_football_text)
    schedule_glance = _parse_schedule_glance(maxpreps_football_text)
    recent_football_updates = _parse_recent_results(maxpreps_football_text)

    football_links = _dedupe_keep_order(
        [
            f"{link.get('text', '')}|{link.get('href', '')}"
            for link in maxpreps_football_links
            if any(
                keyword in f"{link.get('text', '')} {link.get('href', '')}".lower()
                for keyword in (
                    "/football/",
                    "schedule",
                    "roster",
                    "stats",
                    "rankings",
                    "game",
                    "box score",
                    "player",
                )
            )
        ]
    )

    football_team_name = ""
    team_match = re.search(
        r"(Compton Early College Rising Phoenix|Compton Early College Football)",
        maxpreps_football_text,
        flags=re.IGNORECASE,
    )
    if team_match:
        football_team_name = _clean(team_match.group(1))

    athletic_director = ""
    if "Jose Perez" in athletics_text or "Jose Perez" in maxpreps_football_text:
        athletic_director = "Jose Perez"

    football_program_available = any(
        "football" in text.lower()
        for text in (athletics_text, maxpreps_football_text, maxpreps_school_text)
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_athletics_url": ATHLETICS_URL,
        "school_athletics_summary_lines": athletics_summary_lines[:15],
        "school_athletics_links": school_links[:15],
        "fall_sports": fall_sports,
        "winter_sports": winter_sports,
        "spring_sports": spring_sports,
        "athletics_director": {
            "name": athletic_director,
            "source": ATHLETICS_URL,
        },
        "maxpreps_school_url": MAXPREPS_SCHOOL_URL,
        "maxpreps_football_url": MAXPREPS_FOOTBALL_URL,
        "maxpreps_school_summary_lines": maxpreps_school_lines[:15],
        "football_team_name": football_team_name,
        "football_record": maxpreps_record,
        "football_schedule_glance": schedule_glance[:8],
        "football_recent_updates": recent_football_updates[:10],
        "football_links": football_links[:25],
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
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
