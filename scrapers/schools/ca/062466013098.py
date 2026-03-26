"""Deterministic football scraper for El Capitan High (CA)."""

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

NCES_ID = "062466013098"
SCHOOL_NAME = "El Capitan High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://echs.muhsd.org/"
ATHLETICS_URL = "https://echs.muhsd.org/11582_1"
HEAD_COACHES_URL = "https://echs.muhsd.org/41600_2"
TEAMS_SCHEDULES_URL = "https://echs.muhsd.org/41601_2"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, HEAD_COACHES_URL, TEAMS_SCHEDULES_URL]

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


def _extract_keyword_lines(text: str, keywords: tuple[str, ...], limit: int = 40) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if any(keyword in lower for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _normalize_href(href: str, base_url: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    return urljoin(base_url, raw)


def _extract_head_coach(text: str) -> dict[str, str]:
    """Pull the football head coach from the head-coaches page body text."""
    body = _clean(text)
    pattern = re.compile(
        r"\bFootball\b\s+(?P<name>[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,2})\s+"
        r"(?=Volleyball|Girls Water Polo|Boys Water Polo|Cross Country|Girls Golf|Girls Tennis|"
        r"WINTER SPORTS|SPRING SPORTS|Baseball|Softball|Boys Swimming|Girls Swimming|Boys Tennis|"
        r"Boys and Girls Track|Boys Volleyball|Comp Cheer|$)",
        re.IGNORECASE,
    )
    match = pattern.search(body)
    if match:
        return {
            "sport": "Football",
            "name": _clean(match.group("name")),
            "source_text": "Football " + _clean(match.group("name")),
        }
    return {}


def _dedupe_link_dicts(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for item in values:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        key = (text, href)
        if not href or key in seen:
            continue
        seen.add(key)
        out.append({"text": text, "href": href})
    return out


async def _collect_page_snapshot(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=15000)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    normalized_links: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _normalize_href(str(item.get("href") or ""), page.url)
        if href:
            normalized_links.append({"text": text, "href": href})

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": _clean(body_text),
        "football_lines": _extract_keyword_lines(
            body_text,
            keywords=("football", "coach", "schedule", "athletic", "varsity", "jv", "freshman"),
        ),
        "links": normalized_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Visit the school's public athletics pages and extract football evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: list[dict[str, Any]] = []

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
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    snapshots.append(await _collect_page_snapshot(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_lines: list[str] = []
    all_links: list[dict[str, str]] = []
    football_schedule_links: list[dict[str, str]] = []
    head_coach: dict[str, str] = {}

    for snapshot in snapshots:
        page_text = str(snapshot.get("body_text") or "")
        all_lines.extend(snapshot.get("football_lines", []))

        snapshot_links = snapshot.get("links")
        if isinstance(snapshot_links, list):
            for link in snapshot_links:
                if not isinstance(link, dict):
                    continue
                href = _clean(str(link.get("href") or ""))
                text = _clean(str(link.get("text") or ""))
                if not href:
                    continue
                all_links.append({"text": text, "href": href})

                if "41600_2" in href and not head_coach:
                    head_coach = _extract_head_coach(page_text)

                if "football" in (text + " " + href).lower():
                    football_schedule_links.append({"text": text, "href": href})

        if not head_coach and "head coaches" in page_text.lower():
            head_coach = _extract_head_coach(page_text)

    all_lines = _dedupe_keep_order(all_lines)
    all_links = _dedupe_link_dicts(all_links)
    football_schedule_links = _dedupe_link_dicts(football_schedule_links)

    football_program_available = bool(head_coach or football_schedule_links or all_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_athletics_pages")

    football_team_names = _dedupe_keep_order(
        [
            "Football" if head_coach or football_schedule_links else "",
            "Freshman Football" if any("freshfootball" in item["href"].lower() for item in football_schedule_links) else "",
            "JV/V Football" if any("football.pdf" in item["href"].lower() for item in football_schedule_links) else "",
            "Girls Flag Football" if any("girlsflagfootball" in item["href"].lower() for item in football_schedule_links) else "",
        ]
    )
    coach_name = _clean(str(head_coach.get("name") or "the football coach"))

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": ATHLETICS_URL,
        "head_coaches_page_url": HEAD_COACHES_URL,
        "teams_schedules_page_url": TEAMS_SCHEDULES_URL,
        "football_head_coach": head_coach,
        "football_team_names": football_team_names,
        "football_schedule_public": bool(football_schedule_links),
        "football_schedule_links": football_schedule_links,
        "football_keyword_lines": all_lines[:40],
        "school_links": [
            link for link in all_links if any(key in (link["text"] + " " + link["href"]).lower() for key in ("athletic", "football", "coach", "schedule"))
        ][:30],
        "summary": (
            f"El Capitan High's athletics pages publish football coverage, a football head coach listing for {coach_name}, and public freshman, JV/varsity, and girls flag football schedule PDFs."
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
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "target_urls": TARGET_URLS,
            "pages_checked": len(snapshots),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for the school scraper runtime."""
    return await scrape_school()
