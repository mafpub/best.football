"""Deterministic football scraper for Andrew P. Hill High (CA)."""

from __future__ import annotations

import os
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

NCES_ID = "061182001301"
SCHOOL_NAME = "Andrew P. Hill High"
STATE = "CA"
BASE_URL = "https://andrewphill.esuhsd.org"
HOME_URL = f"{BASE_URL}/"
ATHLETICS_URL = f"{BASE_URL}/Athletics/index.html"
FALL_SPORTS_URL = f"{BASE_URL}/Athletics/Fall-Sports/index.html"
COACHES_DIRECTORY_URL = f"{BASE_URL}/Athletics/Coaches-Directory/index.html"
ATHLETICS_CALENDAR_URL = f"{BASE_URL}/Athletics/Athletics-Calendar/index.html"
CLEARANCE_URL = f"{BASE_URL}/Athletics/Athletic-Clearance--Sport-Physical-Information/index.html"
TITLE_IX_URL = f"{BASE_URL}/Athletics/Title-IX--Gender-Equity/index.html"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    FALL_SPORTS_URL,
    COACHES_DIRECTORY_URL,
    ATHLETICS_CALENDAR_URL,
    CLEARANCE_URL,
    TITLE_IX_URL,
]

PROXY_PROFILE = "datacenter"
PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
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


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 50) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_emails(text: str) -> list[str]:
    emails = re.findall(r"[\w.\-+]+@[\w.\-]+\.\w+", text)
    return _dedupe_keep_order(emails)


def _extract_link_targets(links: list[dict[str, Any]]) -> list[str]:
    values: list[str] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(
            keyword in blob
            for keyword in (
                "athletics",
                "football",
                "calendar",
                "clearance",
                "title ix",
                "sports",
            )
        ):
            values.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(values)


def _extract_football_coaches_from_fall_text(text: str) -> list[str]:
    coaches: list[str] = []

    varsity_match = re.search(
        r"Football\s*-\s*Varsity\s+([A-Za-z][A-Za-z.'\-]*(?:\s+[A-Za-z][A-Za-z.'\-]*)*)\s+\(V\)\s+([\w.+-]+@[\w.-]+\.\w+)\s+([A-Za-z][A-Za-z.'\-]*(?:\s+[A-Za-z][A-Za-z.'\-]*)*)\s+\(JV\)\s+([\w.+-]+@[\w.-]+\.\w+)",
        text,
        re.IGNORECASE,
    )
    if varsity_match:
        coaches.append(
            f"Football Varsity: {varsity_match.group(1)} <{varsity_match.group(2)}>"
        )
        coaches.append(f"Football JV: {varsity_match.group(3)} <{varsity_match.group(4)}>")

    flag_match = re.search(
        r"Girl'?s Flag Football\s+([A-Za-z][A-Za-z.'\-]*(?:\s+[A-Za-z][A-Za-z.'\-]*)*)\s+\(V\)\s+([\w.+-]+@[\w.-]+\.\w+)\s+TBD\s+\(JV\)",
        text,
        re.IGNORECASE,
    )
    if flag_match:
        coaches.append(
            f"Girls Flag Football Varsity: {flag_match.group(1)} <{flag_match.group(2)}>"
        )
        coaches.append("Girls Flag Football JV: TBD")

    return _dedupe_keep_order(coaches)


def _extract_football_team_names(fall_text: str, title_ix_text: str) -> list[str]:
    teams: list[str] = []
    if "football - varsity" in fall_text.lower() or "football" in title_ix_text.lower():
        teams.append("Football")
    if "girl's flag football" in fall_text.lower() or "flag football" in title_ix_text.lower():
        teams.append("Girls Flag Football")
    return _dedupe_keep_order(teams)


def _extract_title_ix_counts(text: str) -> dict[str, int] | None:
    counts: dict[str, int] = {}
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        match = re.fullmatch(r"Football\s+(\d+)\s+(\d+|[-])", line, re.IGNORECASE)
        if not match:
            continue
        counts["male_teams"] = int(match.group(1))
        counts["female_teams"] = 0 if match.group(2) == "-" else int(match.group(2))
        break
    player_match = re.search(r"Football\s+\(F/S and V\)\s+(\d+)", text, re.IGNORECASE)
    if player_match:
        counts["players"] = int(player_match.group(1))
    return counts or None


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    body_text = _clean(await page.inner_text("body"))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _extract_lines(
            body_text,
            keywords=("football", "flag football", "coach", "athletics", "clearance"),
        ),
        "emails": _extract_emails(body_text),
        "links": _extract_link_targets(links),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit Andrew P. Hill athletics pages and extract public football details."""
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
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page_signal(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    signal_map = {signal["requested_url"]: signal for signal in page_signals}
    home_signal = signal_map.get(HOME_URL, {})
    fall_signal = signal_map.get(FALL_SPORTS_URL, {})
    title_ix_signal = signal_map.get(TITLE_IX_URL, {})
    clearance_signal = signal_map.get(CLEARANCE_URL, {})
    athletics_signal = signal_map.get(ATHLETICS_URL, {})

    fall_text = str(fall_signal.get("body_text") or "")
    title_ix_text = str(title_ix_signal.get("body_text") or "")
    clearance_text = str(clearance_signal.get("body_text") or "")
    home_text = str(home_signal.get("body_text") or "")
    athletics_text = str(athletics_signal.get("body_text") or "")

    football_coaches = _extract_football_coaches_from_fall_text(fall_text)
    football_team_names = _extract_football_team_names(fall_text, title_ix_text)
    title_ix_counts = _extract_title_ix_counts(title_ix_text)
    football_lines = _dedupe_keep_order(
        _extract_lines(
            " ".join([fall_text, title_ix_text, clearance_text, home_text, athletics_text]),
            keywords=("football", "flag football", "athletic clearance", "sports physical"),
        )
    )
    football_links = _dedupe_keep_order(
        [
            value
            for signal in page_signals
            for value in signal.get("links", [])
        ]
    )
    football_emails = _dedupe_keep_order(
        [
            email
            for signal in page_signals
            for email in signal.get("emails", [])
            if "@" in email
        ]
    )

    football_program_available = bool(football_coaches or football_team_names or title_ix_counts)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_names": football_team_names,
        "football_coaches": football_coaches,
        "football_schedule_public": True,
        "football_schedule_url": ATHLETICS_CALENDAR_URL,
        "athletics_calendar_url": ATHLETICS_CALENDAR_URL,
        "clearance_information_url": CLEARANCE_URL,
        "title_ix_url": TITLE_IX_URL,
        "football_title_ix_counts": title_ix_counts or {},
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "football_contact_emails": football_emails,
        "school_address": "3200 Senter Road, San Jose, CA 95111",
        "school_phone": "408-347-4100",
        "sports_physical_phone": "408-347-4240",
        "summary": (
            "Andrew P. Hill High publicly lists football and girls flag football on its fall sports page, names the varsity and JV coaches, and publishes Title IX football counts plus athletics clearance information."
            if football_program_available
            else ""
        ),
    }

    if title_ix_counts:
        extracted_items["football_title_ix_counts"] = title_ix_counts

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
            "manual_navigation_steps": [
                "home",
                "athletics",
                "fall_sports",
                "coaches_directory",
                "athletics_calendar",
                "clearance",
                "title_ix",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
