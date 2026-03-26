"""Deterministic football scraper for Lathrop High (CA)."""

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

NCES_ID = "062361012065"
SCHOOL_NAME = "Lathrop High"
STATE = "CA"

BASE_URL = "https://lathrophigh.mantecausd.net"
ATHLETICS_URL = f"{BASE_URL}/athletics"
FOOTBALL_HOME_URL = f"{BASE_URL}/athletics/football/football-home"
TARGET_URLS = [ATHLETICS_URL, FOOTBALL_HOME_URL]


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


def _extract_coach_lines(text: str) -> list[str]:
    lines = [_clean(line) for line in text.splitlines()]
    lines = [line for line in lines if line]
    coach_lines: list[str] = []
    for line in lines:
        lower = line.lower()
        if "coaching staff" in lower:
            coach_lines.append(line)
            continue
        if "@" in line and any(tag in lower for tag in ("varsity", "jv", "frosh", "coach")):
            coach_lines.append(line)
    return _dedupe_keep_order(coach_lines)


def _extract_team_names(text: str) -> list[str]:
    found: list[str] = []
    patterns = [
        ("Varsity", r"\bvarsity\b"),
        ("Junior Varsity", r"\bjv\b|\bjunior varsity\b"),
        ("Freshman", r"\bfrosh\b|\bfreshman\b"),
    ]
    for label, pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            found.append(label)
    return _dedupe_keep_order(found)


async def _collect_page_signal(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1500)

    body_text = await page.inner_text("body")
    cleaned_text = _clean(body_text)
    link_items = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: (el.href || '').trim()
        }))""",
    )

    links: list[dict[str, str]] = []
    if isinstance(link_items, list):
        for item in link_items:
            if not isinstance(item, dict):
                continue
            href = _clean(str(item.get("href") or ""))
            if not href:
                continue
            links.append({"text": _clean(str(item.get("text") or "")), "href": href})

    return {
        "requested_url": url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": cleaned_text,
        "links": links,
        "football_mentions": len(re.findall(r"\bfootball\b", cleaned_text, flags=re.IGNORECASE)),
        "coach_lines": _extract_coach_lines(body_text),
        "team_names": _extract_team_names(body_text),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape football-specific athletics content for Lathrop High."""
    require_proxy_credentials(profile="datacenter")
    assert_not_blocklisted(TARGET_URLS, profile="datacenter")

    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    errors: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile="datacenter"),
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    signal = await _collect_page_signal(page, url)
                    page_signals.append(signal)
                    source_pages.append(signal["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_links: list[dict[str, str]] = []
    coach_lines: list[str] = []
    team_names: list[str] = []
    football_keyword_lines: list[str] = []
    football_mentions = 0

    for signal in page_signals:
        football_mentions += int(signal.get("football_mentions", 0))
        all_links.extend(signal.get("links", []))
        coach_lines.extend(signal.get("coach_lines", []))
        team_names.extend(signal.get("team_names", []))

        for raw in str(signal.get("body_text", "")).splitlines():
            line = _clean(raw)
            if not line:
                continue
            if any(token in line.lower() for token in ("football", "varsity", "jv", "frosh", "coaching staff")):
                football_keyword_lines.append(line)

    coach_lines = _dedupe_keep_order(coach_lines)
    team_names = _dedupe_keep_order(team_names)
    football_keyword_lines = _dedupe_keep_order(football_keyword_lines)

    football_links: list[str] = []
    schedule_links: list[str] = []
    coach_contacts: list[str] = []
    for item in all_links:
        href = _clean(item.get("href", ""))
        text = _clean(item.get("text", ""))
        if not href:
            continue
        lower = f"{text} {href}".lower()
        if "mailto:" in href and any(token in lower for token in ("@gmail.com", "@hotmail.com", "coach", "varsity", "jv", "frosh")):
            coach_contacts.append(f"{text}|{href}")
        if any(token in lower for token in ("football", "/athletics/football/", "spartan athlete")):
            football_links.append(f"{text}|{href}")
        if "schedule" in lower:
            schedule_links.append(f"{text}|{href}")

    football_links = _dedupe_keep_order(football_links)
    schedule_links = _dedupe_keep_order(schedule_links)
    coach_contacts = _dedupe_keep_order(coach_contacts)

    football_program_available = bool(
        football_mentions > 0
        and (
            coach_lines
            or coach_contacts
            or any("/athletics/football/" in item for item in football_links)
        )
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": ATHLETICS_URL,
        "football_page_url": FOOTBALL_HOME_URL,
        "football_team_names": team_names,
        "football_coach_lines": coach_lines[:30],
        "football_coach_contacts": coach_contacts[:20],
        "football_links": football_links[:80],
        "football_schedule_links": schedule_links[:20],
        "football_keyword_lines": football_keyword_lines[:100],
        "summary": (
            "Lathrop High publishes a dedicated football page under athletics with coaching staff and team-level contacts."
            if football_program_available
            else ""
        ),
    }

    if not football_program_available:
        errors.append("no_public_football_content_detected")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages or [ATHLETICS_URL, FOOTBALL_HOME_URL],
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": "datacenter",
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "proxy": get_proxy_runtime_meta(profile="datacenter"),
            "focus": "football_only",
        },
        "errors": errors,
    }

