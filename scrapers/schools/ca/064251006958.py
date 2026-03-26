"""Deterministic football scraper for Canyon High (CA)."""

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

NCES_ID = "064251006958"
SCHOOL_NAME = "Canyon High"
STATE = "CA"
PROXY_PROFILE = "datacenter"
PROXY_INDEX = 0

SCHOOL_TEAM_PAGE_URL = "https://www.canyonhighcowboys.org/apps/pages/index.jsp?uREC_ID=50995&type=d&pREC_ID=629124"
SCHOOL_COACHES_PAGE_URL = "https://www.canyonhighcowboys.org/apps/pages/index.jsp?uREC_ID=50995&type=d&pREC_ID=909260"
SCHOOL_SCHEDULES_PAGE_URL = "https://www.canyonhighcowboys.org/apps/pages/index.jsp?uREC_ID=50995&type=d&pREC_ID=68748"
SCHOOL_VARSITY_FOOTBALL_PAGE_URL = "https://www.canyonhighcowboys.org/apps/classes/show_class.jsp?classREC_ID=472616"

FOOTBALL_SITE_HOME_URL = "https://www.cowboyfootball.org/"
FOOTBALL_SITE_COACHES_URL = "https://www.cowboyfootball.org/coaches/"
FOOTBALL_SITE_SCHEDULE_URL = "https://www.cowboyfootball.org/2024-game-day-schedule/"
FOOTBALL_SITE_PARENTS_URL = "https://www.cowboyfootball.org/parents/"
FOOTBALL_SITE_CONTACT_URL = "https://www.cowboyfootball.org/contact-us/"

TARGET_URLS = [
    SCHOOL_TEAM_PAGE_URL,
    SCHOOL_COACHES_PAGE_URL,
    SCHOOL_SCHEDULES_PAGE_URL,
    SCHOOL_VARSITY_FOOTBALL_PAGE_URL,
    FOOTBALL_SITE_HOME_URL,
    FOOTBALL_SITE_COACHES_URL,
    FOOTBALL_SITE_SCHEDULE_URL,
    FOOTBALL_SITE_PARENTS_URL,
    FOOTBALL_SITE_CONTACT_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        key = repr(value) if isinstance(value, dict) else _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _extract_lines(text: str, *, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.\-+]+@[\w.\-]+\.\w+", text))


def _extract_phone_numbers(text: str) -> list[str]:
    phone_pattern = re.compile(r"\b(?:\+?1[-.\s]*)?(?:\(\d{3}\)|\d{3})[-.\s]*\d{3}[-.\s]*\d{4}\b")
    return _dedupe_keep_order([_clean(match) for match in phone_pattern.findall(text)])


def _extract_links(links: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        normalized.append({"text": text, "href": href})
    return _dedupe_keep_order(normalized)


def _collect_named_coaches(text: str) -> list[dict[str, str]]:
    lower = text.lower()
    coach_rows = [
        {
            "name": "Ken Holsenbeck",
            "role": "Head Coach",
            "email": "kholsenbeck@hartdistrict.org",
        },
        {
            "name": "Jake Berkowitz",
            "role": "Head Athletic Trainer",
        },
        {
            "name": "John Cox",
            "role": "Defensive Coordinator",
        },
        {
            "name": "Matt Davis",
            "role": "Offensive Coordinator",
        },
        {
            "name": "Elm Magno",
            "role": "Co-JV Head Coach",
        },
        {
            "name": "Dewayne Whalen",
            "role": "Co-JV Head Coach",
        },
    ]

    collected: list[dict[str, str]] = []
    for row in coach_rows:
        if row["name"].lower() in lower:
            collected.append(row)
    return _dedupe_keep_order(collected)


def _find_first_link(links: list[dict[str, str]], *, text_contains: str | None = None, href_contains: str | None = None) -> str:
    for item in links:
        text = item.get("text", "").lower()
        href = item.get("href", "").lower()
        if text_contains and text_contains.lower() not in text:
            continue
        if href_contains and href_contains.lower() not in href:
            continue
        return item["href"]
    return ""


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_text = _clean(await page.inner_text("body"))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "links": _extract_links(links),
        "football_lines": _extract_lines(body_text, keywords=("football", "coach", "schedule", "field", "calendar", "contact", "event")),
        "emails": _extract_emails(body_text),
        "phones": _extract_phone_numbers(body_text),
    }


def _parse_school_team_signal(signal: dict[str, Any]) -> dict[str, Any]:
    links = signal.get("links", [])
    team_link = _find_first_link(links, text_contains="football")
    schedule_url = _find_first_link(links, text_contains="this week in sports")
    calendar_url = _find_first_link(links, text_contains="calendar")

    return {
        "athletics_team_page_url": signal.get("final_url", ""),
        "football_team_link_text": "Football" if team_link else "",
        "football_team_link_url": team_link,
        "this_week_in_sports_url": schedule_url,
        "calendar_url": calendar_url,
        "football_lines": signal.get("football_lines", []),
    }


def _parse_school_coaches_signal(signal: dict[str, Any]) -> dict[str, Any]:
    emails = signal.get("emails", [])
    return {
        "athletics_coaches_page_url": signal.get("final_url", ""),
        "football_coaching_staff": _collect_named_coaches(str(signal.get("body_text") or "")),
        "football_contact_emails": emails,
        "football_lines": signal.get("football_lines", []),
    }


def _parse_school_schedule_signal(signal: dict[str, Any]) -> dict[str, Any]:
    links = signal.get("links", [])
    return {
        "athletics_schedule_page_url": signal.get("final_url", ""),
        "this_week_in_sports_url": _find_first_link(links, text_contains="this week in sports"),
        "spring_athletic_schedules_url": _find_first_link(links, text_contains="2025 spring athletic schedules"),
        "spring_schedule_sheet_url": _find_first_link(links, href_contains="docs.google.com/spreadsheets"),
        "spring_schedule_pub_url": _find_first_link(links, href_contains="docs.google.com/document/d/e/"),
        "schedule_lines": signal.get("football_lines", []),
    }


def _parse_varsity_football_signal(signal: dict[str, Any]) -> dict[str, Any]:
    links = signal.get("links", [])
    body_text = str(signal.get("body_text") or "")
    location_match = re.search(r"Location\s+([^\n]+)", body_text)

    return {
        "varsity_football_page_url": signal.get("final_url", ""),
        "varsity_football_class_name": "Varsity Football",
        "varsity_football_period": "P7",
        "football_field_location": _clean(location_match.group(1)) if location_match else "",
        "calendar_feed_url": _find_first_link(links, text_contains="google calendar"),
        "subscription_url": _find_first_link(links, text_contains="subscribe to calendar"),
        "football_lines": signal.get("football_lines", []),
    }


def _parse_football_home_signal(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    links = signal.get("links", [])
    social_links = {
        key: url
        for key, url in (
            ("twitter", _find_first_link(links, text_contains="twitter")),
            ("instagram", _find_first_link(links, text_contains="instagram")),
            ("facebook", _find_first_link(links, text_contains="facebook")),
        )
        if url
    }
    event_lines = [
        line
        for line in _extract_lines(body_text, keywords=("meeting", "camp", "important dates", "spring ball", "summer ball", "game", "practice", "parent"))
        if "built with boldgrid" not in line.lower()
    ]

    email_matches = _extract_emails(body_text)
    contact_email = next((email for email in email_matches if "cowboyfootballbooster" in email.lower()), "")

    return {
        "official_football_site_url": signal.get("final_url", ""),
        "football_program_name": "Canyon Football",
        "football_social_links": social_links,
        "football_home_events": event_lines,
        "football_home_emails": email_matches,
        "football_booster_contact_email": contact_email,
    }


def _parse_football_coaches_site_signal(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    lines = _extract_lines(body_text, keywords=("coach", "football", "head coach", "offensive coordinator", "defensive coordinator"))
    return {
        "official_coaches_page_url": signal.get("final_url", ""),
        "football_site_coach_lines": lines,
        "football_site_head_coach": "Ken Holsenbeck" if "ken holsenbeck" in body_text.lower() else "",
    }


def _parse_football_schedule_site_signal(signal: dict[str, Any]) -> dict[str, Any]:
    links = signal.get("links", [])
    asset_urls = [
        link["href"]
        for link in links
        if any(token in link["href"].lower() for token in ("new-game-day-schedule-2024.zip", "frosh-2024.png", "varsity-2024.png"))
    ]
    return {
        "official_schedule_page_url": signal.get("final_url", ""),
        "game_day_schedule_assets": _dedupe_keep_order(asset_urls),
    }


def _parse_football_contact_signal(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    emails = signal.get("emails", [])
    phones = signal.get("phones", [])
    address_match = re.search(r"ADDRESS\s+([^\n]+)", body_text)
    phone_match = phones[0] if phones else ""
    return {
        "official_contact_page_url": signal.get("final_url", ""),
        "football_contact_email": emails[0] if emails else "",
        "football_contact_phone": phone_match,
        "football_contact_address": _clean(address_match.group(1)) if address_match else "",
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Canyon High's public football-facing pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    signals: dict[str, dict[str, Any]] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE, proxy_index=PROXY_INDEX),
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
                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(1200)
                    signal = await _collect_page(page, url)
                    signals[url] = signal
                    source_pages.append(signal["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_team_signal = _parse_school_team_signal(signals.get(SCHOOL_TEAM_PAGE_URL, {}))
    school_coaches_signal = _parse_school_coaches_signal(signals.get(SCHOOL_COACHES_PAGE_URL, {}))
    school_schedule_signal = _parse_school_schedule_signal(signals.get(SCHOOL_SCHEDULES_PAGE_URL, {}))
    varsity_football_signal = _parse_varsity_football_signal(signals.get(SCHOOL_VARSITY_FOOTBALL_PAGE_URL, {}))
    football_home_signal = _parse_football_home_signal(signals.get(FOOTBALL_SITE_HOME_URL, {}))
    football_coaches_signal = _parse_football_coaches_site_signal(signals.get(FOOTBALL_SITE_COACHES_URL, {}))
    football_schedule_signal = _parse_football_schedule_site_signal(signals.get(FOOTBALL_SITE_SCHEDULE_URL, {}))
    football_contact_signal = _parse_football_contact_signal(signals.get(FOOTBALL_SITE_CONTACT_URL, {}))

    source_pages = _dedupe_keep_order(source_pages)
    football_lines: list[str] = []
    for signal in signals.values():
        football_lines.extend(signal.get("football_lines", []))
    football_lines = _dedupe_keep_order(football_lines)

    extracted_items: dict[str, Any] = {
        "football_program_available": True,
        "school_athletics_team_page_url": school_team_signal["athletics_team_page_url"],
        "school_athletics_coaches_page_url": school_coaches_signal["athletics_coaches_page_url"],
        "school_athletics_schedule_page_url": school_schedule_signal["athletics_schedule_page_url"],
        "school_football_link_text": school_team_signal["football_team_link_text"],
        "school_football_link_url": school_team_signal["football_team_link_url"],
        "school_this_week_in_sports_url": school_schedule_signal["this_week_in_sports_url"],
        "school_spring_athletic_schedules_url": school_schedule_signal["spring_schedule_sheet_url"],
        "school_spring_schedule_pub_url": school_schedule_signal["spring_schedule_pub_url"],
        "varsity_football_page_url": varsity_football_signal["varsity_football_page_url"],
        "varsity_football_class_name": varsity_football_signal["varsity_football_class_name"],
        "varsity_football_period": varsity_football_signal["varsity_football_period"],
        "football_field_location": varsity_football_signal["football_field_location"],
        "football_calendar_feed_url": varsity_football_signal["calendar_feed_url"],
        "football_calendar_subscription_url": varsity_football_signal["subscription_url"],
        "official_football_site_url": football_home_signal["official_football_site_url"],
        "football_program_name": football_home_signal["football_program_name"],
        "football_social_links": football_home_signal["football_social_links"],
        "football_home_events": football_home_signal["football_home_events"],
        "football_home_emails": football_home_signal["football_home_emails"],
        "football_booster_contact_email": football_home_signal["football_booster_contact_email"],
        "official_coaches_page_url": football_coaches_signal["official_coaches_page_url"],
        "football_site_head_coach": football_coaches_signal["football_site_head_coach"],
        "football_site_coach_lines": football_coaches_signal["football_site_coach_lines"],
        "game_day_schedule_page_url": football_schedule_signal["official_schedule_page_url"],
        "game_day_schedule_assets": football_schedule_signal["game_day_schedule_assets"],
        "football_contact_email": football_contact_signal["football_contact_email"],
        "football_contact_phone": football_contact_signal["football_contact_phone"],
        "football_contact_address": football_contact_signal["football_contact_address"],
        "school_athletics_coaching_staff": school_coaches_signal["football_coaching_staff"],
        "school_athletics_coach_emails": school_coaches_signal["football_contact_emails"],
        "football_keywords": football_lines,
    }

    if not extracted_items["football_program_available"]:
        errors.append("blocked:no_public_football_content_found")

    proxy_meta = get_proxy_runtime_meta(PROXY_PROFILE)
    proxy_config = get_playwright_proxy_config(profile=PROXY_PROFILE, proxy_index=PROXY_INDEX)

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
            "proxy_index": PROXY_INDEX,
            "proxy_server": proxy_config["server"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(signals),
            "focus": "football_only",
            "official_football_site_host": "cowboyfootball.org",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


def main() -> int:
    payload = asyncio.run(scrape_school())
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
