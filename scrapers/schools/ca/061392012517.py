"""Deterministic football scraper for Jurupa Hills High (CA)."""

from __future__ import annotations

import asyncio
import html
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, unquote

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061392012517"
SCHOOL_NAME = "Jurupa Hills High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://jhills.fusd.net/"
ATHLETICS_URL = "https://jhills.fusd.net/athletics"
FOOTBALL_URL = "https://jhills.fusd.net/athletics/fall-sports/football"
COACH_DIRECTORY_URL = "https://jhills.fusd.net/athletics/coach-directory"
ATHLETICS_HOME_URL = "https://jhills.fusd.net/athletics/athletics-home"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    FOOTBALL_URL,
    COACH_DIRECTORY_URL,
    ATHLETICS_HOME_URL,
]


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


def _absolute_url(href: str, base_url: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


async def _fetch_page_data(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    await page.wait_for_timeout(1_500)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').trim().replace(/\\s+/g, ' '),
            href: e.getAttribute('href') || '',
            absolute_href: e.href || ''
        }))""",
    )
    main_text = ""
    main_html = ""
    main = page.locator("main")
    if await main.count():
        main_text = _clean(await main.inner_text())
        main_html = await main.inner_html()
    else:
        body = page.locator("body")
        main_text = _clean(await body.inner_text())
        main_html = await body.inner_html()
    return {
        "requested_url": url,
        "final_url": page.url,
        "title": await page.title(),
        "main_text": main_text,
        "main_html": main_html,
        "links": links,
    }


def _extract_phone_and_address(home_data: dict[str, Any]) -> dict[str, str]:
    phone = ""
    address = ""
    for link in home_data["links"]:
        href = _clean(link.get("absolute_href", ""))
        text = _clean(link.get("text", ""))
        if not phone and (href.startswith("tel:") or re.search(r"\(\d{3}\)\s*\d{3}-\d{4}", text)):
            phone = text or href.removeprefix("tel:")
    match = re.search(
        r"\d{2,6}\s+.+?Fontana,\s*CA\s+\d{5}",
        home_data["main_text"],
        flags=re.I,
    )
    if match:
        address = _clean(match.group(0))
    return {"phone": phone, "address": address}


def _extract_football_coach(football_data: dict[str, Any]) -> dict[str, str]:
    name = ""
    email = ""
    location = ""
    instagram = ""

    for link in football_data["links"]:
        href = _clean(link.get("absolute_href", ""))
        text = _clean(link.get("text", ""))
        if not email and href.startswith("mailto:"):
            email = _clean(unquote(href.removeprefix("mailto:"))).strip()
            if text and "@" not in text:
                name = text
        if not instagram and "instagram.com/jurupahillsfootball" in href.lower():
            instagram = href

    text = football_data["main_text"]
    if not name:
        match = re.search(r"Football\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)+)", text)
        if match:
            name = _clean(match.group(1))
    if re.search(r"\bON-SITE\b", text, flags=re.I):
        location = "ON-SITE"

    return {"name": name, "email": email, "location_note": location, "team_social_url": instagram}


def _extract_football_coach_from_directory(coach_data: dict[str, Any]) -> dict[str, str]:
    text = coach_data["main_text"]
    match = re.search(
        r"Football:\s*([A-Za-z][A-Za-z .'\-]+?)\s+([A-Za-z0-9._%+\-]+@fusd\.net)",
        text,
        flags=re.I,
    )
    if not match:
        return {"name": "", "email": ""}
    return {"name": _clean(match.group(1)), "email": _clean(match.group(2))}


def _extract_schedule_and_tickets(athletics_home_data: dict[str, Any]) -> dict[str, str]:
    schedule_widget_url = ""
    tickets_url = ""
    html_content = athletics_home_data["main_html"]
    match = re.search(
        r"""<iframe[^>]+src=["']([^"']*cifsshome\.org/widget/calendar[^"']+)["']""",
        html_content,
        flags=re.I,
    )
    if match:
        schedule_widget_url = _absolute_url(unquote(html.unescape(match.group(1))), ATHLETICS_HOME_URL)

    for link in athletics_home_data["links"]:
        href = _clean(link.get("absolute_href", ""))
        if "gofan.co" in href.lower():
            tickets_url = href
            break
    return {"schedule_widget_url": schedule_widget_url, "tickets_url": tickets_url}


def _extract_athletics_nav_links(athletics_data: dict[str, Any]) -> list[dict[str, str]]:
    wanted_labels = {
        "Announcements",
        "Athletic Clearance & Eligibility",
        "Athletic Information and FAQ",
        "Athletic Schedules/Purchasing Tickets",
        "Fall Sports",
        "Football",
        "Girls Flag Football",
        "Coaches Directory",
    }

    links: list[dict[str, str]] = []
    for link in athletics_data["links"]:
        label = _clean(link.get("text", ""))
        href = _absolute_url(link.get("absolute_href", "") or link.get("href", ""), ATHLETICS_URL)
        if label in wanted_labels and href:
            links.append({"label": label, "url": href})
    deduped = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        key = (link["label"], link["url"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    return deduped


async def scrape_school() -> dict[str, Any]:
    """Scrape football-specific public data for Jurupa Hills High."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context()
        page = await context.new_page()

        try:
            home_data = await _fetch_page_data(page, HOME_URL)
            source_pages.append(page.url)

            athletics_data = await _fetch_page_data(page, ATHLETICS_URL)
            source_pages.append(page.url)

            football_data = await _fetch_page_data(page, FOOTBALL_URL)
            source_pages.append(page.url)

            coach_data = await _fetch_page_data(page, COACH_DIRECTORY_URL)
            source_pages.append(page.url)

            athletics_home_data = await _fetch_page_data(page, ATHLETICS_HOME_URL)
            source_pages.append(page.url)
        finally:
            await context.close()
            await browser.close()

    school_contact = _extract_phone_and_address(home_data)
    football_page_coach = _extract_football_coach(football_data)
    directory_football_coach = _extract_football_coach_from_directory(coach_data)
    schedules = _extract_schedule_and_tickets(athletics_home_data)
    athletics_nav_links = _extract_athletics_nav_links(athletics_data)

    football_coach_name = directory_football_coach["name"] or football_page_coach["name"]
    football_coach_email = directory_football_coach["email"] or football_page_coach["email"]

    if not any(
        [
            football_coach_name,
            football_coach_email,
            football_page_coach["team_social_url"],
            schedules["schedule_widget_url"],
            schedules["tickets_url"],
        ]
    ):
        errors.append("blocked:no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "school": {
            "name": SCHOOL_NAME,
            "phone": school_contact["phone"],
            "address": school_contact["address"],
            "athletics_url": ATHLETICS_URL,
            "football_url": FOOTBALL_URL,
        },
        "football_program": {
            "sport": "Football",
            "coach_name": football_coach_name,
            "coach_email": football_coach_email,
            "location_note": football_page_coach["location_note"],
            "team_social_url": football_page_coach["team_social_url"],
            "schedule_widget_url": schedules["schedule_widget_url"],
            "tickets_url": schedules["tickets_url"],
        },
        "athletics_navigation_links": athletics_nav_links,
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
        },
        "errors": errors,
    }


def main() -> None:
    result = asyncio.run(scrape_school())
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
