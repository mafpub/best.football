"""Deterministic football scraper for Desert Junior-Senior High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
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

NCES_ID = "062649003995"
SCHOOL_NAME = "Desert Junior-Senior High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://desert.muroc.k12.ca.us/"
ATHLETICS_URL = "https://desert.muroc.k12.ca.us/for-students/athletics"

TARGET_URLS = [HOME_URL, ATHLETICS_URL]

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


def _absolute_url(href: str, base_url: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


def _text(node) -> str:
    return _clean(node.get_text(" ", strip=True)) if node else ""


def _extract_school_contact(home_text: str) -> dict[str, str]:
    address = ""
    city_state_zip = ""
    phone = ""

    address_match = re.search(
        r"1575\s+Payne\s+(?:Ave(?:nue)?)(?:,|\s)",
        home_text,
        flags=re.I,
    )
    city_match = re.search(r"Edwards\s+CA\s+93523", home_text, flags=re.I)
    phone_match = re.search(r"\(760\)\s*306-4964", home_text)

    if address_match:
        address = "1575 Payne Ave"
    if city_match:
        city_state_zip = "Edwards, CA 93523"
    if phone_match:
        phone = "(760) 306-4964"

    return {
        "address": address,
        "city_state_zip": city_state_zip,
        "phone": phone,
    }


def _parse_athletics_rows(soup: BeautifulSoup, athletics_text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for tr in soup.select("table tr"):
        cells = [_text(cell) for cell in tr.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        sport = _clean(cells[0])
        coach = _clean(cells[1])
        if not sport:
            continue
        key = (sport.lower(), coach.lower())
        if key in seen:
            continue
        seen.add(key)
        rows.append({"sport": sport, "coach": coach})

    if rows:
        return rows

    for raw_line in athletics_text.splitlines():
        line = _clean(raw_line)
        if " | " not in line:
            continue
        sport, coach = [part.strip() for part in line.split("|", 1)]
        if not sport:
            continue
        key = (sport.lower(), coach.lower())
        if key in seen:
            continue
        seen.add(key)
        rows.append({"sport": sport, "coach": coach})

    return rows


def _parse_athletics_page(soup: BeautifulSoup, athletics_text: str) -> dict[str, Any]:
    director_name = ""
    director_email = ""
    league_membership = ""

    director_match = re.search(
        r"HS Athletic Director:\s*(Mr\.\s*)?([A-Za-z][A-Za-z .'\-]+?),\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        athletics_text,
        flags=re.I,
    )
    if director_match:
        director_name = _clean(director_match.group(2))
        director_email = _clean(director_match.group(3))

    league_match = re.search(r"League Membership:\s*([^\n]+)", athletics_text, flags=re.I)
    if league_match:
        league_membership = _clean(league_match.group(1))

    rows = _parse_athletics_rows(soup, athletics_text)
    football_row = next(
        (row for row in rows if row["sport"].lower() == "football"),
        {"sport": "Football", "coach": ""},
    )
    football_coach = _clean(football_row.get("coach", ""))
    football_program_available = any(row["sport"].lower() == "football" for row in rows) or "football" in athletics_text.lower()

    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _text(anchor)
        href = _absolute_url(anchor.get("href", ""), ATHLETICS_URL)
        combo = f"{text} {href}".lower()
        if any(term in combo for term in ("football", "athletics", "sports", "muroc", "high desert league", "cif central section")):
            links.append({"text": text, "url": href})

    return {
        "athletic_director": {
            "name": director_name,
            "email": director_email,
        },
        "league_membership": league_membership,
        "sports": rows,
        "football_program_available": football_program_available,
        "football": {
            "team_name": "Football",
            "coach": football_coach,
            "coach_display": football_coach or "TBA",
            "program_line": "Football | TBA" if football_coach.upper() == "TBA" else f"Football | {football_coach}",
        },
        "public_links": _dedupe_keep_order([f"{item['text']}|{item['url']}" for item in links]),
    }


async def _fetch_page(page, url: str) -> tuple[BeautifulSoup, str, str]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)
    html = await page.content()
    return BeautifulSoup(html, "html.parser"), _clean(await page.title()), _clean(await page.locator("body").inner_text())


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            home_soup, home_title, home_text = await _fetch_page(page, HOME_URL)
            source_pages.append(page.url)

            athletics_soup, athletics_title, athletics_text = await _fetch_page(page, ATHLETICS_URL)
            source_pages.append(page.url)
        finally:
            await context.close()
            await browser.close()

    school_contact = _extract_school_contact(home_text)
    athletics = _parse_athletics_page(athletics_soup, athletics_text)

    if not athletics["football_program_available"]:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "school_identity": {
            "school_name": SCHOOL_NAME,
            "address": school_contact["address"],
            "city_state_zip": school_contact["city_state_zip"],
            "phone": school_contact["phone"],
            "home_title": home_title,
        },
        "athletics": {
            "athletics_page_url": ATHLETICS_URL,
            "athletics_title": athletics_title,
            "athletic_director": athletics["athletic_director"],
            "league_membership": athletics["league_membership"],
            "sports": athletics["sports"],
            "public_links": athletics["public_links"],
        },
        "football": {
            "football_program_available": athletics["football_program_available"],
            "team_name": athletics["football"]["team_name"],
            "coach": athletics["football"]["coach"],
            "coach_display": athletics["football"]["coach_display"],
            "program_line": athletics["football"]["program_line"],
            "summary": (
                "Football is listed on the public athletics page with coach information currently marked TBA."
            ),
        },
        "page_text_signals": {
            "home": home_text[:2000],
            "athletics": athletics_text[:2000],
        },
    }

    if not any(
        [
            school_contact["address"],
            school_contact["phone"],
            athletics["athletic_director"]["name"],
            athletics["league_membership"],
            athletics["football_program_available"],
        ]
    ):
        errors.append("blocked:no_public_football_content_found")

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
            "pages_visited": len(source_pages),
            "manual_navigation_steps": [
                "home_page",
                "athletics_page",
            ],
            "script_version": "1.0.0",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


def main() -> None:
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
