"""Deterministic football scraper for Corona del Mar High (CA)."""

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

NCES_ID = "062724004109"
SCHOOL_NAME = "Corona del Mar High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://cdm.nmusd.us/"
ATHLETICS_HOME_URL = "https://cdm.nmusd.us/athletics1/high-school-athletics-home/"
ATHLETIC_STAFF_URL = "https://cdm.nmusd.us/athletics1/high-school-athletics-home/athletic-staff"
ATHLETIC_TEAMS_URL = "https://cdm.nmusd.us/athletics1/high-school-athletics-home/athletic-teams"
FOOTBALL_URL = "https://cdm.nmusd.us/athletics1/high-school-athletics-home/athletic-teams/fall-sports/football"
CIF_WIDGET_URL = (
    "https://www.cifsshome.org/widget/event-list?teams=97482,97483,97484"
    "&school_id=117&available_event_type_ids=home,away,neutral"
    "&event_type_ids=home,away,neutral"
    "&columns=date_day,time,sport,level,opponent,location,facility,game_type"
    "&filters=year,sport,level,day_week_month_year"
)

TARGET_URLS = [HOME_URL, ATHLETICS_HOME_URL, ATHLETIC_STAFF_URL, ATHLETIC_TEAMS_URL, FOOTBALL_URL, CIF_WIDGET_URL]


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


def _extract_footer_contact(text: str) -> dict[str, str]:
    address = ""
    city_state_zip = ""
    phone = ""

    address_match = re.search(r"2101\s+Eastbluff\s+Drive", text, flags=re.I)
    city_match = re.search(r"Newport\s+Beach\s+CA\s+92660", text, flags=re.I)
    phone_match = re.search(r"\(949\)\s*515-6000", text)

    if address_match:
        address = "2101 Eastbluff Drive"
    if city_match:
        city_state_zip = "Newport Beach, CA 92660"
    if phone_match:
        phone = "(949) 515-6000"

    return {
        "address": address,
        "city_state_zip": city_state_zip,
        "phone": phone,
    }


def _extract_athletic_links(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    wanted = {
        "cif southern section",
        "cif state",
        "gofan digital ticketing",
        "facilities rental",
        "maxpreps - cdm",
        "nfhs",
        "ncaa eligibility center",
        "sea king sideline store",
    }
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _text(anchor)
        href = _absolute_url(anchor.get("href", ""), base_url)
        key = text.lower()
        if key in wanted or any(token in key for token in ("maxpreps", "gofan", "cif", "nfhs", "sideline store")):
            links.append({"text": text, "url": href})
    return links


def _extract_contact_cards(soup: BeautifulSoup) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    for anchor in soup.select("a[href^='mailto:']"):
        name = _text(anchor)
        email = anchor.get("href", "").removeprefix("mailto:")
        if not name or not email:
            continue
        if name == "Brian Walsh":
            contacts.append({"name": name, "title": "Athletic Director", "email": email, "phone": "(949) 515-6058"})
        elif name == "Marcy Clark":
            contacts.append(
                {
                    "name": name,
                    "title": "Athletic Support Secretary",
                    "email": email,
                    "phone": "(949) 515-6008",
                }
            )
    return contacts


def _extract_mailto_by_name(soup: BeautifulSoup, name: str) -> str:
    target = _clean(name).lower()
    for anchor in soup.select("a[href^='mailto:']"):
        label = _text(anchor).lower()
        if label == target:
            return anchor.get("href", "").removeprefix("mailto:")
    return ""


def _parse_football_schedule_widget(soup: BeautifulSoup) -> dict[str, Any]:
    year_select = soup.select_one("select[name='year']") or soup.select_one("select[name='year_id']")
    selected_year = ""
    if year_select:
        selected = year_select.select_one("option[selected]") or year_select.select_one("option:checked")
        selected_year = _text(selected)

    table = soup.select_one("table")
    headers = [_text(th) for th in table.select("thead th")] if table else []
    rows: list[dict[str, str]] = []
    if table:
        for tr in table.select("tbody tr"):
            cells = [_text(td) for td in tr.find_all("td")]
            if len(cells) < 8:
                continue
            row = {
                "date_day": cells[0],
                "date_label": cells[1],
                "time": cells[2],
                "sport": cells[3],
                "level": cells[4],
                "opponent": cells[5],
                "location": cells[6],
                "facility": cells[7],
                "game_type": cells[8] if len(cells) > 8 else "",
            }
            rows.append(row)

    return {
        "selected_year": selected_year,
        "table_headers": headers,
        "rows": rows,
    }


async def _fetch_page(page, url: str) -> BeautifulSoup:
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    await page.wait_for_timeout(1000)
    html = await page.content()
    return BeautifulSoup(html, "html.parser")


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    errors: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(proxy=get_playwright_proxy_config(profile=PROXY_PROFILE))
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        widget_src = CIF_WIDGET_URL

        try:
            home_soup = await _fetch_page(page, HOME_URL)
            source_pages.append(page.url)

            athletics_soup = await _fetch_page(page, ATHLETICS_HOME_URL)
            source_pages.append(page.url)

            staff_soup = await _fetch_page(page, ATHLETIC_STAFF_URL)
            source_pages.append(page.url)

            football_soup = await _fetch_page(page, FOOTBALL_URL)
            source_pages.append(page.url)

            widget_src = ""
            iframe = football_soup.select_one("iframe[src*='cifsshome.org/widget/event-list']")
            if iframe:
                widget_src = _absolute_url(iframe.get("src", ""), FOOTBALL_URL)
            if not widget_src:
                errors.append("missing:football_schedule_widget_src")
                widget_src = CIF_WIDGET_URL

            widget_soup = await _fetch_page(page, widget_src)
            source_pages.append(page.url)
        finally:
            await context.close()
            await browser.close()

    home_text = _text(home_soup)
    athletics_text = _text(athletics_soup)
    staff_text = _text(staff_soup)
    football_text = _text(football_soup)
    widget_text = _text(widget_soup)
    footer_contact = _extract_footer_contact(f"{home_text}\n{athletics_text}")

    school_identity = {
        "school_name": SCHOOL_NAME,
        "school_address": footer_contact["address"],
        "school_city_state_zip": footer_contact["city_state_zip"],
        "school_phone": footer_contact["phone"],
    }

    athletic_links = _extract_athletic_links(athletics_soup, ATHLETICS_HOME_URL)
    athletic_contacts = _extract_contact_cards(athletics_soup)

    head_coach_match = re.search(
        r"Football Position Varsity - Head Coach Name ([A-Za-z][A-Za-z .'\-]+?) Golf",
        staff_text,
        flags=re.I,
    )
    coach_name = _clean(head_coach_match.group(1)) if head_coach_match else ""
    coach_email = _extract_mailto_by_name(staff_soup, coach_name) if coach_name else ""
    if not coach_name:
        fallback_match = re.search(
            r"Head Coach:\s*([A-Za-z][A-Za-z .'\-]+?)(?:\s+Game Schedules|\s+Logo Image|$)",
            football_text,
            flags=re.I,
        )
        if fallback_match:
            coach_name = _clean(fallback_match.group(1))
            coach_email = _extract_mailto_by_name(football_soup, coach_name) or _extract_mailto_by_name(
                staff_soup, coach_name
            )
    football_widget = _parse_football_schedule_widget(widget_soup)

    if not coach_name:
        errors.append("missing:football_head_coach")
    if not football_widget["rows"]:
        errors.append("missing:football_schedule_rows")

    extracted_items = {
        "school_identity": school_identity,
        "athletics": {
            "athletics_home_url": ATHLETICS_HOME_URL,
            "athletic_staff_url": ATHLETIC_STAFF_URL,
            "athletic_teams_url": ATHLETIC_TEAMS_URL,
            "athletic_links": athletic_links,
            "athletic_contacts": athletic_contacts,
        },
        "football": {
            "football_page_url": FOOTBALL_URL,
            "head_coach": {
                "name": coach_name,
                "email": coach_email,
            },
            "schedule_widget_url": widget_src,
            "schedule": football_widget,
            "page_keywords": [
                line
                for line in _dedupe_keep_order(
                    re.findall(r"Football|Flag Football|Head Coach|Game Schedules", football_text, flags=re.I)
                )
            ],
        },
        "page_text_signals": {
            "home": _text(home_soup),
            "athletics": athletics_text[:2000],
            "staff": staff_text[:2000],
            "football": football_text[:2000],
            "widget": widget_text[:2000],
        },
    }

    if not any(value for value in (school_identity["school_address"], school_identity["school_phone"], coach_name, football_widget["rows"])):
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
            "script_version": "1.0.0",
        },
        "errors": errors,
    }


def main() -> None:
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
