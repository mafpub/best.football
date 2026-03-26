"""Deterministic football scraper for Hillcrest High (CA)."""

from __future__ import annotations

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

NCES_ID = "060243013184"
SCHOOL_NAME = "Hillcrest High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://hillcrest.alvordschools.org"
SCHOOL_URL = "https://www.alvordschools.org"
SCHOOL_HOME_URL = f"{BASE_URL}/"
FOOTBALL_HOME_URL = f"{BASE_URL}/18349_4"
FOOTBALL_UPDATES_URL = f"{BASE_URL}/18350_4"
FOOTBALL_SCHEDULE_URL = f"{BASE_URL}/50057_4"
FOOTBALL_ROSTER_URL = f"{BASE_URL}/50058_4"
TARGET_URLS = [
    SCHOOL_URL,
    SCHOOL_HOME_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_UPDATES_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        key = _clean(repr(value)) if isinstance(value, (dict, list, tuple)) else _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _abs_url(href: str, base_url: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    return urljoin(base_url, value)


def _text(node: Any) -> str:
    return _clean(node.get_text(" ", strip=True)) if node else ""


def _extract_footer_contact(text: str) -> dict[str, str]:
    address = ""
    city_state_zip = ""
    phone = ""

    if re.search(r"11800\s+indiana\s+ave", text, flags=re.I):
        address = "11800 Indiana Ave"
    if re.search(r"Riverside\s*,\s*CA\s*92503", text, flags=re.I):
        city_state_zip = "Riverside, CA 92503"
    phone_match = re.search(r"951[.\-]?358[.\-]?1755", text)
    if not phone_match:
        phone_match = re.search(r"\(951\)\s*358[-\s]?1755", text)
    if phone_match:
        phone = phone_match.group(0).replace(" ", "")

    return {
        "address": address,
        "city_state_zip": city_state_zip,
        "phone": phone,
    }


def _extract_athletic_links(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    wanted = {
        "team calendar",
        "hudl tv livestream",
        "maxpreps team page",
        "athletic clearance / physicals",
        "schedule",
        "roster",
    }
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _text(anchor).lower()
        href = _abs_url(anchor.get("href", ""), base_url)
        if not text or not href:
            continue
        if text in wanted or any(token in text for token in ("hudl", "maxpreps", "team calendar")):
            links.append({"text": _text(anchor), "url": href})
    return _dedupe(links)


def _extract_coaching_staff(soup: BeautifulSoup) -> list[dict[str, str]]:
    staff: list[dict[str, str]] = []
    for para in soup.select("p"):
        if "coaching staff" not in _text(para).lower():
            continue
        table = para.find_next("table")
        if not table:
            continue
        for row in table.select("tr"):
            cells = [_clean(cell.get_text(" ", strip=True)) for cell in row.select("td")]
            if len(cells) < 2:
                continue
            email = cells[1] if "@" in cells[1] else ""
            staff.append({
                "name": cells[0],
                "email": email,
                "title": "Head Coach" if "travis" in cells[0].lower() else "Coach",
            })
        break
    return _dedupe(staff)


def _extract_iframe_src(soup: BeautifulSoup, token: str) -> str:
    iframe = soup.select_one(f"iframe[src*='{token}']")
    if not iframe:
        return ""
    return _abs_url(iframe.get("src", ""), BASE_URL)


def _extract_schedule_rows(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table")
    if not table:
        return []

    rows: list[dict[str, str]] = []
    for tr in table.select("tbody tr"):
        cells = [_clean(td.get_text(" ", strip=True)) for td in tr.select("td")]
        if len(cells) < 8:
            continue
        rows.append(
            {
                "day_of_month": cells[0],
                "date_label": cells[1],
                "time": cells[2],
                "sport": cells[3],
                "level": cells[4],
                "opponent": cells[5],
                "location": cells[6],
                "result": cells[7],
                "game_type": cells[8] if len(cells) > 8 else "",
                "raw_text": " ".join(cells[:9]),
            }
        )
    return rows


def _extract_roster_rows(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table")
    if not table:
        return []

    rows: list[dict[str, str]] = []
    for tr in table.select("tbody tr"):
        cells = [_clean(td.get_text(" ", strip=True)) for td in tr.select("td")]
        if not cells:
            continue
        last_name = cells[0]
        first_name = cells[1] if len(cells) > 1 else ""
        grade = cells[2] if len(cells) > 2 else ""
        jersey = cells[3] if len(cells) > 3 else ""
        rows.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "full_name": f"{first_name} {last_name}".strip(),
                "grade": grade,
                "jersey_number": jersey,
            }
        )
    return rows


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1200)
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    return {
        "url": page.url,
        "html": html,
        "soup": soup,
        "text": _text(soup),
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    school_data: dict[str, Any] = {
        "school_contact": {
            "address": "",
            "city_state_zip": "",
            "phone": "",
        },
        "coaching_staff": [],
        "athletic_links": [],
        "football_program_available": False,
        "football_team_name": "Hillcrest Trojans",
        "football_schedule_widget_url": "",
        "football_roster_widget_url": "",
        "football_schedule_events": [],
        "football_roster_players": [],
        "football_schedule_summary": [],
    }

    football_signal: dict[str, Any] = {}
    schedule_signal: dict[str, Any] = {}
    roster_signal: dict[str, Any] = {}
    updates_signal: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1600, "height": 2000},
        )
        page = await context.new_page()

        try:
            school_signal = await _collect_page(page, SCHOOL_HOME_URL)
            source_pages.append(school_signal["url"])

            football_signal = await _collect_page(page, FOOTBALL_HOME_URL)
            source_pages.append(football_signal["url"])

            updates_signal = await _collect_page(page, FOOTBALL_UPDATES_URL)
            source_pages.append(updates_signal["url"])

            schedule_signal = await _collect_page(page, FOOTBALL_SCHEDULE_URL)
            source_pages.append(schedule_signal["url"])

            roster_signal = await _collect_page(page, FOOTBALL_ROSTER_URL)
            source_pages.append(roster_signal["url"])

            schedule_iframe = _extract_iframe_src(schedule_signal["soup"], "cifsshome.org/widget/event-list")
            if schedule_iframe:
                source_pages.append(schedule_iframe)
                schedule_widget_signal = await _collect_page(page, schedule_iframe)
                school_data["football_schedule_widget_url"] = schedule_iframe
                school_data["football_schedule_events"] = _dedupe(
                    _extract_schedule_rows(schedule_widget_signal["html"])
                )
                school_data["football_schedule_summary"] = [
                    item["raw_text"] for item in school_data["football_schedule_events"][:10]
                ]
            else:
                errors.append("missing:football_schedule_iframe")

            roster_iframe = _extract_iframe_src(roster_signal["soup"], "cifsshome.org/widget/rosters")
            if roster_iframe:
                source_pages.append(roster_iframe)
                roster_widget_signal = await _collect_page(page, roster_iframe)
                school_data["football_roster_widget_url"] = roster_iframe
                school_data["football_roster_players"] = _dedupe(_extract_roster_rows(roster_widget_signal["html"]))
            else:
                errors.append("missing:football_roster_iframe")

            contact_text = " ".join(
                [_text(soup) for soup in (school_signal["soup"], football_signal["soup"], updates_signal["soup"])]
            )
            school_data["school_contact"] = _extract_footer_contact(contact_text)

            school_data["coaching_staff"] = _extract_coaching_staff(football_signal["soup"])
            school_data["athletic_links"] = _extract_athletic_links(football_signal["soup"], BASE_URL)

            school_data["football_program_available"] = bool(
                school_data["coaching_staff"]
                or school_data["football_schedule_events"]
                or school_data["football_roster_players"]
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"navigation_failed:{type(exc).__name__}")
        finally:
            await context.close()
            await browser.close()

    if not school_data["football_schedule_events"]:
        errors.append("missing:football_schedule_rows")
    if not school_data["football_roster_players"]:
        errors.append("missing:football_roster_rows")

    source_pages = _dedupe(source_pages)

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "source_urls": [
                SCHOOL_HOME_URL,
                FOOTBALL_HOME_URL,
                FOOTBALL_UPDATES_URL,
                FOOTBALL_SCHEDULE_URL,
                FOOTBALL_ROSTER_URL,
            ],
            "pages_checked": len(source_pages),
            "focus": "football_only",
        }
    )

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": {
            "football_program_available": school_data["football_program_available"],
            "school_contact": school_data["school_contact"],
            "athletic_links": school_data["athletic_links"],
            "coaching_staff": school_data["coaching_staff"],
            "football_team_name": school_data["football_team_name"],
            "football_home_url": FOOTBALL_HOME_URL,
            "football_schedule_url": FOOTBALL_SCHEDULE_URL,
            "football_roster_url": FOOTBALL_ROSTER_URL,
            "football_updates_url": FOOTBALL_UPDATES_URL,
            "football_schedule_widget_url": school_data["football_schedule_widget_url"],
            "football_roster_widget_url": school_data["football_roster_widget_url"],
            "football_schedule_events": school_data["football_schedule_events"],
            "football_schedule_summary": school_data["football_schedule_summary"],
            "football_roster_players": school_data["football_roster_players"],
            "football_updates_title": _text(updates_signal.get("soup", BeautifulSoup("", "html.parser")).select_one("h2.page_title")),
            "football_updates_text": _text(updates_signal.get("soup", BeautifulSoup("", "html.parser")).select_one("main")),
        },
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


def main() -> int:
    payload = __import__("asyncio").run(scrape_school())
    print(
        __import__("json").dumps(payload, ensure_ascii=False, indent=2),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
