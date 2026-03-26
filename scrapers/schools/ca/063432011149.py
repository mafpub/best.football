"""Deterministic football scraper for Crawford High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "063432011149"
SCHOOL_NAME = "Crawford High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_HOME_URL = "https://crawford.sandiegounified.org/home"
CONTACT_URL = "https://crawford.sandiegounified.org/about/contact_us"
ATHLETICS_HOME_URL = "https://www.chscolts.org/"
ATHLETICS_DEPARTMENT_URL = "https://www.chscolts.org/athletic-department/coaching-staff/"

FOOTBALL_HOME_URL = "https://www.chscolts.org/varsity/football/"
FOOTBALL_SCHEDULE_URL = "https://www.chscolts.org/varsity/football/schedule-results"
FOOTBALL_ROSTER_URL = "https://www.chscolts.org/varsity/football/roster"
FOOTBALL_COACHES_URL = "https://www.chscolts.org/varsity/football/coaches"

FLAG_HOME_URL = "https://www.chscolts.org/varsity/flag-football-girls/"
FLAG_SCHEDULE_URL = "https://www.chscolts.org/varsity/flag-football-girls/schedule-results"
FLAG_ROSTER_URL = "https://www.chscolts.org/varsity/flag-football-girls/roster"
FLAG_COACHES_URL = "https://www.chscolts.org/varsity/flag-football-girls/coaches"

TARGET_URLS = [
    SCHOOL_HOME_URL,
    CONTACT_URL,
    ATHLETICS_HOME_URL,
    ATHLETICS_DEPARTMENT_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_COACHES_URL,
    FLAG_HOME_URL,
    FLAG_SCHEDULE_URL,
    FLAG_ROSTER_URL,
    FLAG_COACHES_URL,
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


def _absolute_url(href: str, base_url: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


def _extract_body_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return _clean(soup.get_text(" ", strip=True))


def _extract_contact_info(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    address_match = re.search(r"(\d+\s+Colts Way\s+San Diego,\s+CA\s+\d{5})", text)
    phone_match = re.search(r"Phone number:\s*([()\d\-\s]+)", text)
    fax_match = re.search(r"Fax number:\s*([()\d\-\s]+)", text)

    athletic_director = ""
    athletic_director_phone = ""
    for tr in soup.select("table tr"):
        cells = [_clean(td.get_text(" ", strip=True)) for td in tr.select("td")]
        if len(cells) < 3:
            continue
        if "athletic director" in cells[1].lower():
            athletic_director = cells[0]
            athletic_director_phone = cells[2]
            break

    return {
        "school_address": address_match.group(1) if address_match else "",
        "school_phone": phone_match.group(1).strip() if phone_match else "",
        "school_fax": fax_match.group(1).strip() if fax_match else "",
        "athletic_director": athletic_director,
        "athletic_director_phone": athletic_director_phone,
    }


def _extract_team_coaches(html: str, team_slug: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    coaches: list[dict[str, str]] = []
    for anchor in soup.select(f"a[href*='/coaching-staff/'][href*='{team_slug}']"):
        paragraphs = [_clean(p.get_text(" ", strip=True)) for p in anchor.select("p")]
        if len(paragraphs) < 2:
            continue
        name = paragraphs[0]
        role = paragraphs[1]
        profile_url = _absolute_url(anchor.get("href", ""), "https://www.chscolts.org/")
        if not name or "coach" not in role.lower():
            continue
        coaches.append(
            {
                "name": name,
                "role": role,
                "profile_url": profile_url,
            }
        )
    return _dedupe_keep_order([json.dumps(item, sort_keys=True) for item in coaches])


def _decode_team_coaches(coaches: list[str]) -> list[dict[str, str]]:
    decoded: list[dict[str, str]] = []
    for item in coaches:
        try:
            payload = json.loads(item)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            decoded.append(
                {
                    "name": _clean(str(payload.get("name") or "")),
                    "role": _clean(str(payload.get("role") or "")),
                    "profile_url": _clean(str(payload.get("profile_url") or "")),
                }
            )
    return decoded


def _extract_roster(html: str, base_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    selected_option = soup.select_one("#yearDropdown option[selected]")
    season = _clean(selected_option.get_text(" ", strip=True)) if selected_option else ""
    selected_value = _clean(selected_option.get("value", "")) if selected_option else ""
    roster_div = soup.select_one(f"#coach_div_{selected_value}") if selected_value else None
    table = roster_div.select_one("table.rostertableforoster") if roster_div else None

    players: list[dict[str, str]] = []
    if table:
        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue
            number = _clean(cells[1].get_text(" ", strip=True))
            name_link = cells[2].select_one("a[href]")
            name = (
                _clean(name_link.get_text(" ", strip=True))
                if name_link
                else _clean(cells[2].get_text(" ", strip=True))
            )
            position = _clean(cells[3].get_text(" ", strip=True))
            profile_url = _absolute_url(name_link.get("href", ""), base_url) if name_link else ""
            if not name or name.lower() == "name":
                continue
            if number.lower() == "number" or position.lower() == "position":
                continue
            if name:
                players.append(
                    {
                        "number": number,
                        "name": name,
                        "position": position,
                        "profile_url": profile_url,
                    }
                )

    return {
        "season": season,
        "players": players,
    }


def _extract_schedule(html: str, base_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    record_text = _clean(soup.select_one("h3.record").get_text(" ", strip=True)) if soup.select_one("h3.record") else ""
    overall_record = ""
    league_record = ""
    record_match = re.search(r"Overall Record:\s*([0-9-]+),\s*League Record:\s*([0-9-]+)", record_text)
    if record_match:
        overall_record = record_match.group(1)
        league_record = record_match.group(2)

    events: list[dict[str, str]] = []
    for li in soup.select("li.schedule-and-results-list-item"):
        sport = li.select_one("div.sport")
        date_node = li.select_one("div.date")
        vs_node = li.select_one("div.vs")
        school_node = li.select_one("div.school")
        outcome_node = li.select_one("div.outcome")
        score_node = outcome_node.select_one("div.score") if outcome_node else None
        game_type = _clean(sport.get_text(" ", strip=True)) if sport else ""
        date = _clean(date_node.get_text(" ", strip=True)) if date_node else ""
        opponent_node = school_node.select_one("p") if school_node else None
        opponent = (
            _clean(opponent_node.get_text(" ", strip=True))
            if opponent_node
            else _clean(school_node.get_text(" ", strip=True))
            if school_node
            else ""
        )
        venue = _clean(vs_node.get_text(" ", strip=True)) if vs_node else ""
        result = _clean(outcome_node.get("result", "")) if outcome_node else ""
        score = _clean(score_node.get_text(" ", strip=True)) if score_node else ""
        location_link = school_node.select_one("a.location-link[href]") if school_node else None
        location_url = _absolute_url(location_link.get("href", ""), base_url) if location_link else ""
        events.append(
            {
                "game_type": game_type,
                "date": date,
                "venue": venue,
                "opponent": opponent,
                "result": result,
                "score": score,
                "location_url": location_url,
            }
        )

    return {
        "overall_record": overall_record,
        "league_record": league_record,
        "events": events,
    }


async def _capture_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1500)
    html = await page.content()
    body_text = _extract_body_text(html)
    return {
        "requested_url": url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "html": html,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape publicly available Crawford football data from official school pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, dict[str, Any]] = {}

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
                    captured = await _capture_page(page, url)
                    source_pages.append(captured["final_url"])
                    page_data[url] = captured
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"page_fetch_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    contact_data = _extract_contact_info(page_data.get(CONTACT_URL, {}).get("html", ""))

    football_schedule = _extract_schedule(
        page_data.get(FOOTBALL_SCHEDULE_URL, {}).get("html", ""),
        "https://www.chscolts.org/",
    )
    football_roster = _extract_roster(
        page_data.get(FOOTBALL_ROSTER_URL, {}).get("html", ""),
        "https://www.chscolts.org/",
    )
    football_coaches = _decode_team_coaches(
        _extract_team_coaches(
            page_data.get(FOOTBALL_COACHES_URL, {}).get("html", ""),
            "/coaching-staff/",
        )
    )

    flag_schedule = _extract_schedule(
        page_data.get(FLAG_SCHEDULE_URL, {}).get("html", ""),
        "https://www.chscolts.org/",
    )
    flag_roster = _extract_roster(
        page_data.get(FLAG_ROSTER_URL, {}).get("html", ""),
        "https://www.chscolts.org/",
    )
    flag_coaches = _decode_team_coaches(
        _extract_team_coaches(
            page_data.get(FLAG_COACHES_URL, {}).get("html", ""),
            "/coaching-staff/",
        )
    )

    extracted_items = {
        "school_contact": {
            "school_name": SCHOOL_NAME,
            "school_home_url": SCHOOL_HOME_URL,
            "contact_url": CONTACT_URL,
            **contact_data,
        },
        "football": {
            "team_name": "Football",
            "team_home_url": FOOTBALL_HOME_URL,
            "schedule_url": FOOTBALL_SCHEDULE_URL,
            "roster_url": FOOTBALL_ROSTER_URL,
            "coaches_url": FOOTBALL_COACHES_URL,
            "coaches": football_coaches,
            "schedule": football_schedule,
            "roster": football_roster,
        },
        "girls_flag_football": {
            "team_name": "Flag Football, Girls",
            "team_home_url": FLAG_HOME_URL,
            "schedule_url": FLAG_SCHEDULE_URL,
            "roster_url": FLAG_ROSTER_URL,
            "coaches_url": FLAG_COACHES_URL,
            "coaches": flag_coaches,
            "schedule": flag_schedule,
            "roster": flag_roster,
        },
        "athletics_department": {
            "athletics_home_url": ATHLETICS_HOME_URL,
            "coaching_staff_url": ATHLETICS_DEPARTMENT_URL,
        },
    }

    if not any(
        item
        for item in (
            football_schedule.get("events"),
            football_roster.get("players"),
            football_coaches,
            flag_schedule.get("events"),
            flag_roster.get("players"),
            flag_coaches,
        )
        if item
    ):
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    payload = {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script": __file__,
        },
        "errors": errors,
    }
    return payload


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True))
