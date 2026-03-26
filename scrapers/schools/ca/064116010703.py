from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

NCES_ID = "064116010703"
SCHOOL_NAME = "El Diamante High"
STATE = "CA"

SCHOOL_HOME_URL = "https://eldiamante.vusd.org/"
ATHLETICS_HOME_URL = "https://www.edhsminersathletics.com/"
FOOTBALL_TEAM_URL = "https://www.edhsminersathletics.com/sport/football/boys/"
FOOTBALL_SCHEDULE_URL = (
    "https://www.edhsminersathletics.com/sport/RefreshScheduleStyle3ViewComponent/132101?schoolYear=2025-2026"
)
FOOTBALL_ROSTER_URL = (
    "https://www.edhsminersathletics.com/sport/RefreshRosterAthleteStyle2ViewComponent/132101?schoolYear=2025-2026"
)
FOOTBALL_STAFF_URL = (
    "https://www.edhsminersathletics.com/sport/RefreshRosterStaffStyle4ViewComponent/132101?schoolYear=2025-2026"
)

REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}
TIMEOUT = 25


def _clean(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = _clean(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.text


def _extract_home_signals(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    lines: list[str] = []
    links: list[str] = []
    for text in soup.stripped_strings:
        cleaned = _clean(text)
        if any(token in cleaned.lower() for token in ("football", "athletics", "miners")):
            lines.append(cleaned)
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        text = _clean(anchor.get_text(" ", strip=True))
        if any(token in href.lower() for token in ("football", "athletic", "sport")) or any(
            token in text.lower() for token in ("football", "athletic", "sport")
        ):
            links.append(href)
    return {
        "home_lines": _dedupe_keep_order(lines),
        "home_links": _dedupe_keep_order(links),
        "title": _clean(soup.title.get_text(" ", strip=True) if soup.title else ""),
    }


def _parse_schedule_rows(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    selected_level = _clean(soup.select_one("#content-title-level").get_text(" ", strip=True) if soup.select_one("#content-title-level") else "")
    selected_sport = _clean(soup.select_one("#content-title-sport").get_text(" ", strip=True) if soup.select_one("#content-title-sport") else "")
    selected_year = _clean(soup.select_one("#content-title-year").get_text(" ", strip=True) if soup.select_one("#content-title-year") else "")

    level_options = [
        _clean(option.get_text(" ", strip=True))
        for option in soup.select('select[id^="sub-"] option')
    ]
    year_options = [
        _clean(option.get_text(" ", strip=True))
        for option in soup.select('select[id^="school-year-"] option')
    ]

    schedule_rows: list[dict[str, Any]] = []
    for tr in soup.select("#schedule-table tbody tr"):
        cells = [ _clean(td.get_text(" ", strip=True)) for td in tr.find_all("td") ]
        cells = [cell for cell in cells if cell]
        if len(cells) < 2:
            continue
        main_text = cells[0]
        site_text = cells[1] if len(cells) > 1 else ""
        result_text = cells[2] if len(cells) > 2 else ""

        match = re.match(
            r"^(?P<date>[A-Za-z]{3}\s+\d{1,2})\s*/\s*(?P<time>\d{1,2}:\d{2}\s+[AP]M)\s+(?P<location_type>AT|VS)\s+(?P<rest>.+)$",
            main_text,
        )
        date_text = ""
        time_text = ""
        location_type = ""
        rest = main_text
        if match:
            date_text = match.group("date")
            time_text = match.group("time")
            location_type = match.group("location_type")
            rest = match.group("rest")

        opponent = rest
        if site_text and site_text in rest:
            opponent = _clean(rest.rpartition(site_text)[0])

        schedule_rows.append(
            {
                "date": date_text,
                "time": time_text,
                "location_type": location_type,
                "opponent": opponent,
                "site": site_text,
                "result": result_text,
                "raw": main_text,
            }
        )

    practice_rows: list[dict[str, Any]] = []
    for tr in soup.select("#practice-schedule-table tbody tr"):
        cells = [ _clean(td.get_text(" ", strip=True)) for td in tr.find_all("td") ]
        cells = [cell for cell in cells if cell]
        if len(cells) < 2:
            continue
        practice_rows.append(
            {
                "date_time": cells[0],
                "site": cells[1] if len(cells) > 1 else "",
                "description": cells[2] if len(cells) > 2 else "",
                "raw": " | ".join(cells),
            }
        )

    return {
        "football_selected_level": selected_level,
        "football_selected_sport": selected_sport,
        "football_selected_school_year": selected_year,
        "football_levels": [value for value in _dedupe_keep_order(level_options) if value],
        "available_school_years": [value for value in _dedupe_keep_order(year_options) if value],
        "football_schedule_rows": schedule_rows,
        "football_practice_rows": practice_rows,
    }


def _parse_roster_rows(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    roster_players: list[dict[str, Any]] = []
    for tr in soup.select("#athlete-table tbody tr"):
        cells = [_clean(td.get_text(" ", strip=True)) for td in tr.find_all("td")]
        cells = [cell for cell in cells if cell or len(cells) > 1]
        if len(cells) < 3:
            continue
        jersey_number = cells[0] if len(cells) > 1 and re.fullmatch(r"\d+", cells[1]) else ""
        athlete_text = cells[2] if len(cells) > 2 else ""
        grade = cells[3] if len(cells) > 3 else ""

        name = athlete_text
        position = ""
        weight_lbs = ""
        match = re.match(r"^(?P<name>.+?)\s+(?P<position>[A-Z ,/.-]+?)\s+(?P<weight>\d+)\s+LBS$", athlete_text)
        if match:
            name = _clean(match.group("name"))
            position = _clean(match.group("position"))
            weight_lbs = match.group("weight")

        roster_players.append(
            {
                "jersey_number": jersey_number,
                "name": name,
                "position": position,
                "weight_lbs": weight_lbs,
                "grade": grade,
                "raw": athlete_text,
            }
        )

    return {
        "football_roster_players": roster_players,
        "football_roster_count": len(roster_players),
        "football_roster_names": _dedupe_keep_order([item["name"] for item in roster_players if item["name"]]),
    }


def _parse_staff_rows(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    staff_members: list[dict[str, Any]] = []
    for tr in soup.select("#staff-table tbody tr"):
        name = _clean(tr.select_one(".staff-name").get_text(" ", strip=True) if tr.select_one(".staff-name") else "")
        info_text = _clean(tr.select_one(".staff-info").get_text(" ", strip=True) if tr.select_one(".staff-info") else "")
        role = _clean(info_text.replace("Role:", "", 1))
        email = ""
        email_anchor = tr.find("a", href=re.compile(r"^mailto:", re.I))
        if email_anchor and email_anchor.get("href"):
            email = email_anchor["href"].split("mailto:", 1)[1].strip()
        photo = ""
        img = tr.find("img", src=True)
        if img:
            photo = img["src"].strip()
        if not name and not email and not role:
            continue
        staff_members.append(
            {
                "name": name,
                "role": role,
                "email": email,
                "photo_url": photo,
            }
        )

    return {
        "football_staff": staff_members,
        "football_staff_names": _dedupe_keep_order([item["name"] for item in staff_members if item["name"]]),
        "football_staff_emails": _dedupe_keep_order([item["email"] for item in staff_members if item["email"]]),
    }


def _scrape_sync() -> dict[str, Any]:
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    home_html = _fetch_html(session, SCHOOL_HOME_URL)
    athletics_html = _fetch_html(session, ATHLETICS_HOME_URL)
    football_html = _fetch_html(session, FOOTBALL_TEAM_URL)
    schedule_html = _fetch_html(session, FOOTBALL_SCHEDULE_URL)
    roster_html = _fetch_html(session, FOOTBALL_ROSTER_URL)
    staff_html = _fetch_html(session, FOOTBALL_STAFF_URL)

    home_signals = _extract_home_signals(home_html)
    athletics_signals = _extract_home_signals(athletics_html)
    football_signals = _extract_home_signals(football_html)
    schedule_signals = _parse_schedule_rows(schedule_html)
    roster_signals = _parse_roster_rows(roster_html)
    staff_signals = _parse_staff_rows(staff_html)

    football_program_evidence = _dedupe_keep_order(
        home_signals["home_lines"]
        + athletics_signals["home_lines"]
        + football_signals["home_lines"]
        + [
            schedule_signals["football_selected_sport"],
            schedule_signals["football_selected_level"],
            schedule_signals["football_selected_school_year"],
            "Head Football Coach Introduction",
        ]
        + [row["raw"] for row in schedule_signals["football_schedule_rows"][:5]]
        + roster_signals["football_roster_names"][:10]
        + staff_signals["football_staff_names"]
    )

    football_program_available = bool(
        schedule_signals["football_schedule_rows"]
        or roster_signals["football_roster_players"]
        or staff_signals["football_staff"]
    )

    extracted_items: dict[str, Any] = {
        "official_school_home_url": SCHOOL_HOME_URL,
        "official_athletics_home_url": ATHLETICS_HOME_URL,
        "official_football_team_url": FOOTBALL_TEAM_URL,
        "official_football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "official_football_roster_url": FOOTBALL_ROSTER_URL,
        "official_football_staff_url": FOOTBALL_STAFF_URL,
        "football_program_available": football_program_available,
        "football_team_name": "Football",
        "football_levels": schedule_signals["football_levels"],
        "available_school_years": schedule_signals["available_school_years"],
        "football_selected_level": schedule_signals["football_selected_level"],
        "football_selected_sport": schedule_signals["football_selected_sport"],
        "football_selected_school_year": schedule_signals["football_selected_school_year"],
        "football_schedule_rows": schedule_signals["football_schedule_rows"],
        "football_practice_rows": schedule_signals["football_practice_rows"],
        "football_roster_players": roster_signals["football_roster_players"],
        "football_roster_count": roster_signals["football_roster_count"],
        "football_roster_names": roster_signals["football_roster_names"],
        "football_staff": staff_signals["football_staff"],
        "football_staff_names": staff_signals["football_staff_names"],
        "football_staff_emails": staff_signals["football_staff_emails"],
        "football_home_lines": home_signals["home_lines"],
        "football_home_links": home_signals["home_links"],
        "football_program_evidence": football_program_evidence,
    }

    errors: list[str] = []
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_football_site")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": [
            SCHOOL_HOME_URL,
            ATHLETICS_HOME_URL,
            FOOTBALL_TEAM_URL,
            FOOTBALL_SCHEDULE_URL,
            FOOTBALL_ROSTER_URL,
            FOOTBALL_STAFF_URL,
        ],
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "2026-03-23",
            "source_type": "requests",
            "football_school_year": schedule_signals["football_selected_school_year"],
        },
        "errors": errors,
    }


async def scrape_school() -> dict[str, Any]:
    return await asyncio.to_thread(_scrape_sync)


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
