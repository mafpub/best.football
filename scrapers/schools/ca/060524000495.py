"""Deterministic football scraper for Golden Sierra Junior Senior High (CA)."""

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

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060524000495"
SCHOOL_NAME = "Golden Sierra Junior Senior High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://goldensierra.bomusd.org/"
ATHLETICS_HOME_URL = "https://sites.google.com/bomusd.org/gshsathletics/home"
FOOTBALL_URL = "https://sites.google.com/bomusd.org/gshsathletics/football"
SCHEDULE_SHEET_URL = "https://drive.google.com/open?id=1u_LrDH7JQpLcc2NVR5jlZXqxIEhgf6JbEbJDNB-smiI"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_URL,
    SCHEDULE_SHEET_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _normalize_url(base_url: str, href: str | None) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    return urljoin(base_url, value)


def _visible_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    lines = [_clean(line) for line in text.splitlines()]
    return [line for line in lines if line]


def _first_link(
    soup: BeautifulSoup,
    *,
    base_url: str,
    href_contains: str | None = None,
    text_contains: str | None = None,
) -> str:
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        text = _clean(anchor.get_text(" ", strip=True))
        if href_contains and href_contains not in href:
            continue
        if text_contains and text_contains.lower() not in text.lower():
            continue
        return _normalize_url(base_url, href)
    return ""


def _extract_section_names(
    lines: list[str],
    start_marker: str,
    stop_markers: set[str],
) -> list[str]:
    try:
        start = lines.index(start_marker)
    except ValueError:
        return []

    values: list[str] = []
    for line in lines[start + 1 :]:
        if line in stop_markers:
            break
        if line in {"Report abuse", "Page details", "Page updated"}:
            break
        if re.fullmatch(r"\d+", line):
            continue
        if line in {"202", "5", "-2", "6"}:
            continue
        values.append(line)
    return _dedupe_keep_order(values)


def _extract_practice_windows(lines: list[str]) -> list[dict[str, str]]:
    windows: list[dict[str, str]] = []

    try:
        start = lines.index("July 15-")
        # The page breaks the date range and details across adjacent lines.
        date_range = f"{lines[start]} {lines[start + 1]}".replace("- ", "- ")
        if lines[start + 2] == "Limited Season (pre-season)":
            windows.append(
                {
                    "phase": "Limited Season (pre-season)",
                    "date_range": date_range,
                    "days": lines[start + 3],
                    "schedule": [
                        lines[start + 4],
                        lines[start + 5],
                    ],
                }
            )
    except (ValueError, IndexError):
        pass

    try:
        start = lines.index("July 28")
        if lines[start + 1] == "Practices Begin":
            windows.append(
                {
                    "phase": "Practices Begin",
                    "date": lines[start],
                    "days": lines[start + 2],
                    "schedule": [lines[start + 3]],
                }
            )
    except (ValueError, IndexError):
        pass

    try:
        start = lines.index("End of Season-")
        windows.append(
            {
                "phase": "End of Season",
                "date": f"{lines[start]} {lines[start + 1]}".strip(),
                "notes": lines[start + 2] if start + 2 < len(lines) else "",
            }
        )
    except (ValueError, IndexError):
        pass

    return windows


def _extract_football_page(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    lines = _visible_lines(html)

    maxpreps_url = _first_link(
        soup,
        base_url=FOOTBALL_URL,
        href_contains="maxpreps.com/high-schools",
    )
    athletic_packet_url = _first_link(
        soup,
        base_url=FOOTBALL_URL,
        href_contains="docs.google.com/forms",
    )
    schedule_sheet_url = _first_link(
        soup,
        base_url=FOOTBALL_URL,
        href_contains="drive.google.com/open?id=",
    )
    coach_email_url = _first_link(
        soup,
        base_url=FOOTBALL_URL,
        href_contains="mailto:mpfeifer@bomusd.org",
    )

    coach_names = _dedupe_keep_order(
        [
            "Mark Pfeifer" if "Mark Pfeifer" in lines else "",
            "Becharis McGill" if "Becharis McGill" in lines else "",
            "Jeff Barcal" if "Jeff Barcal" in lines else "",
        ]
    )

    assistant_coach_names = _dedupe_keep_order(
        [name for name in ["Jeff Barcal"] if name in lines]
    )

    varsity_roster_names = _extract_section_names(
        lines,
        "Varsity Roster 2025-26",
        {"Junior Varsity"},
    )
    junior_varsity_roster_names = _extract_section_names(
        lines,
        "Junior Varsity",
        {"Report abuse", "Page details", "Page updated"},
    )

    practice_windows = _extract_practice_windows(lines)

    contact_email = ""
    contact_name = ""
    for line in lines:
        if line == "Mark Pfeifer":
            contact_name = line
            break
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        if href.startswith("mailto:mpfeifer@bomusd.org"):
            contact_email = href.removeprefix("mailto:")
            break

    return {
        "football_page_url": FOOTBALL_URL,
        "football_page_title": _clean(soup.title.get_text(" ", strip=True) if soup.title else ""),
        "maxpreps_url": maxpreps_url,
        "athletic_packet_url": athletic_packet_url,
        "schedule_sheet_url": schedule_sheet_url,
        "head_coach_name": contact_name,
        "head_coach_email": contact_email or coach_email_url.removeprefix("mailto:"),
        "jv_coach_name": "Becharis McGill" if "Becharis McGill" in lines else "",
        "assistant_coach_names": assistant_coach_names,
        "coach_names": coach_names,
        "practice_windows": practice_windows,
        "varsity_roster_player_names": varsity_roster_names,
        "junior_varsity_roster_player_names": junior_varsity_roster_names,
    }


def _extract_schedule_sheet(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return {
            "schedule_sheet_title": _clean(soup.title.get_text(" ", strip=True) if soup.title else ""),
            "schedule_summary": [],
            "schedule_rows": [],
            "coach_contacts": [],
            "school_contact": {},
        }

    rows = tables[0].find_all("tr")
    schedule_summary: list[str] = []
    schedule_rows: list[dict[str, str]] = []
    coach_contacts: list[dict[str, str]] = []
    school_contact = {
        "address": "",
        "phone": "",
        "updated": "",
    }

    for row in rows:
        cells = [_clean(td.get_text(" ", strip=True)) for td in row.find_all(["th", "td"])]
        if len(cells) < 6:
            cells.extend([""] * (6 - len(cells)))
        label = cells[1]

        if label in {
            "Summer 6/1-6/30~ No contact 7/1-7/14",
            "Limited Season 7/15-7/26 ~ First Practice 7/27 ~ First Scrimmage 8/21",
            "First Contest 8/28 ~ Last Contest 11/7 (10 contacts)",
        }:
            schedule_summary.append(label)
            continue

        if label in {"FRI", "FRI/SAT", "SAT", "MON", "TUE", "WED", "THU"} and cells[2]:
            schedule_rows.append(
                {
                    "day": label,
                    "date": cells[2],
                    "opponent": cells[3],
                    "time": cells[4],
                    "bus": cells[5],
                }
            )
            continue

        if label in {"Head Coach", "JV Coach"}:
            coach_contacts.append(
                {
                    "role": label.rstrip(":"),
                    "name": cells[3],
                    "email": cells[4],
                }
            )
            continue

        if cells[1] == "5101 Garden Valley Road":
            school_contact["address"] = cells[1]
        elif cells[1] == "Garden Valley, CA 95633":
            school_contact["address"] = f"{school_contact['address']}, {cells[1]}".strip(", ")
        elif cells[1] == "(530) 333-8330":
            school_contact["phone"] = cells[1]
        elif cells[1] == "Updated 1/6/26":
            school_contact["updated"] = cells[1]

    return {
        "schedule_sheet_title": _clean(soup.title.get_text(" ", strip=True) if soup.title else ""),
        "schedule_summary": schedule_summary,
        "schedule_rows": schedule_rows,
        "coach_contacts": coach_contacts,
        "school_contact": school_contact,
    }


async def _fetch_html(request_context, url: str) -> tuple[str, str]:
    response = await request_context.get(url, timeout=60000)
    body = await response.text()
    return body, response.url


async def scrape_school() -> dict[str, Any]:
    """Scrape Golden Sierra football from public homepage, athletics page, and sheet."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    errors: list[str] = []
    home_html = ""
    athletics_html = ""
    football_html = ""
    schedule_html = ""
    athletics_url = ATHLETICS_HOME_URL
    football_url = FOOTBALL_URL
    schedule_url = SCHEDULE_SHEET_URL

    async with async_playwright() as playwright:
        request_context = await playwright.request.new_context(
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
            extra_http_headers={"User-Agent": USER_AGENT},
        )
        try:
            home_html, home_final_url = await _fetch_html(request_context, HOME_URL)
            source_pages.append(home_final_url)
            home_soup = BeautifulSoup(home_html, "html.parser")
            athletics_url = _first_link(
                home_soup,
                base_url=HOME_URL,
                href_contains="gshsathletics/home",
                text_contains="athletics",
            ) or ATHLETICS_HOME_URL

            athletics_html, athletics_final_url = await _fetch_html(request_context, athletics_url)
            source_pages.append(athletics_final_url)
            athletics_soup = BeautifulSoup(athletics_html, "html.parser")
            football_url = _first_link(
                athletics_soup,
                base_url=ATHLETICS_HOME_URL,
                href_contains="/gshsathletics/football",
            ) or FOOTBALL_URL

            football_html, football_final_url = await _fetch_html(request_context, football_url)
            source_pages.append(football_final_url)
            football_data = _extract_football_page(football_html)
            if football_data.get("schedule_sheet_url"):
                schedule_url = football_data["schedule_sheet_url"]

            schedule_html, schedule_final_url = await _fetch_html(request_context, schedule_url)
            source_pages.append(schedule_final_url)
            schedule_data = _extract_schedule_sheet(schedule_html)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_request_failed:{type(exc).__name__}")
            football_data = {
                "football_page_url": football_url,
                "football_page_title": "",
                "maxpreps_url": "",
                "athletic_packet_url": "",
                "schedule_sheet_url": schedule_url,
                "head_coach_name": "",
                "head_coach_email": "",
                "jv_coach_name": "",
                "assistant_coach_names": [],
                "coach_names": [],
                "practice_windows": [],
                "varsity_roster_player_names": [],
                "junior_varsity_roster_player_names": [],
            }
            schedule_data = {
                "schedule_sheet_title": "",
                "schedule_summary": [],
                "schedule_rows": [],
                "coach_contacts": [],
                "school_contact": {},
            }
        finally:
            await request_context.dispose()

    source_pages = _dedupe_keep_order(source_pages)

    football_program_available = bool(
        football_data.get("coach_names")
        or football_data.get("practice_windows")
        or football_data.get("varsity_roster_player_names")
        or football_data.get("junior_varsity_roster_player_names")
        or schedule_data.get("schedule_rows")
    )

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_home_url": HOME_URL,
        "athletics_home_url": athletics_url,
        "football_page_url": football_data.get("football_page_url", football_url),
        "football_page_title": football_data.get("football_page_title", ""),
        "maxpreps_url": football_data.get("maxpreps_url", ""),
        "athletic_packet_url": football_data.get("athletic_packet_url", ""),
        "schedule_sheet_url": football_data.get("schedule_sheet_url", schedule_url),
        "head_coach_name": football_data.get("head_coach_name", ""),
        "head_coach_email": football_data.get("head_coach_email", ""),
        "jv_coach_name": football_data.get("jv_coach_name", ""),
        "assistant_coach_names": football_data.get("assistant_coach_names", []),
        "coach_names": football_data.get("coach_names", []),
        "practice_windows": football_data.get("practice_windows", []),
        "varsity_roster_player_names": football_data.get("varsity_roster_player_names", []),
        "junior_varsity_roster_player_names": football_data.get("junior_varsity_roster_player_names", []),
        "schedule_sheet_title": schedule_data.get("schedule_sheet_title", ""),
        "schedule_summary": schedule_data.get("schedule_summary", []),
        "football_schedule_rows": schedule_data.get("schedule_rows", []),
        "football_coach_contacts": schedule_data.get("coach_contacts", []),
        "school_contact": schedule_data.get("school_contact", {}),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "football_roster_player_count": len(football_data.get("varsity_roster_player_names", []))
            + len(football_data.get("junior_varsity_roster_player_names", [])),
            "football_schedule_row_count": len(schedule_data.get("schedule_rows", [])),
            "football_coach_count": len(football_data.get("coach_names", [])),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
