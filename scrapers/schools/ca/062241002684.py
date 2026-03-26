"""Deterministic football scraper for Lompoc High (CA)."""

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

NCES_ID = "062241002684"
SCHOOL_NAME = "Lompoc High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

DISTRICT_URL = "https://www.lusd.org/"
SCHOOL_HOME_URL = "https://lompochighschool.lusd.org/"
ATHLETICS_HOME_URL = "https://lompochighschool.lusd.org/athletics"
COACHES_URL = "https://lompocathletics.olinesports.com/coaches.php"
LINKS_FORMS_URL = "https://lompocathletics.olinesports.com/links.php"
VARSITY_COACH_BIO_URL = "https://lompocathletics.olinesports.com/bio_coach.php?coach_id=109"

TEAM_CONFIGS = [
    {"level": "Varsity", "sport_id": "683"},
    {"level": "JV", "sport_id": "725"},
    {"level": "Frosh", "sport_id": "726"},
]


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        marker = repr(value)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(value)
    return out


def _team_urls(sport_id: str) -> dict[str, str]:
    return {
        "news_url": f"https://lompocathletics.olinesports.com/news.php?sport={sport_id}",
        "announcements_url": f"https://lompocathletics.olinesports.com/announcements.php?sport={sport_id}",
        "schedule_url": f"https://lompocathletics.olinesports.com/schedule.php?sport={sport_id}",
        "printable_schedule_url": (
            f"https://lompocathletics.olinesports.com/schedule_printable.php?sport={sport_id}"
        ),
        "roster_url": f"https://lompocathletics.olinesports.com/roster.php?sport={sport_id}",
        "photo_gallery_url": f"https://lompocathletics.olinesports.com/photos.php?sport={sport_id}",
        "videos_url": f"https://lompocathletics.olinesports.com/videos.php?sport={sport_id}",
    }


def _build_target_urls() -> list[str]:
    urls = [
        DISTRICT_URL,
        SCHOOL_HOME_URL,
        ATHLETICS_HOME_URL,
        COACHES_URL,
        LINKS_FORMS_URL,
        VARSITY_COACH_BIO_URL,
    ]
    for config in TEAM_CONFIGS:
        urls.extend(_team_urls(config["sport_id"]).values())
    return _dedupe_keep_order(urls)


TARGET_URLS = _build_target_urls()


async def _goto(page, url: str, source_pages: list[str], errors: list[str]) -> bool:
    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(1500)
        if response is not None and response.status >= 400:
            errors.append(f"http_status:{response.status}:{url}")
        source_pages.append(page.url)
        return True
    except Exception as exc:  # noqa: BLE001
        errors.append(f"navigation_failed:{url}:{type(exc).__name__}")
        return False


async def _extract_links(page) -> list[dict[str, str]]:
    links = await page.locator("a[href]").evaluate_all(
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || el.getAttribute('href') || ''
        }))""",
    )
    normalized: list[dict[str, str]] = []
    for item in links or []:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        normalized.append(
            {
                "text": _clean(str(item.get("text") or "")),
                "href": href,
            }
        )
    return normalized


async def _extract_table_rows(page) -> list[list[str]]:
    rows = await page.locator("tr").evaluate_all(
        """rows => rows.map(row =>
            Array.from(row.querySelectorAll('td,th'))
                .map(cell => (cell.textContent || '').replace(/\\s+/g, ' ').trim())
                .filter(Boolean)
        ).filter(cells => cells.length > 0)""",
    )
    normalized: list[list[str]] = []
    for row in rows or []:
        if not isinstance(row, list):
            continue
        cells = [_clean(str(cell)) for cell in row if _clean(str(cell))]
        if cells:
            normalized.append(cells)
    return normalized


def _parse_printable_schedule_rows(rows: list[list[str]]) -> list[dict[str, str]]:
    header = ["Date", "Day", "Opponent", "Location", "Time/Score"]
    header_index = -1
    for idx, row in enumerate(rows):
        if row[:5] == header:
            header_index = idx
            break
    if header_index == -1:
        return []

    games: list[dict[str, str]] = []
    for row in rows[header_index + 1 :]:
        if row == ["Home games are in bold."]:
            break
        if len(row) < 4:
            continue
        date, day, opponent, location = row[:4]
        time_score = row[4] if len(row) > 4 else ""
        games.append(
            {
                "date": date,
                "day": day,
                "opponent": opponent,
                "location": location,
                "time_or_score": time_score,
            }
        )
    return games


def _parse_roster_rows(rows: list[list[str]]) -> tuple[str, list[str]]:
    roster_header = ["Number", "Name", "Year", "Ht/Wt", "Position"]
    header_index = -1
    for idx, row in enumerate(rows):
        if row[:5] == roster_header:
            header_index = idx
            break
    if header_index == -1:
        return "", []

    head_coach = ""
    names: list[str] = []
    for row in rows[header_index + 1 :]:
        text = _clean(" ".join(row))
        if not text:
            continue
        if text.startswith("Head Coach "):
            head_coach = _clean(text.replace("Head Coach ", "", 1))
            continue
        if any(
            marker in text
            for marker in (
                "Choose a Sport or Activity",
                "News | Announcements",
                "Scoreboard",
                "Email Updates",
                "Sponsors",
            )
        ):
            continue
        if len(row) == 1 and re.fullmatch(r"[A-Za-z.'-]+(?: [A-Za-z.'-]+)+", row[0]):
            names.append(row[0])
    return head_coach, _dedupe_keep_order(names)


def _parse_football_coaches(rows: list[list[str]], links: list[dict[str, str]]) -> list[dict[str, str]]:
    coach_lookup: dict[str, str] = {}
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if text and href:
            coach_lookup[text] = href

    coaches: list[dict[str, str]] = []
    for row in rows:
        if len(row) < 3:
            continue
        sport_name, coach_name, role = row[:3]
        if "football - boys" not in sport_name.lower():
            continue
        coaches.append(
            {
                "team": sport_name,
                "name": coach_name,
                "role": role,
                "profile_url": coach_lookup.get(coach_name, ""),
            }
        )
    return _dedupe_keep_order(coaches)


def _parse_contact_lines(lines: list[str]) -> dict[str, str]:
    email = ""
    phone = ""
    for line in lines:
        if not email:
            match = re.search(r"Email:\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", line, re.I)
            if match:
                email = match.group(1)
        if not phone:
            match = re.search(r"Phone:\s*([0-9()+\-\s]{7,})", line, re.I)
            if match:
                phone = _clean(match.group(1))
    return {"email": email, "phone": phone}


def _filter_relevant_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    kept = []
    for link in links:
        haystack = f"{link.get('text', '')} {link.get('href', '')}"
        if re.search(r"gofan|max preps|maxpreps|physical|transport|progress report|record", haystack, re.I):
            kept.append(link)
    return _dedupe_keep_order(kept)


async def scrape_school() -> dict[str, Any]:
    """Scrape publicly available football data for Lompoc High School."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    requested_pages = list(TARGET_URLS)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            school_athletics_links: list[dict[str, str]] = []
            coaches_rows: list[list[str]] = []
            coaches_links: list[dict[str, str]] = []
            varsity_contact_lines: list[str] = []
            links_forms_links: list[dict[str, str]] = []
            team_summaries: list[dict[str, Any]] = []

            if await _goto(page, SCHOOL_HOME_URL, source_pages, errors):
                school_athletics_links = [
                    link
                    for link in await _extract_links(page)
                    if re.search(r"athletics|coaches|schedules", f"{link['text']} {link['href']}", re.I)
                ]

            await _goto(page, ATHLETICS_HOME_URL, source_pages, errors)

            if await _goto(page, COACHES_URL, source_pages, errors):
                coaches_rows = await _extract_table_rows(page)
                coaches_links = await _extract_links(page)

            if await _goto(page, VARSITY_COACH_BIO_URL, source_pages, errors):
                body_text = await page.locator("body").inner_text()
                varsity_contact_lines = [_clean(line) for line in body_text.splitlines() if _clean(line)]

            if await _goto(page, LINKS_FORMS_URL, source_pages, errors):
                links_forms_links = await _extract_links(page)

            for config in TEAM_CONFIGS:
                urls = _team_urls(config["sport_id"])
                schedule_rows: list[list[str]] = []
                roster_rows: list[list[str]] = []

                if await _goto(page, urls["printable_schedule_url"], source_pages, errors):
                    schedule_rows = await _extract_table_rows(page)

                if await _goto(page, urls["roster_url"], source_pages, errors):
                    roster_rows = await _extract_table_rows(page)

                roster_head_coach, roster_names = _parse_roster_rows(roster_rows)
                games = _parse_printable_schedule_rows(schedule_rows)
                team_summaries.append(
                    {
                        "level": config["level"],
                        "sport_id": config["sport_id"],
                        "team_page_url": urls["schedule_url"],
                        "schedule_url": urls["schedule_url"],
                        "printable_schedule_url": urls["printable_schedule_url"],
                        "roster_url": urls["roster_url"],
                        "news_url": urls["news_url"],
                        "announcements_url": urls["announcements_url"],
                        "photo_gallery_url": urls["photo_gallery_url"],
                        "videos_url": urls["videos_url"],
                        "roster_head_coach": roster_head_coach,
                        "roster_count": len(roster_names),
                        "roster_sample": roster_names[:12],
                        "schedule": games,
                        "has_current_content": bool(games or roster_names),
                    }
                )
        finally:
            await browser.close()

    football_coaches = _parse_football_coaches(coaches_rows, coaches_links)
    varsity_contact = _parse_contact_lines(varsity_contact_lines)

    for coach in football_coaches:
        if coach.get("name") == "Andrew Jones":
            coach["email"] = varsity_contact.get("email", "")
            coach["phone"] = varsity_contact.get("phone", "")

    football_levels = [team["level"] for team in team_summaries]
    football_schedule_links = [
        {
            "level": team["level"],
            "schedule_url": team["schedule_url"],
            "printable_schedule_url": team["printable_schedule_url"],
        }
        for team in team_summaries
    ]
    roster_counts = {team["level"]: team["roster_count"] for team in team_summaries}
    roster_samples = {team["level"]: team["roster_sample"] for team in team_summaries}
    schedules = {team["level"]: team["schedule"] for team in team_summaries}
    home_locations = _dedupe_keep_order(
        [
            game["location"]
            for team in team_summaries
            for game in team["schedule"]
            if game.get("location") and game["location"].lower() in {"lompoc", "homecoming"}
        ]
    )
    relevant_links = _filter_relevant_links(links_forms_links)

    football_program_available = bool(
        football_coaches
        or any(team["schedule"] for team in team_summaries)
        or any(team["roster_count"] for team in team_summaries)
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_home_url": SCHOOL_HOME_URL,
        "athletics_home_url": ATHLETICS_HOME_URL,
        "athletics_platform_url": "https://lompocathletics.olinesports.com/",
        "school_athletics_links": _dedupe_keep_order(school_athletics_links),
        "football_levels": football_levels,
        "football_team_pages": [
            {
                "level": team["level"],
                "sport_id": team["sport_id"],
                "team_page_url": team["team_page_url"],
                "news_url": team["news_url"],
                "roster_url": team["roster_url"],
                "schedule_url": team["schedule_url"],
                "printable_schedule_url": team["printable_schedule_url"],
                "has_current_content": team["has_current_content"],
            }
            for team in team_summaries
        ],
        "football_coaches": football_coaches,
        "football_schedule_links": football_schedule_links,
        "football_schedules": schedules,
        "football_roster_counts": roster_counts,
        "football_roster_samples": roster_samples,
        "football_links_and_forms": relevant_links,
        "football_home_game_locations": home_locations,
        "varsity_head_coach_contact": {
            "name": "Andrew Jones",
            "role": "Head Coach",
            "email": varsity_contact.get("email", ""),
            "phone": varsity_contact.get("phone", ""),
            "profile_url": VARSITY_COACH_BIO_URL,
        },
    }

    if not football_program_available:
        errors.append("no_public_football_content_found")

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "focus": "football_only",
            "pages_requested": requested_pages,
            "pages_visited": len(_dedupe_keep_order(source_pages)),
            **proxy_meta,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
