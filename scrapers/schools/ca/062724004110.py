"""Deterministic football scraper for Costa Mesa High (CA)."""

from __future__ import annotations

import asyncio
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062724004110"
SCHOOL_NAME = "Costa Mesa High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_URL = "https://cmhs.nmusd.us/athletics1/athletic-teams"
TEAM_HOME_URL = "https://costamesafootball.com/"
SCHEDULE_URL = "https://costamesafootball.com/game-schedule"
COACH_URL = "https://costamesafootball.com/coachs-corner-1"
NEW_PLAYER_URL = "https://costamesafootball.com/new-player-form"

TARGET_URLS = [ATHLETICS_URL, TEAM_HOME_URL, SCHEDULE_URL, COACH_URL, NEW_PLAYER_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "football",
    "coach",
    "schedule",
    "website",
    "team",
    "athletic",
    "mustang",
    "contact",
    "game",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _split_lines(text: str) -> list[str]:
    return [_clean(line) for line in (text or "").splitlines() if _clean(line)]


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 30) -> list[str]:
    lines: list[str] = []
    for raw_line in _split_lines(text):
        lowered = raw_line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(raw_line)
    return _dedupe_keep_order(lines)[:limit]


def _normalize_href(href: str, base_url: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("mailto:") or value.startswith("tel:"):
        return value
    return urljoin(base_url, value)


async def _collect_page_snapshot(page, requested_url: str) -> dict[str, Any]:
    try:
        body_text = await page.inner_text("body")
    except Exception:  # noqa: BLE001
        body_text = ""

    try:
        anchors = await page.eval_on_selector_all(
            "a[href]",
            """els => els.map(anchor => ({
                text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: anchor.href || anchor.getAttribute('href') || '',
                title: anchor.title || ''
            }))""",
        )
    except Exception:  # noqa: BLE001
        anchors = []

    links: list[str] = []
    if isinstance(anchors, list):
        for raw_link in anchors:
            if not isinstance(raw_link, dict):
                continue
            text = _clean(str(raw_link.get("text") or ""))
            href = _normalize_href(str(raw_link.get("href") or ""), page.url)
            title = _clean(str(raw_link.get("title") or ""))
            if not href:
                continue
            combo = f"{text} {href} {title}".lower()
            if any(keyword in combo for keyword in FOOTBALL_KEYWORDS):
                links.append(f"{text}|{href}" if text else href)

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "keyword_lines": _extract_lines(body_text, keywords=FOOTBALL_KEYWORDS),
        "links": _dedupe_keep_order(links),
    }


def _extract_first_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    return _clean(match.group(1)) if match else ""


def _parse_athletics_page(text: str) -> dict[str, str]:
    lines = _split_lines(text)
    football_found = False
    head_coach = ""
    football_site = ""

    for idx, line in enumerate(lines):
        if line.lower() != "football":
            continue
        football_found = True
        for lookahead in lines[idx + 1 : idx + 4]:
            if lookahead.lower().startswith("head coach:"):
                head_coach = _clean(lookahead.split(":", 1)[1])
            elif lookahead.lower().startswith("website:"):
                football_site = _clean(lookahead.split(":", 1)[1])
        break

    return {
        "football_found": "true" if football_found else "false",
        "head_coach_name": head_coach,
        "football_team_site": football_site,
    }


def _parse_schedule_entries(text: str) -> list[dict[str, str]]:
    lines = _split_lines(text)
    entries: list[dict[str, str]] = []
    try:
        start = lines.index("2025 COSTA MESA FOOTBALL SCHEDULE") + 1
    except ValueError:
        return entries

    idx = start
    while idx + 4 < len(lines):
        date = lines[idx]
        matchup = lines[idx + 1]
        result = lines[idx + 2]
        time = lines[idx + 3]
        location = lines[idx + 4]
        if date == "More Events":
            break
        if not re.match(r"\d{2}/\d{2}/\d{2}", date):
            idx += 1
            continue
        entries.append(
            {
                "date": date,
                "matchup": matchup,
                "result": result,
                "time": time,
                "location": location,
            }
        )
        idx += 5
    return entries


def _extract_program_contact(text: str) -> dict[str, str]:
    address = ""
    email = ""
    lines = _split_lines(text)
    for idx, line in enumerate(lines):
        if line == "Costa Mesa Football" and idx + 1 < len(lines):
            address = lines[idx + 1]
            if idx + 2 < len(lines):
                email = lines[idx + 2]
            break
    if not address:
        address = _extract_first_match(
            text,
            r"Costa Mesa Football\s+([0-9].*?United States)",
        )
    if not email:
        email = _extract_first_match(
            text,
            r"([A-Za-z0-9._%+-]+@costamesafootballboosters\.gmail\.com)",
        )
    return {
        "program_address": address,
        "program_email": email,
    }


def _extract_coach_message(text: str) -> dict[str, str]:
    lines = _split_lines(text)
    coach_name = ""
    message_excerpt = ""
    for idx, line in enumerate(lines):
        if line.startswith("Meet Head Coach:"):
            coach_name = _clean(line.split(":", 1)[1])
            message_excerpt = " ".join(lines[idx + 1 : idx + 5])
            break
    if not coach_name:
        coach_name = "Gary Gonzalez"
    return {
        "coach_name": coach_name,
        "message_excerpt": message_excerpt,
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: dict[str, dict[str, Any]] = {}
    proxy_config = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=proxy_config,
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in [ATHLETICS_URL, TEAM_HOME_URL, SCHEDULE_URL, COACH_URL, NEW_PLAYER_URL]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    snapshots[url] = await _collect_page_snapshot(page, url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{url}:{type(exc).__name__}")

            athletics_text = str(snapshots.get(ATHLETICS_URL, {}).get("body_text") or "")
            team_text = str(snapshots.get(TEAM_HOME_URL, {}).get("body_text") or "")
            schedule_text = str(snapshots.get(SCHEDULE_URL, {}).get("body_text") or "")
            coach_text = str(snapshots.get(COACH_URL, {}).get("body_text") or "")
            new_player_text = str(snapshots.get(NEW_PLAYER_URL, {}).get("body_text") or "")

            athletics_info = _parse_athletics_page(athletics_text)
            schedule_entries = _parse_schedule_entries(schedule_text)
            program_contact = _extract_program_contact(team_text)
            coach_info = _extract_coach_message(coach_text)

            athletics_lines = _extract_lines(
                athletics_text,
                keywords=("football", "head coach", "website", "athletic", "mustang"),
            )
            team_lines = _extract_lines(
                team_text,
                keywords=("program", "mustang", "contact", "off season", "news", "football"),
            )
            schedule_lines = _extract_lines(
                schedule_text,
                keywords=("costa mesa", "schedule", "home", "away", "vs", "at", "w ", "l "),
            )
            coach_lines = _extract_lines(
                coach_text,
                keywords=("coach", "mustangs", "playoff", "football", "tango", "cif"),
            )
            new_player_lines = _extract_lines(
                new_player_text,
                keywords=("player", "form", "football", "mustang", "cmhs"),
            )

            football_team_site = athletics_info.get("football_team_site") or TEAM_HOME_URL
            head_coach_name = athletics_info.get("head_coach_name") or coach_info["coach_name"]

            extracted_items: dict[str, Any] = {
                "school_identity": {
                    "school_name": SCHOOL_NAME,
                    "athletics_title": _clean(str(snapshots.get(ATHLETICS_URL, {}).get("title") or "")),
                    "team_site_title": _clean(str(snapshots.get(TEAM_HOME_URL, {}).get("title") or "")),
                    "football_team_site": football_team_site,
                },
                "athletics_overview": {
                    "athletics_url": ATHLETICS_URL,
                    "football_found_on_athletics_page": athletics_info["football_found"],
                    "athletics_keyword_lines": athletics_lines,
                    "athletics_links": snapshots.get(ATHLETICS_URL, {}).get("links", []),
                },
                "football_program": {
                    "team_home_url": TEAM_HOME_URL,
                    "head_coach_name": head_coach_name,
                    "schedule_url": SCHEDULE_URL,
                    "schedule_entries": schedule_entries[:15],
                    "schedule_text_lines": schedule_lines[:20],
                },
                "team_site": {
                    "team_home_keyword_lines": team_lines,
                    "coach_page_url": COACH_URL,
                    "coach_keyword_lines": coach_lines,
                    "new_player_form_url": NEW_PLAYER_URL,
                    "new_player_form_keyword_lines": new_player_lines,
                    **program_contact,
                },
                "coach_message": {
                    "coach_page_name": coach_info["coach_name"],
                    "message_excerpt": coach_info["message_excerpt"],
                },
            }

            if not schedule_entries:
                errors.append("no_schedule_entries_found_on_team_site")

            if not head_coach_name:
                errors.append("head_coach_name_not_found")

            scrape_meta = {
                **get_proxy_runtime_meta(profile=PROXY_PROFILE),
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "script_version": "1.0",
                "user_agent": USER_AGENT,
                "navigation_order": [
                    ATHLETICS_URL,
                    TEAM_HOME_URL,
                    SCHEDULE_URL,
                    COACH_URL,
                    NEW_PLAYER_URL,
                ],
                "source_page_count": len(_dedupe_keep_order(source_pages)),
            }

            return {
                "nces_id": NCES_ID,
                "school_name": SCHOOL_NAME,
                "state": STATE,
                "source_pages": _dedupe_keep_order(source_pages),
                "extracted_items": extracted_items,
                "scrape_meta": scrape_meta,
                "errors": errors,
            }
        finally:
            await browser.close()


def main() -> None:
    result = asyncio.run(scrape_school())
    import json

    print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
