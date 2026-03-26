"""Deterministic football scraper for Lincoln High (CA)."""

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

NCES_ID = "064214006897"
SCHOOL_NAME = "Lincoln High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://lhs.wpusd.org/"
FOOTBALL_PAGE_URL = "https://lhs.wpusd.org/athletics/zebra-athletics/fall-sports/football"
MAXPREPS_HOME_URL = "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/"
MAXPREPS_SCHEDULE_URL = "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/schedule/"
MAXPREPS_ROSTER_URL = "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/roster/"
MAXPREPS_STAFF_URL = "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/staff/"
BOOSTER_HOME_URL = "https://fightingzebrasfootball.com/home"
BOOSTER_CONTACT_URL = "https://fightingzebrasfootball.com/contact"

TARGET_URLS = [
    HOME_URL,
    FOOTBALL_PAGE_URL,
    MAXPREPS_HOME_URL,
    MAXPREPS_SCHEDULE_URL,
    MAXPREPS_ROSTER_URL,
    MAXPREPS_STAFF_URL,
    BOOSTER_HOME_URL,
    BOOSTER_CONTACT_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for value in values:
        if isinstance(value, dict):
            key = json.dumps(value, sort_keys=True, ensure_ascii=True)
        else:
            key = _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _abs_url(base: str, href: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    if href.startswith("//"):
        return f"https:{href}"
    return urljoin(base, href)


def _extract_links(html: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _abs_url(base_url, str(anchor.get("href") or ""))
        if not href:
            continue
        links.append({"text": text, "href": href})
    return _dedupe(links)


def _extract_table_rows(html: str, table_index: int = 0) -> list[list[str]]:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.select("table")
    if table_index >= len(tables):
        return []
    rows: list[list[str]] = []
    for row in tables[table_index].select("tr"):
        cells = [
            _clean(cell.get_text(" ", strip=True))
            for cell in row.select("th,td")
        ]
        if any(cells):
            rows.append(cells)
    return rows


def _extract_school_page(page_data: dict[str, Any]) -> dict[str, Any]:
    text = str(page_data.get("text") or "")
    links = page_data.get("links", [])
    football_links: list[dict[str, str]] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        text_value = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        if not href:
            continue
        if re.search(r"football|coach|athlet|maxpreps|zebra|live stream|tickets|finalforms", f"{text_value} {href}", re.I):
            football_links.append({"text": text_value, "href": href})

    relevant_lines = []
    for line in [ln.strip() for ln in text.splitlines() if ln.strip()]:
        if re.search(r"football|coach|athlet|maxpreps|zebra|live stream|tickets|finalforms", line, re.I):
            relevant_lines.append(line)

    return {
        "url": page_data.get("url", ""),
        "title": page_data.get("title", ""),
        "relevant_lines": _dedupe(relevant_lines),
        "football_links": _dedupe(football_links),
    }


def _extract_home_page(page_data: dict[str, Any]) -> dict[str, Any]:
    text = str(page_data.get("text") or "")
    summary = {
        "title": page_data.get("title", ""),
        "overall_record": "",
        "league_record": "",
        "nat_rank": "",
        "ca_rank": "",
    }
    patterns = {
        "overall_record": r"Overall\s+(\d+-\d+)",
        "league_record": r"League\s+(\d+-\d+)",
        "nat_rank": r"NAT Rank\s+(\d+)",
        "ca_rank": r"CA Rank\s+(\d+)",
    }
    for field, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            summary[field] = _clean(match.group(1))

    return {
        "url": page_data.get("url", ""),
        "summary": summary,
        "coaches": [],
        "links": [
            link
            for link in page_data.get("links", [])
            if isinstance(link, dict)
            and re.search(r"schedule|roster|staff|football|coach|home", f"{link.get('text', '')} {link.get('href', '')}", re.I)
        ],
    }


def _extract_schedule_page(page_data: dict[str, Any]) -> dict[str, Any]:
    rows = _extract_table_rows(str(page_data.get("html") or ""), 0)
    games: list[dict[str, Any]] = []
    header = rows[0] if rows else []
    for row in rows[1:]:
        if len(row) < 5:
            continue
        date, opponent, result, game_info, watch = row[:5]
        games.append(
            {
                "date": date,
                "opponent": opponent,
                "result": result,
                "game_info": game_info,
                "watch": watch,
            }
        )

    meta_text = str(page_data.get("text") or "")
    record = {
        "overall": _search(meta_text, r"Overall\s+(\d+-\d+)"),
        "league": _search(meta_text, r"League\s+(\d+-\d+)"),
        "home": _search(meta_text, r"Home\s+(\d+-\d+)"),
        "away": _search(meta_text, r"Away\s+(\d+-\d+)"),
        "neutral": _search(meta_text, r"Neutral\s+(\d+-\d+)"),
        "pf": _search(meta_text, r"PF\s+(\d+)"),
        "pa": _search(meta_text, r"PA\s+(\d+)"),
        "streak": _search(meta_text, r"Streak\s+([A-Z]\d+|[A-Z]{1,2})"),
    }
    return {
        "url": page_data.get("url", ""),
        "title": page_data.get("title", ""),
        "record": record,
        "games": games,
        "game_count": len(games),
        "header": header,
    }


def _search(text: str, pattern: str) -> str:
    match = re.search(pattern, text, re.I)
    return _clean(match.group(1)) if match else ""


def _extract_roster_page(page_data: dict[str, Any]) -> dict[str, Any]:
    rows = _extract_table_rows(str(page_data.get("html") or ""), 0)
    players: list[dict[str, Any]] = []
    for row in rows[1:]:
        if len(row) < 6:
            continue
        number, player, grade, position, height, weight = row[:6]
        players.append(
            {
                "number": number,
                "player": player,
                "grade": grade,
                "position": position,
                "height": height,
                "weight": weight,
            }
        )
    text = str(page_data.get("text") or "")
    player_count = _search(text, r"Players\s+\((\d+)\)")
    staff_count = _search(text, r"Staff\s+\((\d+)\)")
    return {
        "url": page_data.get("url", ""),
        "title": page_data.get("title", ""),
        "player_count": int(player_count) if player_count.isdigit() else len(players),
        "staff_count": int(staff_count) if staff_count.isdigit() else 0,
        "players": players,
    }


def _extract_staff_page(page_data: dict[str, Any]) -> dict[str, Any]:
    text = str(page_data.get("text") or "")
    rows = _extract_table_rows(str(page_data.get("html") or ""), 0)
    staff: list[dict[str, Any]] = []
    for row in rows[1:]:
        if len(row) < 2:
            continue
        staff.append({"name": row[0], "position": row[1]})
    if not staff:
        for match in re.finditer(r"([A-Z][A-Za-z'.\-]+(?:\s+[A-Z][A-Za-z'.\-]+)*)\s+([A-Za-z /.-]*Coach)", text):
            staff.append({"name": _clean(match.group(1)), "position": _clean(match.group(2))})
    return {
        "url": page_data.get("url", ""),
        "title": page_data.get("title", ""),
        "staff": _dedupe(staff),
    }


async def _load_page(page, url: str) -> dict[str, Any]:
    response = await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    html = await page.content()
    text = await page.locator("body").inner_text(timeout=20000)
    return {
        "url": page.url,
        "requested_url": url,
        "title": _clean(await page.title()),
        "status": response.status if response else None,
        "html": html,
        "text": text,
        "links": _extract_links(html, page.url),
    }


async def scrape() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    page_data: dict[str, dict[str, Any]] = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()
        try:
            for url in TARGET_URLS:
                page_data[url] = await _load_page(page, url)
        finally:
            await browser.close()

    school_page = _extract_school_page(page_data[FOOTBALL_PAGE_URL])
    home_page = _extract_home_page(page_data[MAXPREPS_HOME_URL])
    schedule_page = _extract_schedule_page(page_data[MAXPREPS_SCHEDULE_URL])
    roster_page = _extract_roster_page(page_data[MAXPREPS_ROSTER_URL])
    staff_page = _extract_staff_page(page_data[MAXPREPS_STAFF_URL])

    booster_home = page_data[BOOSTER_HOME_URL]
    booster_contact = page_data[BOOSTER_CONTACT_URL]

    extracted_items = {
        "school_page": school_page,
        "maxpreps": {
            "home": home_page,
            "schedule": schedule_page,
            "roster": roster_page,
            "staff": staff_page,
        },
        "booster_site": {
            "home_url": booster_home["url"],
            "home_title": booster_home["title"],
            "contact_url": booster_contact["url"],
            "contact_email": _search(str(booster_contact.get("text") or ""), r"([A-Z0-9._%+-]+@fightingzebrasfootball\.com)"),
        },
    }

    source_pages = [
        {
            "url": data["url"],
            "title": data["title"],
            "status": data["status"],
        }
        for data in page_data.values()
    ]

    payload = {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "football_source": MAXPREPS_HOME_URL,
        },
        "errors": [],
    }

    return payload


async def scrape_school() -> dict[str, Any]:
    return await scrape()


async def scrape_athletics() -> dict[str, Any]:
    return await scrape()


async def main() -> None:
    result = await scrape()
    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(main())
