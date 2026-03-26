"""Deterministic football scraper for Atwater High (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlparse

import requests
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062466003699"
SCHOOL_NAME = "Atwater High"
STATE = "CA"
PROXY_PROFILE = "datacenter"
BASE_URL = "https://ahs.muhsd.org"
ATHLETICS_DIRECTOR_URL = f"{BASE_URL}/38432_2"
TEAMS_AND_SCHEDULES_URL = f"{BASE_URL}/38726_2"
TARGET_URLS = [ATHLETICS_DIRECTOR_URL, TEAMS_AND_SCHEDULES_URL]

PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

DAY_PATTERN = re.compile(r"^(Mon|Tues|Tue|Wed|Thurs|Thu|Fri|Sat|Sun)$", re.IGNORECASE)
COACH_PATTERN = re.compile(
    r"(Head Coach|JV Head Coach):\s*([A-Za-z][A-Za-z .'\-]*)\s+([A-Za-z0-9._%+\-]+@[\w.-]+\.\w+)",
    re.IGNORECASE,
)
URL_EDIT_SUFFIX = "/edit?usp=sharing"


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    ordered: list[Any] = []
    for value in values:
        key = _clean(str(value)) if not isinstance(value, dict) else repr(value)
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(value)
    return ordered


def _proxy_requests_url() -> str:
    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)
    parsed = urlparse(proxy["server"])
    username = quote(str(proxy["username"]), safe="")
    password = quote(str(proxy["password"]), safe="")
    return f"{parsed.scheme}://{username}:{password}@{parsed.hostname}:{parsed.port}"


def _requests_proxy_map() -> dict[str, str]:
    proxy_url = _proxy_requests_url()
    return {"http": proxy_url, "https": proxy_url}


def _export_url(doc_url: str) -> str:
    if URL_EDIT_SUFFIX in doc_url:
        return doc_url.split(URL_EDIT_SUFFIX, 1)[0] + "/export?format=txt"
    if "/edit" in doc_url:
        return doc_url.split("/edit", 1)[0] + "/export?format=txt"
    return doc_url.rstrip("/") + "/export?format=txt"


def _extract_link_entries(links: list[dict[str, Any]]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if "football" in blob:
            entries.append({"text": text, "href": href})
    return _dedupe_keep_order(entries)


def _parse_schedule_rows(text: str) -> list[dict[str, Any]]:
    lines = [_clean(line) for line in text.replace("\ufeff", "").splitlines()]
    lines = [line for line in lines if line]

    try:
        start_index = next(i for i, line in enumerate(lines) if line.upper() == "TIME")
    except StopIteration:
        return []

    rows: list[dict[str, Any]] = []
    i = start_index + 1
    conference_section = False
    stop_prefixes = ("Head Coach:", "JV Head Coach:", "Athletic Director:", "SBO:")

    while i < len(lines):
        line = lines[i]
        if any(line.startswith(prefix) for prefix in stop_prefixes):
            break
        if line == "Central California Conference":
            conference_section = True
            i += 1
            continue
        if not DAY_PATTERN.fullmatch(line):
            i += 1
            continue
        if i + 4 >= len(lines):
            break

        day = lines[i]
        date = lines[i + 1]
        opponent = lines[i + 2]
        location = lines[i + 3]
        time = lines[i + 4]
        rows.append(
            {
                "day": day,
                "date": date,
                "opponent": opponent,
                "location": location,
                "time": time,
                "is_conference_game": conference_section,
                "is_home_game": location.lower().startswith("atwater hs"),
            }
        )
        i += 5

    return rows


def _parse_coaches(text: str, team_label: str) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for match in COACH_PATTERN.finditer(text):
        role = _clean(match.group(1))
        name = _clean(match.group(2))
        email = _clean(match.group(3))
        key = (role.lower(), email.lower())
        if key in seen:
            continue
        seen.add(key)
        coaches.append(
            {
                "team_label": team_label,
                "role": role,
                "name": name,
                "email": email,
            }
        )
    return coaches


def _parse_doc(text: str, *, team_label: str, doc_url: str) -> dict[str, Any]:
    schedule_rows = _parse_schedule_rows(text)
    coaches = _parse_coaches(text, team_label)
    season_match = re.search(r"^\s*(\d{4}(?:-\d{2})?)\s*$", text, re.MULTILINE)
    season = season_match.group(1) if season_match else ""

    return {
        "team_label": team_label,
        "doc_url": doc_url,
        "export_url": _export_url(doc_url),
        "season": season,
        "row_count": len(schedule_rows),
        "schedule_rows": schedule_rows,
        "coaches": coaches,
    }


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    body_text = _clean(await page.inner_text("body"))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_links": _extract_link_entries(links),
    }


def _download_text(url: str) -> str:
    response = requests.get(
        url,
        timeout=30,
        proxies=_requests_proxy_map(),
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    return response.text.replace("\ufeff", "")


async def scrape_school() -> dict[str, Any]:
    """Collect public Atwater football schedules from the school athletics pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

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
                    await page.goto(url, wait_until="commit", timeout=30000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page_signal(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_links: list[dict[str, str]] = []
    for signal in page_signals:
        if signal.get("requested_url") == TEAMS_AND_SCHEDULES_URL:
            football_links.extend(
                item for item in signal.get("football_links", []) if isinstance(item, dict)
            )

    football_links = _dedupe_keep_order(football_links)
    doc_links = [item for item in football_links if "docs.google.com/document/d/" in item["href"]]

    if not doc_links:
        errors.append("blocked:no_public_football_doc_links_found_on_teams_page")

    football_schedules: list[dict[str, Any]] = []
    for link in doc_links:
        doc_url = link["href"]
        try:
            doc_text = _download_text(_export_url(doc_url))
            team_label = link["text"] or "Football"
            football_schedules.append(_parse_doc(doc_text, team_label=team_label, doc_url=doc_url))
            source_pages.append(_export_url(doc_url))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"doc_fetch_failed:{type(exc).__name__}:{doc_url}")

    source_pages = _dedupe_keep_order(source_pages)

    football_team_names = _dedupe_keep_order(
        [schedule["team_label"] for schedule in football_schedules]
    )
    football_coaches: list[dict[str, str]] = []
    for schedule in football_schedules:
        football_coaches.extend(
            coach for coach in schedule.get("coaches", []) if isinstance(coach, dict)
        )
    football_coaches = _dedupe_keep_order(football_coaches)

    football_schedule_docs = [
        {
            "team_label": schedule["team_label"],
            "doc_url": schedule["doc_url"],
            "export_url": schedule["export_url"],
            "season": schedule["season"],
            "row_count": schedule["row_count"],
        }
        for schedule in football_schedules
    ]

    football_program_available = bool(football_schedule_docs)
    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_director_url": ATHLETICS_DIRECTOR_URL,
        "teams_and_schedules_url": TEAMS_AND_SCHEDULES_URL,
        "football_team_names": football_team_names,
        "football_links": football_links,
        "football_schedule_docs": football_schedule_docs,
        "football_schedules": football_schedules,
        "football_coaches": football_coaches,
        "summary": (
            "Atwater High publishes public football schedules for F/S Football, JV/V Football, and Girls Flag Football on its Teams & Schedules page, with Google Docs exports and coach contact information."
            if football_program_available
            else ""
        ),
    }

    extracted_items["football_team_names"] = _dedupe_keep_order(
        [name for name in extracted_items["football_team_names"] if name]
    )

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": PROXY_PROFILE,
            "proxy_server": get_playwright_proxy_config(profile=PROXY_PROFILE)["server"],
            "pages_checked": len(page_signals),
            "docs_checked": len(football_schedule_docs),
            "focus": "football_only",
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import asyncio

    result = asyncio.run(scrape_school())
    print(result)
