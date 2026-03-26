"""Deterministic football scraper for Huntington Beach High School (CA)."""

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

NCES_ID = "061806002230"
SCHOOL_NAME = "Huntington Beach High School"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_HOME_URL = "https://www.hboilers.com/"
ATHLETICS_URL = (
    "https://www.hboilers.com/apps/pages/index.jsp?uREC_ID=1474787&type=d&pREC_ID=1638903"
)
FOOTBALL_ABOUT_URL = (
    "https://www.htosports.com/teams/default.asp?p=about&s=football&u=HBHSOILERFOOTBALL"
)
FOOTBALL_NEWS_URL = (
    "https://www.htosports.com/teams/default.asp?p=news&s=football&u=HBHSOILERFOOTBALL"
)
FOOTBALL_COACHES_URL = (
    "https://www.htosports.com/teams/default.asp?p=coaches&s=football&u=HBHSOILERFOOTBALL"
)
FOOTBALL_ROSTER_URL = (
    "https://www.htosports.com/teams/default.asp?p=roster&s=football&u=HBHSOILERFOOTBALL"
)
FOOTBALL_SCHEDULE_URL = (
    "https://www.htosports.com/teams/default.asp?p=schedule&s=football&u=HBHSOILERFOOTBALL"
)

TARGET_URLS = [
    SCHOOL_HOME_URL,
    ATHLETICS_URL,
    FOOTBALL_ABOUT_URL,
    FOOTBALL_NEWS_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_SCHEDULE_URL,
]

FOOTBALL_KEYWORDS = (
    "oilers football",
    "hbhs football",
    "huntington beach high",
    "football",
    "varsity",
    "jv",
    "freshman",
    "schedule",
    "coach",
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _is_jersey_token(value: str) -> bool:
    token = _clean(value).strip()
    if not token:
        return False
    return bool(re.fullmatch(r"\d{1,2}|7[0-9]{1}", token) and token != "0")


def _is_position(value: str) -> bool:
    value = _clean(value)
    if not value or len(value) > 40:
        return False
    return bool(re.fullmatch(r"[A-Z]{1,8}(?:/[A-Z]{1,8})*(?:\s+[A-Z]{1,8}(?:/[A-Z]{1,8})*)?", value))


def _is_name(value: str) -> bool:
    value = _clean(value)
    if not value or len(value.split()) < 2:
        return False
    return bool(re.fullmatch(r"[A-Za-z.'\- ]{3,}", value))


async def _collect_page_snapshot(page, target_url: str) -> dict[str, Any]:
    await page.goto(target_url, wait_until="domcontentloaded", timeout=90_000)
    await page.wait_for_timeout(1200)
    body_text = await page.locator("body").inner_text(timeout=20_000)
    raw_links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(el => ({
            text: (el.textContent || "").replace(/\\s+/g, " ").trim(),
            href: el.href || ""
        }))""",
    )

    links: list[dict[str, str]] = []
    if isinstance(raw_links, list):
        for entry in raw_links:
            text = _clean(str((entry or {}).get("text", "")))
            href = _clean(str((entry or {}).get("href", "")))
            if text and href:
                links.append({"text": text, "href": href})

    return {
        "requested_url": target_url,
        "final_url": _clean(page.url),
        "title": _clean(await page.title()),
        "body_text": _clean(body_text),
        "links": links,
    }


def _extract_lines_with_keywords(text: str) -> list[str]:
    lines = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in FOOTBALL_KEYWORDS):
            lines.append(line)
    return lines


def _extract_coaches(lines: list[str]) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    role_markers = {"head coach", "coach", "offensive", "defensive", "quarterback"}
    seen: set[tuple[str, str]] = set()

    for i, line in enumerate(lines):
        lowered = line.lower()
        if not any(marker in lowered for marker in role_markers):
            continue
        prev = _clean(lines[i - 1]) if i > 0 else ""
        if not prev or not _is_name(prev):
            continue
        if (prev, line) in seen:
            continue
        seen.add((prev, line))
        coaches.append({"name": prev, "role": line, "source": FOOTBALL_COACHES_URL})

    return coaches


def _extract_roster(lines: list[str]) -> list[dict[str, str]]:
    roster: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    i = 0
    while i < len(lines) - 2:
        jersey = _clean(lines[i])
        if _is_jersey_token(jersey):
            name = _clean(lines[i + 1])
            role = _clean(lines[i + 2])
            if _is_name(name) and _is_position(role):
                key = (name, role)
                if key not in seen:
                    roster.append(
                        {
                            "jersey_number": jersey,
                            "name": name,
                            "position": role,
                            "team": "Varsity",
                        }
                    )
                    seen.add(key)
                i += 3
                continue
        i += 1

    return _dedupe_keep_order([f"{player['name']}" for player in roster])  # ensure deterministic dedupe


def _extract_roster_details(lines: list[str]) -> list[dict[str, str]]:
    """
    Keep parsed roster rows as structured entries; return deterministic top entries.
    """
    roster = []
    seen: set[tuple[str, str, str]] = set()
    i = 0
    while i < len(lines) - 2:
        jersey = _clean(lines[i])
        if _is_jersey_token(jersey):
            name = _clean(lines[i + 1])
            position = _clean(lines[i + 2])
            if _is_name(name) and _is_position(position):
                row = (jersey, name, position)
                if row not in seen:
                    seen.add(row)
                    roster.append({"jersey_number": jersey, "name": name, "position": position})
                i += 3
                continue
        i += 1

    return roster


def _extract_schedule_lines(lines: list[str]) -> list[str]:
    schedule_markers = (
        "week",
        "vs.",
        "vs ",
        "fri",
        "sat",
        "sun",
        "mon",
    )
    output: list[str] = []
    for line in lines:
        lowered = line.lower()
        if any(marker in lowered for marker in schedule_markers) and _is_position("".join(ch for ch in line if ch.isalpha() and ch.isupper() or ch == "/")) is False:
            if "vs" in lowered and "vs." in lowered or any(dow in lowered for dow in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")):
                output.append(_clean(line))
    return _dedupe_keep_order(output)


def _extract_external_football_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        text = _clean(str(link.get("text", "")))
        href = _clean(str(link.get("href", "")))
        if not text and not href:
            continue
        if "football" not in (text + " " + href).lower():
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


async def scrape_school() -> dict[str, Any]:
    """
    Scrape public football content for Huntington Beach High.
    """
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    snapshots: list[dict[str, Any]] = []
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
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for target in TARGET_URLS:
                try:
                    snapshot = await _collect_page_snapshot(page, target)
                    snapshots.append(snapshot)
                    source_pages.append(snapshot["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{target}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    all_links: list[dict[str, str]] = []
    all_lines: list[str] = []

    for snapshot in snapshots:
        all_lines.extend([_clean(line) for line in (snapshot.get("body_text") or "").splitlines() if _clean(line)])
        for link in snapshot.get("links", []):
            text = _clean(str(link.get("text", "")))
            href = _clean(str(link.get("href", "")))
            if text or href:
                all_links.append({"text": text, "href": href})

    football_lines = _extract_lines_with_keywords("\n".join(all_lines))
    football_pages = [
        snapshot["final_url"]
        for snapshot in snapshots
        if "football" in _clean(snapshot.get("body_text", "")).lower()
        or "football" in snapshot.get("title", "").lower()
        or snapshot["final_url"].startswith("https://www.htosports.com")
    ]
    football_pages = _dedupe_keep_order(football_pages)

    coaches = _extract_coaches(all_lines)
    roster = _extract_roster_details(all_lines)
    roster_names = _extract_roster(all_lines)
    schedule_lines = _extract_schedule_lines(all_lines)
    football_links = _extract_external_football_links(all_links)

    football_program_available = bool(football_lines or football_pages or roster or coaches)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_after_navigation")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_name": SCHOOL_NAME,
        "school_nickname": "Oilers",
        "school_athletics_page": ATHLETICS_URL,
        "football_page": FOOTBALL_ABOUT_URL,
        "football_schedule_page": FOOTBALL_SCHEDULE_URL,
        "football_coaches_page": FOOTBALL_COACHES_URL,
        "football_roster_page": FOOTBALL_ROSTER_URL,
        "football_news_page": FOOTBALL_NEWS_URL,
        "football_pages": football_pages,
        "football_keyword_lines": football_lines[:40],
        "football_links": football_links[:30],
        "football_coaches": coaches[:20],
        "football_roster_names_sample": roster_names[:30],
        "football_roster_rows_sample": roster[:40],
        "football_schedule_snippets": schedule_lines[:40],
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


async def _async_main() -> None:
    import json

    print(json.dumps(await scrape_school(), ensure_ascii=True))


def main() -> None:
    import asyncio

    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
