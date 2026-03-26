"""Deterministic football scraper for Estancia High (CA)."""

from __future__ import annotations

import asyncio
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062724004114"
SCHOOL_NAME = "Estancia High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://estancia.nmusd.us/"
ATHLETICS_URL = "https://estancia.nmusd.us/athletics1"
SPORTS_URL = "https://estancia.nmusd.us/athletics1/sports"
COACHES_URL = "https://estancia.nmusd.us/athletics1/coachescorner"
ATHLETIC_CALENDAR_URL = "https://estancia.nmusd.us/athletics1/athleticcalendarandnews"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, SPORTS_URL, COACHES_URL, ATHLETIC_CALENDAR_URL]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
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


def _extract_section(lines: list[str], start_marker: str, end_marker: str) -> list[str]:
    start_idx = None
    end_idx = None
    for idx, line in enumerate(lines):
        if start_idx is None and line == start_marker:
            start_idx = idx
            continue
        if start_idx is not None and line == end_marker:
            end_idx = idx
            break
    if start_idx is None:
        return []
    if end_idx is None:
        end_idx = len(lines)
    return _dedupe_keep_order(lines[start_idx:end_idx])


def _normalize_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return _clean(value)


def _parse_athletic_staff(lines: list[str]) -> list[dict[str, str]]:
    try:
        idx = lines.index("ATHLETIC OFFICE STAFF") + 1
    except ValueError:
        return []

    staff: list[dict[str, str]] = []
    while idx + 1 < len(lines):
        line = lines[idx]
        if line.startswith("Estancia High School"):
            break
        phone = lines[idx + 1]
        if re.fullmatch(r"[\d\- ]{10,}", phone):
            name = ""
            role = ""
            if "," in line:
                name, role = [part.strip() for part in line.split(",", 1)]
            else:
                name = line
            staff.append(
                {
                    "name": _clean(name),
                    "role": _clean(role),
                    "phone": _normalize_phone(phone),
                }
            )
            idx += 2
            continue
        idx += 1
    return staff


async def _collect_page_snapshot(page) -> dict[str, Any]:
    body_text = ""
    try:
        body_text = await page.inner_text("body")
    except Exception:  # noqa: BLE001
        body_text = ""

    links: list[dict[str, str]] = []
    try:
        raw_links = await page.eval_on_selector_all(
            "a[href]",
            """els => els.map(anchor => ({
                text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: anchor.href || anchor.getAttribute('href') || ''
            }))""",
        )
        if isinstance(raw_links, list):
            for item in raw_links:
                if not isinstance(item, dict):
                    continue
                text = _clean(str(item.get("text") or ""))
                href = _clean(str(item.get("href") or ""))
                if href:
                    links.append({"text": text, "href": href})
    except Exception:  # noqa: BLE001
        pass

    return {
        "title": _clean(await page.title()),
        "url": page.url,
        "text": body_text,
        "links": links,
    }


def _extract_football_block(lines: list[str]) -> list[str]:
    try:
        start_idx = next(idx for idx, line in enumerate(lines) if line == "Football")
    except StopIteration:
        return []
    end_idx = min(len(lines), start_idx + 6)
    return _dedupe_keep_order(lines[start_idx:end_idx])


def _extract_fall_sports(lines: list[str]) -> list[str]:
    try:
        start_idx = lines.index("FALL SPORTS: August to December") + 1
        end_idx = lines.index("WINTER SPORTS: November to March")
    except ValueError:
        return []
    return _dedupe_keep_order(lines[start_idx:end_idx])


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: dict[str, dict[str, Any]] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 920},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    snapshots[page.url] = await _collect_page_snapshot(page)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{url}:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_snapshot = snapshots.get(HOME_URL, {})
    athletics_snapshot = snapshots.get(ATHLETICS_URL, {})
    sports_snapshot = snapshots.get(SPORTS_URL, {})
    coaches_snapshot = snapshots.get(COACHES_URL, {})
    calendar_snapshot = snapshots.get(ATHLETIC_CALENDAR_URL, {})

    home_lines = _split_lines(str(home_snapshot.get("text") or ""))
    athletics_lines = _split_lines(str(athletics_snapshot.get("text") or ""))
    sports_lines = _split_lines(str(sports_snapshot.get("text") or ""))
    coaches_lines = _split_lines(str(coaches_snapshot.get("text") or ""))
    calendar_lines = _split_lines(str(calendar_snapshot.get("text") or ""))

    athletics_links = [
        item
        for item in athletics_snapshot.get("links", [])
        if isinstance(item, dict)
        and (
            _clean(str(item.get("text") or ""))
            in {
                "Athletic Calendar and News",
                "Schedules(opens in new window/tab)",
                "Live Stream",
                "Sports",
                "Coaches Corner",
            }
        )
    ]

    football_lines = _dedupe_keep_order(
        [line for line in sports_lines if "football" in line.lower()]
    )
    fall_sports = football_lines
    athletic_staff = _parse_athletic_staff(sports_lines)

    public_football_links = []
    for item in athletics_links:
        href = _clean(str(item.get("href") or ""))
        text = _clean(str(item.get("text") or ""))
        if href:
            public_football_links.append({"text": text, "href": href})

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_lines),
        "football_team_names": ["Football"] if football_lines else [],
        "football_block_lines": football_lines,
        "fall_sports": fall_sports,
        "athletic_office_staff": athletic_staff,
        "public_football_links": _dedupe_keep_order(
            [f"{item['text']}|{item['href']}" for item in public_football_links]
        ),
        "site_context": {
            "school_home_title": _clean(str(home_snapshot.get("title") or "")),
            "athletics_page_title": _clean(str(athletics_snapshot.get("title") or "")),
            "sports_page_title": _clean(str(sports_snapshot.get("title") or "")),
        },
        "football_evidence": _dedupe_keep_order(
            football_lines + [line for line in athletics_lines if "Live Stream" in line or "Schedules" in line]
        ),
    }

    if not extracted_items["football_program_available"]:
        errors.append("no_public_football_content_found")

    scrape_meta = {
        **get_proxy_runtime_meta(profile=PROXY_PROFILE),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "script_version": "1.0.0",
        "focus": "football_only",
        "pages_requested": TARGET_URLS,
        "pages_visited": len(source_pages),
        "source_page_count": len(source_pages),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


def main() -> None:
    result = asyncio.run(scrape_school())
    import json

    print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
