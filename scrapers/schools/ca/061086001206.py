"""Deterministic football scraper for Delano High (CA)."""

from __future__ import annotations

import asyncio
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061086001206"
SCHOOL_NAME = "Delano High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://delano.djuhsd.org"
ATHLETICS_HOME = f"{BASE_URL}/athletics"
COACHES_PAGE = f"{BASE_URL}/athletics/meet-our-coaches"
SCHEDULES_PAGE = f"{BASE_URL}/athletics/sports-schedules"
TARGET_URLS = [ATHLETICS_HOME, COACHES_PAGE, SCHEDULES_PAGE]

SPORT_HEADING_PATTERN = re.compile(r"^[A-Z][A-Z &/-]{2,}$")
COACH_LINE_PATTERN = re.compile(
    r"^(?P<role>Head Varsity|Assistant Varsity|Volunteer Varsity|Head JV|Assistant JV|Volunteer JV)\s+(?P<name>.+?)\s*$"
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


def _split_lines(body_text: str) -> list[str]:
    return [_clean(line) for line in body_text.splitlines() if _clean(line)]


def _extract_contacts(body_text: str) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    text = body_text.replace("\u00a0", " ")
    if "1331 Cecil Ave." in text and "(661) 720-4121" in text:
        contacts.append(
            {
                "type": "school_contact",
                "name": "Delano High School",
                "address": "1331 Cecil Ave., Delano, CA 93215",
                "phone": "(661) 720-4121",
            }
        )
    return contacts


def _extract_football_section(body_text: str) -> dict[str, Any]:
    lines = _split_lines(body_text.replace("\r", "\n"))
    football_lines: list[str] = []
    in_section = False
    for line in lines:
        if line == "FOOTBALL":
            in_section = True
            football_lines.append(line)
            continue
        if in_section and SPORT_HEADING_PATTERN.match(line) and line not in {"FOOTBALL", "POSITION COACH"}:
            break
        if in_section:
            football_lines.append(line)

    football_block = "\n".join(football_lines).strip()
    coach_entries: list[dict[str, str]] = []
    coach_pairs: list[str] = []
    for line in football_lines:
        if line in {"FOOTBALL", "POSITION COACH"}:
            continue
        coach_match = COACH_LINE_PATTERN.match(line.replace("\t", " "))
        if coach_match:
            role = _clean(coach_match.group("role"))
            name = _clean(coach_match.group("name"))
            if role and name:
                coach_entries.append({"role": role, "name": name})
                coach_pairs.append(f"{role}|{name}")

    return {
        "football_block": football_block,
        "football_coaches": coach_entries,
        "football_coach_pairs": _dedupe_keep_order(coach_pairs),
        "football_program_available": bool(coach_entries or football_block),
    }


def _extract_links(links: list[dict[str, Any]]) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        normalized.append({"text": text, "href": href})
    return normalized


def _url_matches(url: str, suffix: str) -> bool:
    return urlparse(url).path.rstrip("/") == suffix.rstrip("/")


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)

    body_text = await page.inner_text("body")
    title = _clean(await page.title())
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    iframes = await page.eval_on_selector_all(
        "iframe",
        """els => els.map(e => ({
            src: e.src || e.getAttribute('src') || '',
            title: e.title || ''
        }))""",
    )
    return {
        "url": page.url,
        "title": title,
        "body": body_text,
        "body_clean": _clean(body_text),
        "links": _extract_links(links if isinstance(links, list) else []),
        "iframes": [
            {"src": _clean(str(item.get("src") or "")), "title": _clean(str(item.get("title") or ""))}
            for item in (iframes if isinstance(iframes, list) else [])
            if isinstance(item, dict)
        ],
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football-facing athletics evidence for Delano High."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    page_snapshots: list[dict[str, Any]] = []
    errors: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()
        try:
            for url in TARGET_URLS:
                try:
                    snapshot = await _collect_page(page, url)
                    page_snapshots.append(snapshot)
                    source_pages.append(snapshot["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_snapshot = next(
        (item for item in page_snapshots if _url_matches(item["url"], "/athletics")),
        {},
    )
    coaches_snapshot = next(
        (item for item in page_snapshots if _url_matches(item["url"], "/athletics/meet-our-coaches")),
        {},
    )
    schedules_snapshot = next(
        (item for item in page_snapshots if _url_matches(item["url"], "/athletics/sports-schedules")),
        {},
    )

    athletics_text_raw = str(athletics_snapshot.get("body") or "")
    coaches_text_raw = str(coaches_snapshot.get("body") or "")
    schedules_text_raw = str(schedules_snapshot.get("body") or "")
    athletics_text = str(athletics_snapshot.get("body_clean") or athletics_text_raw)
    coaches_text = str(coaches_snapshot.get("body_clean") or coaches_text_raw)
    schedules_text = str(schedules_snapshot.get("body_clean") or schedules_text_raw)
    football_section = _extract_football_section(coaches_text_raw)
    contacts = _extract_contacts(coaches_text_raw or athletics_text_raw)

    football_links = [
        {"text": item["text"], "href": item["href"]}
        for snapshot in page_snapshots
        for item in snapshot.get("links", [])
        if "football" in f"{item.get('text', '')} {item.get('href', '')}".lower()
    ]

    schedule_iframes = [
        {"src": iframe["src"], "title": iframe["title"]}
        for iframe in schedules_snapshot.get("iframes", [])
        if isinstance(iframe, dict) and iframe.get("src")
    ]

    football_program_available = bool(football_section["football_program_available"])
    if not football_program_available:
        errors.append("no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "athletics_home_url": ATHLETICS_HOME,
        "athletics_home_title": str(athletics_snapshot.get("title") or ""),
        "meet_our_coaches_url": COACHES_PAGE,
        "sports_schedules_url": SCHEDULES_PAGE,
        "football_program_available": football_program_available,
        "football_block": football_section["football_block"],
        "football_coaches": football_section["football_coaches"],
        "football_coach_pairs": football_section["football_coach_pairs"],
        "football_links": _dedupe_keep_order([f"{item['text']}|{item['href']}" for item in football_links]),
        "schedule_iframes": schedule_iframes,
        "school_contacts": contacts,
        "source_titles": _dedupe_keep_order(
            [
                str(athletics_snapshot.get("title") or ""),
                str(coaches_snapshot.get("title") or ""),
                str(schedules_snapshot.get("title") or ""),
            ]
        ),
        "athletics_summary": _clean(athletics_text[:500]) if athletics_text else "",
        "coaches_summary": _clean(coaches_text[:900]) if coaches_text else "",
        "schedules_summary": _clean(schedules_text[:500]) if schedules_text else "",
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
            "proxy_profile": PROXY_PROFILE,
            "pages_checked": len(page_snapshots),
            "target_urls": TARGET_URLS,
            "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def _main() -> None:
    import json

    payload = await scrape_school()
    print(json.dumps(payload, ensure_ascii=True))


if __name__ == "__main__":
    asyncio.run(_main())
