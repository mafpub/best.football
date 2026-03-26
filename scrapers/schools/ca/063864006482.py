"""Deterministic football scraper for Granger Junior High (CA)."""

from __future__ import annotations

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

NCES_ID = "063864006482"
SCHOOL_NAME = "Granger Junior High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_URL = "https://gjh.sweetwaterschools.org/"
ATHLETICS_URL = "https://gjh.sweetwaterschools.org/students/athletics"
CAMPUS_LIFE_URL = "https://gjh.sweetwaterschools.org/campus-life"

TARGET_URLS = [SCHOOL_URL, ATHLETICS_URL, CAMPUS_LIFE_URL]

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
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _collect_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines


def _collect_keyword_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for line in _collect_lines(text):
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_contacts(text: str) -> list[dict[str, str]]:
    lines = _collect_lines(text)
    contacts: list[dict[str, str]] = []
    email_pattern = re.compile(r"[\w.\-+]+@[\w.\-]+\.\w+")

    for idx, line in enumerate(lines):
        if not email_pattern.search(line):
            continue

        email = email_pattern.search(line).group(0) if email_pattern.search(line) else ""
        name = ""
        role = ""

        for back in range(1, 4):
            candidate_idx = idx - back
            if candidate_idx < 0:
                break
            candidate = lines[candidate_idx]
            if "@" in candidate:
                continue
            if not name and "-" in candidate:
                parts = [part.strip() for part in candidate.split("-", 1)]
                name = parts[0]
                if len(parts) > 1:
                    role = parts[1]
                continue
            if not name and any(token in candidate.lower() for token in ("coach", "coordinator", "director", "supervisor")):
                role = candidate

        contacts.append({"name": name, "role": role, "email": email})

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for contact in contacts:
        key = contact["email"].lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(contact)
    return deduped


async def _capture_page(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text(timeout=15000)
    link_items = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(link_items, list):
        link_items = []

    football_links: list[str] = []
    for item in link_items:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        hay = f"{text} {href}".lower()
        if any(keyword in hay for keyword in ("football", "flag football", "athletics", "sports")):
            football_links.append(f"{text}|{href}" if text else href)

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _collect_keyword_lines(body_text, ("football", "flag football", "athletics", "sports", "coach", "clearance")),
        "football_links": _dedupe_keep_order(football_links),
        "contact_candidates": _extract_contacts(body_text),
    }


def _season_sport_lists(text: str) -> dict[str, list[str]]:
    seasons = {"fall": [], "winter": [], "spring": []}
    lines = _collect_lines(text)
    current = ""
    for line in lines:
        lowered = line.lower()
        if lowered.startswith("fall:"):
            current = "fall"
            tail = _clean(line.split(":", 1)[1])
            if tail:
                seasons[current].append(tail)
            continue
        if lowered.startswith("winter:"):
            current = "winter"
            tail = _clean(line.split(":", 1)[1])
            if tail:
                seasons[current].append(tail)
            continue
        if lowered.startswith("spring:"):
            current = "spring"
            tail = _clean(line.split(":", 1)[1])
            if tail:
                seasons[current].append(tail)
            continue
        if current and not lowered.startswith(("###", "##")):
            if any(ch.isalpha() for ch in line):
                seasons[current].append(line)
    return {key: _dedupe_keep_order(value) for key, value in seasons.items()}


async def scrape_school() -> dict[str, Any]:
    """Scrape official Granger football signals from the school website."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_snapshots: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            locale="en-US",
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    page_snapshots.append(await _capture_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_snapshot = next((snap for snap in page_snapshots if snap.get("requested_url") == ATHLETICS_URL), {})
    campus_snapshot = next((snap for snap in page_snapshots if snap.get("requested_url") == CAMPUS_LIFE_URL), {})

    athletics_text = str(athletics_snapshot.get("body_text") or "")
    campus_text = str(campus_snapshot.get("body_text") or "")

    football_lines = _dedupe_keep_order(
        _collect_keyword_lines(athletics_text, ("football", "coach", "sports clearance"))
        + _collect_keyword_lines(campus_text, ("football", "flag football", "sports teams"))
    )
    football_program_available = any("football" in line.lower() for line in football_lines)

    if not football_program_available:
        errors.append("no_public_football_program_verifiable_on_official_school_pages")

    athletics_contacts = _extract_contacts(athletics_text)
    campus_contacts = _extract_contacts(campus_text)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_signals": football_lines,
        "flag_football_signals": _collect_keyword_lines(campus_text, ("flag football",)),
        "athletics_contact_candidates": athletics_contacts,
        "campus_life_contact_candidates": campus_contacts,
        "sports_by_season": _season_sport_lists(campus_text),
        "page_summaries": {
            "athletics_page": {
                "url": athletics_snapshot.get("final_url", ATHLETICS_URL),
                "title": athletics_snapshot.get("title", ""),
                "football_lines": athletics_snapshot.get("football_lines", []),
            },
            "campus_life_page": {
                "url": campus_snapshot.get("final_url", CAMPUS_LIFE_URL),
                "title": campus_snapshot.get("title", ""),
                "football_lines": campus_snapshot.get("football_lines", []),
            },
        },
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(TARGET_URLS),
            "focus": "football_only",
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
