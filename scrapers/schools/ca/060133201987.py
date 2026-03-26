"""Deterministic football scraper for Grant Union High (CA)."""

from __future__ import annotations

import asyncio
import json
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

NCES_ID = "060133201987"
SCHOOL_NAME = "Grant Union High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://ghs.trusd.net/index.html"
ATHLETICS_URL = "https://ghs.trusd.net/Athletics/index.html"
FOOTBALL_URL = "https://ghs.trusd.net/Athletics/Football/index.html"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, FOOTBALL_URL]

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
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _collect_lines(text: str, *, limit: int = 120) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines[:limit]


def _keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 40) -> list[str]:
    matches: list[str] = []
    for line in _collect_lines(text, limit=250):
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _extract_coach_details(text: str) -> dict[str, str]:
    lines = _collect_lines(text, limit=200)
    coach_name = ""
    phone_numbers: list[str] = []
    email = ""

    for idx, line in enumerate(lines):
        lowered = line.lower()
        if lowered.startswith("head coach -"):
            coach_name = _clean(line.split("-", 1)[1])
            for next_line in lines[idx + 1 : idx + 8]:
                if "@" in next_line and not email:
                    email = next_line
                elif re.search(r"\d{3}[-.\s]\d{3}[-.\s]\d{4}", next_line):
                    phone_numbers.append(next_line)
            break

    return {
        "name": coach_name,
        "phones": _dedupe_keep_order(phone_numbers),
        "email": email,
        "role": "Head Coach" if coach_name else "",
    }


async def _collect_snapshot(page) -> dict[str, Any]:
    body_text = ""
    try:
        body_text = await page.locator("body").inner_text(timeout=15000)
    except Exception:  # noqa: BLE001
        body_text = ""

    title = ""
    try:
        title = await page.title()
    except Exception:  # noqa: BLE001
        title = ""

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    images = await page.eval_on_selector_all(
        "img",
        """els => els.map(img => ({
            alt: img.alt || '',
            src: img.currentSrc || img.src || '',
            title: img.title || ''
        }))""",
    )
    if not isinstance(images, list):
        images = []

    football_links: list[str] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        haystack = f"{text} {href}".lower()
        if any(keyword in haystack for keyword in ("football", "athletics", "coach", "schedule")):
            football_links.append(f"{text}|{href}" if text else href)

    football_images: list[dict[str, str]] = []
    for item in images:
        if not isinstance(item, dict):
            continue
        alt = _clean(str(item.get("alt") or ""))
        src = _clean(str(item.get("src") or ""))
        title_attr = _clean(str(item.get("title") or ""))
        haystack = f"{alt} {title_attr} {src}".lower()
        if "football" in haystack or "schedule" in haystack or "pacer" in haystack:
            football_images.append({"alt": alt, "src": src, "title": title_attr})

    return {
        "url": page.url,
        "title": _clean(title),
        "body_text": body_text,
        "football_keyword_lines": _keyword_lines(
            body_text,
            (
                "football",
                "coach",
                "schedule",
                "pacer",
                "athletics",
            ),
        ),
        "football_links": _dedupe_keep_order(football_links),
        "football_images": football_images,
        "coach_details": _extract_coach_details(body_text),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Grant Union High football details from the public school site."""
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
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 1200},
            locale="en-US",
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    page_snapshots.append(await _collect_snapshot(page))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_snapshot = next(
        (snapshot for snapshot in page_snapshots if snapshot.get("url") == ATHLETICS_URL),
        {},
    )
    football_snapshot = next(
        (snapshot for snapshot in page_snapshots if snapshot.get("url") == FOOTBALL_URL),
        {},
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_snapshot),
        "athletics_page_url": ATHLETICS_URL,
        "football_page_url": FOOTBALL_URL,
        "head_coach": football_snapshot.get("coach_details", {}).get("name", ""),
        "coach_phones": football_snapshot.get("coach_details", {}).get("phones", []),
        "coach_email": football_snapshot.get("coach_details", {}).get("email", ""),
        "football_keyword_lines": football_snapshot.get("football_keyword_lines", []),
        "football_links": football_snapshot.get("football_links", []),
        "football_images": football_snapshot.get("football_images", []),
        "athletics_overview_lines": athletics_snapshot.get("football_keyword_lines", []),
        "athletics_links": athletics_snapshot.get("football_links", []),
    }

    if not extracted_items["head_coach"] and not extracted_items["football_images"]:
        errors.append("no_extractable_football_signals_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
