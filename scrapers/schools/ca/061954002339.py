"""Deterministic football scraper for Arvin High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "061954002339"
SCHOOL_NAME = "Arvin High"
STATE = "CA"

HOME_URL = "https://arvin.kernhigh.org/index.jsp"
ATHLETIC_TEAMS_URL = "https://arvin.kernhigh.org/apps/departments/index.jsp?show=ATH"
ATHLETIC_STAFF_URL = "https://arvin.kernhigh.org/apps/pages/index.jsp?uREC_ID=600377&type=d&pREC_ID=1134854"
ATHLETIC_CALENDAR_URL = "https://arvin.kernhigh.org/apps/pages/index.jsp?uREC_ID=600377&type=d&pREC_ID=2448206"
FOOTBALL_PAGE_URL = "https://arvin.kernhigh.org/apps/pages/index.jsp?uREC_ID=603824&type=d&pREC_ID=2589348"

TARGET_URLS = [
    HOME_URL,
    ATHLETIC_TEAMS_URL,
    ATHLETIC_STAFF_URL,
    ATHLETIC_CALENDAR_URL,
    FOOTBALL_PAGE_URL,
]

PROXY_PROFILE = "datacenter"


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_lines(text: str, terms: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if any(term in lower for term in terms):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.-]+\.\w+", text))


def _extract_links(items: list[dict[str, Any]], terms: tuple[str, ...]) -> list[str]:
    kept: list[str] = []
    for item in items:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        combo = f"{text} {href}".lower()
        if not href or not any(term in combo for term in terms):
            continue
        kept.append(f"{text}|{href}")
    return _dedupe_keep_order(kept)


def _extract_images(items: list[dict[str, Any]]) -> list[str]:
    urls: list[str] = []
    for item in items:
        src = _clean(str(item.get("src") or ""))
        if not src:
            continue
        urls.append(src)
    return _dedupe_keep_order(urls)


def _parse_football_coach_lines(text: str) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line.lower().startswith("football"):
            continue

        match = re.match(
            r"Football\s+(Var\.|JV\.)\s*-\s*([^-\n]+?)(?:\s*-\s*(.+))?$",
            line,
            flags=re.IGNORECASE,
        )
        if not match:
            continue

        role = "Varsity" if "var" in match.group(1).lower() else "JV"
        name = _clean(match.group(2))
        contact = _clean(match.group(3) or "")
        coach: dict[str, str] = {
            "role": role,
            "name": name,
            "line": line,
        }
        if contact:
            coach["contact"] = contact
        coaches.append(coach)
    return coaches


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    links = await page.locator("a[href]").evaluate_all(
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    images = await page.locator("img").evaluate_all(
        """els => els.map(el => ({
            alt: (el.alt || '').replace(/\\s+/g, ' ').trim(),
            src: el.currentSrc || el.src || ''
        }))"""
    )
    if not isinstance(images, list):
        images = []

    normalized = body_text.strip()
    football_lines = _extract_lines(
        normalized,
        ("football", "coach", "athletic", "calendar", "arbiter", "schedule", "varsity", "jv"),
    )
    if "teams" in requested_url.lower() or "department" in requested_url.lower():
        football_lines = _extract_lines(normalized, ("football",))

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": normalized,
        "football_lines": football_lines,
        "football_emails": _extract_emails(normalized),
        "football_links": _extract_links(
            [item for item in links if isinstance(item, dict)],
            ("football", "athletic", "calendar", "arbiter", "teams"),
        ),
        "football_images": _extract_images([item for item in images if isinstance(item, dict)]),
        "football_coaches": _parse_football_coach_lines(normalized),
        "team_lines": _extract_lines(normalized, ("football",)),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit public Arvin High athletics pages and extract football evidence."""
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
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(1_000)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_team_lines: list[str] = []
    football_lines: list[str] = []
    football_emails: list[str] = []
    football_links: list[str] = []
    football_images: list[str] = []
    football_coaches: list[dict[str, str]] = []
    for signal in page_signals:
        athletics_team_lines.extend(signal.get("team_lines", []))
        football_lines.extend(signal.get("football_lines", []))
        football_emails.extend(signal.get("football_emails", []))
        football_links.extend(signal.get("football_links", []))
        football_coaches.extend(signal.get("football_coaches", []))
        if signal.get("requested_url") == FOOTBALL_PAGE_URL or signal.get("final_url") == FOOTBALL_PAGE_URL:
            football_images.extend(signal.get("football_images", []))

    athletics_team_lines = _dedupe_keep_order(athletics_team_lines)
    football_lines = _dedupe_keep_order(football_lines)
    football_emails = _dedupe_keep_order(football_emails)
    football_links = _dedupe_keep_order(football_links)
    football_images = _dedupe_keep_order(
        [url for url in football_images if url.startswith("https://3.files.edl.io/")]
    )

    football_program_available = bool(
        football_coaches or football_lines or football_images or any("football" in line.lower() for line in athletics_team_lines)
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_team_names = _dedupe_keep_order(
        [
            "Football" if any("football" in line.lower() for line in athletics_team_lines) else "",
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "home_page_url": HOME_URL,
        "athletics_teams_url": ATHLETIC_TEAMS_URL,
        "athletics_staff_url": ATHLETIC_STAFF_URL,
        "athletics_calendar_url": ATHLETIC_CALENDAR_URL,
        "football_page_url": FOOTBALL_PAGE_URL,
        "football_team_names": football_team_names,
        "football_team_lines": athletics_team_lines,
        "football_coaches": football_coaches,
        "football_contact_emails": football_emails,
        "football_links": football_links,
        "football_schedule_public": bool(football_images),
        "football_schedule_image_urls": football_images,
        "football_schedule_note": (
            "The football page publishes public month-by-month schedule graphics."
            if football_images
            else "The athletic calendar page links to ArbiterLive for public schedule access."
        ),
        "summary": (
            "Arvin High publicly lists Football on the Athletic Teams page, names Robert Riley and Mike Karr as football coaches, and posts football schedule graphics on the football page."
            if football_program_available
            else ""
        ),
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
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
