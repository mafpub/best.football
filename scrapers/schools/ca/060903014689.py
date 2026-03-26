"""Deterministic football scraper for Garfield Center (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060903014689"
SCHOOL_NAME = "Garfield Center"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SPORTS_URL = "https://garfield.cusd.com/sports"
TARGET_URLS = [SPORTS_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str | None) -> str:
    if not value:
        return ""
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


def _extract_lines(text: str, *, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_links(soup: BeautifulSoup, *, keywords: tuple[str, ...]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _clean(str(anchor.get("href") or ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in keywords):
            links.append({"text": text, "href": href})
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in links:
        key = f"{item['text']}|{item['href']}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _extract_tab_titles(soup: BeautifulSoup) -> list[str]:
    titles: list[str] = []
    for button in soup.select('button[role="tab"]'):
        title = _clean(button.get_text(" ", strip=True))
        if title:
            titles.append(title)
    return _dedupe_keep_order(titles)


def _get_fall_panel(soup: BeautifulSoup) -> tuple[str, Any]:
    for button in soup.select('button[role="tab"][aria-controls]'):
        title = _clean(button.get_text(" ", strip=True))
        if title.lower() != "fall sports":
            continue
        panel_id = _clean(str(button.get("aria-controls") or ""))
        if panel_id:
            panel = soup.select_one(f"#{panel_id}")
            if panel:
                return panel_id, panel

    for panel in soup.select("div.ss-tab-panel"):
        text = _clean(panel.get_text(" ", strip=True))
        if "football" in text.lower():
            panel_id = _clean(str(panel.get("id") or ""))
            return panel_id, panel

    return "", None


def _extract_first_line_matching(lines: list[str], predicate) -> str:
    for line in lines:
        if predicate(line):
            return line
    return ""


def _parse_coaches(fall_lines: list[str]) -> list[str]:
    coach_lines: list[str] = []
    for line in fall_lines:
        if "football coach" in line.lower():
            coach_lines.append(line)
    if not coach_lines:
        return []

    names: list[str] = []
    for line in coach_lines:
        if ":" not in line:
            continue
        tail = line.split(":", 1)[1]
        for part in re.split(r",|&| and ", tail):
            cleaned = _clean(part)
            if cleaned:
                names.append(cleaned)
    return _dedupe_keep_order(names)


async def scrape_school() -> dict[str, Any]:
    """Inspect Garfield's sports page and extract football-specific public content."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_title = ""
    page_heading = ""
    tabs_seen: list[str] = []
    fall_panel_id = ""
    fall_panel_text = ""
    fall_links: list[dict[str, str]] = []

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
            await page.goto(SPORTS_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(1_500)
            source_pages.append(page.url)

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            page_title = _clean(await page.title())
            page_heading = _clean(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")
            tabs_seen = _extract_tab_titles(soup)

            fall_panel_id, fall_panel = _get_fall_panel(soup)
            if fall_panel is not None:
                fall_panel_text = fall_panel.get_text("\n")
                fall_links = _extract_links(
                    fall_panel,
                    keywords=(
                        "football",
                        "permission",
                        "schedule",
                        "handbook",
                        "athletic",
                        "sports",
                    ),
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    fall_lines = _extract_lines(
        fall_panel_text,
        keywords=("football", "coach", "permission", "schedule", "fall sports", "athletic"),
    )
    football_lines = _extract_lines(
        fall_panel_text,
        keywords=("football", "football coach", "fall sports"),
    )
    football_coach_line = _extract_first_line_matching(
        fall_lines,
        lambda line: "football coach:" in line.lower(),
    )
    permission_links = [item for item in fall_links if "permission" in f"{item['text']} {item['href']}".lower()]
    schedule_links = [item for item in fall_links if "schedule" in f"{item['text']} {item['href']}".lower()]

    football_program_available = bool(football_lines or permission_links or schedule_links)
    if not football_program_available:
        errors.append("no_public_football_content_detected")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "page_title": page_title,
        "page_heading": page_heading,
        "tab_titles": tabs_seen,
        "fall_panel_id": fall_panel_id,
        "fall_sports_lines": fall_lines,
        "football_lines": football_lines,
        "football_coach_line": football_coach_line,
        "football_coaches": _parse_coaches([football_coach_line] if football_coach_line else fall_lines),
        "football_links": fall_links,
        "athletic_permission_links": permission_links,
        "fall_sports_schedule_links": schedule_links,
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
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "page_title": page_title,
            "page_heading": page_heading,
            "fall_panel_id": fall_panel_id,
            "football_program_available": football_program_available,
        },
        "errors": errors,
    }
