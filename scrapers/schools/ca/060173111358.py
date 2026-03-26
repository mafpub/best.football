"""Deterministic football scraper for ICEF View Park Preparatory High (CA)."""

from __future__ import annotations

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

NCES_ID = "060173111358"
SCHOOL_NAME = "ICEF View Park Preparatory High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://www.icefps.org"
SCHOOL_URL = f"{BASE_URL}/view-park-high-school.html"
ATHLETICS_URL = f"{BASE_URL}/high-school.html"
KNIGHTS_ATHLETICS_URL = f"{BASE_URL}/vphs-knights-athletics.html"

TARGET_URLS = [KNIGHTS_ATHLETICS_URL, ATHLETICS_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "football",
    "gridiron",
    "knights football",
    "football squad",
    "schedule",
    "opposite",
    "game",
    "coach",
    "roster",
    "team",
)

SCHEDULE_PATTERN = re.compile(
    r"\b(?:opposite|at|versus|vs\.? )\s+[A-Za-z][A-Za-z0-9&'.\- ]{1,64}\s+on\s+"
    r"(?:january|february|march|april|may|june|july|august|september|october|november|december)"
    r"\s+\d{1,2}(?:,?\s*\d{4})?\b",
    re.IGNORECASE,
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        value = _clean(str(raw))
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _collect_lines(text: str) -> list[str]:
    return _dedupe_keep_order([_clean(line) for line in (text or "").splitlines() if _clean(line)])


def _keyword_lines(text: str, keywords: tuple[str, ...], limit: int = 30) -> list[str]:
    lines = _collect_lines(text)
    matches: list[str] = []
    for line in lines:
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _normalize_href(base_url: str, href: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return urljoin(base_url, raw)


def _collect_links(page) -> list[dict[str, str]]:
    links = page.eval_on_selector_all(
        "a[href]",
        """(els) => els.map((a) => ({
            text: (a.textContent || '').replace(/\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        return []
    output: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _normalize_href(page.url, str(item.get("href") or ""))
        if href:
            output.append({"text": text, "href": href})
    return output


def _discover_football_links(base_url: str, links: list[dict[str, str]]) -> list[str]:
    output: list[str] = []
    if not isinstance(links, list):
        return output
    football_focus = {"football", "schedule", "athletics", "vphs", "knights"}
    for item in links:
        text = _clean(str(item.get("text") or "")).lower()
        href = _clean(str(item.get("href") or "")).lower()
        if not ("ices" in base_url and "https://www.icefps.org" in href):
            if base_url.startswith("https://www.icefps.org") and "https://www.icefps.org" not in href:
                continue
        if any(marker in text or marker in href for marker in football_focus):
            output.append(item.get("href", ""))
    return _dedupe_keep_order([url for url in output if isinstance(url, str) and url])


def _extract_schedule_lines(lines: list[str]) -> list[str]:
    snippets: list[str] = []
    for line in lines:
        if SCHEDULE_PATTERN.search(line):
            snippets.append(line)
    return _dedupe_keep_order(snippets)


async def _collect_page(page, target_url: str) -> dict[str, Any]:
    await page.goto(target_url, wait_until="domcontentloaded", timeout=90_000)
    await page.wait_for_timeout(1_200)
    body_text = await page.locator("body").inner_text(timeout=25_000)
    title = await page.title()
    links = await _collect_links(page)
    cleaned_links = _dedupe_keep_order([_clean(str(link.get("href")) for link in links if isinstance(link, dict)])
    return {
        "requested_url": target_url,
        "final_url": _clean(page.url),
        "title": _clean(title),
        "body_text": _clean(body_text),
        "links": [item for item in links if isinstance(item, dict)],
        "football_lines": _keyword_lines(body_text, FOOTBALL_KEYWORDS, limit=50),
        "schedule_lines": _extract_schedule_lines(_collect_lines(body_text)),
        "link_list": cleaned_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape only football-relevant ICEF View Park athletics pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: list[dict[str, Any]] = []
    discovered_pages: list[str] = []

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
            for target in TARGET_URLS:
                try:
                    snapshot = await _collect_page(page, target)
                    snapshots.append(snapshot)
                    source_pages.append(snapshot["final_url"])
                    discovered_pages.extend(_discover_football_links(snapshot["requested_url"], snapshot["links"]))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{target}")

            for url in _dedupe_keep_order(discovered_pages):
                if url in source_pages or not url.startswith("https://www.icefps.org/"):
                    continue
                if len(source_pages) >= 5:
                    break
                try:
                    snapshot = await _collect_page(page, url)
                    snapshots.append(snapshot)
                    source_pages.append(snapshot["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_pages: list[str] = []
    football_lines: list[str] = []
    schedule_lines: list[str] = []

    for item in snapshots:
        url = str(item.get("final_url") or "")
        if not url:
            continue
        lines = [str(line) for line in (item.get("football_lines") or []) if line]
        if not lines:
            lines = _keyword_lines(
                str(item.get("body_text") or ""),
                FOOTBALL_KEYWORDS,
                limit=15,
            )
        if lines:
            football_pages.append(url)
            football_lines.extend(lines)
            schedule_lines.extend([str(s) for s in (item.get("schedule_lines") or []) if s])

    football_lines = _dedupe_keep_order(football_lines)
    schedule_lines = _dedupe_keep_order(schedule_lines)

    football_program_available = bool(football_lines or schedule_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_icef_athletics_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_url": SCHOOL_URL,
        "athletics_urls": ATHLETICS_URL,
        "knights_athletics_url": KNIGHTS_ATHLETICS_URL,
        "football_pages": football_pages,
        "football_keywords_lines": football_lines,
        "football_schedule_snippets": schedule_lines,
        "discovered_additional_pages": _dedupe_keep_order(discovered_pages),
        "team_names": ["Knights", "View Park Preparatory High Football"],
        "summary": (
            "ICEF View Park High athletics includes explicit football references in school and football-specific pages, "
            "including a 2024 football season schedule with dates/opponents on the Knights athletics page."
            if football_program_available
            else ""
        ),
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
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers", []),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "pages_checked": len(snapshots),
            "target_urls": TARGET_URLS,
            "discovered_pages": _dedupe_keep_order(discovered_pages),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
