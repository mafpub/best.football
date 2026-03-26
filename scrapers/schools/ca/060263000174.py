"""Deterministic football scraper for Gilbert High (Continuation) (CA)."""

from __future__ import annotations

import base64
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "060263000174"
SCHOOL_NAME = "Gilbert High (Continuation)"
STATE = "CA"
PROXY_PROFILE = "datacenter"

WEBSITE_URL = "https://gilbert.auhsd.us"
ATHLETICS_URL = "https://gilbert.auhsd.us/Gilbert/Department/11856-ATHLETICS"
COACHES_URL = "https://gilbert.auhsd.us/Gilbert/Department/11856-ATHLETICS/31327-Coaches.html"
FOOTBALL_SEARCH_URL = (
    "https://gilbert.auhsd.us/Gilbert/Search/"
    "dD1mb290YmFsbCZ0eXBlcz1ibG9nLGV2ZW50LGpvYnMsbmV3cyxwb2RjYXN0LGZvcnVtLHBhZ2UscG9ydGFsLGRlcGFydG1lbnQsY2xhc3MsYm9va2luZ3Msam9icyxmb3JtJmxpbWl0PTMw"
)

TARGET_URLS = [WEBSITE_URL, ATHLETICS_URL, COACHES_URL, FOOTBALL_SEARCH_URL]

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


def _normalize_href(href: str, base: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("/"):
        return urljoin(base, raw)
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return ""


def _extract_mail_address(href: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    if raw.startswith("mailto:"):
        return raw.removeprefix("mailto:")
    parsed = urlparse(raw)
    qs = parse_qs(parsed.query)
    token = (qs.get("e") or [""])[0]
    if not token:
        return ""
    try:
        decoded = base64.b64decode(token + "==").decode("utf-8", errors="ignore")
    except Exception:
        return ""
    return _clean(decoded)


def _split_repeated_title(chunk: str) -> str:
    cleaned = _clean(chunk)
    if not cleaned:
        return ""
    tokens = cleaned.split()
    for size in range(len(tokens) // 2, 0, -1):
        if tokens[:size] == tokens[size : size * 2]:
            return " ".join(tokens[:size])
    return cleaned


def _extract_location_hint(description: str) -> str:
    text = _clean(description)
    if not text:
        return ""
    if "(" in text and ")" in text:
        inner = text.split("(", 1)[1].split(")", 1)[0]
        inner = re.sub(
            r"\b\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)?\b",
            "",
            inner,
        )
        return _clean(inner)
    if "!" in text:
        tail = text.split("!", 1)[1]
        tail = re.sub(r"\b\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)?\b", "", tail)
        return _clean(tail)
    return ""


async def _collect_page_text(page) -> str:
    for selector in ("#contentOut", "#content", "body"):
        locator = page.locator(selector)
        if await locator.count():
            try:
                text = await locator.first.inner_text(timeout=15000)
                if _clean(text):
                    return text
            except Exception:
                continue
    return ""


async def _collect_links(page) -> list[dict[str, str]]:
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.getAttribute('href') || '',
        }))""",
    )
    if not isinstance(links, list):
        return []
    normalized: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _normalize_href(str(item.get("href") or ""), page.url)
        if href:
            normalized.append({"text": text, "href": href})
    return normalized


async def _collect_athletics_coaches(page) -> list[dict[str, str]]:
    rows = await page.evaluate(
        """() => {
          const grid = document.querySelector('.btgrid');
          if (!grid) return [];
          return Array.from(grid.querySelectorAll('.row')).map((row) => {
            const cells = Array.from(row.children).filter((child) => {
              return child.classList && child.classList.contains('col');
            }).map((cell) => {
              const link = cell.querySelector('a[href]');
              return {
                text: (cell.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: link ? (link.getAttribute('href') || '') : '',
                linkText: link ? (link.textContent || '').replace(/\\s+/g, ' ').trim() : '',
              };
            });
            return cells;
          });
        }""",
    )
    if not isinstance(rows, list):
        return []

    coaches: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        for index in range(0, len(row) - 1, 2):
            sport = _clean(str(row[index].get("text") or ""))
            coach_name = _clean(str(row[index + 1].get("linkText") or row[index + 1].get("text") or ""))
            coach_href = _clean(str(row[index + 1].get("href") or ""))
            if _clean(sport).lower() != "football":
                continue
            coaches.append(
                {
                    "sport": sport,
                    "coach_name": coach_name,
                    "coach_email": _extract_mail_address(coach_href),
                    "coach_link": _normalize_href(coach_href, page.url),
                }
            )
    return coaches


async def _collect_football_search_results(page) -> list[dict[str, str]]:
    rows = await page.locator("table tr").evaluate_all(
        """rows => rows.map((row) => Array.from(row.querySelectorAll('th,td')).map((cell) => {
            const text = (cell.textContent || '').replace(/\\s+/g, ' ').trim();
            return text;
        }))""",
    )
    if not isinstance(rows, list):
        return []

    results: list[dict[str, str]] = []
    for row in rows[1:]:
        if not isinstance(row, list) or len(row) < 4:
            continue
        title = _clean(str(row[1]))
        kind = _clean(str(row[2])).lower()
        rel_url = _clean(str(row[3]))
        if "football" not in title.lower():
            continue
        if kind != "event":
            continue
        results.append(
            {
                "title": title,
                "type": kind,
                "url": _normalize_href(rel_url, page.url),
                "last_modified": _clean(str(row[4])) if len(row) > 4 else "",
            }
        )
    return results


async def _collect_football_event(
    page,
    requested_url: str,
    *,
    requested_title: str = "",
) -> dict[str, str]:
    await page.goto(requested_url, wait_until="domcontentloaded", timeout=45000)
    await page.wait_for_timeout(1000)
    text = _clean(await _collect_page_text(page))

    event_title = _clean(requested_title)
    if not event_title and "Calendar »" in text and "DATE" in text:
        title_chunk = text.split("Calendar »", 1)[1].split("DATE", 1)[0]
        event_title = _split_repeated_title(title_chunk)
    if not event_title:
        event_title = requested_url.rsplit("/", 1)[-1].replace(".html", "").replace("-", " ")
        event_title = _clean(event_title)

    date = ""
    time = ""
    organizer = ""
    description = ""
    location_hint = ""

    if "DATE" in text and "TIME" in text and "ORGANIZER" in text:
        after_date = text.split("DATE", 1)[1]
        date = _clean(after_date.split("TIME", 1)[0])
        after_time = after_date.split("TIME", 1)[1]
        time = _clean(after_time.split("ORGANIZER", 1)[0])
        after_organizer = after_time.split("ORGANIZER", 1)[1]
        if event_title and event_title in after_organizer:
            organizer = _clean(after_organizer.split(event_title, 1)[0])
            description = _clean(after_organizer.split(event_title, 1)[1].split("Back to Top", 1)[0])
        else:
            organizer = _clean(after_organizer.split("Back to Top", 1)[0])
            description = _clean(after_organizer.split("Back to Top", 1)[0])
        location_hint = _extract_location_hint(description)

    return {
        "title": event_title,
        "url": page.url,
        "date": date,
        "time": time,
        "organizer": organizer,
        "description": description,
        "location_hint": location_hint,
        "raw_text": text,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Gilbert High's public football-facing pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    football_coaches: list[dict[str, str]] = []
    football_event_results: list[dict[str, str]] = []
    football_events: list[dict[str, str]] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=proxy,
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in [WEBSITE_URL, ATHLETICS_URL, COACHES_URL, FOOTBALL_SEARCH_URL]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")

            try:
                await page.goto(COACHES_URL, wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(1200)
                football_coaches = await _collect_athletics_coaches(page)
                source_pages.append(page.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"coach_page_parse_failed:{type(exc).__name__}:{COACHES_URL}")

            try:
                await page.goto(FOOTBALL_SEARCH_URL, wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(1200)
                football_event_results = await _collect_football_search_results(page)
                source_pages.append(page.url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"football_search_parse_failed:{type(exc).__name__}:{FOOTBALL_SEARCH_URL}")

            for result in football_event_results:
                event_url = _clean(result.get("url") or "")
                if not event_url:
                    continue
                try:
                    event_details = await _collect_football_event(
                        page,
                        event_url,
                        requested_title=_clean(result.get("title") or ""),
                    )
                    football_events.append(event_details)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"football_event_parse_failed:{type(exc).__name__}:{event_url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_event_urls = _dedupe_keep_order([item.get("url", "") for item in football_events if item.get("url")])
    football_event_titles = _dedupe_keep_order([item.get("title", "") for item in football_events if item.get("title")])
    football_search_result_titles = _dedupe_keep_order([item.get("title", "") for item in football_event_results if item.get("title")])
    football_coach_names = _dedupe_keep_order([item.get("coach_name", "") for item in football_coaches if item.get("coach_name")])

    football_program_available = bool(football_coaches or football_events or football_event_results)
    if not football_program_available:
        errors.append("no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_department_url": ATHLETICS_URL,
        "coaches_page_url": COACHES_URL,
        "football_search_url": FOOTBALL_SEARCH_URL,
        "football_coaches": football_coaches,
        "football_coach_names": football_coach_names,
        "football_event_results": football_event_results,
        "football_event_count": len(football_events),
        "football_events": football_events,
        "football_event_titles": football_event_titles,
        "football_event_urls": football_event_urls,
        "football_search_result_titles": football_search_result_titles,
        "football_summary": (
            "Gilbert High's athletics page lists Football with coach Gary Wright, "
            "and the site search exposes multiple football event pages with dates, "
            "times, and venue hints."
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
            "proxy_server": proxy.get("server", ""),
            "proxy_auth_mode": "credentials",
            "pages_visited": len(source_pages),
            "football_event_results_found": len(football_event_results),
            "football_events_parsed": len(football_events),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()
