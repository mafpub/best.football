"""Deterministic football scraper for Connections Visual and Performing Arts Academy (CA)."""

from __future__ import annotations

import asyncio
import json
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

NCES_ID = "063828010707"
SCHOOL_NAME = "Connections Visual and Performing Arts Academy"
STATE = "CA"
PROXY_PROFILE = "datacenter"
PROXY_INDEX = 0

SCHOOL_PAGE_URL = "https://www.summbears.net/schools/connections-visual-performing-arts-academy/"
FALL_SPORTS_PAGE_URL = "https://www.summbears.net/athletics/fall-sports/"
FOOTBALL_PAGE_URL = "https://www.summbears.net/athletics/fall-sports/football/"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        key = repr(value) if isinstance(value, dict) else _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.\-+]+@[\w.\-]+\.\w+", text))


def _extract_phone_numbers(text: str) -> list[str]:
    phone_pattern = re.compile(r"\b(?:\+?1[-.\s]*)?(?:\(\d{3}\)|\d{3})[-.\s]*\d{3}[-.\s]*\d{4}\b")
    return _dedupe_keep_order([_clean(match) for match in phone_pattern.findall(text)])


def _lines_with_keywords(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _find_first_link(links: list[dict[str, str]], *, text_contains: str | None = None, href_contains: str | None = None) -> str:
    for link in links:
        text = link.get("text", "").lower()
        href = link.get("href", "").lower()
        if text_contains and text_contains.lower() not in text:
            continue
        if href_contains and href_contains.lower() not in href:
            continue
        return link["href"]
    return ""


async def _collect_page(page) -> dict[str, Any]:
    body_text = _clean(await page.inner_text("body"))
    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    iframes = await page.eval_on_selector_all(
        "iframe",
        """els => els.map(e => ({
            src: e.src || '',
            title: e.title || '',
            name: e.name || ''
        }))""",
    )
    if not isinstance(anchors, list):
        anchors = []
    if not isinstance(iframes, list):
        iframes = []

    return {
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "anchors": [
            {"text": _clean(str(item.get("text") or "")), "href": _clean(str(item.get("href") or ""))}
            for item in anchors
            if isinstance(item, dict) and str(item.get("href") or "").strip()
        ],
        "iframes": [
            {"src": _clean(str(item.get("src") or "")), "title": _clean(str(item.get("title") or ""))}
            for item in iframes
            if isinstance(item, dict)
        ],
    }


def _parse_school_page(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    anchors = signal.get("anchors", [])
    football_link = _find_first_link(anchors, text_contains="football")
    school_context_lines = _lines_with_keywords(
        body_text,
        (
            "integrated with summerville high school",
            "shares facilities",
            "grade level coordinator",
            "office hours",
            "address",
            "phone",
            "email",
        ),
    )

    return {
        "school_page_url": signal.get("final_url", ""),
        "school_page_title": signal.get("title", ""),
        "school_football_link_url": football_link,
        "school_integrated_with_summerville_high_school": "integrated with summerville high school" in body_text.lower(),
        "school_shares_facilities_with_summerville_high_school": "shares facilities with summerville high school" in body_text.lower(),
        "school_phone_numbers": _extract_phone_numbers(body_text),
        "school_emails": _extract_emails(body_text),
        "school_context_lines": school_context_lines,
    }


def _parse_football_page(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    anchors = signal.get("anchors", [])
    iframes = signal.get("iframes", [])

    coach_lines = _lines_with_keywords(body_text, ("coach information", "jv coach", "varsity coach"))
    coaches: list[dict[str, str]] = []
    coach_match_patterns = [
        ("Junior Varsity", r"JV Coach:\s*(.+?)(?=\s+Varsity Coach:|\s+2025 Game Schedule|$)"),
        ("Varsity", r"Varsity Coach:\s*(.+?)(?=\s+2025 Game Schedule|$)"),
    ]
    for level, pattern in coach_match_patterns:
        match = re.search(pattern, body_text, flags=re.IGNORECASE)
        if match:
            coaches.append({"level": level, "name": _clean(match.group(1))})

    schedule_iframe_url = ""
    for item in iframes:
        src = str(item.get("src") or "").strip()
        if "cifsjshome.org/widget/event-list" in src:
            schedule_iframe_url = src
            break

    football_lines = _lines_with_keywords(
        body_text,
        ("summerville football", "upcoming games", "coach information", "game schedule", "summerville bears"),
    )

    return {
        "football_page_url": signal.get("final_url", ""),
        "football_page_title": signal.get("title", ""),
        "football_program_name": "Summerville Football",
        "football_team_name": "Summerville Bears",
        "football_coaches": coaches,
        "football_coach_lines": coach_lines,
        "football_schedule_iframe_url": schedule_iframe_url,
        "football_contact_email": next((email for email in _extract_emails(body_text) if email.lower() == "info@summbears.net"), ""),
        "football_contact_phone_numbers": _extract_phone_numbers(body_text),
        "football_contact_address": "17555 Tuolumne Road, Tuolumne, CA 95379" if "17555 tuolumne road" in body_text.lower() else "",
        "football_nav_link_url": _find_first_link(anchors, text_contains="football"),
        "football_lines": football_lines,
    }


def _parse_schedule_page(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    rows = signal.get("schedule_rows", [])
    options = signal.get("schedule_options", [])

    events: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 7:
            continue
        events.append(
            {
                "day_of_month": _clean(str(row[0])),
                "date_label": _clean(str(row[1])),
                "time": _clean(str(row[2])),
                "level_abbrev": _clean(str(row[3])),
                "opponent": _clean(str(row[4])),
                "location": _clean(str(row[5])),
                "result": _clean(str(row[6])),
                "raw_text": " ".join(_clean(str(cell)) for cell in row if _clean(str(cell))),
            }
        )

    return {
        "football_schedule_widget_title": signal.get("title", ""),
        "football_schedule_levels_available": [str(item.get("text") or "").strip() for item in options if isinstance(item, dict)],
        "football_schedule_events": events,
        "football_schedule_summary_lines": _lines_with_keywords(body_text, ("football", "varsity", "junior varsity", "away", "home", "results")),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape the public football pages connected to Connections VPAA."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted([SCHOOL_PAGE_URL, FALL_SPORTS_PAGE_URL, FOOTBALL_PAGE_URL], profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    school_signal: dict[str, Any] = {}
    football_signal: dict[str, Any] = {}
    schedule_signal: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE, proxy_index=PROXY_INDEX),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in (SCHOOL_PAGE_URL, FALL_SPORTS_PAGE_URL, FOOTBALL_PAGE_URL):
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(1500)
                    signal = await _collect_page(page)
                    source_pages.append(signal["final_url"])
                    if url == SCHOOL_PAGE_URL:
                        school_signal = signal
                    elif url == FOOTBALL_PAGE_URL:
                        football_signal = signal
                        schedule_url = ""
                        for item in signal.get("iframes", []):
                            src = str(item.get("src") or "").strip()
                            if "cifsjshome.org/widget/event-list" in src:
                                schedule_url = src
                                break
                        if schedule_url:
                            try:
                                await page.goto(schedule_url, wait_until="domcontentloaded", timeout=60000)
                                await page.wait_for_timeout(1500)
                                rows = await page.locator("tbody tr").evaluate_all(
                                    "els => els.map(row => Array.from(row.querySelectorAll('th,td')).map(cell => (cell.innerText || '').replace(/\\s+/g, ' ').trim()))"
                                )
                                options = await page.locator("option").evaluate_all(
                                    "els => els.map(e => ({text: (e.textContent || '').replace(/\\s+/g, ' ').trim(), value: e.value || ''}))"
                                )
                                schedule_signal = {
                                    "final_url": page.url,
                                    "title": _clean(await page.title()),
                                    "body_text": _clean(await page.inner_text("body")),
                                    "schedule_rows": rows if isinstance(rows, list) else [],
                                    "schedule_options": options if isinstance(options, list) else [],
                                }
                                source_pages.append(schedule_signal["final_url"])
                            except Exception as exc:  # noqa: BLE001
                                errors.append(f"schedule_navigation_failed:{type(exc).__name__}")
                        else:
                            errors.append("missing_schedule_iframe_url")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_items = _parse_school_page(school_signal)
    football_items = _parse_football_page(football_signal)
    schedule_items = _parse_schedule_page(schedule_signal)

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(
            football_items.get("football_page_url")
            and (football_items.get("football_coaches") or schedule_items.get("football_schedule_events"))
        ),
        **school_items,
        **football_items,
        **schedule_items,
    }

    if not extracted_items["football_program_available"]:
        errors.append("blocked:no_public_football_content_found")

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_index": PROXY_INDEX,
            "target_urls": [SCHOOL_PAGE_URL, FALL_SPORTS_PAGE_URL, FOOTBALL_PAGE_URL],
            "pages_checked": len(source_pages),
            "focus": "football_only",
        }
    )

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


def main() -> int:
    payload = asyncio.run(scrape_school())
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
