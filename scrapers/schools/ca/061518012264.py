"""Deterministic football scraper for Christopher High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061518012264"
SCHOOL_NAME = "Christopher High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://chs.gilroyunified.org/"
ATHLETICS_URL = "https://chs.gilroyunified.org/athletics"
COACHES_URL = "https://chs.gilroyunified.org/athletics/chs-coaches-and-schedules"

TARGET_PAGES = [HOME_URL, ATHLETICS_URL, COACHES_URL]

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
        cleaned = _clean(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        output.append(cleaned)
    return output


def _is_blocked(title: str, text: str) -> bool:
    lowered_title = _clean(title).lower()
    lowered_text = _clean(text).lower()
    return (
        "cloudflare" in lowered_text
        or "attention required" in lowered_text
        or "access denied" in lowered_text
        or "forbidden" in lowered_text
        or "access denied" in lowered_title
        or "forbidden" in lowered_title
        or "attention required" in lowered_title
    )


def _extract_lines(text: str) -> list[str]:
    return [_clean(line) for line in (text or "").splitlines() if _clean(line)]


def _extract_football_section(lines: list[str]) -> dict[str, Any]:
    football_index = next((i for i, line in enumerate(lines) if line.lower() == "football"), None)
    if football_index is None:
        return {"coaches": [], "schedule_label": "", "section_lines": []}

    section_lines: list[str] = []
    for line in lines[football_index + 1 : football_index + 12]:
        if re.match(r"^(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)$", line) and "coach" not in line.lower():
            if line.lower() not in {"varsity", "jv"}:
                break
        section_lines.append(line)

    coaches = [line for line in section_lines if "coach" in line.lower()]
    schedule_label = next((line for line in section_lines if "schedule" in line.lower()), "")

    return {
        "coaches": _dedupe_keep_order(coaches),
        "schedule_label": _clean(schedule_label),
        "section_lines": _dedupe_keep_order(section_lines),
    }


async def _collect_page(page) -> dict[str, Any]:
    raw_text = await page.locator("body").inner_text()
    body_text = _clean(raw_text)
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(anchor => ({"
        "text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),"
        "href: anchor.getAttribute('href') || ''"
        "}))",
    )
    if not isinstance(links, list):
        links = []

    normalized_links: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        normalized_links.append(
            {
                "text": _clean(str(item.get("text") or "")),
                "href": href,
            }
        )

    title = _clean(await page.title())
    return {
        "title": title,
        "url": page.url,
        "text": body_text,
        "lines": _extract_lines(raw_text),
        "links": normalized_links,
        "blocked": _is_blocked(title, body_text),
    }


def _find_schedule_link(links: list[dict[str, str]], base_url: str) -> str:
    for link in links:
        href = (link.get("href") or "").strip()
        if not href:
            continue
        if "football" in href.lower() and href.lower().endswith(".pdf"):
            return urljoin(base_url, href)
    return ""


async def scrape_school() -> dict[str, Any]:
    """Scrape football-specific public evidence from Christopher High."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_PAGES, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_log: list[str] = []
    snapshots: list[dict[str, Any]] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            home_snapshot = await _collect_page(page)
            snapshots.append(home_snapshot)
            source_pages.append(home_snapshot["url"])
            navigation_log.append("visit_home")

            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            athletics_snapshot = await _collect_page(page)
            snapshots.append(athletics_snapshot)
            source_pages.append(athletics_snapshot["url"])
            navigation_log.append("visit_athletics")

            await page.goto(COACHES_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            coaches_snapshot = await _collect_page(page)
            snapshots.append(coaches_snapshot)
            source_pages.append(coaches_snapshot["url"])
            navigation_log.append("visit_coaches_and_schedules")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"navigation_failed:{type(exc).__name__}")
        finally:
            await context.close()
            await browser.close()

    athletics_lines = athletics_snapshot.get("lines", []) if snapshots else []
    athletics_football_line = next(
        (line for line in athletics_lines if "football" in line.lower()),
        "",
    )

    coaches_lines = coaches_snapshot.get("lines", []) if snapshots else []
    football_section = _extract_football_section(coaches_lines)
    schedule_url = _find_schedule_link(coaches_snapshot.get("links", []), COACHES_URL)

    extracted_items: dict[str, Any] = {}
    if football_section["coaches"] or schedule_url or athletics_football_line:
        extracted_items["football"] = {
            "coaches": football_section["coaches"],
            "schedule_url": schedule_url,
            "athletics_overview": _clean(athletics_football_line),
            "section_lines": football_section["section_lines"],
        }

    if not extracted_items and any(snapshot.get("blocked") for snapshot in snapshots):
        errors.append("blocked_page_detected")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "proxy": get_proxy_runtime_meta(PROXY_PROFILE),
            "navigation": navigation_log,
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    result = asyncio.run(scrape_school())
    print(json.dumps(result, indent=2, ensure_ascii=False))
