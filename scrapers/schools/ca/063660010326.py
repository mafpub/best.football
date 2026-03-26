"""Deterministic football scraper for Foothill High (CA)."""

from __future__ import annotations

import asyncio
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

NCES_ID = "063660010326"
SCHOOL_NAME = "Foothill High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_HOME_URL = "https://www.foothillcougars.com/"
FOOTBALL_HOME_URL = "https://www.foothillcougarfb.com/"
VARSITY_PAGE_URL = "https://www.foothillcougarfb.com/team"
VARSITY_SCHEDULE_URL = "https://www.foothillcougarfb.com/schedule-1"
VARSITY_COACHES_URL = "https://www.foothillcougarfb.com/coaches-1"
VARSITY_ROSTER_URL = "https://www.foothillcougarfb.com/roster"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _dedupe_dicts(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for value in values:
        key = (_clean(value.get("label", "")), _clean(value.get("url", "")))
        if not key[1] or key in seen:
            continue
        seen.add(key)
        out.append({"label": key[0], "url": key[1]})
    return out


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(1500)
    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    body_text = _clean(await page.inner_text("body"))
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "anchors": [
            {
                "text": _clean(str(item.get("text") or "")),
                "href": _clean(str(item.get("href") or "")),
            }
            for item in anchors
            if isinstance(item, dict) and str(item.get("href") or "").strip()
        ],
    }


def _find_link(anchors: list[dict[str, str]], text: str) -> str:
    needle = text.lower()
    for anchor in anchors:
        if needle in anchor.get("text", "").lower():
            return anchor.get("href", "")
    return ""


def _collect_nav_links(anchors: list[dict[str, str]], labels: list[str]) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for label in labels:
        url = _find_link(anchors, label)
        if url:
            results.append({"label": label, "url": url})
    return _dedupe_dicts(results)


def _keyword_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe(lines)


async def scrape_school() -> dict[str, Any]:
    """Scrape Foothill High's public football pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(
        [
            SCHOOL_HOME_URL,
            FOOTBALL_HOME_URL,
            VARSITY_PAGE_URL,
            VARSITY_SCHEDULE_URL,
            VARSITY_COACHES_URL,
            VARSITY_ROSTER_URL,
        ],
        profile=PROXY_PROFILE,
    )

    errors: list[str] = []
    source_pages: list[str] = []

    school_signal: dict[str, Any] = {}
    football_signal: dict[str, Any] = {}
    varsity_signal: dict[str, Any] = {}
    schedule_signal: dict[str, Any] = {}
    coaches_signal: dict[str, Any] = {}
    roster_signal: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            school_signal = await _collect_page(page, SCHOOL_HOME_URL)
            football_signal = await _collect_page(page, FOOTBALL_HOME_URL)
            varsity_signal = await _collect_page(page, VARSITY_PAGE_URL)
            schedule_signal = await _collect_page(page, VARSITY_SCHEDULE_URL)
            coaches_signal = await _collect_page(page, VARSITY_COACHES_URL)
            roster_signal = await _collect_page(page, VARSITY_ROSTER_URL)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
            errors.append(str(exc))
        finally:
            await browser.close()

    source_pages = _dedupe(
        [
            school_signal.get("url", ""),
            football_signal.get("url", ""),
            varsity_signal.get("url", ""),
            schedule_signal.get("url", ""),
            coaches_signal.get("url", ""),
            roster_signal.get("url", ""),
        ]
    )

    school_anchors = school_signal.get("anchors", [])
    football_anchors = football_signal.get("anchors", [])

    extracted_items = {
        "school_home_title": school_signal.get("title", ""),
        "school_football_link_url": _find_link(school_anchors, "football"),
        "football_home_title": football_signal.get("title", ""),
        "football_program_name": "Foothill Cougar Football",
        "football_home_url": football_signal.get("url", ""),
        "varsity_page_url": varsity_signal.get("url", ""),
        "varsity_schedule_url": schedule_signal.get("url", ""),
        "varsity_coaches_url": coaches_signal.get("url", ""),
        "varsity_roster_url": roster_signal.get("url", ""),
        "football_navigation_links": _collect_nav_links(
            football_anchors,
            [
                "Varsity",
                "Schedule",
                "Coaches",
                "Roster",
                "JV",
                "Frosh",
                "Full Schedule",
            ],
        ),
        "football_social_links": _dedupe_dicts(
            [
                {"label": anchor.get("text", ""), "url": anchor.get("href", "")}
                for anchor in football_anchors
                if any(
                    domain in anchor.get("href", "").lower()
                    for domain in ("facebook.com", "instagram.com")
                )
            ]
        ),
        "football_keyword_lines": _keyword_lines(
            "\n".join(
                [
                    football_signal.get("body_text", ""),
                    varsity_signal.get("body_text", ""),
                    schedule_signal.get("body_text", ""),
                    coaches_signal.get("body_text", ""),
                    roster_signal.get("body_text", ""),
                ]
            ),
            ("football", "varsity", "jv", "frosh", "schedule", "coach", "roster"),
        )[:20],
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0",
            **get_proxy_runtime_meta(PROXY_PROFILE),
        },
        "errors": _dedupe(errors),
    }


if __name__ == "__main__":
    print(asyncio.run(scrape_school()))
