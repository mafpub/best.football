"""Deterministic football scraper for Bridgepoint High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "062706004080"
SCHOOL_NAME = "Bridgepoint High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.newarkunified.org"
ATHLETICS_HOME_URL = f"{HOME_URL}/cougarathletics/home"
FALL_SPORTS_URL = f"{HOME_URL}/cougarathletics/fall-sports"
FOOTBALL_URL = f"{HOME_URL}/cougarathletics/2020-fall-sports/football"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_HOME_URL,
    FALL_SPORTS_URL,
    FOOTBALL_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_TERMS = (
    "football",
    "coach",
    "roster",
    "schedule",
    "varsity",
    "jv",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


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


def _normalize_link(raw_href: str, base_url: str) -> str:
    href = (raw_href or "").strip()
    if not href:
        return ""
    if href.startswith("mailto:") or href.startswith("tel:"):
        return href
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return f"https:{href}"
    return urljoin(base_url, href)


def _find_matching_url(
    links: list[dict[str, str]],
    *, 
    terms: tuple[str, ...],
) -> str:
    lowered_terms = tuple(term.lower() for term in terms)
    for link in links:
        if not isinstance(link, dict):
            continue
        text = _clean(str(link.get("text") or "")).lower()
        href = _clean(str(link.get("href") or "")).lower()
        if any(term in text or term in href for term in lowered_terms):
            return href
    return ""


def _extract_url_lines(source: str) -> str:
    return _clean(str(source).split("?")[0].strip())


async def _collect_page_snapshot(page, requested_url: str) -> dict[str, Any]:
    try:
        body_text = await page.inner_text("body")
    except Exception:  # noqa: BLE001
        body_text = ""

    try:
        links = await page.eval_on_selector_all(
            "a[href]",
            "els => els.map((anchor) => ({"
            " text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),"
            " href: anchor.getAttribute('href') || ''"
            " }))",
        )
    except Exception:  # noqa: BLE001
        links = []

    normalized_links: list[dict[str, str]] = []
    if isinstance(links, list):
        for item in links:
            if not isinstance(item, dict):
                continue
            text = _clean(str(item.get("text") or ""))
            href = _normalize_link(str(item.get("href") or ""), requested_url)
            if href:
                normalized_links.append({"text": text, "href": href})

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "text": _clean(body_text),
        "links": normalized_links,
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_log: list[str] = []
    page_signals: list[dict[str, Any]] = []
    proxy_config = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=proxy_config,
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            try:
                await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_snapshot(page, HOME_URL))
                navigation_log.append("visit_home")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_home_failed:{type(exc).__name__}")

            athletics_url = ""
            if page_signals:
                athletics_url = _find_matching_url(
                    page_signals[-1]["links"],
                    terms=("cougarathletics", "athletics"),
                )
            if not athletics_url:
                athletics_url = ATHLETICS_HOME_URL

            try:
                await page.goto(athletics_url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_snapshot(page, athletics_url))
                navigation_log.append("navigate_to_athletics")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_athletics_failed:{type(exc).__name__}")

            fall_sports_url = ""
            if page_signals:
                fall_sports_url = _find_matching_url(
                    page_signals[-1]["links"],
                    terms=("fall sports", "fall-sports"),
                )
            if not fall_sports_url:
                fall_sports_url = FALL_SPORTS_URL

            if _extract_url_lines(fall_sports_url):
                try:
                    await page.goto(
                        fall_sports_url,
                        wait_until="domcontentloaded",
                        timeout=90000,
                    )
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page_snapshot(page, fall_sports_url))
                    navigation_log.append("navigate_to_fall_sports")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_fall_sports_failed:{type(exc).__name__}")

            football_url = ""
            if page_signals:
                football_url = _find_matching_url(
                    page_signals[-1]["links"],
                    terms=("football",),
                )
            if not football_url:
                football_url = FOOTBALL_URL

            if _extract_url_lines(football_url) and football_url not in source_pages:
                try:
                    await page.goto(
                        football_url,
                        wait_until="domcontentloaded",
                        timeout=90000,
                    )
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page_snapshot(page, football_url))
                    navigation_log.append("navigate_to_football_page")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_football_failed:{type(exc).__name__}")

            # Deterministic fallback coverage to ensure football evidence is collected even if dynamic
            # navigation varies by environment.
            for fallback_url in (FALL_SPORTS_URL, FOOTBALL_URL, ATHLETICS_HOME_URL):
                if fallback_url in source_pages:
                    continue
                try:
                    await page.goto(fallback_url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page_snapshot(page, fallback_url))
                    navigation_log.append(f"fallback_goto:{fallback_url}")
                except Exception as exc:  # noqa: BLE001
                    errors.append(
                        f"navigation_fallback_failed:{fallback_url}:{type(exc).__name__}"
                    )
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_text = " ".join([str(signal.get("text") or "") for signal in page_signals])
    all_lines = _extract_lines(
        all_text,
        keywords=FOOTBALL_TERMS,
    )
    football_lines = _dedupe_keep_order(
        [line for line in all_lines if "football" in line.lower()]
    )
    coach_lines = _dedupe_keep_order(
        [line for line in all_lines if "coach" in line.lower()]
    )
    roster_lines = _dedupe_keep_order(
        [line for line in all_lines if "roster" in line.lower()]
    )
    schedule_lines = _dedupe_keep_order(
        [line for line in all_lines if "schedule" in line.lower()]
    )

    football_links: list[str] = []
    for signal in page_signals:
        for link in signal.get("links", []):
            if not isinstance(link, dict):
                continue
            text = _clean(str(link.get("text") or ""))
            href = _clean(str(link.get("href") or ""))
            if not href:
                continue
            if "football" in href.lower() or "football" in text.lower():
                football_links.append(f"{text}|{href}" if text else href)

    football_links = _dedupe_keep_order(football_links)

    football_signals = [
        signal
        for signal in page_signals
        if "football" in str(signal.get("final_url", "")).lower()
    ]
    football_title = (
        _clean((football_signals[0].get("title") if football_signals else "") or "")
    )

    football_program_available = bool(football_lines or football_links or football_title)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_program_name": "Football",
        "football_team_names": ["Football"] if football_lines else [],
        "football_pages": [signal.get("final_url") for signal in football_signals],
        "football_title": football_title,
        "football_mentions": football_lines,
        "football_coach_lines": coach_lines,
        "football_schedule_lines": schedule_lines,
        "football_roster_lines": roster_lines,
        "football_links": football_links,
        "navigation_log": navigation_log,
        "source_line_count": len(all_text.splitlines()),
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
            "proxy_server": proxy_config.get("server", ""),
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_log,
            "football_evidence_count": len(football_lines),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
