"""Deterministic football scraper for Highlands High (CA)."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060133201988"
SCHOOL_NAME = "Highlands High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://hhs.trusd.net/"
SPORTS_URL = "https://hhs.trusd.net/Sports/index.html"
FOOTBALL_URL = "https://hhs.trusd.net/Sports/Football/index.html"
TARGET_URLS = [HOME_URL, SPORTS_URL, FOOTBALL_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


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


def _normalize_href(href: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    return value


def _normalize_link(item: Any) -> dict[str, str]:
    return {
        "text": _clean(str(item.get("text") or "")),
        "href": _normalize_href(str(item.get("href") or "")),
    }


def _dedupe_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if not href:
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        out.append({"text": text, "href": href})
    return out


def _is_blocked(title: str, text: str) -> bool:
    combined = f"{title}\n{text}".lower()
    return any(
        token in combined
        for token in (
            "403 forbidden",
            "access denied",
            "attention required",
            "cloudflare",
            "blocked",
            "robot check",
        )
    )


async def _snapshot(page, scope: str = "main") -> dict[str, Any]:
    title = _clean(await page.title())
    scope_locator = page.locator(scope)
    try:
        text = _clean(await scope_locator.first.inner_text(timeout=20_000))
    except Exception:
        text = _clean(await page.locator("body").inner_text(timeout=20_000))

    links = await page.locator("a[href]").evaluate_all(
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    normalized_links = _dedupe_links(
        [_normalize_link(link) for link in links if isinstance(link, dict)]
    )

    return {
        "url": page.url,
        "title": title,
        "text": text,
        "links": normalized_links,
        "blocked": _is_blocked(title, text),
    }


def _extract_maxpreps_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for link in links:
        blob = f"{link.get('text', '')} {link.get('href', '')}".lower()
        if "maxpreps.com" in blob or "team page" in blob:
            out.append({"text": _clean(link.get("text", "")), "href": _clean(link.get("href", ""))})
    return _dedupe_links(out)


def _extract_head_coach(text: str) -> str:
    match = re.search(r"Coach:\s*([A-Z][A-Za-z.' -]+)", text, re.I)
    return _clean(match.group(1)) if match else ""


def _extract_assistant_coaches(text: str) -> list[str]:
    matches = re.findall(r"ASSISTANT COACH:\s*([A-Z][A-Za-z.' -]+)", text, re.I)
    return _dedupe_keep_order(matches)


def _extract_athletic_director(text: str) -> dict[str, str]:
    match = re.search(
        r"ATHLETIC DIRECTOR\s*([A-Z][A-Za-z.' -]+)\s*(\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}(?:\s*ext\.\s*\d+)?)",
        text,
        re.I,
    )
    if not match:
        return {"name": "", "phone": ""}
    return {"name": _clean(match.group(1)), "phone": _clean(match.group(2))}


async def scrape_school() -> dict[str, Any]:
    """Scrape Highlands High's public football page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    home = {"url": HOME_URL, "title": "", "text": "", "links": [], "blocked": False}
    sports = {"url": SPORTS_URL, "title": "", "text": "", "links": [], "blocked": False}
    football = {"url": FOOTBALL_URL, "title": "", "text": "", "links": [], "blocked": False}
    coach_heading = ""
    coach_text = ""
    donation_blurb = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            user_agent=USER_AGENT,
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(1_200)
            home = await _snapshot(page)
            source_pages.append(home["url"])

            await page.goto(SPORTS_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(1_200)
            sports = await _snapshot(page)
            source_pages.append(sports["url"])

            await page.goto(FOOTBALL_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(1_200)
            football = await _snapshot(page)
            source_pages.append(football["url"])

            coach_heading_locator = page.locator(".sidenav .catapultTitle h3 .title div")
            if await coach_heading_locator.count():
                coach_heading = _clean(
                    await coach_heading_locator.first.inner_text(timeout=20_000)
                )

            coach_text_locator = page.locator(".sidenav .FW_EDITOR_STYLE")
            if await coach_text_locator.count():
                coach_text = _clean(
                    await coach_text_locator.first.inner_text(timeout=20_000)
                )

            donation_locator = page.locator(".msbcheckout")
            if await donation_locator.count():
                donation_blurb = _clean(
                    await donation_locator.first.inner_text(timeout=20_000)
                )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    blocked_urls = [
        page_data["url"]
        for page_data in (home, sports, football)
        if page_data.get("blocked") and page_data.get("url")
    ]
    if blocked_urls and len(blocked_urls) == len(source_pages):
        errors.append("blocked:all_target_pages_presented_access_denial")
    elif blocked_urls:
        errors.append(f"access_limited:blocked_pages_present:{'|'.join(blocked_urls)}")

    maxpreps_links = _extract_maxpreps_links(football["links"])
    head_coach = _extract_head_coach(f"{coach_heading} {coach_text}")
    assistant_coaches = _extract_assistant_coaches(coach_text)
    athletic_director = _extract_athletic_director(sports["text"])

    if not maxpreps_links:
        errors.append("football_links_missing:maxpreps_team_pages_not_found")
    if not head_coach:
        errors.append("football_coach_missing:head_coach_not_found")

    extracted_items: dict[str, Any] = {
        "football_page": {
            "title": football["title"],
            "url": football["url"],
        },
        "maxpreps_team_pages": maxpreps_links,
        "head_coach": head_coach,
        "assistant_coaches": assistant_coaches,
        "athletic_director": athletic_director,
        "donation_blurb": donation_blurb,
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "target_urls": TARGET_URLS,
        },
        "errors": errors,
    }

