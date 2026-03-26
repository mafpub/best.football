"""Deterministic football scraper for Impact Academy of Arts & Technology (CA)."""

from __future__ import annotations

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

NCES_ID = "060164711849"
SCHOOL_NAME = "Impact Academy of Arts & Technology"
STATE = "CA"

BASE_URL = "https://www.es-impact.org"
ATHLETICS_URL = f"{BASE_URL}/athletics-and-clubs"
OUR_TEAM_URL = f"{BASE_URL}/our-team"
CONTACT_URL = f"{BASE_URL}/contact"
TARGET_URLS = [BASE_URL, ATHLETICS_URL, OUR_TEAM_URL, CONTACT_URL]
PROXY_PROFILE = "datacenter"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_TERMS = ("flag football", "football")
SPLIT_SPORTS = ("cross country", "flag football", "football", "soccer", "basketball")
SIGNAL_TERMS = (
    "athletics",
    "athletic",
    "sports",
    "team",
    "football",
    "soccer",
    "basketball",
    "cross country",
    "season",
    "schedule",
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


def _normalize_link(item: dict[str, Any]) -> dict[str, str]:
    return {"text": _clean(str(item.get("text") or "")), "href": _clean(str(item.get("href") or ""))}


def _extract_lines(body: str) -> list[str]:
    out: list[str] = []
    for raw_line in body.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        if any(term in line.lower() for term in SIGNAL_TERMS):
            out.append(line)
    return _dedupe_keep_order(out)


def _extract_sport_tokens(lines: list[str]) -> list[str]:
    lowered = " ".join(_dedupe_keep_order(lines)).lower()
    out: list[str] = []
    for sport in SPLIT_SPORTS:
        if sport in lowered:
            if sport == "flag football":
                out.append("Flag Football")
            else:
                out.append(_clean(sport.title() if sport != "cross country" else "Cross Country"))
    return _dedupe_keep_order(out)


def _extract_football_gender(lines: list[str], index: int) -> str:
    for offset in range(1, 4):
        if index + offset >= len(lines):
            break
        candidate = lines[index + offset].lower()
        if "girls and boys" in candidate:
            return "girls and boys"
        if "girls teams" in candidate:
            return "girls"
        if "boys teams" in candidate:
            return "boys"
    return ""


def _extract_football_team_names(lines: list[str]) -> list[str]:
    team_names: list[str] = []
    lowered = [line.lower() for line in lines]
    for idx, line in enumerate(lines):
        normalized = line.lower()
        if "flag football" in normalized:
            gender = _extract_football_gender(lowered, idx)
            if gender:
                team_names.append(f"Flag Football ({gender})")
            else:
                team_names.append("Flag Football")
    return _dedupe_keep_order(team_names)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body = await page.inner_text("body")
    lines = _extract_lines(body)
    links_raw = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )
    links = [_normalize_link(item) for item in links_raw if isinstance(item, dict)]

    football_links = []
    for link in links:
        text = link["text"]
        href = link["href"]
        if not href:
            continue
        combined = f"{text} {href}".lower()
        if any(token in combined for token in ("football", "flag", "athletics", "sports", "team")):
            football_links.append(f"{text}|{href}")

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "lines": lines,
        "links": _dedupe_keep_order([f"{item['text']}|{item['href']}" for item in links]),
        "football_links": _dedupe_keep_order(football_links),
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate the school's football-focused athletics content and extract public signal."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
            args=["--lang=en-US,en"],
        )
        context = await browser.new_context(
            viewport={"width": 1365, "height": 900},
            locale="en-US",
            user_agent=USER_AGENT,
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_200)
                    signal = await _collect_page(page, url)
                    page_signals.append(signal)
                    source_pages.append(signal["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    collected_lines: list[str] = []
    football_links: list[str] = []
    for signal in page_signals:
        collected_lines.extend([line for line in signal.get("lines", []) if isinstance(line, str)])
        football_links.extend([link for link in signal.get("football_links", []) if isinstance(link, str)])

    collected_lines = _dedupe_keep_order(collected_lines)
    football_lines = [line for line in collected_lines if any(term in line.lower() for term in FOOTBALL_TERMS)]
    football_program_available = bool(football_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_program_found_on_school_domain")

    athletics_page_lines = [line for line in collected_lines if any(term in line.lower() for term in ("athletics", "athletic", "sports"))]
    football_team_names = _extract_football_team_names(collected_lines)
    team_lines = [line for line in collected_lines if any(token in line.lower() for token in ("girls and boys", "girls teams", "boys teams"))]

    extracted_items = {
        "football_program_available": football_program_available,
        "football_page_url": ATHLETICS_URL,
        "football_team_names": football_team_names,
        "athletics_sports_list": _extract_sport_tokens(athletics_page_lines),
        "football_keywords": football_lines,
        "football_related_links": _dedupe_keep_order([link for link in football_links if link]),
        "team_gender_notes": team_lines[:8],
        "contact_page_url": CONTACT_URL,
        "summary": (
            "Public athletics page lists Fall and Winter sports, including Flag Football."
            if football_program_available
            else "No football-related lines detected on public athletics pages."
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
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "homepage",
                "our_team",
                "athletics_and_clubs",
                "contact",
            ],
            "manual_navigation_focus": [
                "Athletics and Clubs",
                "Flag Football",
                "Girls and boys teams",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Compatibility entrypoint alias."""
    return await scrape_school()
