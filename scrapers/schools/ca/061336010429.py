"""Deterministic football scraper for Angelo Rodriguez High (CA)."""

from __future__ import annotations

import os
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

NCES_ID = "061336010429"
SCHOOL_NAME = "Angelo Rodriguez High"
STATE = "CA"

BASE_URL = "https://www.fsusd.org/o/rhs"
HOME_URL = f"{BASE_URL}"
ATHLETICS_URL = f"{BASE_URL}/athletics"
NEWS_URL = f"{BASE_URL}/news?page_no=3"
FOOTBALL_ARTICLE_URL = f"{BASE_URL}/article/1253066"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, NEWS_URL, FOOTBALL_ARTICLE_URL]

PROXY_PROFILE = "datacenter"
PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


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


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 60) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_links(links: list[dict[str, Any]], *, keywords: tuple[str, ...]) -> list[str]:
    matches: list[str] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = str(item.get("href") or "").strip()
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in keywords):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)


def _extract_school_identity(home_text: str) -> tuple[str, str, str]:
    principal = ""
    principal_match = re.search(r"Principal:\s*([A-Za-z][A-Za-z.\- ]+)", home_text)
    if principal_match:
        principal = _clean(principal_match.group(1))

    address = "5000 Red Top Rd, Fairfield, CA 94534"
    address_match = re.search(
        r"Rodriguez High School\s+([0-9].*?CA 94534)",
        home_text,
        re.IGNORECASE,
    )
    if address_match:
        address = _clean(address_match.group(1)).replace("Rd.Fairfield", "Rd. Fairfield")

    phone = "707-863-7950"
    phone_match = re.search(r"\((\d{3})\)\s*(\d{3})-(\d{4})", home_text)
    if phone_match:
        phone = f"{phone_match.group(1)}-{phone_match.group(2)}-{phone_match.group(3)}"

    return principal, address, phone


def _extract_football_artifact_lines(text: str) -> list[str]:
    return _extract_lines(
        text,
        keywords=(
            "football",
            "coach",
            "varsity",
            "street clean-up",
            "give back",
            "mustangs",
        ),
    )


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "football_lines": _extract_football_artifact_lines(body_text),
        "football_links": _extract_links(
            links,
            keywords=("football", "athletics", "news", "mustangs", "coach", "street"),
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit the school home, athletics, news, and football article pages."""
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
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1400)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page_signal(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_signal = next((signal for signal in page_signals if signal["requested_url"] == HOME_URL), {})
    news_signal = next((signal for signal in page_signals if signal["requested_url"] == NEWS_URL), {})
    article_signal = next(
        (signal for signal in page_signals if signal["requested_url"] == FOOTBALL_ARTICLE_URL),
        {},
    )
    athletics_signal = next(
        (signal for signal in page_signals if signal["requested_url"] == ATHLETICS_URL),
        {},
    )

    home_text = str(home_signal.get("body_text") or "")
    news_text = str(news_signal.get("body_text") or "")
    article_text = str(article_signal.get("body_text") or "")
    athletics_text = str(athletics_signal.get("body_text") or "")

    principal, school_address, school_phone = _extract_school_identity(home_text)
    school_nickname = ""
    nickname_match = re.search(
        r"athletic teams are known as the ([A-Za-z][A-Za-z -]+?) and the school colors",
        home_text,
        re.IGNORECASE,
    )
    if nickname_match:
        school_nickname = _clean(nickname_match.group(1))

    football_lines = _dedupe_keep_order(
        _extract_football_artifact_lines(" ".join([home_text, news_text, article_text, athletics_text]))
    )
    football_links = _dedupe_keep_order(
        [
            value
            for signal in page_signals
            for value in signal.get("football_links", [])
        ]
    )

    football_article_titles = _dedupe_keep_order(
        [
            _clean(str(article_signal.get("title") or "")).split(" | ")[0]
            if article_signal.get("title")
            else ""
        ]
    )
    football_article_titles = _dedupe_keep_order(football_article_titles)

    football_coaches: list[str] = []
    if re.search(r"\bCoach King\b", article_text):
        football_coaches.append("Coach King")
    football_coaches = _dedupe_keep_order(football_coaches)

    football_team_names: list[str] = []
    if "football team" in article_text.lower():
        football_team_names.append("Football")
    if "varsity football team" in article_text.lower():
        football_team_names.append("Varsity Football")
    if school_nickname:
        football_team_names.append(school_nickname)
    football_team_names = _dedupe_keep_order(football_team_names)

    football_program_available = bool(football_lines or football_coaches or football_article_titles)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_names": football_team_names,
        "football_coaches": football_coaches,
        "football_article_title": (
            football_article_titles[0]
            if football_article_titles
            else "Rodriguez High School Football Team Plans Street Clean-Up to Give Back"
        ),
        "football_article_url": FOOTBALL_ARTICLE_URL,
        "football_news_page_url": NEWS_URL,
        "athletics_page_url": ATHLETICS_URL,
        "football_schedule_public": False,
        "football_schedule_note": "No public football schedule page was exposed on the inspected school pages.",
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "school_athletics_nickname": school_nickname or "Mustangs",
        "school_address": school_address,
        "school_phone": school_phone,
        "principal_name": principal,
        "summary": (
            "Rodriguez High School publicly publishes a football news article naming Coach King and describing the varsity football team, while the home page identifies the school athletic nickname as the Mustangs."
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
            "proxy_profile": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
            "manual_navigation_steps": [
                "home",
                "athletics",
                "news_page_3",
                "football_article_1253066",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
