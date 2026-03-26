"""Deterministic football scraper for High Tech High Chula Vista (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060206112083"
SCHOOL_NAME = "High Tech High Chula Vista"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_URL = "https://www.hightechhigh.org/athletics/"
SCHOOL_URL = "https://www.hightechhigh.org/hthcv/"
SPORTS_PARTICIPANTS_URL = "https://cvbruinathletics.hightechhigh.org/sports-participants"
CONTACT_URL = "https://cvbruinathletics.hightechhigh.org/contact"
TARGET_URLS = [ATHLETICS_URL, SCHOOL_URL, SPORTS_PARTICIPANTS_URL, CONTACT_URL]

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
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_subsite_url(text: str) -> str:
    match = re.search(r"https://cvbruinathletics\.hightechhigh\.org/?", text, re.IGNORECASE)
    return _clean(match.group(0)) if match else ""


def _extract_flag_football_count(text: str) -> int | None:
    match = re.search(r"Girls Flag Football:\s*(\d+)", text, re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1))


def _extract_enrollment(text: str) -> dict[str, int] | dict[str, Any]:
    total_match = re.search(r"High Tech High Chula Vista:\s*(\d+)", text, re.IGNORECASE)
    girls_match = re.search(r"Girls\s*-\s*(\d+)", text, re.IGNORECASE)
    boys_match = re.search(r"Boys\s*-\s*(\d+)", text, re.IGNORECASE)
    return {
        "total_students": int(total_match.group(1)) if total_match else None,
        "girls_students": int(girls_match.group(1)) if girls_match else None,
        "boys_students": int(boys_match.group(1)) if boys_match else None,
    }


def _extract_athlete_totals(text: str) -> dict[str, int] | dict[str, Any]:
    girls_match = re.search(
        r"Total Number who participate in competitive athletics, classified by gender:\s*Girls:\s*(\d+)",
        text,
        re.IGNORECASE,
    )
    boys_match = re.search(
        r"Total Number who participate in competitive athletics, classified by gender:\s*Girls:\s*\d+\s*Boys:\s*(\d+)",
        text,
        re.IGNORECASE,
    )
    return {
        "girls_athletes": int(girls_match.group(1)) if girls_match else None,
        "boys_athletes": int(boys_match.group(1)) if boys_match else None,
    }


def _extract_athletic_director(text: str) -> dict[str, str]:
    match = re.search(
        r"Contact\s+([A-Za-z .'-]+?)\s+HTHCV Athletic Director\s+([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
        text,
        re.IGNORECASE,
    )
    if not match:
        return {}
    return {
        "name": _clean(match.group(1)),
        "title": "HTHCV Athletic Director",
        "email": _clean(match.group(2)).lower(),
    }


async def _fetch_page_text(page, url: str) -> tuple[str, str]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)
    body_text = await page.inner_text("body")
    return page.url, _clean(body_text)


async def scrape_school() -> dict[str, Any]:
    """Scrape public football signals for High Tech High Chula Vista."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)
    errors: list[str] = []
    source_pages: list[str] = []
    page_text_by_url: dict[str, str] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for target_url in TARGET_URLS:
                final_url, text = await _fetch_page_text(page, target_url)
                source_pages.append(final_url)
                page_text_by_url[target_url] = text
        finally:
            await context.close()
            await browser.close()

    athletics_text = page_text_by_url.get(ATHLETICS_URL, "")
    school_text = page_text_by_url.get(SCHOOL_URL, "")
    participants_text = page_text_by_url.get(SPORTS_PARTICIPANTS_URL, "")
    contact_text = page_text_by_url.get(CONTACT_URL, "")

    athletics_subsite_url = _extract_subsite_url(athletics_text)
    girls_flag_football_count = _extract_flag_football_count(participants_text)
    athletic_director = _extract_athletic_director(contact_text)
    enrollment = _extract_enrollment(participants_text)
    athlete_totals = _extract_athlete_totals(participants_text)

    football_program_available = bool(girls_flag_football_count and girls_flag_football_count > 0)
    if not football_program_available:
        errors.append("no_public_flag_football_participant_count_found")

    if not athletic_director:
        errors.append("athletic_director_contact_not_found")

    school_phone_match = re.search(r"\(619\)\s*591-2500", school_text)
    school_address_match = re.search(
        r"1945 Discovery Falls Drive, Chula Vista, CA 91915",
        school_text,
        re.IGNORECASE,
    )
    director_match = re.search(
        r"DIRECTOR\s+([A-Za-z .'-]+)\s+([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
        school_text,
        re.IGNORECASE,
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_program_type": "girls_flag_football" if football_program_available else "",
        "athletics_subsite_url": athletics_subsite_url,
        "school_profile": {
            "school_page_url": SCHOOL_URL,
            "address": _clean(school_address_match.group(0)) if school_address_match else "",
            "phone": _clean(school_phone_match.group(0)) if school_phone_match else "",
            "school_director_name": _clean(director_match.group(1)) if director_match else "",
            "school_director_email": _clean(director_match.group(2)).lower() if director_match else "",
        },
        "football_participation": {
            "season": "2025-2026",
            "team_label": "Girls Flag Football",
            "participant_count": girls_flag_football_count,
            "source_page_url": SPORTS_PARTICIPANTS_URL,
        },
        "athletic_director_contact": athletic_director,
        "athletics_participation_summary": {
            **enrollment,
            **athlete_totals,
        },
        "football_signals": _dedupe_keep_order(
            [
                f"High Tech High athletics page links Chula Vista athletics subsite: {athletics_subsite_url}",
                (
                    f"Sports Participants page lists Girls Flag Football: {girls_flag_football_count}"
                    if girls_flag_football_count is not None
                    else ""
                ),
                (
                    "Contact page lists athletic director: "
                    f"{athletic_director.get('name')} ({athletic_director.get('email')})"
                    if athletic_director
                    else ""
                ),
            ]
        ),
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(TARGET_URLS),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), indent=2, ensure_ascii=True))
