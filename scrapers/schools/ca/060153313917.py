"""Deterministic football scraper for Ingenuity Charter (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "060153313917"
SCHOOL_NAME = "Ingenuity Charter"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.ingenuitycharter.org/"
ATHLETICS_URL = "https://www.ingenuitycharter.org/student-life/athletics"
OFARRELL_ATHLETICS_URL = "https://high.ofarrellschool.org/apps/pages/index.jsp?uREC_ID=3709698&type=d&pREC_ID=2426116"
OFARRELL_FOOTBALL_URL = "https://high.ofarrellschool.org/apps/pages/index.jsp?uREC_ID=3706376&type=d"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    OFARRELL_ATHLETICS_URL,
    OFARRELL_FOOTBALL_URL,
]


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


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


def _extract_email(text: str) -> str:
    match = re.search(r"[\w.+\-']+@[\w.\-]+\.[A-Za-z]{2,}", text)
    return _clean(match.group(0)) if match else ""


def _extract_phone(text: str) -> str:
    match = re.search(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}", text)
    return _clean(match.group(0)) if match else ""


def _parse_schedule_lines(text: str) -> list[dict[str, str]]:
    schedule: list[dict[str, str]] = []
    pattern = re.compile(
        r"(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+"
        r"(?P<day>MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY)\s+"
        r"(?P<time>\d{1,2}:\d{2}\s*[AP]M|)\s*"
        r"Football\s+"
        r"(?P<opponent>.*?)\s+"
        r"(?P<location>@?[A-Za-z0-9'.()&\-\s]+?)(?=\s+\d{1,2}/\d{1,2}/\d{4}|\s*$)",
        flags=re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        schedule.append(
            {
                "date": _clean(match.group("date")),
                "day": _clean(match.group("day")).title(),
                "time": _clean(match.group("time")),
                "opponent": _clean(match.group("opponent")),
                "location": _clean(match.group("location")),
            }
        )
    return schedule


async def _collect_links(page) -> list[tuple[str, str]]:
    raw = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => ({text:(e.textContent || '').replace(/\\s+/g,' ').trim(), href:e.href || ''}))",
    )
    links: list[tuple[str, str]] = []
    if not isinstance(raw, list):
        return links
    for item in raw:
        if not isinstance(item, dict):
            continue
        text = _clean(item.get("text", ""))
        href = _clean(item.get("href", ""))
        if href:
            links.append((text, href))
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        deduped.append(link)
    return deduped


def _select_link(
    links: list[tuple[str, str]],
    *,
    text_contains: tuple[str, ...] = (),
    href_contains: tuple[str, ...] = (),
    fallback: str = "",
) -> str:
    for text, href in links:
        lower_text = text.lower()
        lower_href = href.lower()
        text_ok = not text_contains or any(token in lower_text for token in text_contains)
        href_ok = not href_contains or any(token in lower_href for token in href_contains)
        if text_ok and href_ok:
            return href
    return fallback


async def scrape_school() -> dict[str, Any]:
    """Scrape football-relevant data exposed to Ingenuity students."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    home_text = ""
    athletics_text = ""
    football_text = ""

    discovered_ofarrell_athletics = OFARRELL_ATHLETICS_URL
    discovered_football_page = OFARRELL_FOOTBALL_URL
    home_links: list[tuple[str, str]] = []
    athletics_links: list[tuple[str, str]] = []
    football_links: list[tuple[str, str]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1365, "height": 768},
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            home_text = await page.inner_text("body")
            home_links = await _collect_links(page)
            source_pages.append(page.url)

            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            athletics_text = await page.inner_text("body")
            athletics_links = await _collect_links(page)
            source_pages.append(page.url)

            discovered_ofarrell_athletics = _select_link(
                athletics_links,
                href_contains=("high.ofarrellschool.org", "athletics"),
                fallback=OFARRELL_ATHLETICS_URL,
            )
            await page.goto(discovered_ofarrell_athletics, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            ofarrell_athletics_links = await _collect_links(page)
            source_pages.append(page.url)

            discovered_football_page = _select_link(
                ofarrell_athletics_links,
                text_contains=("football",),
                href_contains=("football",),
                fallback=OFARRELL_FOOTBALL_URL,
            )
            await page.goto(discovered_football_page, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            football_text = await page.inner_text("body")
            football_links = await _collect_links(page)
            source_pages.append(page.url)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await context.close()
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_lines = [
        line
        for line in [
            sentence.strip()
            for sentence in re.split(r"(?<=[.!?])\s+", athletics_text)
            if sentence.strip()
        ]
        if "ingenuity students are permitted to try out and play cif sports" in line.lower()
        or "title ix coordinator" in line.lower()
    ][:6]

    football_lines: list[str] = []
    for raw_line in football_text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(token in lowered for token in ("football", "coach", "schedule", "opponent")):
            football_lines.append(line)
    football_lines = _dedupe_keep_order(football_lines)[:40]

    coach_match = re.search(r"COACH:\s*([^\n\r]+)", football_text, flags=re.IGNORECASE)
    coach_name = _clean(coach_match.group(1)) if coach_match else ""
    contact_email = _extract_email(football_text)
    contact_phone = _extract_phone(football_text)
    football_shop_url = _select_link(
        football_links,
        href_contains=("football.shop",),
        fallback="https://ofarrellfootball.shop/",
    )
    schedule = _parse_schedule_lines(football_text)

    extracted_items: dict[str, Any] = {
        "school_home_url": HOME_URL,
        "school_athletics_url": ATHLETICS_URL,
        "partner_athletics_url": discovered_ofarrell_athletics,
        "football_page_url": discovered_football_page,
        "football_program_available": bool(football_lines or schedule or coach_name),
        "football_team_name": "O'Farrell Falcons Football",
        "football_coach_name": coach_name,
        "football_contact_email": contact_email,
        "football_contact_phone": contact_phone,
        "football_shop_url": football_shop_url,
        "football_schedule": schedule,
        "athletics_access_notes": athletics_lines,
        "football_keyword_lines": football_lines,
        "related_football_links": _dedupe_keep_order(
            [href for text, href in football_links if "football" in f"{text} {href}".lower()]
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
            "focus": "football_only",
            "manual_navigation_steps": [
                "home",
                "student_life_athletics",
                "partner_athletics_page",
                "partner_football_page",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


async def main() -> None:
    result = await scrape_school()
    print(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
