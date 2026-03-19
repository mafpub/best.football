"""Deterministic athletics availability scraper for Academies of the Antelope Valley (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060282013154"
SCHOOL_NAME = "Academies of the Antelope Valley"
STATE = "CA"

INPUT_WEBSITE = "https://www.academyprepjuniorhigh.org/"
HOME_URL = "https://www.avvirtualschool.org/"
OUR_SCHOOLS_URL = "https://avdistrictorg-1813-us-west1-01.preview.finalsitecdn.com/our-schools"
STUDENTS_URL = "https://www.avvirtualschool.org/students"
CLUBS_URL = "https://www.avvirtualschool.org/students/clubs"
PROGRAMS_URL = "https://www.avvirtualschool.org/academics/programs"

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "academy prep",
    "junior high",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "football",
    "basketball",
    "soccer",
    "volleyball",
    "baseball",
    "softball",
    "wrestling",
    "track",
    "cross country",
    "cheer",
)

PROGRAM_SIGNALS = (
    "athletics department",
    "athletic director",
    "athletics homepage",
    "sports schedule",
    "team roster",
    "tryouts",
    "coach",
)

FALSE_POSITIVE_SNIPPETS = (
    "sports jerseys",
    "non-school sports",
)



def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


async def _collect_page_signals(page) -> dict[str, Any]:
    text = await page.inner_text("body")
    lines: list[str] = []
    mentions_academy_prep = False

    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()

        if "academy prep junior high" in lowered:
            mentions_academy_prep = True

        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    athletics_links: list[str] = []
    for link in links:
        label = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        combo = f"{label} {href}".lower()
        if any(keyword in combo for keyword in ATHLETICS_KEYWORDS):
            athletics_links.append(f"{label}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "mentions_academy_prep": mentions_academy_prep,
        "athletics_keyword_lines": _dedupe_keep_order(lines)[:40],
        "athletics_links": _dedupe_keep_order(athletics_links)[:40],
    }


async def scrape_school() -> dict[str, Any]:
    """Manually navigate Academies of the AV pages and detect athletics availability."""
    require_proxy_credentials()

    planned_urls = [
        INPUT_WEBSITE,
        HOME_URL,
        OUR_SCHOOLS_URL,
        STUDENTS_URL,
        CLUBS_URL,
        PROGRAMS_URL,
        *[f"{HOME_URL}search-results?q={query.replace(' ', '+')}" for query in SEARCH_QUERIES],
    ]
    assert_not_blocklisted(planned_urls)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    manual_navigation_steps: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USERNAME,
                "password": PROXY_PASSWORD,
            },
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        async def _visit(url: str, step: str) -> None:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                manual_navigation_steps.append(step)
                source_pages.append(page.url)
                page_signals.append(await _collect_page_signals(page))
            except PlaywrightTimeoutError:
                manual_navigation_steps.append(f"{step}_timeout")
                errors.append(f"navigation_timeout:{url}")
            except Exception as exc:  # noqa: BLE001
                manual_navigation_steps.append(f"{step}_failed")
                errors.append(f"navigation_failed:{url}:{type(exc).__name__}")

        await _visit(INPUT_WEBSITE, "open_input_website_redirect")
        await _visit(OUR_SCHOOLS_URL, "open_our_schools_page")
        await _visit(STUDENTS_URL, "open_students_page")
        await _visit(CLUBS_URL, "open_clubs_page")
        await _visit(PROGRAMS_URL, "open_programs_page")
        for query in SEARCH_QUERIES:
            search_url = f"{HOME_URL}search-results?q={query.replace(' ', '+')}"
            await _visit(search_url, f"search_{query.replace(' ', '_')}")

        await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    academy_prep_mentions: list[str] = []
    athletics_lines: list[str] = []
    athletics_links: list[str] = []

    for signal in page_signals:
        if signal.get("mentions_academy_prep"):
            academy_prep_mentions.append(str(signal.get("url") or ""))

        for line in signal.get("athletics_keyword_lines", []):
            if isinstance(line, str):
                athletics_lines.append(line)

        for link in signal.get("athletics_links", []):
            if isinstance(link, str):
                athletics_links.append(link)

    academy_prep_mentions = _dedupe_keep_order(academy_prep_mentions)
    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_links = _dedupe_keep_order(athletics_links)

    filtered_athletics_lines = [
        line
        for line in athletics_lines
        if not any(snippet in line.lower() for snippet in FALSE_POSITIVE_SNIPPETS)
    ]
    filtered_athletics_links = [
        link
        for link in athletics_links
        if "/search-results" not in link.lower() and "facebook" not in link.lower()
    ]

    strong_signal = any(
        signal in " | ".join(filtered_athletics_lines + filtered_athletics_links).lower()
        for signal in PROGRAM_SIGNALS
    )
    athletics_program_available = bool(strong_signal)

    blocked_reason = ""
    if not athletics_program_available:
        blocked_reason = (
            "No public athletics program content found for Academy Prep Junior High or "
            "Academies of the AV after manual navigation (home redirect, schools/students/clubs/programs) "
            "and on-site search for athletics/sports/academy prep/junior high."
        )
        errors.append("blocked:no_public_athletics_program_content_found")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "academy_prep_mentions_found_on_pages": academy_prep_mentions,
        "athletics_keyword_lines_raw": athletics_lines,
        "athletics_keyword_lines_filtered": filtered_athletics_lines,
        "athletics_links_filtered": filtered_athletics_links,
        "search_queries_checked": SEARCH_QUERIES,
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
            "proxy_server": PROXY_SERVER,
            "pages_checked": len(source_pages),
            "manual_navigation_steps": manual_navigation_steps,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
