"""Deterministic athletics availability scraper for Academy (The)- SF @McAteer (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "063441012526"
SCHOOL_NAME = "Academy (The)- SF @McAteer"
STATE = "CA"
BASE_URL = "https://www.sotacad.org/"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

NAVIGATION_LINK_TEXTS = [
    "About",
    "About Us",
    "Students",
    "Academics",
    "Programs",
    "Activities",
    "Athletics",
    "Sports",
    "Contact",
]

FALLBACK_PATHS = [
    "about",
    "about-us",
    "students",
    "academics",
    "programs",
    "activities",
    "athletics",
    "sports",
    "news",
    "calendar",
]

SCHOOL_KEYWORDS = (
    "academy",
    "mca teer",
    "mcateer",
    "school of the arts",
    "ruth asawa",
    "sota",
    "san francisco",
)

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
    "volleyball",
    "track",
    "cross country",
    "wrestling",
    "tennis",
    "swim",
    "golf",
    "coach",
    "schedule",
    "roster",
    "tryout",
)


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _keyword_lines(text: str, keywords: tuple[str, ...], *, limit: int = 25) -> list[str]:
    matches: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _is_same_domain(candidate_url: str, base_url: str) -> bool:
    candidate_host = (urlparse(candidate_url).hostname or "").lower()
    base_host = (urlparse(base_url).hostname or "").lower()
    if not candidate_host or not base_host:
        return False
    return candidate_host == base_host or candidate_host.endswith(f".{base_host}")


async def _collect_page_signal(page) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    page_title = await page.title()
    school_lines = _keyword_lines(f"{page_title}\n{body_text}", SCHOOL_KEYWORDS)
    athletics_lines = _keyword_lines(body_text, ATHLETICS_KEYWORDS)

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    athletics_links: list[str] = []
    school_links: list[str] = []
    for link in links:
        label = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        combo = f"{label} {href}".lower()
        if any(keyword in combo for keyword in ATHLETICS_KEYWORDS):
            athletics_links.append(f"{label}|{href}")
        if any(keyword in combo for keyword in SCHOOL_KEYWORDS):
            school_links.append(f"{label}|{href}")

    return {
        "url": page.url,
        "title": page_title,
        "school_keyword_lines": school_lines,
        "athletics_keyword_lines": athletics_lines,
        "athletics_links": _dedupe_keep_order(athletics_links)[:25],
        "school_keyword_links": _dedupe_keep_order(school_links)[:25],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate school website pages and detect publicly available athletics content."""
    require_proxy_credentials()

    planned_urls = [BASE_URL, *[urljoin(BASE_URL, path) for path in FALLBACK_PATHS]]
    assert_not_blocklisted(planned_urls)

    source_pages: list[str] = []
    errors: list[str] = []
    signals: list[dict[str, Any]] = []

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

        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            signals.append(await _collect_page_signal(page))

            for link_text in NAVIGATION_LINK_TEXTS:
                link = page.get_by_role("link", name=link_text, exact=False).first
                if await link.count() == 0:
                    continue
                try:
                    await link.click()
                    await page.wait_for_load_state("domcontentloaded")
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    signals.append(await _collect_page_signal(page))
                except Exception:  # noqa: BLE001
                    continue

            for path in FALLBACK_PATHS:
                url = urljoin(BASE_URL, path)
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                signals.append(await _collect_page_signal(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_keyword_lines: list[str] = []
    school_keyword_links: list[str] = []
    athletics_lines: list[str] = []
    athletics_links: list[str] = []
    non_school_destination_urls: list[str] = []

    for signal in signals:
        signal_url = str(signal.get("url") or "")
        if not _is_same_domain(signal_url, BASE_URL):
            non_school_destination_urls.append(signal_url)

        school_keyword_lines.extend(signal.get("school_keyword_lines", []))
        school_keyword_links.extend(signal.get("school_keyword_links", []))
        athletics_lines.extend(signal.get("athletics_keyword_lines", []))
        athletics_links.extend(signal.get("athletics_links", []))

    school_keyword_lines = _dedupe_keep_order(school_keyword_lines)
    school_keyword_links = _dedupe_keep_order(school_keyword_links)
    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_links = _dedupe_keep_order(athletics_links)
    non_school_destination_urls = _dedupe_keep_order(non_school_destination_urls)

    school_identity_confirmed = bool(school_keyword_lines or school_keyword_links)
    athletics_program_available = school_identity_confirmed and bool(
        athletics_lines or athletics_links
    )

    blocked_reason = ""
    if not school_identity_confirmed:
        blocked_reason = (
            "Target domain did not present school-identifiable content; "
            "public school athletics status cannot be verified from this website."
        )
        errors.append("blocked:website_not_school_content_or_domain_redirect")
    elif not athletics_program_available:
        blocked_reason = (
            "No public athletics program content found after manual navigation across "
            "home, menu links, and common subpages."
        )
        errors.append("blocked:no_public_athletics_program_content_found")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "school_identity_confirmed": school_identity_confirmed,
        "blocked_reason": blocked_reason,
        "manual_navigation_link_texts": NAVIGATION_LINK_TEXTS,
        "fallback_paths_checked": FALLBACK_PATHS,
        "school_keyword_lines": school_keyword_lines,
        "school_keyword_links": school_keyword_links,
        "athletics_keyword_lines": athletics_lines,
        "athletics_links": athletics_links,
        "non_school_destination_urls": non_school_destination_urls,
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
            "manual_navigation_steps": [
                "home_page",
                "menu_link_clicks",
                "fallback_subpage_visits",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
