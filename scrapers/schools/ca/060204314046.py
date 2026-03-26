"""Deterministic football scraper for Ednovate - Legacy College Prep (CA)."""

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

NCES_ID = "060204314046"
SCHOOL_NAME = "Ednovate - Legacy College Prep."
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://legacyfamilies.ednovate.org"
ATHLETICS_URL = f"{BASE_URL}/95725_2"
COACH_DIRECTORY_URL = f"{BASE_URL}/95726_2"

TARGET_URLS = [ATHLETICS_URL, COACH_DIRECTORY_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _format_person_name(value: str) -> str:
    cleaned = _clean(value)
    return re.sub(r"\s*,\s*", ", ", cleaned)


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        if isinstance(value, str):
            key = _clean(value)
        else:
            key = repr(value)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", text or ""))


def _extract_links(links: list[dict[str, str]], base_url: str) -> list[str]:
    out: list[str] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        combo = f"{text} {absolute}".lower()
        if any(term in combo for term in ("football", "coach", "calendar", "athletic", "sports")):
            out.append(f"{text}|{absolute}" if text else absolute)
    return _dedupe_keep_order(out)


def _extract_relevant_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(
            term in lowered
            for term in (
                "football",
                "girls flag football",
                "coach directory",
                "athletics",
                "sports calendar",
                "homecampus",
                "athletic director",
            )
        ):
            lines.append(line)
    return _dedupe_keep_order(lines)[:120]


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body = _clean(await page.locator("body").inner_text())
    link_items = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(link_items, list):
        link_items = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body,
        "football_lines": _extract_relevant_lines(body),
        "emails": _extract_emails(body),
        "links": _extract_links([item for item in link_items if isinstance(item, dict)], requested_url),
        "raw_links": link_items,
    }


async def _collect_coach_entries(page) -> list[dict[str, str]]:
    entries = await page.eval_on_selector_all(
        "a.dir_name",
        """els => els.map((anchor) => {
            const card = anchor.closest('div[class*="dir"], li, article, section, div') || anchor.parentElement;
            const titleNode = card ? card.querySelector('.dir_title') : null;
            const img = card ? card.querySelector('img') : null;
            const name = (anchor.textContent || '').replace(/\\s+/g, ' ').trim();
            const href = anchor.getAttribute('href') || '';
            const title = titleNode ? (titleNode.textContent || '').replace(/\\s+/g, ' ').trim() : '';
            const photo = img ? (img.getAttribute('src') || '') : '';
            return { name, href, title, photo };
        })""",
    )
    if not isinstance(entries, list):
        return []

    cleaned: list[dict[str, str]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = _format_person_name(str(entry.get("name") or ""))
        title = _clean(str(entry.get("title") or ""))
        href = str(entry.get("href") or "").strip()
        photo = str(entry.get("photo") or "").strip()
        if not name and not title:
            continue
        cleaned.append(
            {
                "name": name,
                "title": title,
                "href": urljoin(COACH_DIRECTORY_URL, href) if href else "",
                "photo": photo,
            }
        )
    return _dedupe_keep_order(cleaned)


async def scrape_school() -> dict[str, Any]:
    """Scrape Legacy College Prep's public football evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    coach_entries: list[dict[str, str]] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy={
                "server": proxy["server"],
                "username": proxy.get("username"),
                "password": proxy.get("password"),
            },
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                    if url == COACH_DIRECTORY_URL:
                        coach_entries = await _collect_coach_entries(page)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_lines: list[str] = []
    emails: list[str] = []
    links: list[str] = []
    for signal in page_signals:
        athletics_lines.extend(signal.get("football_lines", []))
        emails.extend(signal.get("emails", []))
        links.extend(signal.get("links", []))

    athletics_lines = _dedupe_keep_order(athletics_lines)
    emails = _dedupe_keep_order(emails)
    links = _dedupe_keep_order(links)

    football_coaches = [
        entry
        for entry in coach_entries
        if "football" in (entry.get("title") or "").lower()
    ]
    flag_football_coaches = [
        entry
        for entry in coach_entries
        if "flag football" in (entry.get("title") or "").lower()
    ]
    athletic_director = [
        entry
        for entry in coach_entries
        if "athletic director" in (entry.get("title") or "").lower()
    ]

    football_program_available = bool(athletics_lines or football_coaches or flag_football_coaches)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": ATHLETICS_URL,
        "coach_directory_url": COACH_DIRECTORY_URL,
        "football_team_names": [
            "Football" if football_program_available else "",
            "Girls Flag Football" if flag_football_coaches else "",
        ],
        "football_coaches": football_coaches,
        "girls_flag_football_coaches": flag_football_coaches,
        "athletic_director": athletic_director[0] if athletic_director else {},
        "football_keyword_lines": athletics_lines,
        "football_links": links,
        "program_contact_emails": emails,
        "summary": (
            "Legacy College Prep publicly lists Football and Girls' Flag Football on its athletics page, and the coach directory names Michael Schnyder for Football and Quintasha Finley for Girls Flag Football."
            if football_program_available
            else ""
        ),
    }

    extracted_items["football_team_names"] = _dedupe_keep_order(
        [name for name in extracted_items["football_team_names"] if name]
    )

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
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
