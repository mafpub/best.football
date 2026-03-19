"""Deterministic athletics availability scraper for Academy of Careers and Exploration (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "061686012368"
SCHOOL_NAME = "Academy of Careers and Exploration"
STATE = "CA"

DISTRICT_HOME = "https://www.helendalesd.org/"
SCHOOL_HOME = "https://www.helendalesd.org/Schools/ACE-and-RMS-School/index.html"
ATHLETICS_PAGE = "https://www.helendalesd.org/Schools/ACE-and-RMS-School/Athletics/index.html"
SCHOOL_DOCS_PAGE = "https://www.helendalesd.org/Schools/ACE-and-RMS-School/ACE-School-Documents/index.html"
ACADEMIC_COUNSELING_PAGE = (
    "https://www.helendalesd.org/Schools/ACE-and-RMS-School/Academic-Counseling/index.html"
)

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_URL_SEQUENCE = [
    DISTRICT_HOME,
    SCHOOL_HOME,
    SCHOOL_DOCS_PAGE,
    ACADEMIC_COUNSELING_PAGE,
    ATHLETICS_PAGE,
]

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "athletic directors",
    "coach",
    "mission statement",
    "competition",
    "team",
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


def _keyword_lines(text: str, *, limit: int = 30) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_contacts(text: str) -> list[dict[str, str]]:
    lines = [" ".join(line.split()).strip() for line in text.splitlines()]
    lines = [line for line in lines if line]

    contacts: list[dict[str, str]] = []
    for idx, line in enumerate(lines):
        if not re.fullmatch(r"[\w.\-+]+@[\w.\-]+\.\w+", line):
            continue
        email = line
        name = ""
        role = ""

        for back in range(1, 4):
            prev_idx = idx - back
            if prev_idx < 0:
                break
            candidate = lines[prev_idx]
            if "@" in candidate:
                continue
            if not name and re.search(r"[A-Za-z]{2,}", candidate):
                name = candidate
                continue
            if not role and any(
                token in candidate.lower() for token in ("director", "coach", "athletic")
            ):
                role = candidate

        contacts.append({"name": name, "email": email, "role": role})

    unique_contacts: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in contacts:
        key = f"{item.get('email', '').lower()}|{item.get('name', '').lower()}"
        if not item.get("email") or key in seen:
            continue
        seen.add(key)
        unique_contacts.append(item)

    return unique_contacts[:10]


async def _capture_page(page) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    return {
        "url": page.url,
        "title": await page.title(),
        "athletics_keyword_lines": _keyword_lines(body_text),
    }


async def _try_click(page, nav_log: list[str], description: str, action) -> None:
    try:
        await action()
        await page.wait_for_timeout(1200)
        nav_log.append(f"clicked:{description}")
    except Exception as exc:  # noqa: BLE001
        nav_log.append(f"click_failed:{description}:{type(exc).__name__}")


async def scrape_school() -> dict[str, Any]:
    """Scrape public athletics availability using manual navigation of school subpages."""
    require_proxy_credentials()
    assert_not_blocklisted(MANUAL_URL_SEQUENCE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_snapshots: list[dict[str, Any]] = []
    navigation_log: list[str] = []

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
            await page.goto(DISTRICT_HOME, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1400)
            navigation_log.append("visited:district_home")
            source_pages.append(page.url)
            page_snapshots.append(await _capture_page(page))

            await _try_click(
                page,
                navigation_log,
                "header_schools_link",
                lambda: page.get_by_role("link", name=re.compile(r"^Schools$")).first.click(
                    timeout=7000
                ),
            )
            source_pages.append(page.url)
            page_snapshots.append(await _capture_page(page))

            await _try_click(
                page,
                navigation_log,
                "ace_and_rms_school_link",
                lambda: page.get_by_role(
                    "link",
                    name=re.compile(r"ACE and RMS School", re.IGNORECASE),
                )
                .first.click(timeout=7000),
            )
            source_pages.append(page.url)
            page_snapshots.append(await _capture_page(page))

            await _try_click(
                page,
                navigation_log,
                "athletics_link_from_school_menu",
                lambda: page.get_by_role("link", name=re.compile(r"^Athletics$", re.IGNORECASE))
                .first.click(timeout=7000),
            )
            source_pages.append(page.url)
            page_snapshots.append(await _capture_page(page))

            for url in MANUAL_URL_SEQUENCE[1:]:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1000)
                navigation_log.append(f"visited:{url}")
                source_pages.append(page.url)
                page_snapshots.append(await _capture_page(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_lines: list[str] = []
    athletics_page_seen = False
    athletics_page_text = ""

    for snapshot in page_snapshots:
        url = str(snapshot.get("url") or "")
        lines = snapshot.get("athletics_keyword_lines", [])
        if isinstance(lines, list):
            athletics_lines.extend(item for item in lines if isinstance(item, str))

        if "ACE-and-RMS-School/Athletics" in url:
            athletics_page_seen = True
            athletics_page_text = "\n".join(lines) if isinstance(lines, list) else ""

    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_contacts = _extract_contacts(athletics_page_text)
    athletics_program_available = bool(athletics_page_seen and athletics_lines)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_athletics_program_content_found_on_ace_rms_athletics_page"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "athletics_page_url": ATHLETICS_PAGE,
        "athletics_keyword_mentions": athletics_lines[:30],
        "athletics_contacts": athletics_contacts,
        "manual_navigation_log": navigation_log,
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
                "district_home",
                "schools_menu",
                "ace_rms_school_subpage",
                "athletics_subpage",
                "manual_subpage_verification_sequence",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
