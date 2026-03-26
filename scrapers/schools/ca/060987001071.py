"""Deterministic football scraper for Coronado High (CA)."""

from __future__ import annotations

import csv
import io
import re
import sys
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pypdf import PdfReader

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060987001071"
SCHOOL_NAME = "Coronado High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://chs.coronadousd.net/"
ATHLETICS_URL = "https://chs.coronadousd.net/Athletics/index.html"
ATHLETIC_SCHEDULE_URL = "https://docs.google.com/spreadsheets/d/1ebVAkjo8vl0cTktelzs9UD0B6f78r-2d0CAgFFwl4_Q/edit?gid=0"
ATHLETIC_SCHEDULE_EXPORT_URL = (
    "https://docs.google.com/spreadsheets/d/1ebVAkjo8vl0cTktelzs9UD0B6f78r-2d0CAgFFwl4_Q/"
    "export?format=csv&gid=0"
)
SPORTS_TRYOUTS_PDF_URL = "https://chs.coronadousd.net/documents/Students--Parents/CHS-Sports-2022-23.pdf"

TARGET_URLS = [
    ATHLETICS_URL,
    ATHLETIC_SCHEDULE_EXPORT_URL,
    SPORTS_TRYOUTS_PDF_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_links(html: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = urljoin(base_url, anchor.get("href", "").strip())
        if href:
            links.append({"text": text, "href": href})
    return _dedupe_link_dicts(links)


def _dedupe_link_dicts(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    output: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        key = (text, href)
        if not href or key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 25) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _extract_pdf_text(payload: bytes) -> str:
    reader = PdfReader(io.BytesIO(payload))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _extract_schedule_csv_lines(csv_text: str) -> list[str]:
    lines: list[str] = []
    current_day = ""
    current_date = ""
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        cells = [_clean(cell) for cell in row]
        if not any(cells):
            continue

        if cells[0]:
            current_day = cells[0]
        if len(cells) > 1 and cells[1]:
            current_date = cells[1]

        if "football" not in " ".join(cells).lower():
            continue

        team = cells[2] if len(cells) > 2 else ""
        opponent = cells[3] if len(cells) > 3 else ""
        site = cells[4] if len(cells) > 4 else ""
        game_time = cells[5] if len(cells) > 5 else ""
        release = cells[6] if len(cells) > 6 else ""

        parts = [value for value in (current_day, current_date, team, opponent, site, game_time, release) if value]
        if parts:
            lines.append(" ".join(parts))

    return _dedupe_keep_order(lines)


def _extract_school_contact(text: str) -> dict[str, str]:
    address_match = re.search(r"650 D Avenue\s+Coronado,\s+CA\s+92118", text, flags=re.IGNORECASE)
    phone_match = re.search(r"\(619\)\s*522-8907", text)
    fax_match = re.search(r"\(619\)\s*437-0236", text)
    return {
        "address": "650 D Avenue Coronado, CA 92118" if address_match else "",
        "phone": "(619) 522-8907" if phone_match else "",
        "fax": "(619) 437-0236" if fax_match else "",
    }


def _extract_athletic_director(text: str) -> dict[str, str]:
    name_match = re.search(
        r"e-mail Athletic Director,\s*([A-Za-z][A-Za-z.'\-]*(?:\s+[A-Za-z][A-Za-z.'\-]*)*)\s+at\b",
        text,
        flags=re.IGNORECASE,
    )
    email_match = re.search(
        r"([\w.\-+]+@[\w.\-]+\.\w+)",
        text,
        flags=re.IGNORECASE,
    )
    return {
        "name": _clean(name_match.group(1)) if name_match else "",
        "email": _clean(email_match.group(1)) if email_match else "",
    }


def _extract_football_schedule_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if "football" in lowered and any(day in lowered for day in ("mon", "tue", "wed", "thu", "fri", "sat", "sun")):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_schedule_games(text: str) -> list[str]:
    games: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        if "football" not in line.lower():
            continue
        if re.search(r"\b(?:var|varsity|jv|frosh|freshman)\b", line, flags=re.IGNORECASE):
            games.append(line)
    return _dedupe_keep_order(games)


async def _capture_page(page, requested_url: str) -> dict[str, Any]:
    html = await page.content()
    body_text = _clean(await page.locator("body").inner_text())
    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "links": _extract_links(html, page.url),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football signals from the school athletics pages and PDF."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    schedule_csv_text = ""
    pdf_text = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in (ATHLETICS_URL, ATHLETIC_SCHEDULE_EXPORT_URL):
                try:
                    if url == ATHLETIC_SCHEDULE_EXPORT_URL:
                        response = await context.request.get(url, timeout=90000)
                        source_pages.append(url)
                        schedule_csv_text = await response.text()
                        continue

                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(2000)
                    source_pages.append(page.url)
                    page_signals.append(await _capture_page(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"page_fetch_failed:{type(exc).__name__}:{url}")

            try:
                response = await context.request.get(SPORTS_TRYOUTS_PDF_URL, timeout=90000)
                source_pages.append(str(response.url))
                pdf_text = _extract_pdf_text(await response.body())
            except Exception as exc:  # noqa: BLE001
                errors.append(f"pdf_fetch_failed:{type(exc).__name__}:{SPORTS_TRYOUTS_PDF_URL}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    signal_map = {signal["requested_url"]: signal for signal in page_signals}

    athletics_text = str(signal_map.get(ATHLETICS_URL, {}).get("body_text") or "")
    schedule_text = schedule_csv_text

    athletics_links = _dedupe_link_dicts(
        [
            link
            for signal in page_signals
            for link in signal.get("links", [])
            if isinstance(link, dict)
        ]
    )
    athletics_links = [
        link
        for link in athletics_links
        if any(
            keyword in f"{link.get('text', '')} {link.get('href', '')}".lower()
            for keyword in (
                "athletic schedule",
                "islander sports foundation",
                "registration",
                "sports physical",
            )
        )
    ]

    athletics_summary_lines = _extract_lines(
        athletics_text,
        keywords=("athletics", "athletic schedule", "registration", "sports physical", "islander"),
        limit=25,
    )
    football_schedule_lines = _extract_schedule_csv_lines(schedule_text)
    football_game_lines = football_schedule_lines[:]
    tryout_lines = _extract_lines(
        pdf_text,
        keywords=("football", "athletic director", "try-outs", "sports offered", "register"),
        limit=20,
    )
    school_contact = _extract_school_contact(athletics_text)
    athletic_director = _extract_athletic_director(pdf_text)

    football_program_available = any(
        "football" in text.lower() for text in (athletics_text, schedule_text, pdf_text)
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_athletics_url": ATHLETICS_URL,
        "athletics_summary_lines": athletics_summary_lines,
        "athletics_links": athletics_links,
        "athletic_schedule_url": ATHLETIC_SCHEDULE_URL,
        "athletic_schedule_export_url": ATHLETIC_SCHEDULE_EXPORT_URL,
        "athletic_schedule_lines": football_schedule_lines,
        "football_games": football_game_lines,
        "sports_tryouts_pdf_url": SPORTS_TRYOUTS_PDF_URL,
        "sports_tryouts_lines": tryout_lines,
        "athletic_director": {
            "name": athletic_director.get("name", ""),
            "email": athletic_director.get("email", ""),
            "source": SPORTS_TRYOUTS_PDF_URL,
        },
        "school_contact": {
            "address": school_contact.get("address", ""),
            "phone": school_contact.get("phone", ""),
            "fax": school_contact.get("fax", ""),
            "source": ATHLETICS_URL,
        },
        "football_contact_note": "Coronado High lists football in its sports try-outs PDF and weekly athletic schedule, with football games posted in the Google Sheet.",
        "summary": (
            "Coronado High publicly exposes football through its athletics menu, a Google Sheets athletic schedule with football games, and a sports try-outs PDF that names football and the athletic director contact."
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
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "target_urls": TARGET_URLS,
            "manual_navigation_steps": [
                "athletics",
                "athletic_schedule",
                "sports_tryouts_pdf",
            ],
            "focus": "football_only",
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
