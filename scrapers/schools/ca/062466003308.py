"""Deterministic football scraper for Golden Valley High (CA)."""

from __future__ import annotations

import asyncio
import json
import io
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import requests
from playwright.async_api import async_playwright
from pypdf import PdfReader

from pipeline.proxy import get_httpx_proxy_url
from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062466003308"
SCHOOL_NAME = "Golden Valley High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://gvhs.muhsd.org/"
TEAMS_AND_SCHEDULES_URL = "https://gvhs.muhsd.org/42016_2"
FOOTBALL_SCHEDULE_PDF_URL = (
    "https://files.smartsites.parentsquare.com/4013/football_schedule_2025_2.pdf"
)

TARGET_URLS = [HOME_URL, TEAMS_AND_SCHEDULES_URL, FOOTBALL_SCHEDULE_PDF_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

COACH_PATTERNS = (
    ("Varsity", re.compile(r"Varsity Head Coach\s*[–-]\s*(?P<name>.+)", re.I)),
    ("JV", re.compile(r"JV Head Coach\s*[–-]\s*(?P<name>.+)", re.I)),
    ("Freshman", re.compile(r"Freshmen Head Coach\s*[–-]\s*(?P<name>.+)", re.I)),
)

SCHEDULE_LINE_RE = re.compile(
    r"^(?P<day>[A-Za-z]{3,4}\.)\s+(?P<month>[A-Za-z]{3}\.)\s+(?P<daynum>\d{1,2})\s+"
    r"(?P<body>.+?)\s+(?P<level>Varsity|JV|Frosh/Soph|JV/Frosh-Soph)\s+"
    r"(?P<time>\d{1,2}:\d{2}(?:am|pm))$",
    re.I,
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _keyword_lines(text: str, keywords: tuple[str, ...], limit: int = 20) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _normalize_multiline_text(text: str) -> str:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return "\n".join(lines)


def _collect_links(page_links: list[dict[str, str]], base_url: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for link in page_links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if not href:
            continue
        if href.startswith("//"):
            href = f"https:{href}"
        elif href.startswith("/"):
            href = urljoin(base_url, href)
        elif not href.startswith("http://") and not href.startswith("https://"):
            href = urljoin(base_url, href)
        out.append({"text": text, "href": href})

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for link in out:
        key = (link["text"], link["href"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    return deduped


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)
    title = _clean(await page.title())
    body_text = _clean(await page.locator("body").inner_text(timeout=15000))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []
    return {
        "url": page.url,
        "title": title,
        "body_text": body_text,
        "links": _collect_links([link for link in links if isinstance(link, dict)], page.url),
    }


def _find_football_link(links: list[dict[str, str]]) -> dict[str, str]:
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if text.lower() == "football" or "football_schedule" in href.lower():
            return {"text": text, "href": href}
    return {}


def _proxy_url() -> str:
    return get_httpx_proxy_url(profile=PROXY_PROFILE)


def _download_pdf_text(url: str) -> dict[str, Any]:
    response = requests.get(
        url,
        proxies={"http": _proxy_url(), "https": _proxy_url()},
        timeout=60,
        allow_redirects=True,
    )
    response.raise_for_status()
    reader = PdfReader(io.BytesIO(response.content))
    text = _normalize_multiline_text("\n".join((page.extract_text() or "") for page in reader.pages))
    return {
        "source_url": url,
        "final_url": response.url,
        "content_type": response.headers.get("content-type", ""),
        "filename": Path(response.url).name,
        "page_count": len(reader.pages),
        "text": text,
    }


def _extract_coaches(pdf_text: str) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    for level, pattern in COACH_PATTERNS:
        match = pattern.search(pdf_text)
        if not match:
            continue
        coaches.append(
            {
                "level": level,
                "name": _clean(match.group("name")),
                "source": "football_schedule_pdf",
            }
        )
    return coaches


def _extract_schedule_rows(pdf_text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for raw_line in (pdf_text or "").splitlines():
        line = _clean(raw_line)
        if not line or line.lower().startswith("day date opponent location level time"):
            continue
        match = SCHEDULE_LINE_RE.match(line)
        if not match:
            continue
        body = _clean(match.group("body"))
        rows.append(
            {
                "day": match.group("day"),
                "date": f"{match.group('month')} {match.group('daynum')}",
                "opponent_location": body,
                "level": _clean(match.group("level")),
                "time": _clean(match.group("time")),
                "home_game": "true" if "GVHS" in body else "false",
                "raw_line": line,
            }
        )
    return rows


def _extract_contact(pdf_text: str) -> dict[str, str]:
    match = re.search(
        r"2121 E\. Childs Avenue Merced, CA 95341\s+\((209)\)\s*(325-1865)\s+Fax\s+\((209)\)\s*(385-8002)",
        pdf_text,
        re.I,
    )
    if not match:
        return {}
    return {
        "address": "2121 E. Childs Avenue Merced, CA 95341",
        "phone": f"({match.group(1)}) {match.group(2)}",
        "fax": f"({match.group(3)}) {match.group(4)}",
        "source": "football_schedule_pdf",
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Golden Valley High football information from public pages and PDF schedule."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

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
            home_page = await _collect_page(page, HOME_URL)
            source_pages.append(home_page["url"])

            teams_page = await _collect_page(page, TEAMS_AND_SCHEDULES_URL)
            source_pages.append(teams_page["url"])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{exc}")
            home_page = {"url": HOME_URL, "title": "", "body_text": "", "links": []}
            teams_page = {"url": TEAMS_AND_SCHEDULES_URL, "title": "", "body_text": "", "links": []}
        finally:
            await browser.close()

    home_lines = _keyword_lines(
        home_page.get("body_text", ""),
        keywords=("football", "athletics", "cougar", "coach", "schedule"),
    )
    teams_lines = _keyword_lines(
        teams_page.get("body_text", ""),
        keywords=("football", "coach", "schedule", "athletics", "rick martinez"),
    )
    teams_links = teams_page.get("links", [])
    football_link = _find_football_link(teams_links if isinstance(teams_links, list) else [])

    pdf_url = football_link.get("href") or FOOTBALL_SCHEDULE_PDF_URL
    if pdf_url:
        source_pages.append(pdf_url)

    try:
        pdf_data = _download_pdf_text(pdf_url)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"football_schedule_pdf_failed:{type(exc).__name__}:{exc}")
        pdf_data = {
            "source_url": pdf_url,
            "final_url": pdf_url,
            "content_type": "",
            "filename": "",
            "page_count": 0,
            "text": "",
        }

    coaches = _extract_coaches(pdf_data["text"])
    schedule_rows = _extract_schedule_rows(pdf_data["text"])
    contact = _extract_contact(pdf_data["text"])

    extracted_items: dict[str, Any] = {
        "football_program_available": True,
        "home_page": {
            "url": home_page["url"],
            "title": home_page["title"],
            "football_keyword_lines": home_lines,
        },
        "teams_and_schedules_page": {
            "url": teams_page["url"],
            "title": teams_page["title"],
            "football_keyword_lines": teams_lines,
            "football_schedule_link": football_link,
        },
        "football_schedule_pdf": {
            "source_url": pdf_data["source_url"],
            "final_url": pdf_data["final_url"],
            "filename": pdf_data["filename"],
            "content_type": pdf_data["content_type"],
            "page_count": pdf_data["page_count"],
        },
        "football_coaches": coaches,
        "football_schedule_rows": schedule_rows,
        "football_contact": contact,
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "navigation_steps": [
                "visit_home_page",
                "visit_teams_and_schedules_page",
                "download_football_schedule_pdf",
            ],
        },
        "errors": errors,
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
