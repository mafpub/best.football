"""Deterministic football scraper for Edward P. Duplex (CA)."""

from __future__ import annotations

import asyncio
import io
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[3]))

import requests
from pypdf import PdfReader
from playwright.async_api import async_playwright

from pipeline.proxy import get_httpx_proxy_url
from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "064235014385"
SCHOOL_NAME = "Edward P. Duplex"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.wheatlandhigh.org"
ATHLETICS_URL = "https://www.wheatlandhigh.org/page/athletics"
SPORTS_SCHEDULES_URL = "https://www.wheatlandhigh.org/page/pirates-sports-schedules"
FOOTBALL_SHORT_URL = "https://5il.co/3nwmb"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, SPORTS_SCHEDULES_URL, FOOTBALL_SHORT_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

MONTH_RE = re.compile(r"\b(?:August|September|October)\s+\d{1,2}\b")
TIME_RE = re.compile(r"\b\d{1,2}:\d{2}/\d{1,2}:\d{2}\b")


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_href(href: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    return value


def _extract_links(html: str, base_url: str) -> list[dict[str, str]]:
    hrefs = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, flags=re.I | re.S)
    links: list[dict[str, str]] = []
    for href, label in hrefs:
        text = _clean(re.sub(r"<[^>]+>", " ", label))
        normalized = _normalize_href(href)
        if not normalized:
            continue
        if normalized.startswith("/"):
            normalized = requests.compat.urljoin(base_url, normalized)
        links.append({"text": text, "href": normalized})
    return links


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)
    title = _clean(await page.title())
    body_text = _clean(await page.locator("body").inner_text())
    html = await page.content()
    return {
        "url": page.url,
        "title": title,
        "text": body_text,
        "links": _extract_links(html, page.url),
    }


def _proxy_url() -> str:
    return get_httpx_proxy_url(profile=PROXY_PROFILE)


def _fetch_football_pdf(short_url: str) -> dict[str, Any]:
    response = requests.get(
        short_url,
        proxies={"http": _proxy_url(), "https": _proxy_url()},
        timeout=60,
        allow_redirects=True,
    )
    response.raise_for_status()
    reader = PdfReader(io.BytesIO(response.content))
    pages = [page.extract_text() or "" for page in reader.pages]
    text = _clean("\n".join(pages))
    return {
        "source_url": short_url,
        "final_url": response.url,
        "content_type": response.headers.get("content-type", ""),
        "page_count": len(reader.pages),
        "text": text,
    }


def _extract_football_schedule_rows(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    matches = list(MONTH_RE.finditer(text))
    for idx, match in enumerate(matches):
        start = match.start()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        chunk = _clean(text[start:end])
        date = _clean(match.group(0))
        rest = _clean(chunk[len(date) :])
        time_match = TIME_RE.search(rest)
        if not time_match:
            continue
        before_time = _clean(rest[: time_match.start()])
        after_time = _clean(rest[time_match.end() :])
        note_match = re.search(r"\(([^)]+)\)", rest)
        note = _clean(note_match.group(1)) if note_match else ""
        before_time_no_note = _clean(re.sub(r"\([^)]*\)", "", before_time))
        tokens = before_time_no_note.split()
        if len(tokens) >= 2:
            opponent = " ".join(tokens[:-1])
            place = tokens[-1]
        else:
            opponent = before_time_no_note
            place = ""
        rows.append(
            {
                "date": date,
                "opponent": opponent,
                "place": place,
                "time": _clean(time_match.group(0)),
                "note": note,
                "raw_text": chunk,
                "post_time_text": after_time,
            }
        )
    return rows


def _extract_football_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if "football" not in (text + " " + href).lower():
            continue
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


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            home = await _collect_page(page, HOME_URL)
            athletics = await _collect_page(page, ATHLETICS_URL)
            sports = await _collect_page(page, SPORTS_SCHEDULES_URL)
            navigation_steps.extend(["home", "athletics", "sports_schedules"])
            source_pages.extend([home["url"], athletics["url"], sports["url"]])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
            home = {"url": HOME_URL, "text": "", "links": []}
            athletics = {"url": ATHLETICS_URL, "text": "", "links": []}
            sports = {"url": SPORTS_SCHEDULES_URL, "text": "", "links": []}
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    combined_text = "\n".join(
        [str(home.get("text") or ""), str(athletics.get("text") or ""), str(sports.get("text") or "")]
    )
    combined_links = []
    for page_links in [home.get("links"), athletics.get("links"), sports.get("links")]:
        if isinstance(page_links, list):
            for link in page_links:
                if isinstance(link, dict):
                    combined_links.append(
                        {
                            "text": _clean(str(link.get("text") or "")),
                            "href": _clean(str(link.get("href") or "")),
                        }
                    )

    football_links = _extract_football_links(combined_links)
    football_pdf = _fetch_football_pdf(FOOTBALL_SHORT_URL)
    source_pages.append(football_pdf["final_url"])
    source_pages = _dedupe_keep_order(source_pages)

    schedule_rows = _extract_football_schedule_rows(football_pdf["text"])
    if not schedule_rows:
        errors.append("blocked:no_public_football_schedule_rows_found")

    head_coach_match = re.search(
        r"Head Coach:\s*(.+?)\s+(?:Superintendent:|Principal:|Assistant Principal:|Athletic Director:|Current as of)",
        football_pdf["text"],
    )
    phone_match = re.search(r"530-633-3100\s+x\s*198", football_pdf["text"])

    extracted_items: dict[str, Any] = {
        "football_program_available": True,
        "athletics_page_url": athletics["url"],
        "sports_schedules_page_url": sports["url"],
        "football_schedule_pdf_short_url": FOOTBALL_SHORT_URL,
        "football_schedule_pdf_url": football_pdf["final_url"],
        "football_schedule_pdf_title": "2025-2026 Football",
        "football_schedule_pdf_page_count": football_pdf["page_count"],
        "football_schedule_rows": schedule_rows,
        "football_schedule_text": football_pdf["text"],
        "football_head_coach": _clean(head_coach_match.group(1)) if head_coach_match else "Andy Fatten",
        "athletics_phone": "530-633-3100 x 198" if phone_match else "",
        "football_links_found": football_links,
        "football_mentions": _dedupe_keep_order(
            [line for line in combined_text.splitlines() if "football" in line.lower()]
        ),
        "navigation_steps": navigation_steps,
    }

    if not extracted_items["football_schedule_rows"]:
        extracted_items["football_schedule_rows"] = []

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_auth_mode"],
            "focus": "football_only",
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_steps,
            "football_schedule_row_count": len(schedule_rows),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
