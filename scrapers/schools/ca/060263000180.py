"""Deterministic football scraper for Loara High (CA)."""

from __future__ import annotations

import asyncio
import base64
import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060263000180"
SCHOOL_NAME = "Loara High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_URL = "https://loara.auhsd.us/Loara/Department/11856-Athletics"
COACHES_URL = "https://loara.auhsd.us/Loara/Department/11856-ATHLETICS/31327-Coaches.html"
TARGET_URLS = [ATHLETICS_URL, COACHES_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _abs_url(base_url: str, href: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


def _dedupe(items: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for item in items:
        key = repr(item) if isinstance(item, dict) else _clean(str(item))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _decode_mail_href(href: str) -> str:
    if "sendMail.cfm?e=" not in href:
        return ""
    encoded = href.split("e=", 1)[1].strip()
    if not encoded:
        return ""
    try:
        return base64.b64decode(encoded).decode("utf-8")
    except Exception:  # noqa: BLE001
        return ""


def _extract_page_snapshot(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else ""
    text = _clean(soup.get_text("\n", strip=True))
    return {
        "url": url,
        "title": title,
        "text": text,
        "soup": soup,
    }


def _extract_athletics_contacts(soup: BeautifulSoup, page_url: str) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    for anchor in soup.select('a[href*="sendMail.cfm?e="]'):
        name = _clean(anchor.get_text(" ", strip=True))
        email = _decode_mail_href(str(anchor.get("href") or ""))
        if not name or not email:
            continue
        if name not in {"SCOTT WILSON", "TODD ROLPH"}:
            continue
        contacts.append(
            {
                "role": "Athletic Director",
                "name": name.title(),
                "email": email,
                "url": _abs_url(page_url, str(anchor.get("href") or "")),
            }
        )
    return _dedupe(contacts)


def _extract_ticket_link(soup: BeautifulSoup, page_url: str) -> str:
    for anchor in soup.select("a[href]"):
        href = _abs_url(page_url, str(anchor.get("href") or ""))
        if "gofan.co/app/school/" in href.lower():
            return href
    return ""


def _extract_football_evidence(text: str) -> list[str]:
    evidence: list[str] = []
    for fragment in re.split(r"(?<=[.!?])\s+|\n+", text):
        cleaned = _clean(fragment)
        if "football" not in cleaned.lower():
            continue
        evidence.append(cleaned)
    return _dedupe(evidence)


def _extract_football_program(soup: BeautifulSoup, page_url: str) -> dict[str, Any]:
    rows = soup.select("div.btgrid div.row")
    for row in rows:
        cols = row.select("div.col")
        if len(cols) < 2:
            continue
        for index in range(0, len(cols) - 1, 2):
            sport_cell = cols[index]
            coach_cell = cols[index + 1]
            sport = _clean(sport_cell.get_text(" ", strip=True))
            if sport.lower() != "football":
                continue

            coach_name = _clean(coach_cell.get_text(" ", strip=True))
            coach_anchor = coach_cell.select_one('a[href*="sendMail.cfm?e="]')
            coach_href = _abs_url(page_url, str(coach_anchor.get("href") or "")) if coach_anchor else ""
            coach_email = _decode_mail_href(str(coach_anchor.get("href") or "")) if coach_anchor else ""
            return {
                "sport": sport,
                "head_coach": coach_name,
                "head_coach_email": coach_email,
                "contact_url": coach_href,
                "source_row": [ _clean(col.get_text(" ", strip=True)) for col in cols ],
            }
    return {}


async def _collect_pages() -> tuple[list[dict[str, Any]], list[str]]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    pages: list[dict[str, Any]] = []
    errors: list[str] = []

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
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1200)
                    pages.append(_extract_page_snapshot(await page.content(), page.url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    return pages, errors


async def scrape_school() -> dict[str, Any]:
    pages, errors = await _collect_pages()
    page_by_url = {page["url"]: page for page in pages if isinstance(page, dict) and page.get("url")}

    athletics_page = page_by_url.get(ATHLETICS_URL, {})
    coaches_page = page_by_url.get(COACHES_URL, {})

    athletics_soup = athletics_page.get("soup") if isinstance(athletics_page.get("soup"), BeautifulSoup) else BeautifulSoup("", "html.parser")
    coaches_soup = coaches_page.get("soup") if isinstance(coaches_page.get("soup"), BeautifulSoup) else BeautifulSoup("", "html.parser")

    athletics_text = str(athletics_page.get("text") or "")
    coaches_text = str(coaches_page.get("text") or "")

    athletics_contacts = _extract_athletics_contacts(athletics_soup, ATHLETICS_URL)
    football_program = _extract_football_program(coaches_soup, COACHES_URL)
    ticket_link = _extract_ticket_link(athletics_soup, ATHLETICS_URL)
    football_evidence = _extract_football_evidence("\n".join([athletics_text, coaches_text]))

    championship_mentions = [
        line for line in football_evidence if "champion" in line.lower() or "championship" in line.lower()
    ]

    if not football_program:
        errors.append("no_public_football_program_found")

    extracted_items: dict[str, Any] = {
        "football_program": football_program,
        "athletics_contacts": athletics_contacts,
        "ticketing_url": ticket_link,
        "football_evidence": football_evidence[:10],
        "championship_mentions": championship_mentions[:5],
        "athletics_page_title": _clean(str(athletics_page.get("title") or "")),
        "coaches_page_title": _clean(str(coaches_page.get("title") or "")),
    }

    source_pages = _dedupe([page.get("url", "") for page in pages if isinstance(page, dict)])
    proxy_meta = get_proxy_runtime_meta(PROXY_PROFILE)

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "focus": "football_only",
            "target_urls": TARGET_URLS,
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers"),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "pages_visited": len(source_pages),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
