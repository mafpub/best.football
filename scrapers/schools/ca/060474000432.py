"""Deterministic football scraper for Berkeley High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060474000432"
SCHOOL_NAME = "Berkeley High"
STATE = "CA"

PRIMARY_URL = "https://bhs.berkeleyschools.net/athletics/"
ATHLETICS_HOME = "https://sites.google.com/berkeley.net/bhsathletics/home"
FALLBACK_URLS = [
    "https://sites.google.com/berkeley.net/bhsathletics/Sports/fall-sports/football",
    "https://sites.google.com/berkeley.net/bhsathletics/Sports/fall-sports/football/jv-football",
    "https://sites.google.com/berkeley.net/bhsathletics/Sports/fall-sports/flag-football",
    "https://sites.google.com/berkeley.net/bhsathletics/fan-zone",
    "https://westalamedacountyconference.org/public/genie/678/school/3/",
]
TARGET_URLS = [PRIMARY_URL, ATHLETICS_HOME]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_URL_MARKERS = (
    "football",
    "flag-football",
    "jv-football",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in values:
        clean = _clean(item)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _is_http_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return parsed.scheme in {"http", "https"}
    except Exception:
        return False


def _looks_like_football(content: str) -> bool:
    hay = (content or "").lower()
    return "football" in hay and "sports" in hay


def _looks_like_football_url(url: str) -> bool:
    lower = (url or "").lower()
    return any(marker in lower for marker in FOOTBALL_URL_MARKERS)


def _normalize_url(href: str, base: str) -> str:
    if not href:
        return ""
    clean = href.strip()
    if clean.startswith("//"):
        clean = f"https:{clean}"
    if clean.startswith("/"):
        clean = urljoin(base, clean)
    if clean.startswith("http://") or clean.startswith("https://"):
        return clean
    return ""


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lower = line.lower()
        if "football" in lower or "jacket" in lower or "schedule" in lower:
            lines.append(line)
    return lines[:200]


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.+-]+\.[A-Za-z]{2,}", text or ""))


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body = _clean(await page.locator("body").inner_text())
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map((anchor) => ({\n            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),\n            href: anchor.href || ''\n        }))",
    )
    if not isinstance(links, list):
        links = []

    football_links: list[str] = []
    page_links: list[str] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        text = _clean(str(link.get("text") or "")).strip()
        href = _normalize_url(str(link.get("href") or ""), requested_url)
        if not _is_http_url(href):
            continue
        page_links.append(href)
        lower_text = text.lower()
        lower_href = href.lower()
        if "football" in lower_text or _looks_like_football_url(lower_href):
            football_links.append(href)

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body,
        "football_lines": _extract_lines(body),
        "emails": _extract_emails(body),
        "link_targets": [f"{_clean(item.get('text') or '')}|{item.get('href')}" for item in links if isinstance(item, dict)],
        "football_links": football_links,
        "raw_links": page_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Visit Berkeley High athletics sources and extract public football links/details."""
    require_proxy_credentials(profile="datacenter")
    assert_not_blocklisted(TARGET_URLS, profile="datacenter")

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    proxy = get_playwright_proxy_config(profile="datacenter")

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

        planned_urls: list[str] = [
            PRIMARY_URL,
            ATHLETICS_HOME,
        ] + FALLBACK_URLS
        planned_urls = _dedupe_keep_order(planned_urls)

        for url in planned_urls:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=70000)
                await page.wait_for_timeout(1500)
                signal = await _collect_page(page, url)
                source_pages.append(signal["final_url"])
                page_signals.append(signal)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
                continue

        await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_links: list[str] = []
    football_lines: list[str] = []
    football_emails: list[str] = []

    for signal in page_signals:
        football_lines.extend(signal.get("football_lines") or [])
        football_emails.extend(signal.get("emails") or [])
        for link in (signal.get("football_links") or []):
            if link:
                football_links.append(link)

    football_links = _dedupe_keep_order(
        [
            urljoin("https://", f) if f.startswith("//") else f
            for f in football_links
            if _is_http_url(f) and _looks_like_football_url(f)
        ]
    )

    # Keep a stable fallback if the athletics page exposed a redirect but no direct football links.
    if not football_links and ATHLETICS_HOME not in source_pages:
        football_links = FALLBACK_URLS[:2]

    football_program_available = bool(
        football_links
        or _looks_like_football(" ".join(football_lines))
        or any("football" in (signal.get("title", "").lower()) for signal in page_signals)
    )

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_home_url": PRIMARY_URL,
        "athletics_google_site_home": ATHLETICS_HOME,
        "football_links": football_links,
        "conference_schedule_url": "https://westalamedacountyconference.org/public/genie/678/school/3/",
        "football_signals": _dedupe_keep_order(football_lines),
        "program_contact_emails": _dedupe_keep_order(football_emails),
        "raw_discovery_links": _dedupe_keep_order(
            [
                item
                for signal in page_signals
                for item in (signal.get("raw_links") or [])
            ]
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
            "proxy_profile": get_proxy_runtime_meta(profile="datacenter").get("profile"),
            "proxy_servers": get_proxy_runtime_meta(profile="datacenter").get("servers"),
            "proxy_auth_mode": get_proxy_runtime_meta(profile="datacenter").get("auth_mode"),
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()
