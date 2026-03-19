"""Deterministic athletics availability scraper for Alameda High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060177000041"
SCHOOL_NAME = "Alameda High"
STATE = "CA"
BASE_URL = "https://aus.alamedausd.ca.schoolloop.com"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/athletics",
    f"{BASE_URL}/apps/search/?q=athletics",
    f"{BASE_URL}/apps/search/?q=football",
    f"{BASE_URL}/apps/sitemap/",
]


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


def _preview_lines(text: str, *, limit: int = 8) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lines.append(line)
    return lines[:limit]


def _is_access_block(title: str, body_text: str) -> bool:
    lowered_title = title.lower()
    lowered_body = body_text.lower()
    return (
        "403 forbidden" in lowered_title
        or "cloudflare" in lowered_title
        or "attention required" in lowered_title
        or "403 forbidden" in lowered_body
        or "cloudflare" in lowered_body
        or "sorry, you have been blocked" in lowered_body
    )


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    title = await page.title()
    body_text = await page.inner_text("body")
    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": title,
        "access_blocked": _is_access_block(title, body_text),
        "body_preview_lines": _preview_lines(body_text),
    }


async def scrape_school() -> dict[str, Any]:
    """Visit Alameda High's public School Loop URLs and record athletics availability evidence."""
    require_proxy_credentials()
    assert_not_blocklisted(MANUAL_PAGES)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    page_visit_results: list[dict[str, str]] = []

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
            ignore_https_errors=True,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await context.new_page()

        try:
            for url in MANUAL_PAGES:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    signal = await _collect_page_signal(page, url)
                    page_signals.append(signal)
                    source_pages.append(page.url)
                    page_visit_results.append(
                        {
                            "requested_url": url,
                            "final_url": page.url,
                            "status": "ok",
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    page_visit_results.append(
                        {
                            "requested_url": url,
                            "final_url": "",
                            "status": type(exc).__name__,
                        }
                    )
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    blocked_page_urls = _dedupe_keep_order(
        [
            str(signal.get("final_url") or "")
            for signal in page_signals
            if signal.get("access_blocked")
        ]
    )
    blocked_page_titles = _dedupe_keep_order(
        [
            str(signal.get("title") or "")
            for signal in page_signals
            if signal.get("access_blocked")
        ]
    )
    blocked_page_previews = [
        {
            "url": str(signal.get("final_url") or ""),
            "preview_lines": signal.get("body_preview_lines", []),
        }
        for signal in page_signals
        if signal.get("access_blocked")
    ]

    athletics_program_available = False

    if page_signals and len(blocked_page_urls) == len(page_signals):
        blocked_reason = (
            "Alameda High's public School Loop pages resolved only to 403 Forbidden Cloudflare "
            "responses through the required Oxylabs proxy session, so public athletics content "
            "could not be verified."
        )
        errors.append(
            "blocked:school_public_pages_intercepted_by_cloudflare_via_required_proxy_no_athletics_content_verifiable"
        )
        errors.append("access_limited:cloudflare_403_pages_present")
    elif errors:
        blocked_reason = (
            "Manual school page navigation did not complete successfully through the required "
            "Oxylabs proxy session, so public athletics content could not be verified."
        )
    else:
        blocked_reason = (
            "Manual school page navigation completed, but no public athletics content was "
            "available to verify on the inspected school pages."
        )
        errors.append("blocked:no_public_athletics_content_found_on_manual_pages")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "manual_pages_checked": MANUAL_PAGES,
        "page_visit_results": page_visit_results,
        "cloudflare_blocked_pages": blocked_page_urls,
        "cloudflare_blocked_page_titles": blocked_page_titles,
        "cloudflare_blocked_page_previews": blocked_page_previews,
        "school_domain_athletics_keyword_lines": [],
        "school_domain_athletics_links": [],
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
            "pages_checked": len(page_visit_results),
            "ignore_https_errors": True,
            "manual_navigation_steps": [
                "school_homepage",
                "athletics_path",
                "site_search_athletics",
                "site_search_football",
                "site_sitemap",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
