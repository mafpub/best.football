"""Deterministic athletics scraper for 21st Century Learning Institute (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060429013779"
SCHOOL_NAME = "21st Century Learning Institute"
STATE = "CA"
SCHOOL_URL = "https://21cli.beaumontusd.us/"
SEARCH_URLS = [
    "https://21cli.beaumontusd.us/apps/search/?q=athletics",
    "https://21cli.beaumontusd.us/apps/search/?q=sports",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "sport",
    "football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
    "volleyball",
    "wrestling",
    "track",
    "cross country",
    "cif",
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


def _extract_keyword_lines(body_text: str, limit: int = 20) -> list[str]:
    lines: list[str] = []
    for raw_line in body_text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _extract_document_links(page) -> list[dict[str, str]]:
    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            href: e.href || "",
            text: (e.textContent || "").trim()
        }))""",
    )
    out: list[dict[str, str]] = []
    seen: set[str] = set()

    for anchor in anchors:
        href = str(anchor.get("href") or "").strip()
        text = " ".join(str(anchor.get("text") or "").split()).strip()
        if not href:
            continue
        href_lower = href.lower()
        if not href_lower.endswith((".pdf", ".doc", ".docx")):
            continue
        if "files.edl.io" not in href_lower:
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append({"title": text or href.split("/")[-1], "url": href})

    return out


async def scrape_school() -> dict[str, Any]:
    """Scrape publicly available athletics indicators for this school."""
    require_proxy_credentials()
    assert_not_blocklisted([SCHOOL_URL, *SEARCH_URLS])

    errors: list[str] = []
    source_pages: list[str] = []
    all_keyword_lines: list[str] = []
    athletics_docs: list[dict[str, str]] = []

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

        urls_to_visit = [SCHOOL_URL, *SEARCH_URLS]
        for url in urls_to_visit:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1200)
                source_pages.append(page.url)
                body_text = await page.inner_text("body")
                all_keyword_lines.extend(_extract_keyword_lines(body_text))
                if "/apps/search/" in page.url:
                    athletics_docs.extend(await _extract_document_links(page))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"visit_failed:{url}:{type(exc).__name__}")

        await browser.close()

    keyword_lines = _dedupe_keep_order(all_keyword_lines)
    unique_docs: list[dict[str, str]] = []
    seen_doc_urls: set[str] = set()
    for item in athletics_docs:
        url = item.get("url", "").strip()
        if not url or url in seen_doc_urls:
            continue
        seen_doc_urls.add(url)
        unique_docs.append(item)
        source_pages.append(url)

    source_pages = _dedupe_keep_order(source_pages)

    extracted_items: dict[str, Any] = {
        "athletics_program_available": bool(keyword_lines or unique_docs),
        "athletics_keyword_mentions": keyword_lines[:20],
        "athletics_documents": unique_docs[:10],
        "athletics_notes": [
            line
            for line in keyword_lines
            if (
                "california interscholastic federation" in line.lower()
                or "sports at bhs" in line.lower()
                or "athletic registration packet" in line.lower()
                or "beaumont high school" in line.lower()
            )
        ][:10],
    }

    if not extracted_items["athletics_program_available"]:
        errors.append("no_public_athletics_content_detected")

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
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()

