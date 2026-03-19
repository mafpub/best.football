"""Deterministic athletics availability scraper for ACE Charter High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060156113023"
SCHOOL_NAME = "ACE Charter High"
STATE = "CA"
BASE_URL = "https://www.acecharter.org"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_PAGES = [
    f"{BASE_URL}/",
    f"{BASE_URL}/our-story/ace-charter-high-school/",
    f"{BASE_URL}/ace-documents/",
    f"{BASE_URL}/titleix/",
]
SEARCH_QUERIES = ["athletics", "sports", "football", "basketball", "soccer", "volleyball"]
PROGRAM_KEYWORDS = (
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
    "roster",
    "schedule",
    "coach",
    "tryout",
    "league",
)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        v = value.strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _keyword_lines(text: str, *, allow_titleix: bool) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lower = line.lower()
        if not any(token in lower for token in PROGRAM_KEYWORDS):
            continue
        # Ignore policy-only Title IX boilerplate unless explicitly requested.
        if (
            not allow_titleix
            and ("title ix" in lower or "equitable opportunity" in lower or "scholarship" in lower)
        ):
            continue
        lines.append(line)
    return _dedupe(lines)[:25]


async def _collect_signal(page, *, allow_titleix: bool) -> dict[str, Any]:
    body = await page.inner_text("body")
    lines = _keyword_lines(body, allow_titleix=allow_titleix)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").trim(),
            href: e.href || ""
        }))""",
    )

    candidate_links: list[str] = []
    for link in links:
        label = " ".join(str(link.get("text") or "").split()).strip()
        href = str(link.get("href") or "").strip()
        combo = f"{label} {href}".lower()
        if any(token in combo for token in PROGRAM_KEYWORDS):
            candidate_links.append(f"{label}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "keyword_lines": lines,
        "candidate_links": _dedupe(candidate_links)[:25],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate ACE Charter pages and determine public athletics program availability."""
    require_proxy_credentials()

    planned_urls = MANUAL_PAGES + [f"{BASE_URL}/?s={q}" for q in SEARCH_QUERIES]
    assert_not_blocklisted(planned_urls)

    source_pages: list[str] = []
    errors: list[str] = []
    page_signals: list[dict[str, Any]] = []

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
            for url in MANUAL_PAGES:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(
                    await _collect_signal(
                        page,
                        allow_titleix=page.url.rstrip("/").endswith("/titleix"),
                    )
                )

            for query in SEARCH_QUERIES:
                search_url = f"{BASE_URL}/?s={query}"
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1000)
                source_pages.append(page.url)
                page_signals.append(await _collect_signal(page, allow_titleix=False))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe(source_pages)

    all_lines: list[str] = []
    all_links: list[str] = []
    non_titleix_program_lines: list[str] = []
    non_titleix_program_links: list[str] = []

    for signal in page_signals:
        url = str(signal.get("url") or "")
        lines = [x for x in signal.get("keyword_lines", []) if isinstance(x, str)]
        links = [x for x in signal.get("candidate_links", []) if isinstance(x, str)]
        all_lines.extend(lines)
        all_links.extend(links)

        if "/titleix/" not in url and "/?s=athletic" not in url and "/?s=athletics" not in url:
            non_titleix_program_lines.extend(lines)
            non_titleix_program_links.extend(links)

    all_lines = _dedupe(all_lines)
    all_links = _dedupe(all_links)
    non_titleix_program_lines = _dedupe(non_titleix_program_lines)
    non_titleix_program_links = _dedupe(non_titleix_program_links)

    athletics_program_available = bool(non_titleix_program_lines or non_titleix_program_links)

    if not athletics_program_available:
        errors.append(
            "blocked:no_public_athletics_program_content_found_on_school_site_or_search_results"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": (
            "No public athletics program content found; only Title IX policy language mentions "
            "athletics in a compliance context."
            if not athletics_program_available
            else ""
        ),
        "manual_pages_checked": MANUAL_PAGES,
        "search_queries_checked": SEARCH_QUERIES,
        "program_keyword_lines_non_policy": non_titleix_program_lines,
        "program_candidate_links_non_policy": non_titleix_program_links,
        "policy_or_general_keyword_lines": all_lines,
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
                "ace_charter_high_school_page",
                "ace_documents_page",
                "titleix_page",
                "site_search_results",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()

