"""Deterministic athletics availability scraper for Adult Transition Program (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "060561013847"
SCHOOL_NAME = "Adult Transition Program"
STATE = "CA"
SCHOOL_URL = "https://do.bonita.k12.ca.us/"
DISTRICT_URLS = [
    "https://www.bonita.k12.ca.us/",
    "https://bonita.k12.ca.us/",
]

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "football",
    "basketball",
    "baseball",
    "softball",
    "soccer",
    "volleyball",
    "wrestling",
    "track",
    "cross country",
    "cheer",
    "roster",
    "schedule",
    "coach",
    "cif",
)
PROGRAM_KEYWORDS = (
    "adult transition program",
    "adult transition",
    "transition program",
)
DISCOVERY_KEYWORDS = (
    "adult",
    "transition",
    "program",
    "about",
    "contact",
    "staff",
)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _same_host(candidate_url: str, base_url: str) -> bool:
    return (urlparse(candidate_url).hostname or "").lower() == (
        urlparse(base_url).hostname or ""
    ).lower()


def _relevant_program_text(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in PROGRAM_KEYWORDS)


def _extract_program_athletics_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS) and _relevant_program_text(line):
            lines.append(line)
    return _dedupe(lines)[:25]


async def _collect_signal(page, seed_url: str) -> dict[str, Any]:
    body = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    candidate_links: list[str] = []
    follow_up_urls: list[str] = []
    for link in links:
        label = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        combo = f"{label} {href}".lower()
        if _same_host(href, seed_url) and any(keyword in combo for keyword in DISCOVERY_KEYWORDS):
            follow_up_urls.append(href)
        if any(keyword in combo for keyword in ATHLETICS_KEYWORDS) and _relevant_program_text(combo):
            candidate_links.append(f"{label}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "program_athletics_lines": _extract_program_athletics_lines(body),
        "candidate_links": _dedupe(candidate_links)[:25],
        "follow_up_urls": _dedupe(follow_up_urls)[:6],
    }


async def scrape_school() -> dict[str, Any]:
    """Navigate the school and district hosts to determine athletics availability."""
    require_proxy_credentials()

    seed_urls = [SCHOOL_URL, *DISTRICT_URLS]
    assert_not_blocklisted(seed_urls)

    source_pages: list[str] = []
    errors: list[str] = []
    page_signals: list[dict[str, Any]] = []
    visit_results: list[dict[str, str]] = []

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

        discovered_urls: list[str] = []
        try:
            for url in seed_urls:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    signal = await _collect_signal(page, url)
                    page_signals.append(signal)
                    discovered_urls.extend(signal["follow_up_urls"])
                    visit_results.append(
                        {
                            "requested_url": url,
                            "final_url": page.url,
                            "status": "ok",
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    visit_results.append(
                        {
                            "requested_url": url,
                            "final_url": "",
                            "status": type(exc).__name__,
                        }
                    )

            for url in _dedupe(discovered_urls)[:4]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_signal(page, url))
                    visit_results.append(
                        {
                            "requested_url": url,
                            "final_url": page.url,
                            "status": "ok",
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    visit_results.append(
                        {
                            "requested_url": url,
                            "final_url": "",
                            "status": type(exc).__name__,
                        }
                    )
        finally:
            await browser.close()

    source_pages = _dedupe(source_pages)

    program_athletics_lines: list[str] = []
    program_athletics_links: list[str] = []
    for signal in page_signals:
        program_athletics_lines.extend(
            [line for line in signal.get("program_athletics_lines", []) if isinstance(line, str)]
        )
        program_athletics_links.extend(
            [line for line in signal.get("candidate_links", []) if isinstance(line, str)]
        )

    program_athletics_lines = _dedupe(program_athletics_lines)
    program_athletics_links = _dedupe(program_athletics_links)

    accessible_pages = [
        item["final_url"]
        for item in visit_results
        if item.get("status") == "ok" and item.get("final_url")
    ]
    inaccessible_pages = [
        item["requested_url"]
        for item in visit_results
        if item.get("status") != "ok" and item.get("requested_url")
    ]

    athletics_program_available = bool(program_athletics_lines or program_athletics_links)

    if athletics_program_available:
        blocked_reason = ""
    elif accessible_pages:
        blocked_reason = (
            "Accessible school and district pages did not expose any program-specific athletics or sports "
            "content for Adult Transition Program."
        )
        errors.append(
            "blocked:no_public_adult_transition_program_athletics_content_found_on_accessible_school_or_district_pages"
        )
    else:
        blocked_reason = (
            "Adult Transition Program and Bonita district public hosts did not load through the Oxylabs-backed "
            "browser session, so no public athletics content could be verified."
        )
        errors.append(
            "blocked:adult_transition_program_public_hosts_unreachable_via_proxy_no_athletics_content_verifiable"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "manual_pages_checked": seed_urls,
        "accessible_pages": accessible_pages,
        "inaccessible_pages": inaccessible_pages,
        "page_visit_results": visit_results,
        "program_athletics_keyword_lines": program_athletics_lines,
        "program_athletics_candidate_links": program_athletics_links,
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
            "pages_checked": len(visit_results),
            "manual_navigation_steps": [
                "school_homepage",
                "district_homepage_www",
                "district_homepage_root",
                "program_relevant_internal_links_if_accessible",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
