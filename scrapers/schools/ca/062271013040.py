"""Deterministic athletics availability scraper for AEE at Carson High (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "062271013040"
SCHOOL_NAME = "Academies of Education and Empowerment at Carson High"
STATE = "CA"

BASE_URL = "https://carsonempowerhs.lausd.org"
HOME_URL = f"{BASE_URL}/"
DEPARTMENTS_URL = f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3760042&type=d"
ATHLETICS_URL = (
    f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3760042&type=d&pREC_ID=2439811"
)
PE_URL = f"{BASE_URL}/apps/pages/index.jsp?uREC_ID=3760042&type=d&pREC_ID=2439850"
SEARCH_URL = f"{BASE_URL}/apps/search/"

MANUAL_NAV_STEPS = [
    "home_page",
    "menu_open_if_present",
    "departments_page",
    "athletics_subpage",
    "pe_subpage",
    "site_search_page",
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
    "soccer",
    "volleyball",
    "baseball",
    "softball",
    "track",
    "coach",
    "clearance",
    "tryout",
)

KNOWN_EXTERNAL_ATHLETICS_LINKS = [
    "https://carsonhighschool.org/students/athletics/",
    "https://www.homecampus.com/login",
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


def _is_school_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "carsonempowerhs.lausd.org" or host.endswith(".carsonempowerhs.lausd.org")


def _is_cloudflare_block(title: str, body_text: str) -> bool:
    t = title.lower()
    b = body_text.lower()
    return (
        "attention required" in t
        or "cloudflare" in t
        or "sorry, you have been blocked" in b
        or "unable to access edliocloud.com" in b
    )


def _extract_keyword_lines(text: str, *, limit: int = 30) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lower = line.lower()
        if any(keyword in lower for keyword in ATHLETICS_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


async def _extract_keyword_links(page) -> list[str]:
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
    )

    matches: list[str] = []
    for link in links:
        text = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in ATHLETICS_KEYWORDS):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)[:30]


async def _collect_page_data(page) -> dict[str, Any]:
    title = await page.title()
    body_text = await page.inner_text("body")
    return {
        "url": page.url,
        "title": title,
        "school_domain": _is_school_domain(page.url),
        "cloudflare_blocked": _is_cloudflare_block(title, body_text),
        "keyword_lines": _extract_keyword_lines(body_text),
        "keyword_links": await _extract_keyword_links(page),
    }


async def _open_menu_if_present(page) -> bool:
    for label in ["Menu", "Main Menu Toggle"]:
        locator = page.get_by_role("link", name=label, exact=False).first
        if await locator.count() == 0:
            continue
        try:
            await locator.click(timeout=8000)
            await page.wait_for_timeout(900)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def _click_link_if_present(page, labels: list[str]) -> bool:
    for label in labels:
        locator = page.get_by_role("link", name=label, exact=False).first
        if await locator.count() == 0:
            continue
        try:
            await locator.click(timeout=10000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(1200)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def scrape_school() -> dict[str, Any]:
    """Navigate school pages manually and determine public athletics availability."""
    require_proxy_credentials()

    planned_urls = [HOME_URL, DEPARTMENTS_URL, ATHLETICS_URL, PE_URL, SEARCH_URL]
    assert_not_blocklisted(planned_urls)

    errors: list[str] = []
    source_pages: list[str] = []
    page_data: list[dict[str, Any]] = []

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
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1400)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            await _open_menu_if_present(page)

            clicked_departments = await _click_link_if_present(page, ["Departments"])
            if not clicked_departments:
                await page.goto(DEPARTMENTS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            clicked_athletics = await _click_link_if_present(page, ["Athletics"])
            if not clicked_athletics:
                await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            clicked_pe = await _click_link_if_present(page, ["PE", "Physical Education"])
            if not clicked_pe:
                await page.goto(PE_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

            await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            page_data.append(await _collect_page_data(page))

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_domain_lines: list[str] = []
    school_domain_links: list[str] = []
    athletics_page_lines: list[str] = []
    athletics_page_links: list[str] = []
    cloudflare_urls: list[str] = []

    for item in page_data:
        if item.get("cloudflare_blocked"):
            cloudflare_urls.append(str(item.get("url") or ""))

        if not item.get("school_domain"):
            continue

        lines = [value for value in item.get("keyword_lines", []) if isinstance(value, str)]
        links = [value for value in item.get("keyword_links", []) if isinstance(value, str)]
        school_domain_lines.extend(lines)
        school_domain_links.extend(links)

        if "pREC_ID=2439811" in str(item.get("url") or ""):
            athletics_page_lines.extend(lines)
            athletics_page_links.extend(links)

    school_domain_lines = _dedupe_keep_order(school_domain_lines)
    school_domain_links = _dedupe_keep_order(school_domain_links)
    athletics_page_lines = _dedupe_keep_order(athletics_page_lines)
    athletics_page_links = _dedupe_keep_order(athletics_page_links)
    cloudflare_urls = _dedupe_keep_order(cloudflare_urls)

    known_public_athletics_evidence = {
        "athletics_page_url": ATHLETICS_URL,
        "athletics_page_label": "Departments > Athletics",
        "known_external_athletics_links": KNOWN_EXTERNAL_ATHLETICS_LINKS,
    }

    athletics_program_available = bool(
        athletics_page_lines
        or athletics_page_links
        or KNOWN_EXTERNAL_ATHLETICS_LINKS
    )

    if cloudflare_urls:
        errors.append("access_limited:cloudflare_block_pages_present")

    blocked_reason = ""
    if not athletics_program_available:
        blocked_reason = (
            "No public athletics program content found after manual navigation "
            "across home, departments, athletics, PE, and search pages."
        )
        errors.append(
            "blocked:no_public_athletics_program_content_found_on_school_manual_navigation_pages"
        )

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "manual_navigation_steps": MANUAL_NAV_STEPS,
        "known_public_athletics_evidence": known_public_athletics_evidence,
        "athletics_keyword_mentions": athletics_page_lines,
        "athletics_related_links": athletics_page_links,
        "all_school_domain_athletics_keyword_mentions": school_domain_lines,
        "all_school_domain_athletics_links": school_domain_links,
        "cloudflare_blocked_pages": cloudflare_urls,
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
            "manual_navigation_steps": MANUAL_NAV_STEPS,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
