"""Deterministic athletics availability scraper for Albert Einstein Continuation (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "062271002976"
SCHOOL_NAME = "Albert Einstein Continuation"
STATE = "CA"

BASE_URL = "https://einsteinchs.lausd.org"
HOME_URL = f"{BASE_URL}/"
SITEMAP_URL = f"{BASE_URL}/apps/sitemap/"
SEARCH_URL = f"{BASE_URL}/apps/search/"
SEARCH_ATHLETICS_URL = f"{BASE_URL}/apps/search/?q=athletics"
SEARCH_FOOTBALL_URL = f"{BASE_URL}/apps/search/?q=football"
NEWS_URL = f"{BASE_URL}/apps/news/"
CALENDAR_URL = f"{BASE_URL}/apps/calendar/"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_NAV_STEPS = [
    "school_homepage",
    "sitemap",
    "site_search",
    "search_athletics",
    "search_football",
    "news",
    "calendar",
    "menu_open_if_present",
    "menu_link_departments",
    "menu_link_athletics",
    "menu_link_physical_education",
]

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
    "tennis",
    "wrestling",
    "track",
    "cross country",
    "clearnce",
    "tryout",
    "coach",
    "gym",
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


def _is_school_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host == "einsteinchs.lausd.org" or host.endswith(".einsteinchs.lausd.org")


def _is_blocked(title: str, body_text: str) -> bool:
    lowered_title = title.lower()
    lowered_body = body_text.lower()
    return (
        "403 forbidden" in lowered_title
        or "cloudflare" in lowered_title
        or "attention required" in lowered_title
        or "sorry, you have been blocked" in lowered_body
        or "unable to access edliocloud.com" in lowered_body
    )


def _collect_lines(text: str, *, limit: int = 24) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        lines.append(line)
    return lines[:limit]


def _extract_keyword_lines(text: str, *, limit: int = 20) -> list[str]:
    matches: list[str] = []
    for line in _collect_lines(text, limit=200):
        normalized = line.lower()
        if any(keyword in normalized for keyword in ATHLETICS_KEYWORDS):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


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
        hay = f"{text} {href}".lower()
        if any(keyword in hay for keyword in ATHLETICS_KEYWORDS):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)[:30]


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    title = await page.title()
    try:
        body_text = await page.inner_text("body")
    except Exception:  # noqa: BLE001
        body_text = ""
    keyword_lines = _extract_keyword_lines(body_text)
    keyword_links = await _extract_keyword_links(page)
    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": title,
        "school_domain": _is_school_domain(page.url),
        "blocked": _is_blocked(title, body_text),
        "keyword_lines": keyword_lines,
        "keyword_links": keyword_links,
        "body_preview_lines": _collect_lines(body_text),
    }


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


async def _open_menu_if_present(page) -> bool:
    for label in ["Menu", "Main Menu", "Open", "MAIN MENU", "Menu ☰"]:
        locator = page.get_by_role("button", name=label, exact=False).first
        if await locator.count() == 0:
            locator = page.get_by_role("link", name=label, exact=False).first
        if await locator.count() == 0:
            continue
        try:
            await locator.click(timeout=8000)
            await page.wait_for_timeout(850)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


async def scrape_school() -> dict[str, Any]:
    """Navigate the site manually and report whether public athletics content is available."""
    require_proxy_credentials()

    planned_urls = [
        HOME_URL,
        SITEMAP_URL,
        SEARCH_URL,
        SEARCH_ATHLETICS_URL,
        SEARCH_FOOTBALL_URL,
        NEWS_URL,
        CALENDAR_URL,
    ]
    assert_not_blocklisted(planned_urls)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []
    page_visit_results: list[dict[str, str]] = []
    clicked_pages: list[str] = []

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
        )
        page = await context.new_page()

        try:
            for url in planned_urls:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1400)
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

            try:
                menu_opened = await _open_menu_if_present(page)
                if menu_opened:
                    clicked_pages.append("menu_open")
            except Exception:  # noqa: BLE001
                clicked_pages.append("menu_open_failed")

            for label_group, step_name in [
                (["Departments"], "departments"),
                (["Athletics", "Athletic"], "athletics"),
                (["Physical Education", "PE"], "pe"),
                (["Activities", "Student Activities", "Clubs"], "activities"),
            ]:
                try:
                    clicked = await _click_link_if_present(page, label_group)
                    clicked_pages.append(step_name if clicked else f"{step_name}:not_found")
                    if clicked:
                        signal = await _collect_page_signal(page, f"menu:{step_name}")
                        page_signals.append(signal)
                        source_pages.append(page.url)
                        page_visit_results.append(
                            {
                                "requested_url": f"menu:{step_name}",
                                "final_url": page.url,
                                "status": "ok",
                            }
                        )
                        await page.wait_for_timeout(900)
                except Exception as exc:  # noqa: BLE001
                    clicked_pages.append(f"{step_name}:click_error:{type(exc).__name__}")

        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    blocked_urls = _dedupe_keep_order(
        [str(item.get("final_url") or "") for item in page_signals if item.get("blocked")]
    )

    school_domain_lines: list[str] = []
    school_domain_links: list[str] = []
    athletics_lines: list[str] = []
    athletics_links: list[str] = []

    for signal in page_signals:
        if not signal.get("school_domain"):
            continue
        lines = [v for v in signal.get("keyword_lines", []) if isinstance(v, str)]
        links = [v for v in signal.get("keyword_links", []) if isinstance(v, str)]
        school_domain_lines.extend(lines)
        school_domain_links.extend(links)

        current_url = str(signal.get("final_url") or "").lower()
        if "athletics" in current_url or "sports" in current_url:
            athletics_lines.extend(lines)
            athletics_links.extend(links)

    school_domain_lines = _dedupe_keep_order(school_domain_lines)
    school_domain_links = _dedupe_keep_order(school_domain_links)
    athletics_lines = _dedupe_keep_order(athletics_lines)
    athletics_links = _dedupe_keep_order(athletics_links)

    athletics_program_available = bool(athletics_lines or athletics_links)
    blocked_reason = ""

    if blocked_urls and len(blocked_urls) == len(_dedupe_keep_order([str(i.get("final_url") or "") for i in page_signals])):
        blocked_reason = (
            "School Loop domain pages loaded only as Cloudflare/Edlio security interstitials via the required "
            "Oxylabs proxy session, so public athletics content could not be verified."
        )
        errors.append(
            "blocked:school_public_pages_intercepted_by_cloudflare_via_required_proxy_no_athletics_content_verifiable"
        )
        errors.append("access_limited:cloudflare_block_pages_present")
    elif blocked_urls:
        blocked_reason = (
            "Some manual navigation targets were blocked by Cloudflare/Edlio; athletics verification may be partial."
        )
        errors.append("access_partial:some_pages_blocked_by_cloudflare")
    elif not athletics_program_available:
        blocked_reason = (
            "Manual home/sitemap/search/departments-style navigation completed, but no public athletics "
            "content was found on school-domain pages."
        )
        errors.append("blocked:no_public_athletics_content_found_on_manual_pages")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "manual_navigation_steps": MANUAL_NAV_STEPS,
        "clicked_menu_steps": clicked_pages,
        "school_domain_athletics_keyword_mentions": school_domain_lines,
        "school_domain_athletics_links": school_domain_links,
        "athletics_keyword_mentions": athletics_lines,
        "athletics_related_links": athletics_links,
        "manual_pages_requested": planned_urls,
        "page_signals": [
            {
                "requested_url": item.get("requested_url"),
                "final_url": item.get("final_url"),
                "title": item.get("title"),
                "cloudflare_blocked": item.get("blocked"),
                "school_domain": item.get("school_domain"),
            }
            for item in page_signals
        ],
        "source_pages_visited": source_pages,
        "page_visit_results": page_visit_results,
        "cloudflare_blocked_pages": blocked_urls,
        "proxy_server": PROXY_SERVER,
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
            "manual_navigation_steps": MANUAL_NAV_STEPS,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
