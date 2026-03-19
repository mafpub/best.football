"""Deterministic athletics availability scraper for Alessandro High School (CA)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "061692002149"
SCHOOL_NAME = "Alessandro High"
STATE = "CA"

HOME_URL = "https://www.alessandrohighschool.org/"
PLUS_URL = "https://www.alessandrohighschool.org/o/alessandrohighschool/page/plus"
EVENTS_URL = "https://www.alessandrohighschool.org/o/alessandrohighschool/events"
NEWS_URL = "https://www.alessandrohighschool.org/o/alessandrohighschool/news"
LIVE_FEED_URL = "https://www.alessandrohighschool.org/o/alessandrohighschool/live-feed"
LINKS_URL = "https://www.alessandrohighschool.org/o/alessandrohighschool/page/links"
PEACHJAR_URL = (
    "https://my.peachjar.com/explore/all?audienceId=55477&tab=school"
    "&districtId=5262&audienceType=school"
)

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

SEARCH_QUERIES = [
    "athletics",
    "sports",
    "football",
    "basketball",
    "soccer",
    "volleyball",
]

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "sports",
    "sport",
    "football",
    "basketball",
    "soccer",
    "volleyball",
    "baseball",
    "softball",
    "track",
    "cross country",
    "wrestling",
    "cheer",
)

PLANNED_URLS = [
    HOME_URL,
    PLUS_URL,
    EVENTS_URL,
    NEWS_URL,
    LIVE_FEED_URL,
    LINKS_URL,
    PEACHJAR_URL,
]


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = " ".join(value.split()).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _clean_lines(text: str) -> list[str]:
    return _dedupe_keep_order(
        [
            " ".join(raw_line.split()).strip()
            for raw_line in text.splitlines()
            if raw_line.strip()
        ]
    )


def _preview_lines(text: str, *, limit: int = 12) -> list[str]:
    return _clean_lines(text)[:limit]


def _extract_keyword_lines(text: str, *, limit: int = 30) -> list[str]:
    matches: list[str] = []
    for line in _clean_lines(text):
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            matches.append(line)
    return matches[:limit]


def _is_school_domain(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in {"alessandrohighschool.org", "www.alessandrohighschool.org"}


async def _extract_keyword_links(page, *, limit: int = 25) -> list[str]:
    anchors = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            href: e.href || "",
            text: (e.textContent || "").replace(/\\s+/g, " ").trim()
        }))""",
    )

    matches: list[str] = []
    current_url = page.url.rstrip("/")
    for anchor in anchors:
        href = str(anchor.get("href") or "").strip()
        text = str(anchor.get("text") or "").strip()
        if not href:
            continue
        if href.rstrip("/") == current_url:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in ATHLETICS_KEYWORDS):
            matches.append(f"{text}|{href}" if text else href)
    return _dedupe_keep_order(matches)[:limit]


async def _collect_page_signal(page, *, label: str) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    return {
        "label": label,
        "url": page.url,
        "title": await page.title(),
        "school_hosted": _is_school_domain(page.url),
        "keyword_lines": _extract_keyword_lines(body_text),
        "keyword_links": await _extract_keyword_links(page),
        "body_preview_lines": _preview_lines(body_text),
    }


async def _goto_home(page) -> None:
    await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1800)


async def _open_submenu(page, label: str) -> None:
    button = page.get_by_role("button", name=f"Show submenu for {label}", exact=False).first
    await button.click(timeout=15000)
    await page.wait_for_timeout(1200)


async def _visible_links(page) -> list[dict[str, str]]:
    links = await page.locator("a:visible[href]").evaluate_all(
        """els => els.map(e => ({
            href: e.href || "",
            text: (e.textContent || "").replace(/\\s+/g, " ").trim()
        }))"""
    )

    output: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        href = str(link.get("href") or "").strip()
        text = str(link.get("text") or "").strip()
        key = (text, href)
        if not href or key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


async def _collect_activities_menu_links(page) -> list[str]:
    discovered: list[str] = []
    for link in await _visible_links(page):
        text = link["text"]
        href = link["href"]
        if text == "PLUS" or "School Activities and Information" in text or href == PEACHJAR_URL:
            discovered.append(f"{text}|{href}")
    return _dedupe_keep_order(discovered)


async def _click_link_or_goto(page, *, link_name: str, fallback_url: str) -> None:
    locator = page.get_by_role("link", name=link_name, exact=False).first
    if await locator.count() > 0:
        try:
            await locator.click(timeout=15000)
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(1800)
            return
        except Exception:  # noqa: BLE001
            pass

    await page.goto(fallback_url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1800)


def _extract_search_results_lines(body_text: str) -> list[str]:
    lines = _clean_lines(body_text)
    if not lines:
        return []

    start_index = 0
    if "Clear all" in lines:
        start_index = lines.index("Clear all") + 1

    end_index = len(lines)
    if "This is the end of the search results" in lines:
        end_index = lines.index("This is the end of the search results")

    return lines[start_index:end_index]


async def _run_site_search(page, query: str) -> dict[str, Any]:
    await _goto_home(page)

    opened = await page.evaluate(
        """() => {
            const button = Array.from(document.querySelectorAll('#searchbutton')).find(
                element => !!(element.offsetWidth || element.offsetHeight || element.getClientRects().length)
            );
            if (!button) {
                return false;
            }
            button.click();
            return true;
        }"""
    )
    if not opened:
        raise RuntimeError("search_button_not_found")

    await page.wait_for_timeout(1000)
    search_input = page.locator("#search-input")
    await search_input.fill(query)
    await page.wait_for_timeout(300)
    await search_input.press("Enter")
    await page.wait_for_timeout(2500)

    body_text = await page.inner_text("body")
    result_lines = _extract_search_results_lines(body_text)
    keyword_lines = _extract_keyword_lines("\n".join(result_lines))

    return {
        "query": query,
        "url": page.url,
        "title": await page.title(),
        "result_preview_lines": result_lines[:12],
        "result_keyword_lines": keyword_lines,
        "results_empty": len(result_lines) == 0,
    }


async def scrape_school() -> dict[str, Any]:
    """Manually navigate Alessandro High's live site and verify athletics availability."""
    require_proxy_credentials()
    assert_not_blocklisted(PLANNED_URLS)

    errors: list[str] = []
    source_pages: list[str] = []
    page_visit_results: list[dict[str, str]] = []
    manual_page_signals: list[dict[str, Any]] = []
    search_signals: list[dict[str, Any]] = []
    activities_menu_links: list[str] = []

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
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        try:
            await _goto_home(page)
            source_pages.append(page.url)
            manual_page_signals.append(await _collect_page_signal(page, label="Home"))
            page_visit_results.append(
                {
                    "label": "Home",
                    "requested_url": HOME_URL,
                    "final_url": page.url,
                    "status": "ok",
                }
            )

            try:
                await _open_submenu(page, "Activities")
                activities_menu_links.extend(await _collect_activities_menu_links(page))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"activities_menu_open_failed:{type(exc).__name__}")

            for label, link_name, url, preopen_menu in [
                ("PLUS", "PLUS", PLUS_URL, "Activities"),
                (
                    "School Activities and Information",
                    "School Activities and Information",
                    PEACHJAR_URL,
                    "Activities",
                ),
                ("Links", "Links", LINKS_URL, "About"),
                ("Events", "See All Events", EVENTS_URL, ""),
                ("News", "See All News", NEWS_URL, ""),
                ("Live Feed", "See All Posts", LIVE_FEED_URL, ""),
            ]:
                try:
                    await _goto_home(page)
                    if preopen_menu:
                        await _open_submenu(page, preopen_menu)
                    await _click_link_or_goto(page, link_name=link_name, fallback_url=url)
                    source_pages.append(page.url)
                    manual_page_signals.append(await _collect_page_signal(page, label=label))
                    page_visit_results.append(
                        {
                            "label": label,
                            "requested_url": url,
                            "final_url": page.url,
                            "status": "ok",
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"page_visit_failed:{label}:{type(exc).__name__}")
                    page_visit_results.append(
                        {
                            "label": label,
                            "requested_url": url,
                            "final_url": "",
                            "status": type(exc).__name__,
                        }
                    )

            for query in SEARCH_QUERIES:
                try:
                    signal = await _run_site_search(page, query)
                    search_signals.append(signal)
                    source_pages.append(signal["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"site_search_failed:{query}:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    activities_menu_links = _dedupe_keep_order(activities_menu_links)

    school_hosted_keyword_lines: list[str] = []
    school_hosted_keyword_links: list[str] = []
    external_activity_keyword_lines: list[str] = []
    external_activity_keyword_links: list[str] = []
    manual_page_titles: list[str] = []

    for signal in manual_page_signals:
        manual_page_titles.append(str(signal.get("title") or ""))
        lines = [value for value in signal.get("keyword_lines", []) if isinstance(value, str)]
        links = [value for value in signal.get("keyword_links", []) if isinstance(value, str)]
        if signal.get("school_hosted"):
            school_hosted_keyword_lines.extend(lines)
            school_hosted_keyword_links.extend(links)
        else:
            external_activity_keyword_lines.extend(lines)
            external_activity_keyword_links.extend(links)

    school_hosted_keyword_lines = _dedupe_keep_order(school_hosted_keyword_lines)
    school_hosted_keyword_links = _dedupe_keep_order(school_hosted_keyword_links)
    external_activity_keyword_lines = _dedupe_keep_order(external_activity_keyword_lines)
    external_activity_keyword_links = _dedupe_keep_order(external_activity_keyword_links)
    manual_page_titles = _dedupe_keep_order(manual_page_titles)

    search_keyword_lines: list[str] = []
    search_observations: list[str] = []
    search_preview_map: dict[str, list[str]] = {}
    for signal in search_signals:
        query = str(signal.get("query") or "")
        preview_lines = [
            value for value in signal.get("result_preview_lines", []) if isinstance(value, str)
        ]
        keyword_lines = [
            value for value in signal.get("result_keyword_lines", []) if isinstance(value, str)
        ]
        search_preview_map[query] = preview_lines[:10]
        search_keyword_lines.extend(keyword_lines)

        if signal.get("results_empty"):
            search_observations.append(f"{query}:no_visible_results")
        elif keyword_lines:
            search_observations.append(f"{query}:athletics_keywords_in_search_results")
        else:
            search_observations.append(f"{query}:results_without_athletics_keywords")

    search_keyword_lines = _dedupe_keep_order(search_keyword_lines)
    search_observations = _dedupe_keep_order(search_observations)

    athletics_program_available = bool(
        school_hosted_keyword_lines
        or school_hosted_keyword_links
        or external_activity_keyword_lines
        or external_activity_keyword_links
        or search_keyword_lines
    )

    blocked_reason = ""
    if not athletics_program_available:
        blocked_reason = (
            "No public athletics or sports program content was found on Alessandro High "
            "School's homepage, Activities submenu destinations, About->Links page, "
            "school-hosted events/news/live-feed pages, or the site's own search results."
        )
        errors.append("blocked:no_public_athletics_content_found_after_manual_navigation")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "blocked_reason": blocked_reason,
        "manual_navigation_pages_checked": [
            HOME_URL,
            PLUS_URL,
            PEACHJAR_URL,
            LINKS_URL,
            EVENTS_URL,
            NEWS_URL,
            LIVE_FEED_URL,
        ],
        "manual_page_titles_checked": manual_page_titles,
        "activities_menu_links_discovered": activities_menu_links,
        "page_visit_results": page_visit_results,
        "school_hosted_athletics_keyword_lines": school_hosted_keyword_lines,
        "school_hosted_athletics_keyword_links": school_hosted_keyword_links,
        "external_activity_page_athletics_keyword_lines": external_activity_keyword_lines,
        "external_activity_page_athletics_keyword_links": external_activity_keyword_links,
        "site_search_queries_checked": SEARCH_QUERIES,
        "site_search_keyword_lines": search_keyword_lines,
        "site_search_observations": search_observations,
        "site_search_preview_lines": search_preview_map,
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
            "ignore_https_errors": True,
            "pages_checked": len(page_visit_results),
            "manual_navigation_labels": [
                "Home",
                "Activities submenu",
                "PLUS",
                "School Activities and Information",
                "About submenu",
                "Links",
                "Events",
                "News",
                "Live Feed",
            ],
            "site_search_queries": SEARCH_QUERIES,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
