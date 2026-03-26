"""Deterministic football scraper for Career Technical Education Charter (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060212414297"
SCHOOL_NAME = "Career Technical Education Charter"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://ctec.fcoe.org"
HOME_URL = f"{BASE_URL}/"
NEWS_URL = f"{BASE_URL}/news"
SEARCH_URL = f"{BASE_URL}/search/node?keys=football"

TARGET_PAGES = [
    HOME_URL,
    NEWS_URL,
    SEARCH_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "flag football",
    "flag-football",
    "flag football season",
    "flag football playoff",
    "football season",
    "football team",
    "football program",
    "football players",
    "football game",
    "football games",
)

FOOTBALL_NODE_REGEX = re.compile(r"https://ctec\.fcoe\.org/node/\d+")


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_lines(body_text: str, keywords: tuple[str, ...]) -> list[str]:
    output: list[str] = []
    for raw_line in body_text.splitlines():
        line = _clean(raw_line)
        lower = line.lower()
        if not line:
            continue
        if any(keyword in lower for keyword in keywords):
            output.append(line)
    return _dedupe_keep_order(output)


async def _collect_page(page, target_url: str) -> dict[str, Any]:
    await page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(1_200)
    body_text = _clean(await page.inner_text("body"))
    html = await page.content()
    title = _clean(await page.title())
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: (el.href || '').trim(),
        }))""",
    )

    if not isinstance(links, list):
        links = []

    return {
        "requested_url": target_url,
        "final_url": _clean(page.url),
        "title": title,
        "body_text": body_text,
        "html": html,
        "links": [str(item) for item in links if str(item)],
    }


def _collect_football_node_urls(html: str, body_text: str) -> list[str]:
    found = FOOTBALL_NODE_REGEX.findall(html or "")
    # Keep this deterministic and conservative: only pages surfaced by a football query.
    football_context_urls: list[str] = []

    for url in found:
        if "ctec.fcoe.org/node/" not in url:
            continue
        football_context_urls.append(url)

    if not found:
        # Fallback in case the markup changes: still capture known node links from body lines.
        for match in re.findall(r"/node/\d+", body_text or ""):
            football_context_urls.append(f"{BASE_URL}{match}")

    return _dedupe_keep_order(football_context_urls)[:8]


async def scrape_school() -> dict[str, Any]:
    """Navigate CTEC site pages and extract public football program evidence."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_PAGES, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    snapshots: list[dict[str, Any]] = []
    discovered_urls: list[str] = []
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
            for target in TARGET_PAGES:
                try:
                    snapshot = await _collect_page(page, target)
                    snapshots.append(snapshot)
                    source_pages.append(snapshot["final_url"])
                    if target == SEARCH_URL:
                        discovered_urls.extend(
                            _collect_football_node_urls(snapshot.get("html", ""), snapshot.get("body_text", ""))
                        )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{target}")
            for target in _dedupe_keep_order(discovered_urls):
                if target in source_pages or target in {HOME_URL, NEWS_URL, SEARCH_URL}:
                    continue
                try:
                    snapshot = await _collect_page(page, target)
                    snapshots.append(snapshot)
                    source_pages.append(snapshot["final_url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{target}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_hits: list[dict[str, Any]] = []
    football_lines: list[str] = []
    football_pages: list[str] = []
    football_posts: list[dict[str, str]] = []

    for snapshot in snapshots:
        body = str(snapshot.get("body_text") or "")
        lines = _extract_lines(body.lower(), FOOTBALL_KEYWORDS)
        if not lines:
            # Keep generic football token check for pages where the phrase may be split by punctuation.
            if "football" in body.lower():
                lines = [_clean(body[:240])]
        if lines:
            url = str(snapshot.get("final_url"))
            football_pages.append(url)
            for line in lines:
                football_lines.append(line)
            football_hits.append(
                {
                    "url": url,
                    "title": str(snapshot.get("title")),
                }
            )

            if "node" in url and url.startswith(f"{BASE_URL}/node/"):
                football_posts.append(
                    {
                        "url": url,
                        "title": str(snapshot.get("title") or ""),
                        "snippet": lines[0] if lines else "",
                    }
                )

    deduped_hits: list[dict[str, Any]] = []
    seen_hits: set[tuple[str, str]] = set()
    for item in football_hits:
        key = (str(item.get("url")), str(item.get("title")))
        if key in seen_hits:
            continue
        seen_hits.add(key)
        deduped_hits.append(item)
    football_hits = deduped_hits

    deduped_posts: list[dict[str, str]] = []
    seen_posts: set[tuple[str, str]] = set()
    for item in football_posts:
        url = str(item.get("url", ""))
        title = str(item.get("title", ""))
        key = (url, title)
        if key in seen_posts:
            continue
        seen_posts.add(key)
        deduped_posts.append({"url": url, "title": title})
    football_posts = deduped_posts

    football_pages = _dedupe_keep_order(football_pages)
    football_lines = _dedupe_keep_order(football_lines)
    football_program_available = bool(football_hits or football_lines)

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_after_site_search_and_news_navigation")

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_pages": football_pages,
        "football_hits": football_hits,
        "football_posts": football_posts,
        "football_keyword_lines": football_lines,
        "discovered_football_nodes": _dedupe_keep_order(discovered_urls),
        "search_url": SEARCH_URL,
        "news_url": NEWS_URL,
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
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers", []),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "pages_checked": len(snapshots),
            "target_pages": TARGET_PAGES,
            "discovered_node_count": len(discovered_urls),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
