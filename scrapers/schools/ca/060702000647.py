"""Deterministic football scraper for Calistoga Junior/Senior High (CA)."""

from __future__ import annotations

import json
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

NCES_ID = "060702000647"
SCHOOL_NAME = "Calistoga Junior/Senior High"
STATE = "CA"

HOME_URL = "https://cjshs.calistogaschools.org/"
ATHLETICS_URL = "https://cjshs.calistogaschools.org/athletics/"
ATHLETICS_DEPT_URL = "https://cjshs.calistogaschools.org/page/athleticsdept/"
SITEMAP_URL = "https://cjshs.calistogaschools.org/sitemap.xml"

MANUAL_PAGES = [
    HOME_URL,
    ATHLETICS_URL,
    ATHLETICS_DEPT_URL,
    SITEMAP_URL,
]

SPORT_PATTERNS = [
    ("Football", r"\bFootball\b"),
    ("Women's Volleyball", r"Women['’]s Volleyball"),
    ("Men's & Women's Swimming", r"Men['’]s & Women['’]s Swimming"),
    ("Men's & Women's Soccer", r"Men['’]s & Women['’]s Soccer"),
    ("Men's & Women's Basketball", r"Men['’]s & Women['’]s Basketball"),
    ("Co-ed Tennis", r"Co[- ]?ed Tennis"),
    ("Co-ed Track & Field", r"Co[- ]?Ed Track & Field"),
    ("Co-ed Sideline Cheer", r"Co[- ]?ed Sideline Cheer"),
    ("Baseball & Softball", r"Baseball & Softball"),
]


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = " ".join(str(value).split()).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_text(value: str) -> str:
    return (
        value.replace("’", "'")
        .replace("“", '"')
        .replace("”", '"')
        .replace("\u00a0", " ")
    )


def _parse_sports_list(raw_text: str) -> list[str]:
    normalized = _normalize_text(raw_text)
    parsed: list[str] = []
    for canonical, pattern in SPORT_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            parsed.append(canonical)
    return _dedupe_keep_order(parsed)


def _extract_urls_from_sitemap(sitemap_text: str) -> list[str]:
    matches = re.findall(r"https://cjshs\.calistogaschools\.org/[^\s<]+", sitemap_text)
    relevant = [
        url
        for url in matches
        if "/athletics/" in url or "/page/athleticsdept" in url or "/documents/departments/athletics/" in url
    ]
    return _dedupe_keep_order(relevant)


async def _collect_athletics_department_signal(page) -> dict[str, Any]:
    main = page.locator("div.main")
    main_text = _normalize_text(await main.inner_text()) if await main.count() else _normalize_text(await page.inner_text("body"))

    paragraph_texts = [
        _normalize_text(text)
        for text in await page.locator("div.main p").all_inner_texts()
    ]
    sports_paragraph = next((text for text in paragraph_texts if "Football" in text), "")
    junior_high_paragraph = next((text for text in paragraph_texts if "Junior High School Athletics" in text), "")
    sports_block_match = re.search(
        r"Sports(.*?)(Junior High School Athletics|Go Fan Tickets|Find Us)",
        main_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    sports_block = _normalize_text(sports_block_match.group(1)) if sports_block_match else sports_paragraph

    links = await page.locator("div.main a[href]").evaluate_all(
        """els => els.map(e => ({
            text: (e.textContent || "").trim(),
            href: e.href || "",
        }))"""
    )

    familyid_url = ""
    gofan_url = ""
    athletics_links: list[dict[str, str]] = []
    for link in links:
        text = " ".join(str(link.get("text") or "").split()).strip()
        href = str(link.get("href") or "").strip()
        combo = f"{text} {href}".lower()
        if "arbitersports.com" in combo:
            familyid_url = href or familyid_url
        if "gofan.co" in combo:
            gofan_url = href or gofan_url
        if any(token in combo for token in ("arbitersports", "gofan", "athleticsdept")):
            athletics_links.append({"text": text, "href": href})

    return {
        "url": page.url,
        "main_text": main_text,
        "sports_paragraph": sports_paragraph,
        "junior_high_paragraph": junior_high_paragraph,
        "sports_block": sports_block,
        "sports_list": _parse_sports_list(sports_block or sports_paragraph),
        "football_present": bool(re.search(r"\bFootball\b", sports_block or sports_paragraph, flags=re.IGNORECASE)),
        "familyid_url": familyid_url,
        "gofan_url": gofan_url,
        "athletics_links": _dedupe_keep_order([f"{item['text']}|{item['href']}" for item in athletics_links]),
    }


async def _collect_football_events(page) -> list[dict[str, Any]]:
    try:
        raw_events = await page.evaluate(
            """() => {
                const seen = new Set();
                const out = [];
                const walk = (value) => {
                    if (!value || typeof value !== 'object' || seen.has(value)) return;
                    seen.add(value);
                    if (Array.isArray(value)) {
                        for (const item of value) walk(item);
                        return;
                    }
                    if (typeof value.title === 'string' && /football/i.test(value.title)) {
                        out.push({
                            id: value.id ?? null,
                            title: value.title ?? '',
                            start_at: value.start_at ?? value.start ?? '',
                            end_at: value.end_at ?? value.end ?? '',
                            venue: value.venue ?? '',
                            description: value.description ?? '',
                            custom_section_name: value.custom_section_name ?? '',
                        });
                    }
                    for (const key of Object.keys(value)) {
                        walk(value[key]);
                    }
                };
                walk(window.__NUXT__);
                return out;
            }"""
        )
    except Exception:
        return []

    if not isinstance(raw_events, list):
        return []

    events: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for item in raw_events:
        if not isinstance(item, dict):
            continue
        key = (
            item.get("id"),
            item.get("title"),
            item.get("start_at"),
            item.get("end_at"),
            item.get("venue"),
        )
        if key in seen:
            continue
        seen.add(key)
        events.append(item)

    events.sort(key=lambda item: (str(item.get("start_at") or ""), str(item.get("title") or "")))
    return events


async def _collect_home_signal(page) -> dict[str, Any]:
    text = _normalize_text(await page.inner_text("body"))
    return {
        "url": page.url,
        "text_preview": text[:2500],
        "mentions_football": bool(re.search(r"\bfootball\b", text, flags=re.IGNORECASE)),
        "mentions_athletics": bool(re.search(r"\bathletic", text, flags=re.IGNORECASE)),
    }


async def _collect_sitemap_signal(page) -> dict[str, Any]:
    text = _normalize_text(await page.inner_text("body"))
    return {
        "url": page.url,
        "relevant_urls": _extract_urls_from_sitemap(text),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Calistoga Junior/Senior High's public football-facing athletics content."""
    require_proxy_credentials(profile="datacenter")
    assert_not_blocklisted(MANUAL_PAGES, profile="datacenter")

    source_pages: list[str] = []
    errors: list[str] = []
    home_signal: dict[str, Any] = {}
    athletics_signal: dict[str, Any] = {}
    sitemap_signal: dict[str, Any] = {}
    football_events: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile="datacenter"),
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            home_signal = await _collect_home_signal(page)

            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            football_events = await _collect_football_events(page)

            await page.goto(ATHLETICS_DEPT_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            athletics_signal = await _collect_athletics_department_signal(page)

            await page.goto(SITEMAP_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(800)
            source_pages.append(page.url)
            sitemap_signal = await _collect_sitemap_signal(page)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_links = _dedupe_keep_order(list(athletics_signal.get("athletics_links", [])))
    sports_list = _dedupe_keep_order(list(athletics_signal.get("sports_list", [])))
    football_event_titles = _dedupe_keep_order(
        [
            str(item.get("title") or "")
            for item in football_events
            if isinstance(item, dict)
        ]
    )

    extracted_items: dict[str, Any] = {
        "athletics_landing_url": ATHLETICS_URL,
        "athletics_department_url": ATHLETICS_DEPT_URL,
        "home_mentions": home_signal,
        "athletics_department": {
            "football_present": bool(athletics_signal.get("football_present")),
            "sports_list": sports_list,
            "sports_paragraph": athletics_signal.get("sports_paragraph", ""),
            "junior_high_paragraph": athletics_signal.get("junior_high_paragraph", ""),
            "familyid_url": athletics_signal.get("familyid_url", ""),
            "gofan_url": athletics_signal.get("gofan_url", ""),
            "links": athletics_links,
        },
        "football_events": football_events,
        "football_event_titles": football_event_titles,
        "sitemap_relevant_urls": sitemap_signal.get("relevant_urls", []),
    }

    if not athletics_signal.get("football_present"):
        errors.append("no_football_marker_found_on_athletics_department_page")

    if not sports_list:
        errors.append("sports_list_extraction_empty")

    if not athletics_signal.get("familyid_url") and not athletics_signal.get("gofan_url"):
        errors.append("athletics_links_missing_public_contact_and_ticket_urls")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy": get_proxy_runtime_meta(profile="datacenter"),
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "homepage",
                "athletics_landing",
                "athletics_department",
                "sitemap",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


async def main() -> None:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
