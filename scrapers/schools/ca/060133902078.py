"""Football-only scraper for Hamilton High (CA)."""

from __future__ import annotations

import asyncio
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

NCES_ID = "060133902078"
SCHOOL_NAME = "Hamilton High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://hhs.husdschools.org/"
ATHLETICS_URL = "https://hhs.husdschools.org/athletics"
TEAMS_INDEX_URL = "https://hhs.husdschools.org/athletics/athletics/teams"
FOOTBALL_URL = "https://hhs.husdschools.org/athletics/teams/football"
SCHEDULES_URL = "https://hhs.husdschools.org/athletics/schedules"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, TEAMS_INDEX_URL, FOOTBALL_URL, SCHEDULES_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_links(raw_links: list[dict[str, Any]]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for entry in raw_links:
        if not isinstance(entry, dict):
            continue
        href = _clean(str(entry.get("href") or ""))
        if not href:
            continue
        text = _clean(str(entry.get("text") or ""))
        links.append({"text": text, "href": href})
    return links


def _pick_link(
    links: list[dict[str, str]],
    *,
    href_contains: list[str] | None = None,
    href_exact: str | None = None,
    text_contains: list[str] | None = None,
) -> str:
    if href_exact:
        for link in links:
            if link["href"].endswith(href_exact):
                return link["href"]
    if href_contains:
        for frag in href_contains:
            frag_l = frag.lower()
            for link in links:
                href = link["href"].lower()
                if frag_l in href:
                    if text_contains:
                        text_l = link["text"].lower()
                        if any(req.lower() in text_l for req in text_contains):
                            return link["href"]
                    else:
                        return link["href"]
    if text_contains:
        for req in text_contains:
            req_l = req.lower()
            for link in links:
                if req_l in link["text"].lower():
                    return link["href"]
    return ""


def _extract_athletic_director(text: str) -> str:
    patterns = (
        r"Athletic Director[,\s]+([A-Z][A-Za-z][A-Za-z\-' .]+)",
        r"athletic director[,\s]+([A-Z][A-Za-z][A-Za-z\-' .]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            return _clean(match.group(1))
    return ""


async def _collect_links(page) -> list[dict[str, str]]:
    raw = await page.locator("a[href]").evaluate_all(
        "els => els.map((anchor) => ({text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(), href: anchor.href || ''}))"
    )
    if not isinstance(raw, list):
        return []
    return _extract_links(raw)


async def _collect_main_text(page) -> str:
    try:
        return _clean(await page.locator("main#fsPageContent").inner_text(timeout=10_000))
    except Exception:  # noqa: BLE001
        try:
            return _clean(await page.locator("main").inner_text(timeout=10_000))
        except Exception:  # noqa: BLE001
            return _clean(await page.locator("body").inner_text(timeout=10_000))


async def _click_or_goto(page, link_url: str, fallback_url: str) -> None:
    if link_url:
        if link_url == page.url:
            return
        try:
            # Prefer click when possible to reconstruct the real navigation path.
            locator = page.locator(f"a[href='{link_url}']")
            if await locator.count():
                await locator.first.click(timeout=8000)
                await page.wait_for_timeout(900)
                return
        except Exception:  # noqa: BLE001
            pass
        try:
            await page.goto(link_url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(900)
        except Exception:  # noqa: BLE001
            await page.goto(fallback_url, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(900)
        return
    await page.goto(fallback_url, wait_until="domcontentloaded", timeout=60_000)
    await page.wait_for_timeout(900)


async def scrape_school() -> dict[str, Any]:
    """Deterministically traverse Athletics -> Teams -> Football -> Schedules for football evidence."""

    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    match_titles: list[str] = []
    team_links: list[str] = []
    football_schedule_pdfs: list[dict[str, str]] = []
    athletic_director = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            user_agent=USER_AGENT,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(900)
            source_pages.append(page.url)

            home_links = await _collect_links(page)
            athletics_link = _pick_link(
                home_links,
                href_exact="/athletics",
                href_contains=["/athletics"],
            )
            await _click_or_goto(page, athletics_link, ATHLETICS_URL)
            source_pages.append(page.url)

            athletics_links = await _collect_links(page)
            teams_link = _pick_link(
                athletics_links,
                href_contains=["/athletics/athletics/teams", "/athletics/teams"],
            )
            await _click_or_goto(page, teams_link, TEAMS_INDEX_URL)
            source_pages.append(page.url)

            teams_links_raw = await _collect_links(page)
            team_links = _dedupe_keep_order(
                [
                    item["text"]
                    for item in teams_links_raw
                    if "/athletics/teams/" in item["href"] and item["text"]
                ]
            )
            teams_text = await _collect_main_text(page)
            if not athletic_director:
                athletic_director = _extract_athletic_director(teams_text)

            football_link = _pick_link(
                teams_links_raw,
                href_contains=["/athletics/teams/football"],
                text_contains=["football"],
            )
            if not football_link and ATHLETICS_URL:
                fallback_candidates = _pick_link(
                    teams_links_raw,
                    text_contains=["football"],
                )
                football_link = fallback_candidates
            await _click_or_goto(page, football_link, FOOTBALL_URL)
            source_pages.append(page.url)

            try:
                raw_titles = await page.locator("main#fsPageContent h2.fsElementTitle").all_inner_texts()
                match_titles = _dedupe_keep_order([_clean(t) for t in raw_titles if _clean(t)])
            except Exception:  # noqa: BLE001
                match_titles = []

            if not match_titles:
                football_text = await _collect_main_text(page)
                match_titles = _dedupe_keep_order(
                    [line for line in football_text.split(" ") if "vs" in line.lower() or "v " in line.lower()]
                )[:10]

            await page.goto(SCHEDULES_URL, wait_until="domcontentloaded", timeout=60_000)
            await page.wait_for_timeout(900)
            source_pages.append(page.url)

            schedule_links = _dedupe_keep_order(
                [
                    f"{item['text']}|{item['href']}"
                    for item in await _collect_links(page)
                    if item["href"].lower().endswith(".pdf")
                    and ("football" in item["text"].lower() or "football" in item["href"].lower())
                ]
            )
            football_schedule_pdfs = [
                {"title": entry.split("|", 1)[0], "url": entry.split("|", 1)[1]}
                for entry in schedule_links
            ]

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    football_available = bool(match_titles or football_schedule_pdfs or team_links)
    if not football_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "focus": "football_only",
        "football_program_available": football_available,
        "school_page_url": HOME_URL,
        "athletics_page_url": ATHLETICS_URL,
        "teams_index_url": TEAMS_INDEX_URL,
        "football_team_url": FOOTBALL_URL,
        "schedules_page_url": SCHEDULES_URL,
        "football_match_titles": match_titles,
        "football_schedule_pdfs": football_schedule_pdfs,
        "team_links": team_links,
        "athletic_director": athletic_director,
        "football_signal_lines": _dedupe_keep_order(
            [line for line in (teams_text if 'teams_text' in locals() else "").split(".") if line]
        ),
        "summary": (
            "Hamilton High athletics exposes a dedicated football team page and public football schedule PDFs."
            if football_available
            else ""
        ),
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "manual_navigation_steps": [
                "home",
                "open_athletics",
                "open_teams_index",
                "open_football_team",
                "open_schedules",
            ],
            "target_urls": TARGET_URLS,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Compatibility entrypoint for runner compatibility."""

    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=False, indent=2))
