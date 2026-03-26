"""Deterministic football scraper for Blair High School (CA)."""

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

NCES_ID = "062994004662"
SCHOOL_NAME = "Blair High"
STATE = "CA"

PROXY_PROFILE = "datacenter"
HOME_URL = "https://blair.pusd.us"
ATHLETICS_PAGE = "https://blair.pusd.us/students/athletics"
TITLE_IX_PAGE = (
    "https://www.pusd.us/departments/athletics/title-ix-data-2023-2024/blair-title-ix-2023-2024"
)

TARGET_PAGES = [HOME_URL, ATHLETICS_PAGE, TITLE_IX_PAGE]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

SPORT_KEYWORDS = (
    "football",
    "cross country",
    "girls volleyball",
    "boys girls water polo",
    "girls water polo",
    "boys water polo",
    "boys girls basketball",
    "girls basketball",
    "boys basketball",
    "baseball",
    "boys girls soccer",
    "girls soccer",
    "boys soccer",
    "cheer",
    "softball",
    "swim",
    "tennis",
    "track",
    "track and field",
)



def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()



def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out



def _extract_contacts(raw_text: str) -> list[dict[str, str]]:
    text = raw_text.replace("\u00a0", " ")
    matches = re.findall(
        r"([A-Z][A-Za-z.\-' ]+):\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})",
        text,
    )
    contacts: list[dict[str, str]] = []
    for name, email in matches:
        name_clean = _clean(name)
        email_clean = _clean(email)
        if name_clean and email_clean:
            contacts.append({"name": name_clean, "email": email_clean})
    return _dedupe_keep_order([f"{c['name']}|{c['email']}" for c in contacts])



def _extract_lines(raw_text: str, *, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in raw_text.splitlines():
        line = _clean(raw_line.replace("&amp;", "&").replace("&#39;", "'"))
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return lines



def _collect_page_snapshot(page) -> dict[str, Any]:
    body_text = page.inner_text("body")
    text = _clean(body_text)
    links = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => ({text: (e.textContent || '').replace(/\\s+/g,' ').trim(), href: e.getAttribute('href') || ''}))",
    )
    if not isinstance(links, list):
        links = []

    normalized_links: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = str(item.get("href") or "").strip()
        if href:
            normalized_links.append({"text": text, "href": href})

    return {
        "title": _clean(page.title() or ""),
        "url": page.url,
        "text": text,
        "links": normalized_links,
    }



async def scrape_school() -> dict[str, Any]:
    """Scrape publicly available football evidence from Blair High athletics content."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_PAGES, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_log: list[str] = []
    snapshots: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 920},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1400)
            source_pages.append(page.url)
            navigation_log.append("visit_home")
            snapshots.append(await _collect_page_snapshot(page))

            try:
                athletics_link = page.locator("a[href*='/students/athletics']").first
                if await athletics_link.count() > 0:
                    await athletics_link.click(timeout=7000)
                    await page.wait_for_timeout(1600)
                    navigation_log.append("click_home_athletics_link")
                else:
                    await page.goto(ATHLETICS_PAGE, wait_until="domcontentloaded", timeout=90000)
                    navigation_log.append("fallback_direct_athletics")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_to_athletics_failed:{type(exc).__name__}")
                await page.goto(ATHLETICS_PAGE, wait_until="domcontentloaded", timeout=90000)
                navigation_log.append("fallback_direct_athletics")

            source_pages.append(page.url)
            snapshots.append(await _collect_page_snapshot(page))

            try:
                title_ix_link = page.locator("a[href*='title-ix-data-2023-2024'][href*='blair-title-ix-2023-2024']").first
                if await title_ix_link.count() > 0:
                    await title_ix_link.click(timeout=7000)
                    await page.wait_for_timeout(1200)
                    navigation_log.append("click_title_ix_link")
                    source_pages.append(page.url)
                    snapshots.append(await _collect_page_snapshot(page))
                    await page.go_back()
                    await page.wait_for_timeout(900)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_title_ix_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    seen_contacts: list[dict[str, str]] = []
    all_lines: list[str] = []
    all_links: list[dict[str, str]] = []
    schedule_links: list[dict[str, str]] = []
    title_ix_seen = False

    for snapshot in snapshots:
        page_text = str(snapshot.get("text") or "")
        all_lines.extend(_extract_lines(page_text, keywords=SPORT_KEYWORDS))
        all_lines.extend(_extract_lines(page_text, keywords=("football", "athletics", "athletic")))

        snapshot_links = snapshot.get("links")
        if isinstance(snapshot_links, list):
            for link in snapshot_links:
                if not isinstance(link, dict):
                    continue
                href = str(link.get("href") or "").strip()
                text = _clean(str(link.get("text") or ""))
                if not href:
                    continue
                all_links.append({"text": text, "href": href})

                if "title-ix-data-2023-2024" in href.lower():
                    title_ix_seen = True

                if "schedule" in href.lower() and "docs.google.com" in href.lower():
                    schedule_links.append({"text": text, "href": href})

    all_lines = _dedupe_keep_order(all_lines)
    football_lines = [line for line in all_lines if "football" in line.lower()]
    football_lines = _dedupe_keep_order(football_lines)

    for contact in _extract_contacts("\n".join(all_lines)):
        name, email = contact.split("|", 1)
        seen_contacts.append({"name": name, "email": email, "role": "Athletic Director"})

    athletics_links = [
        entry
        for entry in all_links
        if "/students/athletics" in (entry.get("href") or "")
    ]

    football_schedule_links = [
        entry
        for entry in schedule_links
        if "football" in (entry.get("text") or "").lower()
        or "football" in (entry.get("href") or "").lower()
    ]
    football_general_schedule_links = [
        entry for entry in schedule_links if entry not in football_schedule_links
    ]

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_lines),
        "football_team_names": _dedupe_keep_order(
            [line for line in football_lines if line.lower() == "football"]
            or ["Football"]
        )[:1],
        "fall_sports": _dedupe_keep_order(
            [line for line in all_lines if any(token in line.lower() for token in ("cross country", "football", "girls volleyball", "water polo", "cheer"))]
        ),
        "other_sports_lines": _dedupe_keep_order(
            [line for line in all_lines if any(token in line.lower() for token in ("basketball", "soccer", "swim", "tennis", "track", "baseball", "softball"))]
        ),
        "athletic_director_contacts": seen_contacts,
        "athletics_page_title": next((s.get("title") for s in snapshots if ATHLETICS_PAGE in str(s.get("url", ""))), ""),
        "athletics_page_url": ATHLETICS_PAGE,
        "title_ix_data_url": TITLE_IX_PAGE if title_ix_seen else "",
        "football_mentions": football_lines,
        "football_schedule_links": football_schedule_links,
        "sports_schedule_links": football_general_schedule_links,
        "athletics_internal_links": athletics_links[:10],
        "navigation_log": navigation_log,
        "source_line_count": len(_clean("\n".join(all_lines)).splitlines()),
    }

    if not extracted_items["football_program_available"]:
        errors.append("blocked:no_public_football_content_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_auth_mode"],
            "focus": "football_only",
            "pages_requested": TARGET_PAGES,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_log,
            "football_evidence_count": len(football_lines),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
