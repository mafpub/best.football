"""Deterministic football scraper for Folsom High (CA)."""

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

NCES_ID = "061389001570"
SCHOOL_NAME = "Folsom High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

FOOTBALL_HUB_URL = "https://fhs.fcusd.org/athletics/teams/football-information"
FOOTBALL_COACHES_URL = (
    "https://fhs.fcusd.org/athletics/teams/football-information/meet-the-football-coaches"
)

TARGET_URLS = [FOOTBALL_HUB_URL, FOOTBALL_COACHES_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _collect_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines


async def _collect_page_snapshot(page) -> dict[str, Any]:
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "body_text": await page.locator("main").inner_text(timeout=10000),
        "links": await page.locator("main a[href]").evaluate_all(
            """els => els.map(a => ({
                text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: a.href || a.getAttribute('href') || ''
            }))"""
        ),
    }


def _normalize_link_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _football_link_items(links: list[dict[str, Any]]) -> dict[str, Any]:
    items: list[dict[str, str]] = []
    roster_urls: dict[str, str] = {"fs": "", "jv": "", "varsity": ""}
    schedule_url = ""
    coaches_link_url = ""

    for link in links:
        if not isinstance(link, dict):
            continue
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        if not href:
            continue

        text_key = _normalize_link_text(text)
        href_key = href.lower()
        if not (
            "football" in text_key
            or "roster" in text_key
            or "schedule" in text_key
            or "coach" in text_key
            or "/football-information" in href_key
            or "tinyurl.com" in href_key
            or "docs.google.com" in href_key
        ):
            continue

        items.append({"text": text, "href": href})

        if text_key == "fsroster":
            roster_urls["fs"] = href
        elif text_key == "jvroster":
            roster_urls["jv"] = href
        elif text_key == "varsityroster":
            roster_urls["varsity"] = href
        elif text_key == "schedule":
            schedule_url = href
        elif "meetthecoaches" in text_key or "meetthefootballcoaches" in text_key:
            coaches_link_url = href

    return {
        "football_team_links": _dedupe_keep_order([f"{item['text']}|{item['href']}" for item in items]),
        "football_team_link_items": items,
        "football_roster_urls": roster_urls,
        "football_schedule_url": schedule_url,
        "football_coaches_link_url": coaches_link_url,
    }


def _extract_coach_sections(lines: list[str]) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    current_label = ""
    current_names: list[str] = []

    def flush() -> None:
        nonlocal current_label, current_names
        if current_label and current_names:
            sections.append(
                {
                    "section": current_label,
                    "names": _dedupe_keep_order(current_names),
                }
            )
        current_label = ""
        current_names = []

    for line in lines:
        if line.endswith(":"):
            flush()
            current_label = line[:-1].strip()
            continue
        if current_label:
            current_names.append(line)

    flush()
    return sections


async def scrape_school() -> dict[str, Any]:
    """Scrape public football evidence from Folsom High athletics pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy_meta = get_proxy_runtime_meta(PROXY_PROFILE)
    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []
    snapshots: list[dict[str, Any]] = []

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
            for url, step in [
                (FOOTBALL_HUB_URL, "visit_football_hub"),
                (FOOTBALL_COACHES_URL, "visit_football_coaches"),
            ]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    source_pages.append(page.url)
                    navigation_steps.append(step)
                    snapshots.append(await _collect_page_snapshot(page))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{step}:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    snapshots_by_url = {str(item.get("url") or ""): item for item in snapshots}

    hub_snapshot = snapshots_by_url.get(FOOTBALL_HUB_URL, {})
    coach_snapshot = snapshots_by_url.get(FOOTBALL_COACHES_URL, {})

    hub_text = str(hub_snapshot.get("body_text") or "")
    hub_lines = _collect_lines(hub_text)
    hub_links = hub_snapshot.get("links") if isinstance(hub_snapshot.get("links"), list) else []
    football_links = _football_link_items([item for item in hub_links if isinstance(item, dict)])

    coach_text = str(coach_snapshot.get("body_text") or "")
    coach_lines = _collect_lines(coach_text)
    coach_sections = _extract_coach_sections(coach_lines)

    football_coach_names = _dedupe_keep_order(
        name
        for section in coach_sections
        for name in (section.get("names") or [])
        if isinstance(name, str)
    )

    football_program_available = bool(
        football_links["football_team_link_items"] and football_coach_names
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found")

    football_evidence = _dedupe_keep_order(
        [
            *[item["text"] for item in football_links["football_team_link_items"]],
            *coach_lines[:12],
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_hub_url": FOOTBALL_HUB_URL,
        "football_hub_title": _clean(str(hub_snapshot.get("title") or "")),
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_coaches_title": _clean(str(coach_snapshot.get("title") or "")),
        "football_team_links": football_links["football_team_link_items"],
        "football_team_link_labels": football_links["football_team_links"],
        "football_roster_urls": football_links["football_roster_urls"],
        "football_schedule_url": football_links["football_schedule_url"],
        "football_coaches_link_url": football_links["football_coaches_link_url"],
        "football_coach_sections": coach_sections,
        "football_coach_names": football_coach_names,
        "football_hub_lines": [line for line in hub_lines if "football" in line.lower() or "roster" in line.lower() or "schedule" in line.lower()],
        "football_coaches_lines": [line for line in coach_lines if "coach" in line.lower() or "staff" in line.lower()],
        "football_evidence": football_evidence,
        "summary": (
            "Folsom High has a public football hub with fs/jv/varsity roster links, a schedule link, and a public coaches page listing multiple assistant coaches plus field staff."
            if football_program_available
            else ""
        ),
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
            "proxy_servers": proxy_meta.get("proxy_servers"),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "focus": "football_only",
            "pages_requested": TARGET_URLS,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_steps,
            "football_evidence_count": len(football_evidence),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
