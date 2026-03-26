"""Deterministic football scraper for Literacy First Charter (CA)."""

from __future__ import annotations

import sys
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060209810374"
SCHOOL_NAME = "Literacy First Charter"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.lfcsinc.org/"
ATHLETICS_URL = "https://athletics.libertychs.org/"
COACHES_URL = "https://athletics.libertychs.org/coaches/"
FLAG_FOOTBALL_URL = "https://athletics.libertychs.org/flag-football/"

TARGET_PAGES = [HOME_URL, ATHLETICS_URL, COACHES_URL, FLAG_FOOTBALL_URL]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

KEYWORDS = (
    "athletics",
    "coach",
    "coaches",
    "flag football",
    "football",
    "liberty lions",
    "lchs",
    "sports",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _extract_emails(text: str) -> list[str]:
    emails = re.findall(r"[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}", text)
    return _dedupe_keep_order([email.lower() for email in emails])


def _keyword_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)


async def _snapshot(page) -> dict[str, Any]:
    text = _clean(await page.inner_text("body"))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )
    if not isinstance(links, list):
        links = []

    normalized_links: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        text_value = _clean(str(item.get("text") or ""))
        if href:
            normalized_links.append({"text": text_value, "href": href})

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": text,
        "lines": _keyword_lines(text),
        "links": normalized_links,
        "emails": _extract_emails(text),
    }


def _find_first(lines: list[str], predicate) -> str:
    for line in lines:
        if predicate(line):
            return line
    return ""


async def scrape_school() -> dict[str, Any]:
    """Scrape football-specific public athletics evidence for Literacy First Charter."""
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
            viewport={"width": 1440, "height": 960},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            snapshots.append(await _snapshot(page))
            navigation_log.append("home")

            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            snapshots.append(await _snapshot(page))
            navigation_log.append("athletics_home")

            await page.goto(COACHES_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            snapshots.append(await _snapshot(page))
            navigation_log.append("coaches")

            await page.goto(FLAG_FOOTBALL_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            snapshots.append(await _snapshot(page))
            navigation_log.append("flag_football")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_snapshot = next((item for item in snapshots if item.get("url", "").startswith(HOME_URL)), {})
    athletics_snapshot = next((item for item in snapshots if item.get("url", "").startswith(ATHLETICS_URL)), {})
    coaches_snapshot = next((item for item in snapshots if item.get("url", "").startswith(COACHES_URL)), {})
    flag_snapshot = next((item for item in snapshots if item.get("url", "").startswith(FLAG_FOOTBALL_URL)), {})

    athletics_lines = list(athletics_snapshot.get("lines", []))
    coaches_lines = list(coaches_snapshot.get("lines", []))
    flag_lines = list(flag_snapshot.get("lines", []))
    home_lines = list(home_snapshot.get("lines", []))

    athletics_links = [
        link
        for link in athletics_snapshot.get("links", [])
        if isinstance(link, dict) and link.get("href")
    ]
    coaches_links = [
        link
        for link in coaches_snapshot.get("links", [])
        if isinstance(link, dict) and link.get("href")
    ]

    sports_nav_links = _dedupe_keep_order(
        [
            f"{link.get('text', '')}|{link.get('href', '')}"
            for link in athletics_links
            if any(
                token in f"{link.get('text', '')} {link.get('href', '')}".lower()
                for token in ("cross country", "volleyball", "flag football", "basketball", "soccer", "baseball", "softball", "track")
            )
        ]
    )

    football_coach_line = _find_first(
        coaches_lines, lambda line: "flag football" in line.lower() and "shawn brown" in line.lower()
    )
    athletic_director_line = _find_first(
        coaches_lines, lambda line: "linn dunton" in line.lower() or "athletic director" in line.lower()
    )
    flag_page_title = _find_first(flag_lines, lambda line: line.lower() == "flag football")
    home_sports_story = _find_first(home_lines, lambda line: "sports!!!" in line.lower())

    football_mentions = _dedupe_keep_order(
        [
            *[line for line in athletics_lines if "flag football" in line.lower() or "football" in line.lower()],
            *[line for line in coaches_lines if "flag football" in line.lower() or "football" in line.lower()],
            *[line for line in flag_lines if "flag football" in line.lower() or "football" in line.lower()],
            *[line for line in home_lines if "sports" in line.lower()],
        ]
    )

    football_program_available = bool(flag_page_title or football_coach_line or any("flag football" in line.lower() for line in football_mentions))

    if not football_program_available and not errors:
        errors.append("blocked:no_public_football_or_flag_football_content_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_type": "flag football" if football_program_available else "",
        "school_system": "Literacy First Charter / Liberty Charter High School athletics",
        "athletics_home_url": ATHLETICS_URL,
        "coaches_page_url": COACHES_URL,
        "flag_football_page_url": FLAG_FOOTBALL_URL,
        "athletic_director": {
            "name": "Linn Dunton" if "linn dunton" in athletic_director_line.lower() else "",
            "email": "linn.dunton@lfcsinc.org" if "linn dunton" in athletic_director_line.lower() else "",
            "line": athletic_director_line,
        },
        "football_coach": {
            "team": "Flag Football" if football_coach_line else "",
            "name": "Shawn Brown" if "shawn brown" in football_coach_line.lower() else "",
            "email": "shawn.brown@lfcsinc.org" if "shawn brown" in football_coach_line.lower() else "",
            "line": football_coach_line,
        },
        "sports_offered_links": sports_nav_links,
        "football_mentions": football_mentions,
        "home_sports_story": home_sports_story,
        "home_page_title": home_snapshot.get("title", ""),
        "athletics_page_title": athletics_snapshot.get("title", ""),
        "coaches_page_title": coaches_snapshot.get("title", ""),
        "flag_football_page_title": flag_snapshot.get("title", ""),
    }

    scrape_meta = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "script_version": "1.0.0",
        "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_profile"],
        "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"],
        "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_auth_mode"],
        "focus": "football_only",
        "pages_requested": TARGET_PAGES,
        "pages_visited": len(source_pages),
        "navigation_steps": navigation_log,
        "football_evidence_count": len(football_mentions),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


async def scrape_football() -> dict[str, Any]:
    return await scrape_school()


async def main() -> None:
    import json

    payload = await scrape_school()
    print(json.dumps(payload, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
