"""Deterministic football scraper for Lincoln High (CA)."""

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

NCES_ID = "063432011939"
SCHOOL_NAME = "Lincoln High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.lincoln.sandiegounified.org/"
ATHLETICS_URL = "https://www.lincoln.sandiegounified.org/students/Athletics_Website"
FOOTBALL_TEAM_URL = "https://www.lhshornets.com/varsity/football/"
FOOTBALL_SCHEDULE_URL = "https://www.lhshornets.com/varsity/football/schedule-results?hl=0"
FOOTBALL_COACHES_URL = "https://www.lhshornets.com/varsity/football/coaches?hl=0"
FOOTBALL_ROSTER_URL = "https://www.lhshornets.com/varsity/football/roster?hl=0"
HEAD_COACH_LINKS_URL = "https://www.lhshornets.com/head-coaches-links/"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    FOOTBALL_TEAM_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_ROSTER_URL,
    HEAD_COACH_LINKS_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

NAV_EXCLUSIONS = {
    "Lincoln Athletics",
    "Lincoln High School",
    "GO HORNETS!",
    "Home",
    "Teams",
    "Calendar",
    "Sync Schedule",
    "Athletic Department",
    "Athletic Admin Staff",
    "Coaching Staff",
    "Eligibility",
    "SDUSD Athletic Eligibility Grading Periods",
    "NCAA Eligibility",
    "CIF San Diego",
    "The Hive Athletics Inquiry Form",
    "Access to Athletic Events",
    "Emergency Action Plans",
    "SB 1349",
    "Coaches",
    "Coaching at LHS for 2024-25",
    "Grade Change Process",
    "Coaches Pay Process",
    "Registration",
    "SDUSD Athletic Physical",
    "Tickets",
    "SDUSD Athletics",
    "SD City Conference",
    "SDUSD SWEAR",
    "Head Coaches Links",
    "Schedules",
    "Roster",
    "Facebook",
    "Instagram",
    "Home Town",
    "NFHS",
    "MaxPreps",
    "Powered By HOME CAMPUS",
    "Terms and Conditions",
    "Privacy Policy",
}


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _normalize_href(href: str, base: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("/"):
        from urllib.parse import urljoin

        return urljoin(base, value)
    return value


async def _collect_text(page) -> str:
    for selector in ("main", "#content", "body"):
        locator = page.locator(selector)
        if await locator.count():
            try:
                text = await locator.first.inner_text(timeout=15000)
                cleaned = _clean(text)
                if cleaned:
                    return cleaned
            except Exception:
                continue
    return ""


async def _collect_links(page) -> list[dict[str, str]]:
    links = await page.locator("a[href]").evaluate_all(
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.getAttribute('href') || '',
        }))""",
    )
    if not isinstance(links, list):
        return []

    normalized: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _normalize_href(str(item.get("href") or ""), page.url)
        if not text or not href:
            continue
        normalized.append({"text": text, "href": href})
    return normalized


async def _collect_home_page(page) -> dict[str, Any]:
    text = await _collect_text(page)
    links = await _collect_links(page)
    athletics_links = [
        item
        for item in links
        if "athletic" in item["text"].lower() or "athletic" in item["href"].lower()
    ]
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": text,
        "athletics_links": athletics_links,
    }


async def _collect_team_page(page) -> dict[str, Any]:
    text = await _collect_text(page)
    levels: list[str] = []
    years: list[str] = []
    try:
        selects = page.locator("select")
        select_count = await selects.count()
        for index in range(select_count):
            options = await selects.nth(index).locator("option").evaluate_all(
                "els => els.map((opt) => (opt.textContent || '').replace(/\\s+/g, ' ').trim())",
            )
            if not isinstance(options, list):
                continue
            cleaned = [_clean(str(value)) for value in options if _clean(str(value))]
            if not cleaned:
                continue
            if not levels:
                levels = cleaned
            elif not years:
                years = cleaned
    except Exception:
        pass

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": text,
        "levels": _dedupe_keep_order(levels),
        "years": _dedupe_keep_order(years),
    }


async def _collect_coaches_page(page) -> dict[str, Any]:
    text = await _collect_text(page)
    rows = await page.locator("a[href*='/coaching-staff/']").evaluate_all(
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.getAttribute('href') || '',
        }))""",
    )
    coaches: list[dict[str, str]] = []
    if isinstance(rows, list):
        for item in rows:
            if not isinstance(item, dict):
                continue
            coach_text = _clean(str(item.get("text") or ""))
            if coach_text in NAV_EXCLUSIONS:
                continue
            if " coach" not in coach_text.lower():
                continue
            href = _normalize_href(str(item.get("href") or ""), page.url)
            name = _clean(re.sub(r"\b(?:Head|Assistant)\s+Coach\b", "", coach_text, flags=re.I))
            role_match = re.search(r"\b(Head Coach|Assistant Coach)\b", coach_text, flags=re.I)
            role = role_match.group(1) if role_match else "Coach"
            coaches.append(
                {
                    "name": name,
                    "role": role,
                    "profile_url": href,
                }
            )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": text,
        "coaches": coaches,
        "coach_names": [item["name"] for item in coaches if item.get("name")],
    }


async def _collect_schedule_page(page) -> dict[str, Any]:
    text = await _collect_text(page)
    record_text = ""
    schedule_rows: list[dict[str, str]] = []

    try:
        await page.wait_for_selector(
            ".schedule-results li.schedule-and-results-list-item, li.schedule-and-results-list-item",
            timeout=60000,
        )
        await page.wait_for_timeout(1200)
    except Exception:
        pass

    record_locator = page.locator(".schedule-results h3.record, h3.record")
    if await record_locator.count():
        try:
            record_text = _clean(await record_locator.first.inner_text(timeout=10000))
        except Exception:
            record_text = ""

    rows = await page.locator(
        ".schedule-results li.schedule-and-results-list-item, li.schedule-and-results-list-item"
    ).evaluate_all(
        """els => els.map((li) => {
            const links = Array.from(li.querySelectorAll('a[href]')).map((a) => ({
                text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
                href: a.getAttribute('href') || ''
            }));
            return {
                date: (li.querySelector('.date')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                sport: (li.querySelector('.sport')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                vs: (li.querySelector('.vs')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                opponent: (li.querySelector('.school p:first-child')?.innerText || li.querySelector('.school')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                location: (li.querySelector('.location2')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                time: (li.querySelector('.time')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                outcome: (li.querySelector('.outcome')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                result: (li.querySelector('.outcome strong')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                score: (li.querySelector('.outcome .score')?.innerText || '').replace(/\\s+/g, ' ').trim(),
                links,
                raw_text: (li.innerText || '').replace(/\\s+/g, ' ').trim(),
            };
        })""",
    )
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            location_url = ""
            for item in row.get("links") or []:
                if not isinstance(item, dict):
                    continue
                href = _normalize_href(str(item.get("href") or ""), page.url)
                if href:
                    location_url = href
                    break
            schedule_rows.append(
                {
                    "date": _clean(str(row.get("date") or "")),
                    "sport": _clean(str(row.get("sport") or "")),
                    "match_type": _clean(str(row.get("vs") or "")).lower(),
                    "opponent": _clean(str(row.get("opponent") or "")),
                    "location": _clean(str(row.get("location") or "")),
                    "location_url": location_url,
                    "time": _clean(str(row.get("time") or "")),
                    "outcome": _clean(str(row.get("outcome") or "")),
                    "result": _clean(str(row.get("result") or "")),
                    "score": _clean(str(row.get("score") or "")),
                    "raw_text": _clean(str(row.get("raw_text") or "")),
                }
            )

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": text,
        "record_text": record_text,
        "schedule_rows": schedule_rows,
        "available_years": [],
    }


async def _collect_roster_page(page) -> dict[str, Any]:
    text = await _collect_text(page)
    rows = await page.locator("a[href]").evaluate_all(
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.getAttribute('href') || ''
        }))""",
    )
    players: list[dict[str, str]] = []
    seen: set[str] = set()
    if isinstance(rows, list):
        for item in rows:
            if not isinstance(item, dict):
                continue
            label = _clean(str(item.get("text") or ""))
            href = _normalize_href(str(item.get("href") or ""), page.url)
            if not label or not href or label in NAV_EXCLUSIONS:
                continue
            if label.lower() in {value.lower() for value in NAV_EXCLUSIONS}:
                continue
            if not re.search(r"[A-Za-z]", label):
                continue
            if " " not in label and not re.search(r"[A-Z].*[A-Z]", label):
                continue
            if label in seen:
                continue
            if label.lower() in {
                "varsity",
                "junior varsity",
                "frosh/soph",
                "freshman",
                "football",
            }:
                continue
            seen.add(label)
            players.append({"name": label, "profile_url": href})

    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": text,
        "players": players,
        "player_names": [item["name"] for item in players],
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Lincoln High football sources and return a deterministic envelope."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)
    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, dict[str, Any]] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url, collector, key in [
                (HOME_URL, _collect_home_page, "home"),
                (ATHLETICS_URL, _collect_home_page, "athletics"),
                (FOOTBALL_TEAM_URL, _collect_team_page, "team"),
                (FOOTBALL_COACHES_URL, _collect_coaches_page, "coaches"),
                (FOOTBALL_SCHEDULE_URL, _collect_schedule_page, "schedule"),
                (FOOTBALL_ROSTER_URL, _collect_roster_page, "roster"),
            ]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    page_data[key] = await collector(page)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{key}:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    coaches = page_data.get("coaches", {}).get("coaches", []) or []
    coach_names = _dedupe_keep_order(
        [str(item.get("name") or "") for item in coaches if isinstance(item, dict)]
    )
    roster_players = page_data.get("roster", {}).get("players", []) or []
    schedule_rows = page_data.get("schedule", {}).get("schedule_rows", []) or []
    team_levels = page_data.get("team", {}).get("levels", []) or []
    team_years = page_data.get("team", {}).get("years", []) or []

    football_program_available = bool(coach_names or roster_players or schedule_rows)
    if not football_program_available:
        return {
            "nces_id": NCES_ID,
            "school_name": SCHOOL_NAME,
            "state": STATE,
            "source_pages": source_pages,
            "extracted_items": {},
            "scrape_meta": {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "proxy_profile": proxy_meta.get("proxy_profile"),
                "proxy_servers": proxy_meta.get("proxy_servers"),
                "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
                "target_urls": TARGET_URLS,
                "pages_checked": len(page_data),
            },
            "errors": errors + ["no_public_football_content_found"],
        }

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_home_url": FOOTBALL_TEAM_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_URL,
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "home_page": page_data.get("home", {}),
        "athletics_page": page_data.get("athletics", {}),
        "team_page": page_data.get("team", {}),
        "head_coaches": coaches,
        "coach_names": coach_names,
        "football_schedule_record": page_data.get("schedule", {}).get("record_text", ""),
        "football_schedule_rows": schedule_rows,
        "football_schedule_count": len(schedule_rows),
        "football_roster_players": roster_players,
        "football_roster_count": len(roster_players),
        "available_levels": team_levels,
        "available_years": team_years,
        "summary": (
            "Lincoln High has a public Home Campus football portal with a head coach page, public schedule results, and a public roster."
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
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers"),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_data),
            "manual_navigation_steps": [
                "home",
                "athletics",
                "football_team",
                "football_coaches",
                "football_schedule",
                "football_roster",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


async def main() -> None:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
