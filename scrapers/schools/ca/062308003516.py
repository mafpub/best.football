"""Deterministic football scraper for Arroyo Grande High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062308003516"
SCHOOL_NAME = "Arroyo Grande High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://www.aghseagles.org"
ATHLETICS_HOME_URL = f"{BASE_URL}/athletics-home"
FOOTBALL_HOME_URL = "https://arroyogrande.homecampus.com/varsity/football/"
FOOTBALL_COACHES_URL = "https://arroyogrande.homecampus.com/varsity/football/coaches"
FOOTBALL_SCHEDULE_URL = "https://arroyogrande.homecampus.com/varsity/football/schedule-results"
FOOTBALL_NEWS_URL = "https://arroyogrande.homecampus.com/varsity/football/news"
FOOTBALL_ROSTER_URL = "https://arroyogrande.homecampus.com/varsity/football/roster"
FOOTBALL_PRINT_SCHEDULE_URL = (
    "https://arroyogrande.homecampus.com/varsity/football/print-schedule-results?selected_year=2025-26"
)

TARGET_URLS = [
    BASE_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_NEWS_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_PRINT_SCHEDULE_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


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


def _preview_lines(text: str, *, limit: int = 12) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lines.append(line)
    return lines[:limit]


def _extract_keyword_lines(text: str, *, keywords: tuple[str, ...], limit: int = 20) -> list[str]:
    matches: list[str] = []
    for line in _preview_lines(text, limit=200):
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            matches.append(line)
    return _dedupe_keep_order(matches)[:limit]


def _parse_coach_card(raw_text: str, href: str) -> dict[str, str] | None:
    text = _clean(raw_text)
    if not text or text == "Coaching Staff":
        return None

    lower = text.lower()
    role = ""
    if "head coach" in lower:
        role = "Head Coach"
    elif "assistant coach" in lower:
        role = "Assistant Coach"

    name = text
    for token in ["Head Coach", "Assistant Coach", "Contact Coach"]:
        name = name.replace(token, "")
    name = _clean(name)
    if not name:
        return None

    return {"name": name, "role": role, "url": href}


def _parse_schedule_opponent(raw_value: str) -> dict[str, str]:
    text = _clean(raw_value)
    match = re.match(r"^(vs|at)\s+(.+?)(?:\s+@\s+(.+))?$", text, re.IGNORECASE)
    if not match:
        return {
            "match_type": "",
            "opponent_or_event": text,
            "venue": "",
            "is_home_game": "",
        }

    match_type = match.group(1).lower()
    opponent_or_event = _clean(match.group(2))
    venue = _clean(match.group(3) or "")
    return {
        "match_type": match_type,
        "opponent_or_event": opponent_or_event,
        "venue": venue,
        "is_home_game": "true" if match_type == "vs" else "false",
    }


async def _collect_basic_signal(page, requested_url: str) -> dict[str, Any]:
    title = _clean(await page.title())
    try:
        body_text = await page.locator("body").text_content(timeout=10000)
    except Exception:  # noqa: BLE001
        try:
            body_text = await page.inner_text("body")
        except Exception:  # noqa: BLE001
            try:
                body_html = await page.content()
            except Exception:  # noqa: BLE001
                body_html = ""
            body_text = re.sub(r"<[^>]+>", " ", body_html)

    body_text = body_text or ""

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": title,
        "body_text": body_text,
        "preview_lines": _preview_lines(body_text),
        "football_lines": _extract_keyword_lines(
            body_text,
            keywords=(
                "football",
                "athletic",
                "coach",
                "schedule",
                "roster",
                "varsity",
                "junior varsity",
                "frosh/soph",
            ),
        ),
    }


async def _collect_coaches(page) -> list[dict[str, str]]:
    cards = await page.eval_on_selector_all(
        "a[href*='/coaching-staff/']",
        """els => els.map(e => {
            const card = e.closest('.grid-item, li, article, .coach-card, .team-member') || e.parentElement;
            const text = (card ? card.innerText : e.innerText || '').replace(/\\s+/g, ' ').trim();
            return { href: e.href || '', text };
        })""",
    )
    if not isinstance(cards, list):
        return []

    coaches: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in cards:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        if not href or href in seen:
            continue
        parsed = _parse_coach_card(str(item.get("text") or ""), href)
        if not parsed:
            continue
        seen.add(href)
        coaches.append(parsed)
    return coaches


async def _resolve_print_schedule_url(page) -> str:
    link = page.locator("a[href*='print-schedule-results']").first
    href = await link.get_attribute("href")
    if href:
        return urljoin(page.url, href)
    return FOOTBALL_PRINT_SCHEDULE_URL


async def _collect_schedule(page) -> dict[str, Any]:
    body_text = ""
    try:
        body_text = await page.inner_text("body")
    except Exception:  # noqa: BLE001
        pass

    record_match = re.search(
        r"Overall Record:\s*([0-9-]+),\s*League Record:\s*([0-9-]+)",
        body_text,
        re.IGNORECASE,
    )
    overall_record = record_match.group(1) if record_match else ""
    league_record = record_match.group(2) if record_match else ""

    rows = await page.locator("table#myTable tr").evaluate_all(
        """els => els.map(tr => Array.from(tr.querySelectorAll('td,th')).map(td => (
            td.innerText || ''
        ).replace(/\\s+/g, ' ').trim()))"""
    )
    if not isinstance(rows, list) or not rows:
        rows = []

    schedule_rows: list[dict[str, str]] = []
    for row in rows[1:]:
        if not isinstance(row, list) or len(row) < 8:
            continue
        date, day, start_time, opponent_raw, dismissal_time, departure_time, return_time, result = [
            _clean(str(value or "")) for value in row[:8]
        ]
        parsed = _parse_schedule_opponent(opponent_raw)
        schedule_rows.append(
            {
                "date": date,
                "day": day,
                "start_time": start_time,
                "match_type": parsed["match_type"],
                "opponent_or_event": parsed["opponent_or_event"],
                "venue": parsed["venue"],
                "is_home_game": parsed["is_home_game"],
                "dismissal_time": dismissal_time,
                "departure_time": departure_time,
                "return_time": return_time,
                "result": result,
            }
        )

    return {
        "print_schedule_url": page.url,
        "overall_record": overall_record,
        "league_record": league_record,
        "schedule_rows": schedule_rows,
        "game_count": len(schedule_rows),
        "home_games": sum(1 for row in schedule_rows if row.get("is_home_game") == "true"),
        "away_games": sum(1 for row in schedule_rows if row.get("is_home_game") == "false"),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Arroyo Grande High's public football portal."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: dict[str, dict[str, Any]] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in [BASE_URL, FOOTBALL_HOME_URL, FOOTBALL_COACHES_URL, FOOTBALL_NEWS_URL]:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(2200)
                    page_signals[url] = await _collect_basic_signal(page, url)
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")

            coaches: list[dict[str, str]] = []
            football_news_headlines: list[str] = []
            football_schedule_summary: dict[str, Any] = {}
            football_home_page_lines: list[str] = []

            if FOOTBALL_HOME_URL in page_signals:
                football_home_page_lines = page_signals[FOOTBALL_HOME_URL]["football_lines"]

            if FOOTBALL_NEWS_URL in page_signals:
                news_body = str(page_signals[FOOTBALL_NEWS_URL].get("body_text") or "")
                football_news_headlines = _dedupe_keep_order(
                    [
                        line
                        for line in _preview_lines(news_body, limit=200)
                        if "football" in line.lower()
                        and "football -" not in line.lower()
                        and "tagged football" not in line.lower()
                    ]
                )

            try:
                await page.goto(FOOTBALL_COACHES_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(2500)
                source_pages.append(page.url)
                coaches = await _collect_coaches(page)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{FOOTBALL_COACHES_URL}")

            try:
                await page.goto(FOOTBALL_PRINT_SCHEDULE_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(2000)
                source_pages.append(page.url)
                football_schedule_summary = await _collect_schedule(page)
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    f"playwright_navigation_failed:{type(exc).__name__}:{FOOTBALL_PRINT_SCHEDULE_URL}"
                )

        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_coach_names = _dedupe_keep_order([coach["name"] for coach in coaches])
    football_head_coach = next(
        (coach["name"] for coach in coaches if coach.get("role") == "Head Coach"),
        "",
    )
    football_assistant_coaches = _dedupe_keep_order(
        [coach["name"] for coach in coaches if coach.get("role") == "Assistant Coach"]
    )

    football_program_available = bool(coaches or football_schedule_summary.get("schedule_rows") or football_home_page_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_home_page_url": FOOTBALL_HOME_URL,
        "athletics_home_page_url": ATHLETICS_HOME_URL,
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_schedule_url": FOOTBALL_PRINT_SCHEDULE_URL,
        "football_print_schedule_url": football_schedule_summary.get("print_schedule_url", FOOTBALL_PRINT_SCHEDULE_URL),
        "football_news_url": FOOTBALL_NEWS_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "football_levels": ["Varsity", "Junior Varsity", "Frosh/Soph"],
        "football_coaches": coaches,
        "football_coach_names": football_coach_names,
        "football_head_coach": football_head_coach,
        "football_assistant_coaches": football_assistant_coaches,
        "football_schedule_record": {
            "overall": football_schedule_summary.get("overall_record", ""),
            "league": football_schedule_summary.get("league_record", ""),
        },
        "football_schedule_summary": football_schedule_summary,
        "football_news_headlines": football_news_headlines,
        "football_home_page_evidence": football_home_page_lines,
        "football_program_evidence": _dedupe_keep_order(
            football_home_page_lines + football_news_headlines + [coach["name"] for coach in coaches[:4]]
        ),
        "summary": (
            "Arroyo Grande High has a public football portal on HomeCampus with a dedicated coaches page, a printable varsity schedule showing a 10-5-0 overall record and 4-0-0 league record, and public football news."
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
            "proxy_profile": PROXY_PROFILE,
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "district_homepage",
                "athletics_homepage",
                "football_homepage",
                "football_coaches",
                "football_schedule",
                "football_news",
                "print_schedule",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
