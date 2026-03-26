"""Deterministic football scraper for California Military Institute (CA)."""

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

NCES_ID = "063021011184"
SCHOOL_NAME = "California Military Institute"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.cmicharter.org"
ATHLETICS_URL = f"{HOME_URL}/athletics"
FOOTBALL_URL = f"{HOME_URL}/football"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, FOOTBALL_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


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


def _extract_lines(text: str, *, keywords: tuple[str, ...] | None = None) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        if keywords and not any(keyword in line.lower() for keyword in keywords):
            continue
        lines.append(line)
    return _dedupe_keep_order(lines)


def _normalize_href(href: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return value


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    body_text = _clean(await page.inner_text("body"))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || e.getAttribute('href') || ''
        }))""",
    )
    normalized_links: list[dict[str, str]] = []
    if isinstance(links, list):
        for raw_link in links:
            if not isinstance(raw_link, dict):
                continue
            href = _normalize_href(str(raw_link.get("href") or ""))
            if not href:
                continue
            normalized_links.append(
                {
                    "text": _clean(str(raw_link.get("text") or "")),
                    "href": href,
                }
            )

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "links": normalized_links,
        "football_lines": _extract_lines(
            body_text,
            keywords=("football", "coach", "schedule", "varsity", "roughrider"),
        ),
    }


async def _click_football_link(page) -> bool:
    try:
        locator = page.locator("a[href$='/football']").first
        if await locator.count() == 0:
            return False
        await locator.click(timeout=15000)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(1200)
        return True
    except Exception:  # noqa: BLE001
        return False


async def _parse_schedule_rows(page) -> list[dict[str, str]]:
    rows = await page.locator("section#fsEl_21375 table table tr").evaluate_all(
        """els => els.map(tr => Array.from(tr.querySelectorAll('td,th')).map(td => (
            td.innerText || ''
        ).replace(/\\s+/g, ' ').trim()))"""
    )
    if not isinstance(rows, list):
        return []

    schedule_rows: list[dict[str, str]] = []
    for row in rows[1:]:
        if not isinstance(row, list) or len(row) < 5:
            continue
        date, day, time, opponent, location = [_clean(str(value or "")) for value in row[:5]]
        schedule_rows.append(
            {
                "date": date,
                "day": day,
                "time": time,
                "opponent": opponent,
                "location": location,
                "is_bye": "true" if date.upper() == "BYE" else "false",
            }
        )
    return schedule_rows


async def _parse_coaches(page) -> list[dict[str, str]]:
    cards = await page.locator("section#fsEl_21403 p").evaluate_all(
        """els => els.map(p => ({
            text: (p.innerText || '').replace(/\\s+/g, ' ').trim(),
            html: p.innerHTML || ''
        }))"""
    )
    if not isinstance(cards, list):
        return []

    coaches: list[dict[str, str]] = []
    for card in cards:
        if not isinstance(card, dict):
            continue
        text = _clean(str(card.get("text") or ""))
        if not text:
            continue

        name_match = re.search(r"<strong>([^<]+)</strong>", str(card.get("html") or ""), re.I)
        role_match = re.search(r"<em>([^<]+)</em>", str(card.get("html") or ""), re.I)
        email_match = re.search(r"[\w.\-+]+@[\w.\-]+\.\w+", text)
        phone_match = re.search(r"\(\d{3}\)\s*\d{3}-\d{4}(?:\s*ext\.?\s*\d+)?", text, re.I)
        room_match = re.search(r"\bRoom\s+([A-Z0-9-]+)\b", text, re.I)
        team_room_match = re.search(r"\bTeamroom\s+([A-Z0-9-]+)\b", text, re.I)

        coaches.append(
            {
                "name": _clean(name_match.group(1)) if name_match else "",
                "role": _clean(role_match.group(1)) if role_match else "",
                "phone": _clean(phone_match.group(0)) if phone_match else "",
                "email": _clean(email_match.group(0)) if email_match else "",
                "room": _clean(room_match.group(1)) if room_match else "",
                "team_room": _clean(team_room_match.group(1)) if team_room_match else "",
                "notes": text,
            }
        )

    return coaches


async def _collect_download_links(page) -> list[dict[str, str]]:
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || e.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        return []

    output: list[dict[str, str]] = []
    for raw_link in links:
        if not isinstance(raw_link, dict):
            continue
        text = _clean(str(raw_link.get("text") or ""))
        href = _normalize_href(str(raw_link.get("href") or ""))
        if not href:
            continue
        if "football schedule" in text.lower() or "resource-manager/view" in href.lower():
            output.append({"text": text, "href": href})
    return output


async def scrape_school() -> dict[str, Any]:
    """Scrape CMI football's public schedule and coach information."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []
    page_signals: list[dict[str, Any]] = []

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
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            home_signal = await _collect_page_signal(page, HOME_URL)
            page_signals.append(home_signal)
            source_pages.append(home_signal["final_url"])
            navigation_steps.append("visit_home")

            if await _click_football_link(page):
                navigation_steps.append("click_home_football_link")
            else:
                await page.goto(FOOTBALL_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                navigation_steps.append("fallback_direct_football_url")

            football_signal = await _collect_page_signal(page, FOOTBALL_URL)
            page_signals.append(football_signal)
            source_pages.append(football_signal["final_url"])
            navigation_steps.append("visit_football_page")

            schedule_rows = await _parse_schedule_rows(page)
            coaches = await _parse_coaches(page)
            download_links = await _collect_download_links(page)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_flow_failed:{type(exc).__name__}")
            schedule_rows = []
            coaches = []
            download_links = []
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    football_lines = _dedupe_keep_order(
        [
            line
            for signal in page_signals
            for line in _extract_lines(str(signal.get("body_text") or ""), keywords=("football",))
        ]
    )
    coach_names = _dedupe_keep_order([coach.get("name", "") for coach in coaches if coach.get("name")])
    football_links = _dedupe_keep_order(
        [
            f"{link.get('text', '')}|{link.get('href', '')}"
            for signal in page_signals
            for link in signal.get("links", [])
            if "football" in str(link.get("text", "")).lower()
            or "football" in str(link.get("href", "")).lower()
        ]
    )

    football_program_available = bool(schedule_rows or coaches or football_links or football_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    head_coach = next((coach for coach in coaches if "head coach" in coach.get("role", "").lower()), {})
    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_page_url": FOOTBALL_URL,
        "athletics_page_url": ATHLETICS_URL,
        "school_home_url": HOME_URL,
        "team_name": "CMI Football",
        "team_mascot": "Roughriders",
        "season_label": "Current Season",
        "football_schedule_rows": schedule_rows,
        "football_coaches": coaches,
        "football_coach_names": coach_names,
        "football_head_coach": head_coach.get("name", ""),
        "football_contact_email": head_coach.get("email", ""),
        "football_contact_phone": head_coach.get("phone", ""),
        "football_contact_room": head_coach.get("room", ""),
        "football_contact_team_room": head_coach.get("team_room", ""),
        "football_schedule_download_links": _dedupe_keep_order(
            [f"{link.get('text', '')}|{link.get('href', '')}" for link in download_links]
        ),
        "football_links": football_links,
        "football_evidence_lines": football_lines[:20],
        "summary": (
            "California Military Institute publicly lists CMI Football with a varsity schedule, a head coach, contact details, and downloadable monthly football schedules."
            if football_program_available
            else ""
        ),
    }

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_steps,
            "target_urls": TARGET_URLS,
            "football_schedule_row_count": len(schedule_rows),
            "football_coach_count": len(coaches),
        }
    )

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
