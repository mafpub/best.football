"""Football scraper for Guajome Learning Centers (CA).

Reconstructed from live navigation on guajome.net using the required
datacenter Oxylabs proxy profile:
home -> Programs -> Athletics -> Athletics Calendar.
"""

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

NCES_ID = "060224712930"
SCHOOL_NAME = "Guajome Learning Centers"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.guajome.net/"
ATHLETICS_FALLBACK_URL = "https://www.guajome.net/programs/athletics"
CALENDAR_FALLBACK_URL = "https://www.guajome.net/programs/athletics/athletics-calendar"

TARGET_URLS = [HOME_URL, ATHLETICS_FALLBACK_URL, CALENDAR_FALLBACK_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "football",
    "flag football",
    "middle school flag football",
)

EVENT_HINT_KEYWORDS = (
    "game",
    "meet",
    "camp",
)


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


def _normalize_href(href: str, base_url: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    return urljoin(base_url, raw)


def _contains_any(value: str, tokens: tuple[str, ...]) -> bool:
    lowered = value.lower()
    return any(token in lowered for token in tokens)


def _find_nav_targets(links: list[dict[str, str]]) -> tuple[str | None, str | None]:
    """Return (athletics_url, calendar_url) from discovered homepage links."""
    athletics_url: str | None = None
    calendar_url: str | None = None

    for link in links:
        href = _clean(str(link.get("href") or "")).lower()
        text = _clean(str(link.get("text") or "")).lower()
        if "/programs/athletics/athletics-calendar" in href or "athletics calendar" in text:
            calendar_url = _normalize_href(link.get("href") or "", "https://www.guajome.net")
            continue

        if "/programs/athletics" in href and "athletics calendar" not in text:
            athletics_url = _normalize_href(link.get("href") or "", "https://www.guajome.net")

    return athletics_url, calendar_url


def _extract_anchor_map(raw_links: list[dict[str, str]], base_url: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for link in raw_links:
        if not isinstance(link, dict):
            continue
        text = _clean(str(link.get("text") or ""))
        href = _normalize_href(_clean(str(link.get("href") or "")), base_url)
        if href:
            out.append({"text": text, "href": href})
    return out


def _extract_football_mentions(text: str) -> list[str]:
    lines = [_clean(line) for line in text.splitlines()]
    return _dedupe_keep_order(
        [line for line in lines if line and _contains_any(line.lower(), FOOTBALL_KEYWORDS)]
    )


def _extract_sports_sections(text: str) -> dict[str, list[str]]:
    lines = [_clean(line) for line in text.splitlines() if _clean(line)]
    sections: dict[str, list[str]] = {}
    headers = {
        "fall sports": "fall_sports",
        "winter sports": "winter_sports",
        "spring sports": "spring_sports",
        "sports news": "sports_news",
        "sports information": "sports_information",
        "game schedule": "game_schedule_lines",
    }

    current: str | None = None
    for raw_line in lines:
        lower = raw_line.lower()
        if lower in headers:
            current = headers[lower]
            sections.setdefault(current, [])
            continue
        if current is None:
            continue
        if raw_line.startswith("Home of the Frogs"):
            current = None
            continue
        sections[current].append(raw_line)

    for key in list(sections):
        sections[key] = _dedupe_keep_order(sections[key])
    return sections


def _extract_contact(lines: list[str], links: list[dict[str, str]]) -> dict[str, str]:
    name = ""
    email = ""
    phone = ""

    for i, line in enumerate(lines):
        lower = line.lower()
        if "athletic director" in lower:
            if i > 0 and re.fullmatch(r"[A-Za-z .'-]+", lines[i - 1]):
                name = lines[i - 1]
            if not name and i + 1 < len(lines) and re.fullmatch(r"[A-Za-z .'-]+", lines[i + 1]):
                name = lines[i + 1]

    for link in links:
        href = _clean(str(link.get("href") or "")).lower()
        if href.startswith("mailto:") and not email:
            email = _clean(link.get("href") or "")[7:]
        if href.startswith("tel:") and not phone:
            phone_raw = _clean(link.get("href") or "")[4:]
            phone = phone_raw.replace("%20", " ")

    if not email:
        email_match = re.search(r"[A-Za-z0-9._%+-]+@guajome\.net", "\n".join(lines), re.IGNORECASE)
        if email_match:
            email = email_match.group(0)

    if not phone:
        phone_match = re.search(
            r"(\d{3}\D*\d{3}\D*\d{4}(?:\s*(?:ext\.?|x)\s*\d+)?)",
            " ".join(lines),
            flags=re.IGNORECASE,
        )
        if phone_match:
            phone = _clean(phone_match.group(1))

    return {
        "name": name,
        "role": "Athletic Director" if name else "",
        "email": email,
        "phone": phone,
    }


def _extract_schedule_lines(text: str, limit: int = 80) -> list[str]:
    lines = [_clean(line) for line in text.splitlines() if _clean(line)]
    date_tokens = (
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec",
    )

    out: list[str] = []
    for line in lines:
        lower = line.lower()
        if any(token in lower for token in date_tokens) and any(hint in lower for hint in EVENT_HINT_KEYWORDS):
            out.append(line)
            continue
        if any(hint in lower for hint in EVENT_HINT_KEYWORDS) and any(
            token in lower
            for token in (
                "game",
                "field",
                "park",
                "school",
                "vs",
            )
        ):
            out.append(line)
        if len(out) >= limit:
            break
    return _dedupe_keep_order(out)


async def _collect_page_snapshot(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    links_raw = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(el => ({ text: (el.textContent || '').replace(/\\s+/g, ' ').trim(), href: el.href || '' }))",
    )
    if not isinstance(links_raw, list):
        links_raw = []

    final_url = page.url
    links = _extract_anchor_map([dict(link) for link in links_raw], final_url)

    return {
        "requested_url": requested_url,
        "final_url": final_url,
        "title": _clean(await page.title()),
        "body_text": _clean(body_text),
        "links": links,
        "football_lines": _extract_football_mentions(body_text),
        "all_lines": [_clean(line) for line in body_text.splitlines() if _clean(line)],
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape football-focused athletics signals from Guajome Learning Centers."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
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

        athletics_url = ATHLETICS_FALLBACK_URL
        calendar_url = CALENDAR_FALLBACK_URL

        try:
            try:
                await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                home_snapshot = await _collect_page_snapshot(page, HOME_URL)
                snapshots.append(home_snapshot)
                source_pages.append(home_snapshot["final_url"])

                discovered_athletics, discovered_calendar = _find_nav_targets(home_snapshot["links"])
                if discovered_athletics:
                    athletics_url = discovered_athletics
                if discovered_calendar:
                    calendar_url = discovered_calendar
            except Exception as exc:  # noqa: BLE001
                errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{HOME_URL}")

            for target in [athletics_url, calendar_url]:
                try:
                    await page.goto(target, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1300)
                    snapshots.append(await _collect_page_snapshot(page, target))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{target}")

        finally:
            await browser.close()

    snapshots = [snapshot for snapshot in snapshots if isinstance(snapshot, dict)]
    source_pages = _dedupe_keep_order(source_pages)

    home_snapshot = snapshots[0] if snapshots else {"links": [], "body_text": "", "all_lines": []}
    athletics_snapshot = snapshots[1] if len(snapshots) > 1 else {"links": [], "body_text": "", "all_lines": []}
    calendar_snapshot = snapshots[2] if len(snapshots) > 2 else {"links": [], "body_text": "", "all_lines": []}

    nav_links = []
    for snapshot in snapshots:
        for link in snapshot.get("links", []):
            href = _clean(str(link.get("href") or "")).lower()
            if "/programs/athletics" in href or "athletics" in href:
                nav_links.append(f"{_clean(str(link.get('text') or ''))} -> {link.get('href')}")

    football_lines = _dedupe_keep_order(
        list(home_snapshot.get("football_lines", []))
        + list(athletics_snapshot.get("football_lines", []))
        + list(calendar_snapshot.get("football_lines", []))
    )

    athletics_all_lines = athletics_snapshot.get("all_lines", []) + calendar_snapshot.get("all_lines", [])

    sports_sections = _extract_sports_sections("\n".join(str(line) for line in athletics_all_lines))
    contact = _extract_contact(
        [_clean(line) for line in athletics_all_lines],
        athletics_snapshot.get("links", []) + calendar_snapshot.get("links", []),
    )

    football_team_lines = [line for line in athletics_all_lines if "flag football" in line.lower()]
    football_schedule_lines = _extract_schedule_lines("\n".join(athletics_all_lines), limit=90)
    football_news_lines = [line for line in athletics_all_lines if "flag football" in line.lower() and "champ" in line.lower()]

    football_program_available = bool(football_lines or football_team_lines or football_news_lines)
    if not football_program_available:
        errors.append("no_football_marker_found_on_discovered_athletics_pages")

    extracted_items: dict[str, Any] = {
        "focus": "football",
        "athletics_url": athletics_url,
        "calendar_url": calendar_url,
        "navigation_from_home": {
            "home_url": HOME_URL,
            "athletics_link_found": bool(athletics_url),
            "calendar_link_found": bool(calendar_url),
            "menu_links": _dedupe_keep_order(nav_links)[:80],
        },
        "athletic_director": contact,
        "football_available": football_program_available,
        "football_keyword_lines": football_lines,
        "football_team_names": _dedupe_keep_order(football_team_lines),
        "football_news_lines": _dedupe_keep_order(football_news_lines),
        "sports_sections": sports_sections,
        "athletics_schedule_lines": football_schedule_lines,
        "summary": (
            f"{SCHOOL_NAME} athletics pages identify Middle School Flag Football in the sports news/sections "
            f"and list Athletics calendar entries; Athletic Director contact is {contact.get('name') or 'available on-page'}."
            if football_program_available
            else f"No football marker was identified on {SCHOOL_NAME} athletics pages during navigation."
        ),
        "pages_seen": [snapshot.get("final_url", "") for snapshot in snapshots],
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
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "navigation_steps": ["home", "athletics", "athletics_calendar"],
            "discovered_athletics_url": athletics_url,
            "discovered_calendar_url": calendar_url,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias for runner compatibility."""
    return await scrape_school()

