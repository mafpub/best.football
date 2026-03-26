"""Deterministic football scraper for Cordova High (CA)."""

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

NCES_ID = "061389001568"
SCHOOL_NAME = "Cordova High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://chs.fcusd.org/"
ATHLETICS_URL = "https://chs.fcusd.org/athletics"
SPORTS_URL = "https://chs.fcusd.org/athletics/sports"
FOOTBALL_URL = "https://chs.fcusd.org/athletics/football"
SCHEDULE_URL = "https://chs.fcusd.org/fs/pages/24877"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, SPORTS_URL, FOOTBALL_URL]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _collect_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines


def _find_line_index(lines: list[str], needle: str) -> int:
    needle_norm = _normalize(needle)
    for index, line in enumerate(lines):
        if needle_norm in _normalize(line):
            return index
    return -1


def _collect_section_lines(
    lines: list[str],
    *,
    start_marker: str,
    stop_markers: tuple[str, ...],
) -> list[str]:
    start_index = _find_line_index(lines, start_marker)
    if start_index < 0:
        return []

    stop_norms = tuple(_normalize(item) for item in stop_markers)
    captured: list[str] = []
    for line in lines[start_index + 1 :]:
        normalized = _normalize(line)
        if normalized and any(marker in normalized for marker in stop_norms):
            break
        captured.append(line)
    return _dedupe_keep_order(captured)


def _collect_contact_links(links: list[dict[str, Any]]) -> dict[str, str]:
    out = {
        "coach_email": "",
        "school_phone": "",
        "schedule_link": "",
    }
    for item in links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        text = _clean(str(item.get("text") or ""))
        combo = f"{text} {href}".lower()
        if href.startswith("mailto:") and "jdolliver@" in href:
            out["coach_email"] = href.removeprefix("mailto:")
        elif href.startswith("tel:") and not out["school_phone"]:
            out["school_phone"] = href.removeprefix("tel:")
        elif "calendar" in combo and not out["schedule_link"]:
            out["schedule_link"] = href
    return out


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.href || a.getAttribute('href') || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": body_text,
        "lines": _collect_lines(body_text),
        "links": links,
    }


def _extract_football_details(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    lines = [str(item) for item in signal.get("lines", []) if isinstance(item, str)]
    links = [item for item in signal.get("links", []) if isinstance(item, dict)]

    contact_links = _collect_contact_links(links)

    coach_name = ""
    coach_title = ""
    coach_email = contact_links["coach_email"]
    coach_block = re.search(
        r"Meet the Coach\s+(.+?)\s+Head\s+Var\s*ity\s+Coach",
        body_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if coach_block:
        coach_name = _clean(coach_block.group(1))
        coach_title = "Head Varsity Coach"
    else:
        coach_index = _find_line_index(lines, "Meet the Coach")
        if coach_index >= 0:
            for line in lines[coach_index + 1 : coach_index + 5]:
                if not coach_name and line:
                    coach_name = line
                    continue
                if not coach_title and "coach" in _normalize(line):
                    coach_title = line
                    continue
                if not coach_email and "@" in line:
                    email_match = re.search(r"[\w.\-+]+@[\w.\-]+\.\w+", line)
                    if email_match:
                        coach_email = email_match.group(0)
                        break

    practice_location = ""
    for line in lines:
        if _normalize("Weight Room") == _normalize(line):
            practice_location = line
            break

    practice_start_index = next(
        (index for index, line in enumerate(lines) if _normalize(line) == _normalize("Practices")),
        -1,
    )
    practice_section_lines: list[str] = []
    if practice_start_index >= 0:
        for line in lines[practice_start_index + 1 :]:
            normalized = _normalize(line)
            if normalized and any(
                marker in normalized
                for marker in (
                    _normalize("Calendar"),
                    _normalize("Sport Physical Form"),
                    _normalize("Athletic Clearance"),
                )
            ):
                break
            practice_section_lines.append(line)

    summer_preseason = {
        "dates": "",
        "varsity": "",
        "jv": "",
    }
    preseason_match = re.search(
        r"Football Summer Pre Season Practice:\s*([A-Za-z0-9 \-]+)\s*Var\s*ity\s*([0-9:APM \-]+)\s*JV\s*([0-9:APM \-]+)",
        body_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if preseason_match:
        summer_preseason = {
            "dates": _clean(preseason_match.group(1)),
            "varsity": _clean(preseason_match.group(2)),
            "jv": _clean(preseason_match.group(3)),
        }
    else:
        for index, line in enumerate(practice_section_lines):
            if "summerpreseasonpractice" not in _normalize(line):
                continue
            dates = practice_section_lines[index + 1] if index + 1 < len(practice_section_lines) else ""
            varsity = practice_section_lines[index + 2] if index + 2 < len(practice_section_lines) else ""
            jv = practice_section_lines[index + 3] if index + 3 < len(practice_section_lines) else ""
            if dates:
                summer_preseason["dates"] = _clean(dates)
            if varsity:
                varsity_match = re.search(r"([0-9]{1,2}:[0-9]{2}\s*[AP]M\s*-\s*[0-9]{1,2}:[0-9]{2}\s*[AP]M)", varsity, flags=re.IGNORECASE)
                summer_preseason["varsity"] = _clean(varsity_match.group(1)) if varsity_match else _clean(varsity)
            if jv:
                jv_match = re.search(r"([0-9]{1,2}:[0-9]{2}\s*[AP]M\s*-\s*[0-9]{1,2}:[0-9]{2}\s*[AP]M)", jv, flags=re.IGNORECASE)
                summer_preseason["jv"] = _clean(jv_match.group(1)) if jv_match else _clean(jv)
            break

    season_practice_notes = ""
    season_match = re.search(
        r"Football Season Practice:\s*(.+?)(?:Calendar|Sport Physical Form|Athletic Clearance|$)",
        body_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if season_match:
        candidate = _clean(season_match.group(1))
        if _normalize(candidate) not in {"calendar", "calender"}:
            season_practice_notes = candidate
    elif practice_section_lines:
        season_index = _find_line_index(practice_section_lines, "Football Season Practice")
        if season_index >= 0 and season_index + 1 < len(practice_section_lines):
            candidate = _clean(practice_section_lines[season_index + 1])
            if _normalize(candidate) not in {"calendar", "calender"}:
                season_practice_notes = candidate

    football_page_links = []
    for item in links:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        combo = f"{text} {href}".lower()
        if any(keyword in combo for keyword in ("football", "calendar", "coach", "athletic", "email")):
            football_page_links.append(f"{text}|{href}")

    football_page_links = _dedupe_keep_order(football_page_links)
    footbal_contact = {
        "coach_name": coach_name,
        "coach_title": coach_title,
        "coach_email": coach_email,
        "school_phone": contact_links["school_phone"],
        "practice_location": practice_location,
        "schedule_link": contact_links["schedule_link"] or SCHEDULE_URL,
    }

    return {
        "football_page_title": _clean(str(signal.get("title") or "")),
        "football_page_url": str(signal.get("final_url") or ""),
        "football_contact": footbal_contact,
        "summer_preseason_practice": summer_preseason,
        "season_practice_notes": season_practice_notes,
        "football_page_links": football_page_links,
    }


def _extract_sports_details(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    sports = [
        "Football",
        "Girls Flag Football",
        "Girls Golf",
        "Girls Tennis",
        "Girls Volleyball",
        "Cross Country",
        "Boys Basketball",
        "Boys Soccer",
        "Girls Basketball",
        "Girls Soccer",
        "Wrestling",
        "Baseball",
        "Boys Golf",
        "Boys Tennis",
        "Boys Volleyball",
        "Softball",
        "Swim-Dive",
        "Spirit Cheer (Stunt)",
        "Track & Field",
    ]
    offered = [sport for sport in sports if sport.lower() in body_text.lower()]
    return {
        "sports_page_title": _clean(str(signal.get("title") or "")),
        "sports_page_url": str(signal.get("final_url") or ""),
        "fall_sports_offered": offered,
    }


def _extract_home_details(signal: dict[str, Any]) -> dict[str, Any]:
    body_text = str(signal.get("body_text") or "")
    mascot_match = re.search(r"Home of the\s+([A-Za-z]+)", body_text, flags=re.IGNORECASE)
    return {
        "home_page_title": _clean(str(signal.get("title") or "")),
        "home_page_url": str(signal.get("final_url") or ""),
        "mascot": _clean(mascot_match.group(1)) if mascot_match else "Lancer",
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

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
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    source_pages.append(page.url)
                    page_signals.append(await _collect_page(page, url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_signal = next((signal for signal in page_signals if str(signal.get("final_url") or "").startswith(HOME_URL)), {})
    sports_signal = next(
        (signal for signal in page_signals if "/athletics/sports" in str(signal.get("final_url") or "")),
        {},
    )
    football_signal = next(
        (signal for signal in page_signals if "/athletics/football" in str(signal.get("final_url") or "")),
        {},
    )

    football_details = _extract_football_details(football_signal) if football_signal else {}
    sports_details = _extract_sports_details(sports_signal) if sports_signal else {}
    home_details = _extract_home_details(home_signal) if home_signal else {}

    athletics_links: list[str] = []
    football_page_links: list[str] = []
    for signal in page_signals:
        links = [item for item in signal.get("links", []) if isinstance(item, dict)]
        for item in links:
            text = _clean(str(item.get("text") or ""))
            href = _clean(str(item.get("href") or ""))
            combo = f"{text} {href}".lower()
            if any(keyword in combo for keyword in ("athletic", "football", "schedule", "calendar", "coach")):
                athletics_links.append(f"{text}|{href}")
            if signal is football_signal and any(keyword in combo for keyword in ("football", "calendar", "coach", "email", "athletic")):
                football_page_links.append(f"{text}|{href}")

    athletics_links = _dedupe_keep_order(athletics_links)
    football_page_links = _dedupe_keep_order(football_page_links)

    football_program_available = bool(
        football_details
        and (football_details.get("football_contact") or football_details.get("football_page_links"))
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_identity": {
            "school_name": SCHOOL_NAME,
            "mascot": home_details.get("mascot", "Lancer"),
            "home_title": home_details.get("home_page_title", ""),
            "athletics_title": _clean(str(next((signal.get("title") for signal in page_signals if "/athletics" in str(signal.get("final_url") or "")), ""))),
        },
        "football_page": football_details,
        "sports_page": sports_details,
        "athletics_links": athletics_links,
        "football_page_links": football_page_links,
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
            "proxy": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "pages_checked": len(page_signals),
            "manual_navigation_steps": [
                "school_homepage",
                "athletics_hub",
                "sports_offerings",
                "football_page",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()
