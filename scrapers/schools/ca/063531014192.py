"""Deterministic athletics scraper for Advanced Learning Academy (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials

NCES_ID = "063531014192"
SCHOOL_NAME = "Advanced Learning Academy"
STATE = "CA"

HOME_URL = "https://ala.sausd.us/"
SPORTS_URL = "https://ala.sausd.us/ala-sports"
REGISTRATION_PACKET_URL = "https://ala.sausd.us/ala-sports/athletic-registration-packet"
SCHEDULE_URL = "https://ala.sausd.us/ala-sports/2526-sports-schedule"
NONDISCRIMINATION_URL = "https://ala.sausd.us/ala-sports/sausd-nondiscrimination-statement"

PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

MANUAL_URL_SEQUENCE = [
    HOME_URL,
    SPORTS_URL,
    REGISTRATION_PACKET_URL,
    SCHEDULE_URL,
    NONDISCRIMINATION_URL,
]

ATHLETICS_KEYWORDS = (
    "athletics",
    "athletic",
    "student-athlete",
    "student-athletes",
    "sport",
    "sports",
    "volleyball",
    "basketball",
    "soccer",
    "track and field",
    "track",
    "pickleball",
    "coach",
    "athletic coordinator",
)

SPORT_NAME_PATTERNS = {
    "volleyball": re.compile(r"\bvolleyball\b", re.IGNORECASE),
    "basketball": re.compile(r"\bbasketball\b", re.IGNORECASE),
    "soccer": re.compile(r"\bsoccer\b", re.IGNORECASE),
    "track and field": re.compile(r"\btrack and field\b", re.IGNORECASE),
    "pickleball": re.compile(r"\bpickleball\b", re.IGNORECASE),
}

EMAIL_PATTERN = re.compile(r"[\w.\-+]+@[\w.\-]+\.\w+")


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _clean_lines(text: str) -> list[str]:
    return [" ".join(line.split()).strip() for line in text.splitlines() if line.strip()]


def _keyword_lines(text: str, *, limit: int = 40) -> list[str]:
    results: list[str] = []
    for line in _clean_lines(text):
        lowered = line.lower()
        if any(keyword in lowered for keyword in ATHLETICS_KEYWORDS):
            results.append(line)
    return _dedupe_keep_order(results)[:limit]


def _extract_sports_offered(text: str) -> list[str]:
    found: list[str] = []
    for sport, pattern in SPORT_NAME_PATTERNS.items():
        if pattern.search(text):
            found.append(sport)
    return found


def _extract_contacts(text: str) -> list[dict[str, str]]:
    lines = _clean_lines(text)
    contacts: list[dict[str, str]] = []

    for idx, line in enumerate(lines):
        if not EMAIL_PATTERN.fullmatch(line):
            continue

        email = line
        name = ""
        role = ""

        for back in range(1, 4):
            prev_idx = idx - back
            if prev_idx < 0:
                break
            candidate = lines[prev_idx]
            if "@" in candidate:
                continue
            if not name and "," in candidate:
                parts = [part.strip() for part in candidate.split(",", 1)]
                name = parts[0]
                if len(parts) > 1:
                    role = parts[1]
                continue
            if not role and any(
                token in candidate.lower() for token in ("coach", "athletic", "coordinator")
            ):
                role = candidate

        contacts.append({"name": name, "role": role, "email": email})

    unique_contacts: list[dict[str, str]] = []
    seen: set[str] = set()
    for contact in contacts:
        key = contact["email"].lower()
        if key in seen:
            continue
        seen.add(key)
        unique_contacts.append(contact)

    return unique_contacts[:10]


async def _capture_page(page) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: e.href || ""
        }))""",
    )

    athletics_links: list[str] = []
    for link in links:
        text = str(link.get("text") or "").strip()
        href = str(link.get("href") or "").strip()
        combo = f"{text} {href}".lower()
        if any(keyword in combo for keyword in ATHLETICS_KEYWORDS):
            athletics_links.append(f"{text}|{href}")

    return {
        "url": page.url,
        "title": await page.title(),
        "body_text": body_text,
        "athletics_keyword_lines": _keyword_lines(body_text),
        "athletics_links": _dedupe_keep_order(athletics_links)[:25],
    }


async def _goto_and_capture(page, url: str, label: str, navigation_log: list[str]) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)
    navigation_log.append(f"visited:{label}")
    return await _capture_page(page)


async def scrape_school() -> dict[str, Any]:
    """Navigate ALA athletics pages and extract public sports program details."""
    require_proxy_credentials()
    assert_not_blocklisted(MANUAL_URL_SEQUENCE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_snapshots: list[dict[str, Any]] = []
    navigation_log: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USERNAME,
                "password": PROXY_PASSWORD,
            },
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 1400},
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            navigation_log.append("visited:home")
            home_snapshot = await _capture_page(page)
            source_pages.append(page.url)
            page_snapshots.append(home_snapshot)

            if any(
                "/ala-sports" in link
                for link in home_snapshot.get("athletics_links", [])
                if isinstance(link, str)
            ):
                navigation_log.append("observed:home_has_ala_sports_link")
            else:
                errors.append("navigation_missing_ala_sports_link")

            sports_snapshot = await _goto_and_capture(
                page,
                SPORTS_URL,
                "ala_sports_direct",
                navigation_log,
            )
            source_pages.append(page.url)
            page_snapshots.append(sports_snapshot)

            packet_snapshot = await _goto_and_capture(
                page,
                REGISTRATION_PACKET_URL,
                "athletic_registration_packet_direct",
                navigation_log,
            )
            source_pages.append(page.url)
            page_snapshots.append(packet_snapshot)

            schedule_snapshot = await _goto_and_capture(
                page,
                SCHEDULE_URL,
                "sports_schedule_direct",
                navigation_log,
            )
            source_pages.append(page.url)
            page_snapshots.append(schedule_snapshot)

            nondiscrimination_snapshot = await _goto_and_capture(
                page,
                NONDISCRIMINATION_URL,
                "sausd_nondiscrimination_statement",
                navigation_log,
            )
            source_pages.append(page.url)
            page_snapshots.append(nondiscrimination_snapshot)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    sports_snapshot = next(
        (snapshot for snapshot in page_snapshots if str(snapshot.get("url") or "") == SPORTS_URL),
        None,
    )
    packet_snapshot = next(
        (
            snapshot
            for snapshot in page_snapshots
            if str(snapshot.get("url") or "") == REGISTRATION_PACKET_URL
        ),
        None,
    )
    schedule_snapshot = next(
        (snapshot for snapshot in page_snapshots if str(snapshot.get("url") or "") == SCHEDULE_URL),
        None,
    )

    sports_body = str((sports_snapshot or {}).get("body_text") or "")
    packet_body = str((packet_snapshot or {}).get("body_text") or "")

    sports_keyword_mentions = []
    for snapshot in page_snapshots:
        sports_keyword_mentions.extend(snapshot.get("athletics_keyword_lines", []))
    sports_keyword_mentions = _dedupe_keep_order(
        [item for item in sports_keyword_mentions if isinstance(item, str)]
    )[:40]

    athletics_links = []
    for snapshot in page_snapshots:
        athletics_links.extend(snapshot.get("athletics_links", []))
    athletics_links = _dedupe_keep_order([item for item in athletics_links if isinstance(item, str)])[
        :25
    ]

    sports_offered = _extract_sports_offered(sports_body)
    athletics_contacts = _extract_contacts(sports_body)
    athletics_page_seen = sports_snapshot is not None
    registration_packet_seen = packet_snapshot is not None
    schedule_page_seen = schedule_snapshot is not None

    athletics_program_available = bool(
        athletics_page_seen and sports_offered and athletics_contacts and registration_packet_seen
    )

    if not athletics_program_available:
        errors.append("blocked:no_public_athletics_program_content_found_on_ala_sports_pages")

    extracted_items: dict[str, Any] = {
        "athletics_program_available": athletics_program_available,
        "athletics_page_url": SPORTS_URL,
        "athletic_registration_packet_url": REGISTRATION_PACKET_URL,
        "sports_schedule_url": SCHEDULE_URL,
        "sports_offered": sports_offered,
        "athletics_contacts": athletics_contacts,
        "athletics_keyword_mentions": sports_keyword_mentions,
        "athletics_navigation_links": athletics_links,
        "registration_packet_mentions": _keyword_lines(packet_body),
        "schedule_page_seen": schedule_page_seen,
        "manual_navigation_log": navigation_log,
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
            "proxy_server": PROXY_SERVER,
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "home",
                "observe_ala_sports_link",
                "ala_sports_direct",
                "athletic_registration_packet_direct",
                "sports_schedule_direct",
                "sausd_nondiscrimination_statement",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
