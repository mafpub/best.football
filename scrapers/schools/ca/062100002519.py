"""Deterministic football scraper for Calabasas High (CA)."""

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

NCES_ID = "062100002519"
SCHOOL_NAME = "Calabasas High"
STATE = "CA"

PROXY_PROFILE = "datacenter"
HOME_URL = "https://www.calabasashigh.net"
ATHLETICS_HOME_URL = "https://www.calabasasathletics.net/"
FALL_SPORTS_URL = "https://www.calabasasathletics.net/blank-7"
TARGET_PAGES = [HOME_URL, ATHLETICS_HOME_URL, FALL_SPORTS_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_SELECTOR = 'div[aria-label="FOOTBALL"]'
FOOTBALL_COACH_EMAIL = "charris@lvusd.org"


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        if isinstance(value, dict):
            marker = repr(sorted(value.items()))
        else:
            marker = _clean(str(value))
        if not marker or marker in seen:
            continue
        seen.add(marker)
        out.append(value)
    return out


def _extract_coach_name(label: str) -> str:
    text = _clean(label)
    match = re.match(r"^(?:Head Coach|Coach)\s*:?\s*(.+)$", text, flags=re.I)
    if match:
        return _clean(match.group(1))
    return text


def _is_heading_line(line: str) -> bool:
    text = _clean(line)
    if not text or len(text) > 60:
        return False
    if not any(ch.isalpha() for ch in text):
        return False
    return text.upper() == text


def _extract_lines(text: str) -> list[str]:
    return [_clean(line) for line in (text or "").splitlines() if _clean(line)]


def _extract_mailto_contacts(links: list[dict[str, str]]) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    for link in links:
        href = _clean(link.get("href", ""))
        if not href.lower().startswith("mailto:"):
            continue
        email = href[len("mailto:") :]
        label = _clean(link.get("text", ""))
        contacts.append(
            {
                "label": label,
                "name": _extract_coach_name(label),
                "email": email,
            }
        )
    return _dedupe_keep_order(contacts)


def _parse_fall_sports_sections(lines: list[str]) -> list[dict[str, str]]:
    try:
        start = lines.index("FALL SPORTS")
    except ValueError:
        return []

    try:
        end = lines.index("YEAR-ROUND", start + 1)
    except ValueError:
        end = len(lines)

    section_lines = lines[start + 1 : end]
    sections: list[dict[str, str]] = []
    current: dict[str, str] | None = None

    for line in section_lines:
        if _is_heading_line(line):
            if current:
                sections.append(current)
            current = {"sport": _clean(line), "coach_line": "", "coach_name": ""}
            continue

        if current is None:
            continue

        if re.match(r"^(?:Head Coach|Coach)\s*:?", line, flags=re.I):
            current["coach_line"] = _clean(line)
            current["coach_name"] = _extract_coach_name(line)

    if current:
        sections.append(current)

    return sections


def _extract_school_contact(lines: list[str]) -> dict[str, str]:
    joined = "\n".join(lines)
    address_match = re.search(
        r"(\d+\s+Mulholland Hwy\.?,?\s+Calabasas,?\s+CA\s+\d{5}(?:-\d{4})?)",
        joined,
        flags=re.I,
    )
    phone_match = re.search(r"(\(?818\)?[-\s.]222[-\s.]7177)", joined)
    return {
        "address": _clean(address_match.group(1)) if address_match else "",
        "phone": _clean(phone_match.group(1)) if phone_match else "",
    }


async def _collect_page(page) -> dict[str, Any]:
    raw_text = await page.locator("body").inner_text()
    body_text = _clean(raw_text)
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map((anchor) => ({"
        "text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),"
        "href: anchor.getAttribute('href') || anchor.href || ''"
        "}))",
    )
    if not isinstance(links, list):
        links = []

    normalized_links: list[dict[str, str]] = []
    for item in links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        normalized_links.append(
            {
                "text": _clean(str(item.get("text") or "")),
                "href": href,
            }
        )

    return {
        "title": _clean(await page.title()),
        "url": page.url,
        "text": body_text,
        "lines": _extract_lines(raw_text),
        "links": normalized_links,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football evidence from Calabasas High athletics pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_PAGES, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_log: list[str] = []
    page_snapshots: list[dict[str, Any]] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=proxy,
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
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            navigation_log.append("visit_school_home")
            page_snapshots.append(await _collect_page(page))

            athletics_link = page.locator("a[href='https://www.calabasashigh.net/athletics']").first
            if await athletics_link.count():
                try:
                    await athletics_link.click(timeout=7000)
                    await page.wait_for_timeout(1200)
                    navigation_log.append("click_school_athletics_link")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"school_athletics_click_failed:{type(exc).__name__}")
                    await page.goto(ATHLETICS_HOME_URL, wait_until="domcontentloaded", timeout=90000)
                    navigation_log.append("goto_athletics_home_fallback")
            else:
                await page.goto(ATHLETICS_HOME_URL, wait_until="domcontentloaded", timeout=90000)
                navigation_log.append("goto_athletics_home")

            if ATHLETICS_HOME_URL not in page.url:
                await page.goto(ATHLETICS_HOME_URL, wait_until="domcontentloaded", timeout=90000)
                navigation_log.append("goto_athletics_home_direct")

            source_pages.append(page.url)
            page_snapshots.append(await _collect_page(page))

            fall_sports_link = page.locator("a[href='https://www.calabasasathletics.net/blank-7']").first
            if await fall_sports_link.count():
                try:
                    await fall_sports_link.click(timeout=7000)
                    await page.wait_for_timeout(1200)
                    navigation_log.append("click_fall_sports_link")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"fall_sports_click_failed:{type(exc).__name__}")
                    await page.goto(FALL_SPORTS_URL, wait_until="domcontentloaded", timeout=90000)
                    navigation_log.append("goto_fall_sports_fallback")
            else:
                await page.goto(FALL_SPORTS_URL, wait_until="domcontentloaded", timeout=90000)
                navigation_log.append("goto_fall_sports")

            if FALL_SPORTS_URL not in page.url:
                await page.goto(FALL_SPORTS_URL, wait_until="domcontentloaded", timeout=90000)
                navigation_log.append("goto_fall_sports_direct")

            source_pages.append(page.url)
            page_snapshots.append(await _collect_page(page))
        finally:
            await browser.close()

    source_pages = [page for page in _dedupe_keep_order(source_pages) if isinstance(page, str)]

    all_lines: list[str] = []
    all_links: list[dict[str, str]] = []
    for snapshot in page_snapshots:
        all_lines.extend(snapshot.get("lines") or [])
        snapshot_links = snapshot.get("links") or []
        for link in snapshot_links:
            if isinstance(link, dict) and _clean(link.get("href", "")):
                all_links.append(
                    {
                        "text": _clean(str(link.get("text") or "")),
                        "href": _clean(str(link.get("href") or "")),
                    }
                )

    all_lines = _dedupe_keep_order(all_lines)
    all_links = _dedupe_keep_order(all_links)

    fall_sections = _parse_fall_sports_sections(all_lines)
    mailto_contacts = _extract_mailto_contacts(all_links)
    school_contact = _extract_school_contact(all_lines)

    football_section = next((section for section in fall_sections if section.get("sport") == "FOOTBALL"), {})
    football_contact = next(
        (
            contact
            for contact in mailto_contacts
            if contact.get("email", "").lower() == FOOTBALL_COACH_EMAIL
            or "cary harris" in contact.get("name", "").lower()
        ),
        {},
    )

    football_links = [
        link
        for link in all_links
        if "football" in link.get("text", "").lower() or "football" in link.get("href", "").lower()
    ]

    athletics_links = [
        link
        for link in all_links
        if "calabasasathletics.net" in link.get("href", "").lower()
    ]

    football_program_available = bool(football_section)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_name": football_section.get("sport", "FOOTBALL") if football_program_available else "",
        "football_head_coach": {
            "name": football_section.get("coach_name", "") if football_program_available else "",
            "label": football_section.get("coach_line", "") if football_program_available else "",
            "email": football_contact.get("email", "") if football_contact else "",
            "mailto": f"mailto:{football_contact.get('email', '')}" if football_contact else "",
        },
        "football_section": football_section,
        "fall_sports_sections": fall_sections,
        "football_mentions": [
            line for line in all_lines if "football" in line.lower() or "head coach" in line.lower()
        ],
        "football_links": football_links,
        "athletics_links": athletics_links,
        "school_contact": school_contact,
        "mailto_contacts": mailto_contacts,
    }

    if not football_program_available:
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
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_auth_mode"],
            "focus": "football_only",
            "pages_requested": TARGET_PAGES,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_log,
            "selectors": {
                "football_card": FOOTBALL_SELECTOR,
                "football_coach_email": f'a[href="mailto:{FOOTBALL_COACH_EMAIL}"]',
                "athletics_home_link": 'a[href="https://www.calabasashigh.net/athletics"]',
                "fall_sports_link": 'a[href="https://www.calabasasathletics.net/blank-7"]',
            },
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
