"""Deterministic football scraper for East Union High (CA)."""

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

NCES_ID = "062361003577"
SCHOOL_NAME = "East Union High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://eastunion.mantecausd.net/"
COACHES_URL = (
    "https://eastunion.mantecausd.net/athletics-calendar/"
    "athletic-coaches-contact-information"
)
TRYOUTS_URL = "https://eastunion.mantecausd.net/athletics-calendar/tryout-informatio"
ANNUAL_SPORTS_URL = "https://eastunion.mantecausd.net/athletics-calendar/annual-sports-schedule"

TARGET_URLS = [HOME_URL, COACHES_URL, TRYOUTS_URL, ANNUAL_SPORTS_URL]

FOOTBALL_BUTTON_HREF = "#fs-panel-55221"
FOOTBALL_PANEL_CONTENT_ID = "fsPanelContent_55221"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

EMAIL_PATTERN = re.compile(r"[\w.\-+]+@[\w.\-]+\.\w+")
ZERO_WIDTH_PATTERN = re.compile(r"[\u200b\u200c\u200d\ufeff]")


def _clean(value: str) -> str:
    value = ZERO_WIDTH_PATTERN.sub("", value or "")
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        value = _clean(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return lines


def _normalize_level(value: str) -> str:
    cleaned = _clean(value)
    if cleaned == "JV":
        return "Junior Varsity"
    return cleaned


def _section_slice(lines: list[str], section_title: str, stop_titles: set[str]) -> list[str]:
    try:
        start = lines.index(section_title)
    except ValueError:
        return []

    end = len(lines)
    for stop_title in stop_titles:
        try:
            stop_index = lines.index(stop_title, start + 1)
        except ValueError:
            continue
        if stop_index < end:
            end = stop_index

    return lines[start + 1 : end]


def _extract_name_email(lines: list[str], start_index: int) -> tuple[str, str, int]:
    name = ""
    email = ""
    index = start_index

    while index < len(lines):
        line = lines[index]
        if line == "Head Coach":
            index += 1
            continue
        if not name:
            name = line
            email_match = EMAIL_PATTERN.search(line)
            if email_match:
                email = email_match.group(0)
                name = _clean(line.replace(email, ""))
            index += 1
            continue
        if not email:
            email_match = EMAIL_PATTERN.search(line)
            if email_match:
                email = email_match.group(0)
                index += 1
                break
            if line in {"Assistant Coaches", "Assistant Coaching Staff", "Assistant Coach/Medical Trainer"}:
                break
            index += 1
            continue
        break

    return name, email, index


def _extract_name_list(
    lines: list[str],
    start_index: int,
    stop_titles: set[str],
) -> tuple[list[str], int]:
    names: list[str] = []
    index = start_index

    while index < len(lines):
        line = lines[index]
        if line in stop_titles:
            break
        if line != "Assistant Coaches":
            names.append(line)
        index += 1

    return _dedupe_keep_order(names), index


def _parse_football_section(text: str) -> dict[str, Any]:
    lines = _extract_lines(text)
    if not lines:
        return {}

    team_titles = ["Varsity", "JV", "Freshman"]
    all_stop_titles = set(team_titles + ["Assistant Coaching Staff", "Assistant Coach/Medical Trainer"])
    sections: dict[str, Any] = {}

    for title in team_titles:
        slice_lines = _section_slice(lines, title, all_stop_titles - {title})
        if not slice_lines:
            continue

        section: dict[str, Any] = {
            "title": _normalize_level(title),
            "head_coach": {"name": "", "email": ""},
            "assistant_coaches": [],
        }
        cursor = 0
        while cursor < len(slice_lines):
            line = slice_lines[cursor]
            if line == "Head Coach":
                name, email, cursor = _extract_name_email(slice_lines, cursor + 1)
                section["head_coach"] = {"name": name, "email": email}
                continue
            if line == "Assistant Coaches":
                assistants, cursor = _extract_name_list(
                    slice_lines,
                    cursor + 1,
                    all_stop_titles,
                )
                section["assistant_coaches"] = assistants
                continue
            cursor += 1

        sections[title] = section

    staff_slice = _section_slice(
        lines,
        "Assistant Coaching Staff",
        {"Assistant Coach/Medical Trainer"},
    )
    if staff_slice:
        sections["assistant_coaching_staff"] = _dedupe_keep_order(
            [line for line in staff_slice if line not in {"Assistant Coaches", "Head Coach"}]
        )

    trainer_slice = _section_slice(lines, "Assistant Coach/Medical Trainer", set())
    if trainer_slice:
        sections["assistant_coach_medical_trainer"] = _dedupe_keep_order(
            [line for line in trainer_slice if line not in {"Assistant Coaches", "Head Coach"}]
        )

    return sections


async def _collect_page_text(page, selector: str = "main") -> str:
    for candidate in (selector, "body"):
        try:
            text = await page.locator(candidate).inner_text(timeout=10000)
            if text and text.strip():
                return text
        except Exception:  # noqa: BLE001
            continue
    return ""


async def _collect_links(page, selector: str = "a[href]") -> list[dict[str, str]]:
    raw_links = await page.eval_on_selector_all(
        selector,
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: (a.href || a.getAttribute('href') || '').trim()
        }))""",
    )
    if not isinstance(raw_links, list):
        return []

    links: list[dict[str, str]] = []
    for item in raw_links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        links.append(
            {
                "text": _clean(str(item.get("text") or "")),
                "href": href,
            }
        )
    return links


def _football_schedule_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    filtered: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or "")).lower()
        href = _clean(str(link.get("href") or "")).lower()
        if text == "football 2025-26" or "footballateu" in href:
            filtered.append(link)
    return _dedupe_links(filtered)


def _dedupe_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    output: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        if not href:
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


def _instagram_profile_from_src(src: str) -> tuple[str, str]:
    cleaned = _clean(src)
    if not cleaned:
        return "", ""
    match = re.search(r"instagram\.com/([^/]+)/embed", cleaned)
    handle = match.group(1) if match else ""
    profile_url = f"https://www.instagram.com/{handle}/" if handle else ""
    return handle, profile_url


async def scrape_school() -> dict[str, Any]:
    """Scrape public football evidence from East Union High's school pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    home_text = ""
    coaches_text = ""
    tryout_text = ""
    annual_text = ""

    football_section: dict[str, Any] = {}
    football_schedule_links: list[dict[str, str]] = []
    annual_has_football = False

    instagram_src = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            home_text = await _collect_page_text(page)

            await page.goto(COACHES_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            coaches_text = await _collect_page_text(page)

            football_button = page.locator(
                f'a[role="button"][href="{FOOTBALL_BUTTON_HREF}"]'
            ).first
            await football_button.click(timeout=15000)
            await page.wait_for_timeout(1200)

            football_panel = page.locator(f"#{FOOTBALL_PANEL_CONTENT_ID}").first
            football_text = await football_panel.inner_text(timeout=10000)
            football_section = _parse_football_section(football_text)

            iframe = page.locator(
                f'#{FOOTBALL_PANEL_CONTENT_ID} iframe.instagram-media'
            ).first
            if await iframe.count():
                instagram_src = _clean(await iframe.get_attribute("src") or "")

            await page.goto(TRYOUTS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            tryout_text = await _collect_page_text(page)
            football_schedule_links = _football_schedule_links(await _collect_links(page))

            await page.goto(ANNUAL_SPORTS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            annual_text = await _collect_page_text(page)
            annual_has_football = "football" in annual_text.lower()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    instagram_handle, instagram_profile_url = _instagram_profile_from_src(instagram_src)
    football_coaching_staff = {
        "varsity": football_section.get("Varsity", {}),
        "junior_varsity": football_section.get("JV", {}),
        "freshman": football_section.get("Freshman", {}),
        "assistant_coaching_staff": football_section.get("assistant_coaching_staff", []),
        "assistant_coach_medical_trainer": football_section.get(
            "assistant_coach_medical_trainer", []
        ),
    }

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_section),
        "football_source_pages": {
            "home": HOME_URL,
            "coaches_contact": COACHES_URL,
            "tryout_schedule": TRYOUTS_URL,
            "annual_sports_schedule": ANNUAL_SPORTS_URL,
        },
        "football_coaching_staff": football_coaching_staff,
        "football_social_media": {
            "instagram_handle": instagram_handle,
            "instagram_profile_url": instagram_profile_url,
            "instagram_embed_src": instagram_src,
        },
        "football_schedule_resources": football_schedule_links,
        "annual_sports_schedule_has_football_text": annual_has_football,
        "evidence_notes": _dedupe_keep_order(
            [
                "Football accordion content lives in the coaches contact page under the exact Finalsite content id fsPanelContent_55221.",
                "Tryout & Summer Camp/Schedule Information publishes a Football 2025-26 resource link.",
                (
                    "Annual Sports Schedule page does not include football text."
                    if not annual_has_football
                    else "Annual Sports Schedule page includes football text."
                ),
            ]
        ),
        "page_snippets": {
            "home": _dedupe_keep_order(_extract_lines(home_text)[:8]),
            "coaches": _dedupe_keep_order(_extract_lines(coaches_text)[:8]),
            "tryouts": _dedupe_keep_order(_extract_lines(tryout_text)[:12]),
            "annual": _dedupe_keep_order(_extract_lines(annual_text)[:6]),
        },
    }

    football_program_available = bool(football_section)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_east_union_high_pages")

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "proxy_profile": PROXY_PROFILE,
            "football_button_href": FOOTBALL_BUTTON_HREF,
            "football_panel_content_id": FOOTBALL_PANEL_CONTENT_ID,
            "annual_has_football_text": annual_has_football,
            "tryout_schedule_links_found": len(football_schedule_links),
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


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
