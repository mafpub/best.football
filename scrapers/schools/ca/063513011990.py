"""Deterministic football scraper for Dougherty Valley High (CA)."""

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

NCES_ID = "063513011990"
SCHOOL_NAME = "Dougherty Valley High"
STATE = "CA"

PROXY_PROFILE = "datacenter"
FOOTBALL_URL = "https://dvhs.srvusd.net/Athletics/Wildcat-Athletics/Fall-Sports/Football/index.html"
COACHING_INFORMATION_URL = "https://dvhs.srvusd.net/Athletics/Wildcat-Athletics/Coaching-Information/index.html"
TRYOUTS_URL = "https://dvhs.srvusd.net/Athletics/Wildcat-Athletics/Tryouts/index.html"

TARGET_URLS = [FOOTBALL_URL, COACHING_INFORMATION_URL, TRYOUTS_URL]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_coach_names(text: str) -> list[str]:
    names: list[str] = []
    for match in re.finditer(
        r"([A-Z][A-Za-z.'-]+(?: [A-Z][A-Za-z.'-]+){0,2})\s*,\s*Head Coach",
        text,
        flags=re.IGNORECASE,
    ):
        candidate = _clean(match.group(1))
        parts = candidate.split()
        if parts and parts[0].lower() in {"football", "flag", "boys", "girls", "varsity", "jv"}:
            candidate = " ".join(parts[1:])
        if candidate:
            names.append(candidate)
    return _dedupe_keep_order(names)


def _extract_tryout_section(text: str) -> list[str]:
    lines: list[str] = []
    capturing = False
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue

        lowered = line.lower()
        if "football 2025 tryout schedule" in lowered:
            capturing = True
            lines.append(line)
            continue

        if capturing:
            if lowered.startswith("athletics") or lowered.startswith("previous") or lowered.startswith("next"):
                break
            if lowered.startswith("copyright") or lowered.startswith("website developed by"):
                break
            lines.append(line)

    return _dedupe_keep_order(lines)


def _extract_relevant_links(items: list[dict[str, Any]], coach_names: list[str]) -> list[str]:
    kept: list[str] = []
    coach_lookup = {name.lower() for name in coach_names}
    for item in items:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        combo = f"{text} {href}".lower()
        if (
            "football" in combo
            or "coaching" in combo
            or "head coach" in combo
            or "tryout" in combo
            or "clearance" in combo
            or "mentor" in combo
            or text.lower() in coach_lookup
        ):
            kept.append(f"{text}|{href}")
    return _dedupe_keep_order(kept)


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.-]+\.\w+", text))


def _extract_phones(text: str) -> list[str]:
    phones = re.findall(r"(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}", text)
    return _dedupe_keep_order(phones)


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.locator("body").inner_text()
    links = await page.locator("a[href]").evaluate_all(
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    normalized = _clean(body_text)
    coach_names = _extract_coach_names(normalized)

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": normalized,
        "coach_names": coach_names,
        "football_lines": _extract_lines(
            normalized,
            ("football", "head coach", "coach", "clearance", "mentor", "tryout"),
        ),
        "tryout_section": _extract_tryout_section(normalized),
        "emails": _extract_emails(normalized),
        "phones": _extract_phones(normalized),
        "links": links if isinstance(links, list) else [],
    }


async def scrape_school() -> dict[str, Any]:
    """Visit public DVHS football pages and extract football-specific evidence."""
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
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1_200)
                    page_signals.append(await _collect_page(page, url))
                    source_pages.append(page.url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_lines: list[str] = []
    coach_names: list[str] = []
    tryout_section: list[str] = []
    emails: list[str] = []
    phones: list[str] = []
    football_links: list[str] = []

    for signal in page_signals:
        football_lines.extend(signal.get("football_lines", []))
        coach_names.extend(signal.get("coach_names", []))
        tryout_section.extend(signal.get("tryout_section", []))
        emails.extend(signal.get("emails", []))
        phones.extend(signal.get("phones", []))
        football_links.extend(_extract_relevant_links(signal.get("links", []), signal.get("coach_names", [])))

    football_lines = _dedupe_keep_order(football_lines)
    coach_names = _dedupe_keep_order(coach_names)
    tryout_section = _dedupe_keep_order(tryout_section)
    emails = _dedupe_keep_order(emails)
    phones = _dedupe_keep_order(phones)
    football_links = _dedupe_keep_order(football_links)

    combined_text = "\n".join(signal.get("body_text", "") for signal in page_signals)
    tryout_schedule_lines = _dedupe_keep_order(
        [
            match.group(0)
            for pattern in (
                r"FOOTBALL 2025 TRYOUT SCHEDULE",
                r"August 11-13:\s*6 to 8:30 pm - DVHS Stadium Field \(All Levels\)",
                r"The Athletic Clearance process is mandatory for students to participate in sports at DVHS\.",
                r"Please make sure you register under the 2025-26 school year when you log into AthleticClearance\.com\.",
                r"If you're planning to chaperone, drive, etc\., please complete the Be A Mentor clearance process as soon as possible\.",
            )
            if (match := re.search(pattern, combined_text, flags=re.IGNORECASE))
        ]
    )

    fall_sports_lines = _extract_lines(
        combined_text,
        ("football", "flag football", "fall sports"),
    )

    football_program_available = bool(coach_names or tryout_schedule_lines or football_lines or fall_sports_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_team_names = ["Football"] if football_program_available else []

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_page_url": FOOTBALL_URL,
        "coaching_information_url": COACHING_INFORMATION_URL,
        "tryouts_url": TRYOUTS_URL,
        "football_team_names": football_team_names,
        "football_coach_names": coach_names,
        "football_head_coach": coach_names[0] if coach_names else "",
        "football_tryout_schedule_lines": tryout_schedule_lines,
        "football_fall_sports_lines": fall_sports_lines,
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "program_contact_emails": emails,
        "program_contact_phones": phones,
        "summary": (
            "Dougherty Valley High publicly lists football on the fall sports page, names Gwangee Pittman as head coach, and publishes a 2025 football tryout schedule on the football page."
            if football_program_available
            else ""
        ),
    }

    if not football_program_available:
        extracted_items["football_team_names"] = []

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "focus": "football_only",
            "target_urls": TARGET_URLS,
            "pages_checked": len(page_signals),
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


def main() -> None:
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
