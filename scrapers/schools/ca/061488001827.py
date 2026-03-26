"""Deterministic football scraper for Bolsa Grande High (CA)."""

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

NCES_ID = "061488001827"
SCHOOL_NAME = "Bolsa Grande High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.bolsagrande.org"
ATHLETICS_DEPT_URL = "https://www.bolsagrande.org/apps/pages/index.jsp?uREC_ID=60642&type=d"
SPORTS_URL = "https://www.bolsagrande.org/apps/pages/index.jsp?uREC_ID=60642&type=d&pREC_ID=2734649"
FOOTBALL_HOME_URL = "https://www.bolsagrande.org/apps/pages/index.jsp?uREC_ID=211869&type=d&pREC_ID=470722"
FOOTBALL_ROSTER_URL = "https://www.bolsagrande.org/apps/pages/index.jsp?uREC_ID=211869&type=d&pREC_ID=470808"
FOOTBALL_SCHEDULE_URL = "https://www.bolsagrande.org/apps/pages/index.jsp?uREC_ID=211869&type=d&pREC_ID=470809"
COACHING_STAFF_URL = "https://www.bolsagrande.org/apps/pages/index.jsp?uREC_ID=211869&type=d&pREC_ID=2471610"

TARGET_PAGES = [
    HOME_URL,
    ATHLETICS_DEPT_URL,
    SPORTS_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_SCHEDULE_URL,
    COACHING_STAFF_URL,
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


def _split_lines(body_text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in body_text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lines.append(line)
    return lines


def _extract_phone_numbers(text: str) -> list[str]:
    matches = re.findall(r"\(\d{3}\) \d{3}-\d{4}", text)
    return _dedupe_keep_order(matches)


def _extract_emails(text: str) -> list[str]:
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return _dedupe_keep_order(emails)


def _extract_lines(body_text: str, *, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for line in _split_lines(body_text):
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_address(lines: list[str]) -> str:
    for line in lines:
        if re.match(r"\d{4,} .+, Garden Grove, CA", line, re.I):
            return line
    for line in lines:
        if "Garden Grove" in line and "CA" in line:
            return line
    return ""


def _looks_like_name(value: str) -> bool:
    return bool(
        re.match(
            r"^(?:[A-Z][a-z]+(?:\s+[A-Z][A-Za-z.\-']+)*(?:\s+(?:Jr\.?|Sr\.?))?)$",
            value,
        )
    )


def _is_section_break(value: str) -> bool:
    lowered = value.lower()
    breaks = {
        "coaching staff",
        "coaching staff page",
        "football home",
        "team roster",
        "game schedule",
        "links",
        "social media",
        "athletics calendar",
        "athletic staff and coaches",
        "athletic clearance",
        "sports",
        "summer dates:",
    }
    if lowered in breaks:
        return True
    if lowered.startswith("skip to"):
        return True
    return False


def _extract_coaches(body_text: str) -> list[dict[str, str]]:
    lines = _split_lines(body_text)
    coaches: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    markers = {
        "head coach": "Head Coach",
        "varsity assistant coaches:": "Varsity Assistant Coach",
        "fr/so coaches:": "Freshman/Sophomore Coach",
    }

    for index, line in enumerate(lines):
        role = markers.get(line.lower())
        if not role:
            continue

        for offset in range(1, 8):
            if index + offset >= len(lines):
                break
            candidate = _clean(lines[index + offset])
            if not candidate:
                continue
            lower = candidate.lower()
            if lower in markers or _is_section_break(lower):
                break
            if _looks_like_name(candidate):
                row = (role, candidate)
                if row in seen:
                    continue
                seen.add(row)
                coaches.append({"role": role, "name": candidate})
            else:
                break

    return coaches


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\s+/g, ' ').trim(),
            href: (el.getAttribute('href') || '')
        }))""",
    )
    if not isinstance(links, list):
        links = []

    iframes = await page.eval_on_selector_all(
        "iframe[src]",
        """els => els.map(el => ({
            src: el.src || '',
            title: (el.title || '').replace(/\s+/g, ' ').trim(),
        }))""",
    )
    if not isinstance(iframes, list):
        iframes = []

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_text": _clean(body_text),
        "links": [
            f"{_clean(str(item.get('text') or ''))}|{str(item.get('href') or '').strip()}"
            for item in links
            if isinstance(item, dict) and str(item.get("href") or "").strip()
        ],
        "iframes": [
            f"{_clean(str(item.get('title') or ''))}|{str(item.get('src') or '').strip()}"
            for item in iframes
            if isinstance(item, dict) and str(item.get("src") or "").strip()
        ],
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public Bolsa Grande football pages for deterministic evidence."""
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
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            for target in TARGET_PAGES:
                try:
                    await page.goto(target, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_timeout(1_200)
                    snapshot = await _collect_page(page, target)
                    snapshots.append(snapshot)
                    source_pages.append(snapshot["final_url"])
                    navigation_log.append(f"visited:{target}")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{target}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_lines: list[str] = []
    all_links: list[str] = []
    all_iframes: list[str] = []
    football_home_text = ""
    football_coach_text = ""

    for snapshot in snapshots:
        body = _clean(str(snapshot.get("body_text") or ""))
        lines = _split_lines(body)
        all_lines.extend(_extract_lines(body, keywords=("football", "coach", "roster", "schedule", "clearance", "sports", "athletics")))
        all_lines.extend(lines)
        all_links.extend(str(link) for link in snapshot.get("links", []) if isinstance(link, str))
        all_iframes.extend(str(item) for item in snapshot.get("iframes", []) if isinstance(item, str))

        if snapshot.get("requested_url") == FOOTBALL_HOME_URL:
            football_home_text = body
        if snapshot.get("requested_url") == COACHING_STAFF_URL:
            football_coach_text = body

    all_lines = _dedupe_keep_order(all_lines)

    coaching_block = _extract_coaches("\n".join([football_home_text, football_coach_text]))
    if not coaching_block:
        coaching_block = _extract_coaches("\n".join(all_lines))

    football_lines = _extract_lines("\n".join(all_lines), keywords=("football", "head coach", "coaching staff", "team roster", "game schedule"))
    schedule_iframes = []
    schedule_links = []

    for item in all_iframes:
        if "cifsshome.org/widget/event-list" in item:
            parts = item.split("|", 1)
            schedule_iframes.append(parts[-1] if len(parts) == 2 else item)

    for link in all_links:
        if "|" not in link:
            continue
        text, href = link.split("|", 1)
        lower_href = href.lower()
        if "cifsshome.org/widget/event-list" in lower_href or "apps/pages/index.jsp" in lower_href:
            if any(token in lower_href for token in ("school_id", "uREC_ID=211869")):
                schedule_links.append(href)

    football_addresses = _extract_address(all_lines)
    football_phone_numbers = _extract_phone_numbers("\n".join(all_lines))
    football_emails = _extract_emails("\n".join(all_lines))

    football_program_available = bool(football_lines)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_names": ["Football"] if football_program_available else [],
        "school": SCHOOL_NAME,
        "athletics_department_url": ATHLETICS_DEPT_URL,
        "sports_matrix_url": SPORTS_URL,
        "football_home_url": FOOTBALL_HOME_URL,
        "football_coaching_staff_url": COACHING_STAFF_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "football_schedule_page_url": FOOTBALL_SCHEDULE_URL,
        "football_coach_profiles": coaching_block,
        "football_coach_lines": _extract_lines("\n".join(all_lines), keywords=("head coach", "assistant", "fr/so coaches", "coaching staff")),
        "football_schedule_iframe_urls": _dedupe_keep_order(schedule_iframes),
        "football_schedule_related_links": _dedupe_keep_order(schedule_links),
        "football_keywords": football_lines[:80],
        "football_schedule_text_sample": _extract_lines("\n".join(all_lines), keywords=("summer dates", "schedule", "home", "away", "opponent", "result")),
        "school_phone_numbers": football_phone_numbers,
        "school_emails": football_emails,
        "school_address": football_addresses,
        "navigation_log": navigation_log,
    }

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found")

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": proxy_meta.get("proxy_profile"),
            "proxy_servers": proxy_meta.get("proxy_servers", []),
            "proxy_auth_mode": proxy_meta.get("proxy_auth_mode"),
            "focus": "football_only",
            "pages_checked": len(snapshots),
            "target_pages": TARGET_PAGES,
            "navigation_log": navigation_log,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
