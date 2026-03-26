"""Deterministic football scraper for Huntington Park Senior High & STEAM Magnet (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062271003098"
SCHOOL_NAME = "Huntington Park Senior High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://huntingtonparkhs.lausd.org/"
ATHLETICS_HOMEPAGE_URL = "https://huntingtonparkhs.lausd.org/apps/pages/index.jsp?uREC_ID=3755539&type=d"
ATHLETICS_OVERVIEW_URL = "https://huntingtonparkhs.lausd.org/apps/pages/index.jsp?uREC_ID=3755539&type=d&pREC_ID=2719440"
FLAG_FOOTBALL_URL = "https://huntingtonparkhs.lausd.org/apps/pages/index.jsp?uREC_ID=4395308&type=d&pREC_ID=2616241"
COACHES_URL = "https://huntingtonparkhs.lausd.org/apps/pages/index.jsp?uREC_ID=3755539&type=d&pREC_ID=2719501"
FOOTBALL_SITE_URL = "https://sites.google.com/lausd.net/hpfootball/home"
FOOTBALL_SCHEDULE_URL = "https://sites.google.com/lausd.net/hpfootball/game-schedule"
FOOTBALL_ROSTER_URL = "https://sites.google.com/lausd.net/hpfootball/roster"

SOURCE_URLS = (
    HOME_URL,
    ATHLETICS_HOMEPAGE_URL,
    ATHLETICS_OVERVIEW_URL,
    FLAG_FOOTBALL_URL,
    COACHES_URL,
    FOOTBALL_SITE_URL,
    FOOTBALL_SCHEDULE_URL,
    FOOTBALL_ROSTER_URL,
)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "football",
    "flag football",
    "team",
    "roster",
    "schedule",
    "coach",
    "head coach",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


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


def _extract_lines(text: str, *, keywords: tuple[str, ...] = FOOTBALL_KEYWORDS) -> list[str]:
    out: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(token in lowered for token in keywords):
            out.append(line)
    return _dedupe_keep_order(out)


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.+-]+\.[A-Za-z]{2,}", text or ""))


def _extract_phones(text: str) -> list[str]:
    matches = re.findall(r"\b(?:\(\d{3}\)\s*\d{3}-\d{4}|\d{3}[-.]\d{3}[-.]\d{4})\b", text or "")
    return _dedupe_keep_order(matches)


def _is_blocked(title: str, body: str) -> bool:
    lowered_title = (title or "").lower()
    lowered_body = (body or "").lower()
    return (
        "attention required!" in lowered_title
        or "cloudflare" in lowered_title
        or "unable to access edliocloud" in lowered_body
    )


def _is_school_domain(url: str, *, domain: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return bool(host) and (host == domain or host.endswith(f".{domain}"))


def _extract_coach_lines(lines: list[str]) -> list[dict[str, Any]]:
    coaches: list[dict[str, Any]] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        m = re.match(r"^coach\s+(.+)$", line, re.IGNORECASE)
        if not m:
            index += 1
            continue

        coach_name = _clean(m.group(1))
        if not coach_name:
            index += 1
            continue

        coach: dict[str, Any] = {"name": coach_name}
        emails: list[str] = []
        role_tokens: list[str] = []

        index += 1
        while index < len(lines):
            current = lines[index]
            if re.match(r"^coach\s+.+$", current, re.IGNORECASE):
                break
            if "@" in current and any(char.isalpha() for char in current):
                emails.extend(_extract_emails(current))
                index += 1
                continue
            if current:
                role_tokens.append(current)
            index += 1

        if emails:
            coach["emails"] = _dedupe_keep_order(emails)
        if role_tokens:
            coach["roles"] = _dedupe_keep_order(role_tokens)

        coaches.append(coach)

    return coaches


def _extract_football_roster(lines: list[str]) -> dict[str, list[str]]:
    varsity: list[str] = []
    junior_varsity: list[str] = []
    current = ""

    name_rx = re.compile(r"^(?:[A-Z][a-z]+(?: [A-Z][a-z]+)+|[A-Z][a-z]+,\\s*[A-Z][a-z]+)$")

    for raw_line in lines:
        line = _clean(raw_line)
        if not line:
            continue
        upper = line.upper()
        if upper == "ROSTER":
            continue
        if upper == "VARSITY":
            current = "varsity"
            continue
        if upper == "JUNIOR VARSITY":
            current = "junior_varsity"
            continue
        if upper == "PAGE UPDATED":
            break
        if not name_rx.match(line):
            continue

        if current == "varsity":
            varsity.append(line)
        elif current == "junior_varsity":
            junior_varsity.append(line)
        else:
            varsity.append(line)

    return {
        "varsity": _dedupe_keep_order(varsity)[:30],
        "junior_varsity": _dedupe_keep_order(junior_varsity)[:30],
    }


async def _collect_page_snapshot(page, requested_url: str) -> dict[str, Any]:
    title = _clean(await page.title())
    final_url = _clean(page.url)
    body_text = await page.locator("body").inner_text()
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(a => ({"
        "text: (a.textContent || '').replace(/\\s+/g, ' ').trim(), "
        "href: a.getAttribute('href') || ''"
        "}))",
    )
    normalized_links: list[dict[str, str]] = []
    link_items = links if isinstance(links, list) else []
    for item in link_items:
        href = _clean(str(item.get("href")))
        text = _clean(str(item.get("text")))
        if href:
            normalized_links.append({"text": text, "href": href})

    return {
        "requested_url": requested_url,
        "final_url": final_url,
        "title": title,
        "text": _clean(body_text),
        "lines": [_clean(line) for line in (body_text or "").splitlines() if _clean(line)],
        "links": normalized_links,
        "blocked": _is_blocked(title, body_text),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football indicators and team-facing links."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(list(SOURCE_URLS), profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: dict[str, dict[str, Any]] = {}

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        for url in SOURCE_URLS:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1100)
                snapshot = await _collect_page_snapshot(page, url)
                snapshots[url] = snapshot
                source_pages.append(snapshot["final_url"])
            except Exception as exc:  # noqa: BLE001
                errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")

        await context.close()
        await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_snapshot = snapshots.get(HOME_URL, {})
    ath_home_snapshot = snapshots.get(ATHLETICS_HOMEPAGE_URL, {})
    ath_overview_snapshot = snapshots.get(ATHLETICS_OVERVIEW_URL, {})
    flag_snapshot = snapshots.get(FLAG_FOOTBALL_URL, {})
    coaches_snapshot = snapshots.get(COACHES_URL, {})
    football_site_snapshot = snapshots.get(FOOTBALL_SITE_URL, {})
    schedule_snapshot = snapshots.get(FOOTBALL_SCHEDULE_URL, {})
    roster_snapshot = snapshots.get(FOOTBALL_ROSTER_URL, {})

    football_mention_lines = _dedupe_keep_order(
        _extract_lines(str(ath_home_snapshot.get("text", "")))
        + _extract_lines(str(ath_overview_snapshot.get("text", "")))
        + _extract_lines(str(flag_snapshot.get("text", "")))
        + _extract_lines(str(coaches_snapshot.get("text", "")))
        + _extract_lines(str(football_site_snapshot.get("text", "")))
    )

    schedule_lines = _extract_lines(str(schedule_snapshot.get("text", "")), keywords=("fall 2026", "schedule"))
    home_site_links = [item for item in football_site_snapshot.get("links", []) if item.get("href")]
    lausd_athletics_links = [item for item in ath_home_snapshot.get("links", []) if "index.jsp" in (item.get("href") or "")]
    football_site_links = _dedupe_keep_order([item.get("href", "") for item in home_site_links if item.get("href", "").startswith("https://")])[:25]

    coach_lines = [_clean(line) for line in football_site_snapshot.get("lines", [])]
    coaches = _extract_coach_lines(coach_lines)

    roster_lines = [_clean(line) for line in roster_snapshot.get("lines", [])]
    football_roster = _extract_football_roster(roster_lines)

    combined_text = " ".join(
        [
            str(ath_home_snapshot.get("text", "")),
            str(football_site_snapshot.get("text", "")),
            str(schedule_snapshot.get("text", "")),
        ]
    )
    football_emails = _dedupe_keep_order(
        [email for email in _extract_emails(combined_text) if email.endswith("lausd.net")]
    )
    football_phones = _dedupe_keep_order(_extract_phones(combined_text))

    football_program_available = bool(football_mention_lines or coaches or football_roster["varsity"] or football_roster["junior_varsity"])

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_pages: list[dict[str, str]] = []
    for url in [ATHLETICS_HOMEPAGE_URL, ATHLETICS_OVERVIEW_URL, FLAG_FOOTBALL_URL, COACHES_URL, FOOTBALL_SITE_URL]:
        if url in snapshots:
            football_pages.append({"page": url, "resolved_url": snapshots[url].get("final_url", "")})

    extracted_items: dict[str, Any] = {
        "school_sport_focus": "football",
        "program_available": football_program_available,
        "program_urls": {
            "homepage": HOME_URL,
            "athletics_page": ATHLETICS_HOMEPAGE_URL,
            "athletics_overview": ATHLETICS_OVERVIEW_URL,
            "flag_football_page": FLAG_FOOTBALL_URL,
            "athletic_coaches_page": COACHES_URL,
            "team_page": ATHLETICS_HOMEPAGE_URL,
            "google_football_page": FOOTBALL_SITE_URL,
            "football_schedule_page": FOOTBALL_SCHEDULE_URL,
            "football_roster_page": FOOTBALL_ROSTER_URL,
        },
        "football_mentions": football_mention_lines,
        "football_schedule_lines": schedule_lines,
        "football_coaches": coaches,
        "coach_emails": football_emails,
        "coach_phones": football_phones,
        "roster_preview": football_roster,
        "lausd_football_links": football_site_links,
        "athletics_links": _dedupe_keep_order([item.get("href") for item in lausd_athletics_links if item.get("href")]) ,
        "football_pages": football_pages,
        "summary": (
            "Huntington Park Senior High exposes Athletics with football entries and links to a dedicated Google Sites football page "
            "listing coaches, schedule, and roster."
            if football_program_available
            else "No public football program evidence found on primary athletics pages."
        ),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "focus": "football_only",
            "proxy": get_proxy_runtime_meta(PROXY_PROFILE),
            "pages_visited": len(source_pages),
            "notes": "Scraped Huntington Park HS athletics landing and dedicated HPHS football page.",
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    result = asyncio.run(scrape_school())
    print(json.dumps(result, indent=2, ensure_ascii=False))
