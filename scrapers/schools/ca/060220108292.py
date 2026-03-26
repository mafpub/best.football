"""Deterministic football scraper for The Grove School (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "060220108292"
SCHOOL_NAME = "The Grove School"
STATE = "CA"

BASE_URL = "https://www.thegroveschool.org"
HOME_URL = f"{BASE_URL}/"
ATHLETICS_URL = f"{BASE_URL}/athletics/"
TARGET_URLS = [HOME_URL, ATHLETICS_URL]

PROXY_PROFILE = "datacenter"

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


def _extract_lines(text: str, *, keywords: tuple[str, ...], limit: int = 120) -> list[str]:
    out: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            out.append(line)
    return _dedupe_keep_order(out)[:limit]


def _normalize_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    value = _clean(url)
    if not value:
        return ""
    if value.startswith("/"):
        return f"{BASE_URL}{value}"
    return value


def _pick_url(links: list[tuple[str, str]], keywords: tuple[str, ...]) -> str:
    for text, href in links:
        haystack = f"{text} {href}".lower()
        if any(keyword in haystack for keyword in keywords):
            return _normalize_url(href)
    return ""


async def _extract_links(page) -> list[tuple[str, str]]:
    raw = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => ({text:(e.textContent||'').replace(/\\s+/g,' ').trim(), href:e.getAttribute('href')||''}))",
    )
    if not isinstance(raw, list):
        return []

    links: list[tuple[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        if href:
            links.append((text, href))
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for text, href in links:
        if (text, href) in seen:
            continue
        seen.add((text, href))
        deduped.append((text, href))
    return deduped


async def _collect_page_signal(page, requested_url: str) -> dict[str, Any]:
    try:
        title = _clean(await page.title())
    except Exception:  # noqa: BLE001
        title = ""

    try:
        body = _clean(await page.inner_text("body"))
    except Exception:  # noqa: BLE001
        body = ""

    body_lines = _extract_lines(
        body,
        keywords=("flag football", "football", "athletics", "sports", "calendar"),
        limit=200,
    )

    links = await _extract_links(page)

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": title,
        "body_text": body,
        "body_lines": body_lines,
        "links": links,
    }


def _extract_football_team_names(lines: list[str]) -> list[str]:
    names: list[str] = []
    for line in lines:
        lowered = line.lower()
        if "flag football" in lowered:
            names.append("Flag Football")
        if re.search(r"\bfootball\b", lowered) and "flag football" not in lowered:
            names.append("Football")
    return _dedupe_keep_order(names)


def _extract_football_links(links: list[tuple[str, str]]) -> list[str]:
    output: list[str] = []
    for text, href in links:
        haystack = f"{text} {href}".lower()
        if any(token in haystack for token in ("football", "flag football", "athletic")):
            output.append(_normalize_url(href))
    return _dedupe_keep_order(output)


def _extract_school_identity(body: str, links: list[tuple[str, str]]) -> dict[str, str]:
    address = next((line for line in body.splitlines() if "Redlands" in line or "REDLANDS" in line), "")
    email = ""
    phone = ""

    emails = re.findall(r"[\w.+\-']+@[\w.\-]+\.[A-Za-z]{2,}", body)
    if emails:
        email = emails[0].lower()

    for _, href in links:
        if href.startswith("mailto:"):
            email = href.replace("mailto:", "", 1)
            break

    phones = re.findall(r"\b\d{3}[\-\.]?\d{3}[\-\.]?\d{4}\b", body)
    if phones:
        phone = phones[0]

    return {
        "address": _clean(address),
        "email": _clean(email),
        "phone": _clean(phone),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape The Grove School's public football signals from site navigation."""
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
            viewport={"width": 1365, "height": 768},
        )

        try:
            page = await context.new_page()

            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            home_signal = await _collect_page_signal(page, HOME_URL)
            source_pages.append(home_signal["final_url"])
            page_signals.append(home_signal)

            home_links = home_signal.get("links", [])
            discovered_athletics = _pick_url(home_links, ("athletics",))
            if not discovered_athletics:
                discovered_athletics = ATHLETICS_URL

            await page.goto(discovered_athletics, wait_until="domcontentloaded", timeout=70000)
            await page.wait_for_timeout(1200)
            athletics_signal = await _collect_page_signal(page, ATHLETICS_URL)
            source_pages.append(athletics_signal["final_url"])
            page_signals.append(athletics_signal)

            athletics_links = athletics_signal.get("links", [])
            calendar_url = _pick_url(athletics_links, ("athletics calendar", "calendar"))
            if calendar_url:
                try:
                    await page.goto(calendar_url, wait_until="domcontentloaded", timeout=30000)
                    await page.wait_for_timeout(800)
                    calendar_signal = await _collect_page_signal(page, calendar_url)
                    source_pages.append(calendar_signal["final_url"])
                    page_signals.append(calendar_signal)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"calendar_navigation_failed:{type(exc).__name__}:{calendar_url}")

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await context.close()
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_lines: list[str] = []
    all_links: list[tuple[str, str]] = []
    all_body: list[str] = []
    for signal in page_signals:
        all_lines.extend(signal.get("body_lines", []))
        all_links.extend(signal.get("links", []))
        all_body.append(str(signal.get("body_text") or ""))

    football_lines = [line for line in _extract_lines("\n".join(all_body), keywords=("flag football", "football"), limit=200)]
    football_links = _extract_football_links(all_links)
    football_team_names = _extract_football_team_names(football_lines)

    home_signal = next((signal for signal in page_signals if signal.get("requested_url") == HOME_URL), {})
    home_body = str(home_signal.get("body_text") or "") if isinstance(home_signal, dict) else ""
    home_links = home_signal.get("links", []) if isinstance(home_signal, dict) else []
    school_identity = _extract_school_identity(home_body, home_links)

    football_program_available = bool(
        football_lines
        or football_links
        or any("football" in body.lower() for body in all_body)
        or football_team_names
    )

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "home_url": HOME_URL,
        "athletics_url": ATHLETICS_URL,
        "discovered_athletics_url": discovered_athletics if page_signals else ATHLETICS_URL,
        "football_program_available": football_program_available,
        "football_team_names": football_team_names,
        "football_keyword_lines": football_lines,
        "football_links": football_links,
        "football_schedule_note": (
            "No public football game schedule page was exposed; athletics content lists Flag Football."
            if football_program_available
            else ""
        ),
        "school_identity": {
            "name": SCHOOL_NAME,
            "address": school_identity.get("address", ""),
            "phone": school_identity.get("phone", ""),
            "email": school_identity.get("email", ""),
        },
        "athletics_calendar_url": _pick_url(all_links, ("athletics calendar", "calendar")),
        "sports_summary_lines": all_lines,
        "all_body_text": all_body,
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.1",
            "proxy_profile": PROXY_PROFILE,
            "pages_checked": len(page_signals),
            "target_urls": TARGET_URLS,
            "manual_navigation_steps": ["home", "athletics", "athletics_calendar"],
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


async def main() -> None:
    result = await scrape_school()
    print(result)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
