"""Deterministic football scraper for Daniel Pearl Journalism & Communications Magnet (CA)."""

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

NCES_ID = "062271012525"
SCHOOL_NAME = "Daniel Pearl Journalism & Communications Magnet"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://pearlhs.lausd.org/"
ATHLETICS_URL = "https://pearlhs.lausd.org/apps/pages/index.jsp?uREC_ID=4275606&type=d&pREC_ID=2522480"
SPORTS_URL = "https://pearlhs.lausd.org/apps/pages/index.jsp?uREC_ID=4275606&type=d&pREC_ID=2522478"

TARGET_PAGES = [HOME_URL, ATHLETICS_URL, SPORTS_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
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


def _extract_lines(text: str) -> list[str]:
    return [_clean(line) for line in (text or "").splitlines() if _clean(line)]


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.+-]+\.[A-Za-z]{2,}", text or ""))


def _extract_phone_numbers(text: str) -> list[str]:
    phones = re.findall(r"\b(?:\d{3}[.\-]\d{3}[.\-]\d{4}|\d{3}\s\d{3}\s\d{4})\b", text or "")
    return _dedupe_keep_order(phones)


def _extract_football_lines(lines: list[str]) -> list[str]:
    keywords = (
        "football",
        "coach rose",
        "coach avila",
        "stadium/weight room",
        "stadium",
        "weight room",
        "818.535.7813",
        "213.256.2131",
    )
    out = [line for line in lines if any(keyword in line.lower() for keyword in keywords)]
    return _dedupe_keep_order(out)


def _extract_football_block(lines: list[str]) -> list[str]:
    football_index = next((i for i, line in enumerate(lines) if line.lower().startswith("football:")), None)
    if football_index is None:
        return []

    block: list[str] = [lines[football_index]]
    for line in lines[football_index + 1 : football_index + 5]:
        if re.match(r"^[A-Z][A-Z ,/&-]+(?:\s+[A-Z][A-Z ,/&-]+)*:?", line) and "football" not in line.lower():
            break
        block.append(line)
    return _dedupe_keep_order(block)


def _extract_links(page_links: list[dict[str, str]]) -> list[dict[str, str]]:
    filtered: list[dict[str, str]] = []
    for link in page_links:
        text = _clean(link.get("text") or "")
        href = _clean(link.get("href") or "")
        combo = f"{text} {href}".lower()
        if any(token in combo for token in ("football", "athletics", "coach", "sports")):
            filtered.append({"text": text, "href": href})
    return filtered


async def _collect_page(page) -> dict[str, Any]:
    body = await page.locator("body").inner_text()
    links = await page.eval_on_selector_all(
        "a[href]",
        "els => els.map(anchor => ({"
        "text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),"
        "href: anchor.getAttribute('href') || ''"
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

    text_lines = _extract_lines(body)
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": _clean(body),
        "lines": text_lines,
        "football_lines": _extract_football_lines(text_lines),
        "football_block": _extract_football_block(text_lines),
        "emails": _extract_emails(body),
        "phones": _extract_phone_numbers(body),
        "links": normalized_links,
        "football_links": _extract_links(normalized_links),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public football signals from Daniel Pearl Magnet's LAUSD athletics pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_PAGES, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    snapshots: list[dict[str, Any]] = []

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

        for url in TARGET_PAGES:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                snapshot = await _collect_page(page)
                snapshots.append(snapshot)
                source_pages.append(snapshot["url"])
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:{url}")

        await context.close()
        await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_snapshot = next((snap for snap in snapshots if snap.get("url") == HOME_URL), {})
    athletics_snapshot = next((snap for snap in snapshots if snap.get("url") == ATHLETICS_URL), {})
    sports_snapshot = next((snap for snap in snapshots if snap.get("url") == SPORTS_URL), {})

    athletics_lines = athletics_snapshot.get("lines", [])
    sports_lines = sports_snapshot.get("lines", [])
    sports_text = " | ".join(sports_lines)

    football_block = sports_snapshot.get("football_block", [])
    football_lines = _dedupe_keep_order(
        football_block
        + sports_snapshot.get("football_lines", [])
        + athletics_snapshot.get("football_lines", [])
        + home_snapshot.get("football_lines", [])
    )
    football_links = _dedupe_keep_order(
        [f"{item.get('text', '')}|{item.get('href', '')}" for snap in snapshots for item in snap.get("football_links", [])]
    )
    football_emails = _dedupe_keep_order(
        [email for snap in snapshots for email in snap.get("emails", []) if "pearlhs.lausd.org" in email.lower()]
    )
    phone_numbers = _dedupe_keep_order([phone for snap in snapshots for phone in snap.get("phones", [])])

    football_contact_lines = _dedupe_keep_order(
        [
            line
            for line in sports_lines
            if any(
                token in line.lower()
                for token in (
                    "contact coach rose",
                    "contact coach avila",
                    "4:00–7:00 pm",
                    "stadium/weight room",
                    "football:",
                )
            )
        ]
    )
    coach_names = _dedupe_keep_order(
        re.findall(r"Coach ([A-Z][a-z]+(?: [A-Z][a-z]+)*)", " ".join(football_contact_lines))
    )

    football_program_available = any(
        "football" in text.lower()
        for text in (
            " ".join(football_lines),
            sports_text,
            " ".join(athletics_lines),
        )
    )
    if not football_program_available:
        errors.append("no_public_football_content_found_on_lausd_athletics_pages")

    extracted_items: dict[str, Any] = {
        "football": {
            "program_available": football_program_available,
            "team_names": ["Football"] if football_program_available else [],
            "athletics_page_url": ATHLETICS_URL,
            "sports_page_url": SPORTS_URL,
            "football_block_lines": football_block,
            "football_keyword_lines": football_lines,
            "football_contact_lines": football_contact_lines,
            "coach_names": coach_names,
            "contact_emails": football_emails,
            "contact_phones": phone_numbers,
            "emails": football_emails,
            "phones": phone_numbers,
            "links": football_links,
            "summary": (
                "Daniel Pearl Magnet's LAUSD athletics page lists football in fall sports with practice time in the Stadium/Weight Room and contacts for Coach Rose and Coach Avila."
                if football_program_available
                else "No public football program evidence was found on the athletics pages."
            ),
        }
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "proxy": get_proxy_runtime_meta(PROXY_PROFILE),
            "focus": "football_only",
            "notes": "Official LAUSD athletics pages publish football in the fall sports list and provide coach contact information.",
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    result = asyncio.run(scrape_school())
    print(json.dumps(result, indent=2, ensure_ascii=False))
