"""Deterministic football scraper for Irvine High School (CA)."""

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

NCES_ID = "068450007059"
SCHOOL_NAME = "Irvine High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://irvinehigh.iusd.org/"
ATHLETICS_URL = "https://irvinehigh.iusd.org/athletics"
ATHLETIC_SCHEDULES_URL = "https://irvinehigh.iusd.org/athletics/athletic-schedules"
FALL_SPORTS_URL = "https://irvinehigh.iusd.org/athletics/fall-sports"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    ATHLETIC_SCHEDULES_URL,
    FALL_SPORTS_URL,
]

FOOTBALL_TERMS = (
    "football",
    "flag football",
    "varsity football",
    "jv football",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _dedupe_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    output: list[dict[str, str]] = []
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if not text and not href:
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


async def _capture_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    await page.wait_for_timeout(1200)

    title = _clean(await page.title())
    final_url = _clean(page.url)
    body_text = await page.locator("body").inner_text(timeout=20_000)

    raw_links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(el => ({
            text: (el.textContent || "").replace(/\\s+/g, " ").trim(),
            href: el.href || ""
        }))""",
    )

    links: list[dict[str, str]] = []
    if isinstance(raw_links, list):
        for entry in raw_links:
            text = _clean(str((entry or {}).get("text", "")))
            href = _clean(str((entry or {}).get("href", "")))
            if text or href:
                links.append({"text": text, "href": href})

    return {
        "requested_url": url,
        "final_url": final_url,
        "title": title,
        "body_text": body_text,
        "links": _dedupe_links(links),
    }


def _football_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw in (text or "").splitlines():
        line = _clean(raw)
        if not line:
            continue
        lowered = line.lower()
        if any(term in lowered for term in FOOTBALL_TERMS):
            lines.append(line)
    return _dedupe_strings(lines)


def _football_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if not text and not href:
            continue
        blob = f"{link.get('text', '')} {link.get('href', '')}".lower()
        if "football" in blob:
            output.append({"text": text, "href": href})
    return _dedupe_links(output)


def _staff_contacts(links: list[dict[str, str]]) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    for link in links:
        href = link.get("href", "")
        text = _clean(link.get("text", ""))
        if href.startswith("mailto:"):
            contacts.append(
                {
                    "type": "email",
                    "label": text,
                    "value": href.replace("mailto:", "", 1),
                }
            )
        if href.startswith("tel:"):
            contacts.append(
                {
                    "type": "phone",
                    "label": text,
                    "value": href.replace("tel:", "", 1),
                }
            )
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for item in contacts:
        key = (item["type"], item["value"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


async def scrape_school() -> dict[str, Any]:
    errors: list[str] = []
    snapshots: list[dict[str, Any]] = []

    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(ignore_https_errors=True)
        page = await context.new_page()

        for url in TARGET_URLS:
            try:
                snapshots.append(await _capture_page(page, url))
            except Exception as exc:  # pragma: no cover
                errors.append(f"capture_failed:{url}:{type(exc).__name__}")

        await context.close()
        await browser.close()

    source_pages = _dedupe_strings([s["final_url"] for s in snapshots if s.get("final_url")])
    all_text = "\n".join(s.get("body_text", "") for s in snapshots)
    all_links: list[dict[str, str]] = []
    for snap in snapshots:
        all_links.extend(snap.get("links", []))
    all_links = _dedupe_links(all_links)

    football_lines = _football_lines(all_text)
    football_links = _football_links(all_links)
    contacts = _staff_contacts(all_links)

    football_program_available = bool(football_lines)
    if not football_program_available:
        errors.append("no_public_football_content_found")

    extracted_items = {
        "football_program_available": football_program_available,
        "athletics_page": ATHLETICS_URL,
        "athletic_schedules_page": ATHLETIC_SCHEDULES_URL,
        "fall_sports_page": FALL_SPORTS_URL,
        "football_keyword_lines": football_lines[:80],
        "football_related_links": football_links[:40],
        "athletics_contact_points": contacts[:25],
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
        },
        "errors": errors,
    }


async def _async_main() -> None:
    import json

    print(json.dumps(await scrape_school(), ensure_ascii=True))


def main() -> None:
    import asyncio

    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
