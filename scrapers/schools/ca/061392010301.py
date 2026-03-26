"""Deterministic football scraper for A.B. Miller High School (CA)."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061392010301"
SCHOOL_NAME = "A.B. Miller High School"
STATE = "CA"
PROXY_PROFILE = "datacenter"

FOOTBALL_URL = "https://abmiller.fusd.net/athletics/fall-sports/football"
TARGET_URLS = [FOOTBALL_URL]

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
    for raw in values:
        item = _clean(raw)
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _strip_mailto(value: str) -> str:
    clean = _clean(value)
    if clean.lower().startswith("mailto:"):
        return clean.split(":", 1)[1]
    return clean


async def _collect_football_page(page) -> dict[str, Any]:
    h1_texts = [_clean(text) for text in await page.locator("main h1").all_inner_texts()]
    h2_texts = [_clean(text) for text in await page.locator("main h2").all_inner_texts()]

    coach_cards = await page.locator(
        "main .fsLayout.fsThreeColumnLayout > .fsDiv > .fsContent"
    ).evaluate_all(
        """els => els.map((el) => {
            const h2 = el.querySelector('h2');
            const h3 = el.querySelector('h3');
            const mailto = el.querySelector('a[href^="mailto:"]');
            return {
                role: h2 ? (h2.textContent || '').replace(/\\s+/g, ' ').trim() : '',
                name: h3 ? (h3.textContent || '').replace(/\\s+/g, ' ').trim() : '',
                email: mailto ? (mailto.textContent || '').replace(/\\s+/g, ' ').trim() : '',
                email_href: mailto ? (mailto.href || '') : '',
                text: (el.innerText || '').replace(/\\s+/g, ' ').trim(),
            };
        })"""
    )
    if not isinstance(coach_cards, list):
        coach_cards = []

    coaches: list[dict[str, str]] = []
    for raw in coach_cards:
        if not isinstance(raw, dict):
            continue
        role = _clean(str(raw.get("role") or ""))
        name = _clean(str(raw.get("name") or ""))
        email = _clean(str(raw.get("email") or "")) or _strip_mailto(str(raw.get("email_href") or ""))
        if not (role or name or email):
            continue
        coaches.append(
            {
                "role": role,
                "name": name,
                "email": email,
            }
        )

    preseason_docs = await page.locator("main .fsResourceElement a.fsResourceLink").evaluate_all(
        """els => els.map((a) => {
            const img = a.querySelector('img');
            return {
                url: a.href || '',
                resource_title: a.getAttribute('data-resource-title') || '',
                resource_type: a.getAttribute('data-resource-type') || '',
                alt: img ? (img.getAttribute('alt') || '') : '',
            };
        })"""
    )
    if not isinstance(preseason_docs, list):
        preseason_docs = []

    docs: list[dict[str, str]] = []
    for raw in preseason_docs:
        if not isinstance(raw, dict):
            continue
        url = _clean(str(raw.get("url") or ""))
        if not url:
            continue
        docs.append(
            {
                "url": url,
                "resource_title": _clean(str(raw.get("resource_title") or "")),
                "resource_type": _clean(str(raw.get("resource_type") or "")),
                "alt": _clean(str(raw.get("alt") or "")),
            }
        )

    coach_names = _dedupe_keep_order([str(item.get("name") or "") for item in coaches])
    coach_emails = _dedupe_keep_order([str(item.get("email") or "") for item in coaches])

    evidence_lines = _dedupe_keep_order(
        [
            *[str(item.get("role") or "") for item in coaches],
            *[str(item.get("name") or "") for item in coaches],
            *[str(item.get("email") or "") for item in coaches],
            *[str(item.get("alt") or "") for item in docs],
            *[str(item.get("resource_title") or "") for item in docs],
        ]
    )

    football_program_available = bool(coaches or docs)

    return {
        "page_url": page.url,
        "page_title": _clean(await page.title()),
        "h1_texts": h1_texts,
        "h2_texts": h2_texts,
        "football_program_available": football_program_available,
        "football_coaches": coaches,
        "football_coach_names": coach_names,
        "football_coach_emails": coach_emails,
        "football_preseason_documents": docs,
        "football_source_signals": evidence_lines,
        "summary": (
            "Public football page lists Varsity, Junior Varsity, and Freshman head coaches plus a preseason information PDF."
            if football_program_available
            else ""
        ),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape A.B. Miller High School football signals from the public Finalsite page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)
    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            await page.goto(FOOTBALL_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_load_state("networkidle", timeout=60000)
            source_pages.append(page.url)
            page_data = await _collect_football_page(page)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"navigation_failed:{type(exc).__name__}:{FOOTBALL_URL}")
            errors.append(str(exc))
        finally:
            await context.close()
            await browser.close()

    football_program_available = bool(page_data.get("football_program_available"))
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_page_url": page_data.get("page_url") or FOOTBALL_URL,
        "football_page_title": page_data.get("page_title") or "",
        "football_heading": (page_data.get("h1_texts") or [""])[0] if page_data.get("h1_texts") else "",
        "football_coaches": page_data.get("football_coaches") or [],
        "football_coach_names": page_data.get("football_coach_names") or [],
        "football_coach_emails": page_data.get("football_coach_emails") or [],
        "football_preseason_documents": page_data.get("football_preseason_documents") or [],
        "football_source_signals": page_data.get("football_source_signals") or [],
        "summary": page_data.get("summary") or "",
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
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(source_pages),
            "focus": "football_only",
            "manual_navigation_steps": ["football_page"],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
