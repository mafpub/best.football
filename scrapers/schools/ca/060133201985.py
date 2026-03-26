"""Deterministic football scraper for Foothill High (CA)."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[3]))

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060133201985"
SCHOOL_NAME = "Foothill High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://fhs.trusd.net/"
ATHLETICS_URL = "https://fhs.trusd.net/ATHLETICS/index.html"
FOOTBALL_URL = "https://fhs.trusd.net/ATHLETICS/FALL-SPORTS/Football/index.html"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, FOOTBALL_URL]

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
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _is_blocked(title: str, text: str) -> bool:
    combined = f"{title}\n{text}".lower()
    return any(
        token in combined
        for token in (
            "403 forbidden",
            "access denied",
            "attention required",
            "cloudflare",
            "blocked",
            "robot check",
        )
    )


def _normalize_href(href: str) -> str:
    value = _clean(href)
    if not value:
        return ""
    if value.startswith("//"):
        return f"https:{value}"
    return value


def _normalize_link(item: Any) -> dict[str, str]:
    text = _clean(str(item.get("text") or ""))
    href = _normalize_href(str(item.get("href") or ""))
    return {"text": text, "href": href}


def _dedupe_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        if not href:
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        out.append({"text": text, "href": href})
    return out


async def _snapshot(page, scope: str = "main") -> dict[str, Any]:
    title = _clean(await page.title())
    scope_locator = page.locator(scope)
    try:
        text = _clean(await scope_locator.first.inner_text(timeout=20_000))
    except Exception:
        text = _clean(await page.locator("body").inner_text(timeout=20_000))

    links = await page.locator("a[href]").evaluate_all(
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || ''
        }))"""
    )
    if not isinstance(links, list):
        links = []

    normalized_links = _dedupe_links(
        [_normalize_link(link) for link in links if isinstance(link, dict)]
    )

    return {
        "url": page.url,
        "title": title,
        "text": text,
        "links": normalized_links,
        "blocked": _is_blocked(title, text),
    }


def _find_link(
    links: list[dict[str, str]],
    *,
    text: str | None = None,
    href_contains: str | None = None,
    text_regex: str | None = None,
) -> dict[str, str] | None:
    text_value = _clean(text or "").lower() if text is not None else ""
    text_re = re.compile(text_regex, re.I) if text_regex else None
    href_value = _clean(href_contains or "").lower() if href_contains is not None else ""

    for link in links:
        link_text = _clean(link.get("text", ""))
        link_href = _clean(link.get("href", ""))
        if not link_href:
            continue
        if text_value and link_text.lower() != text_value:
            continue
        if text_re and not text_re.search(link_text):
            continue
        if href_value and href_value not in link_href.lower():
            continue
        return {"text": link_text, "href": link_href}
    return None


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.+-]+@[\w.-]+\.\w+", text))


def _extract_relevant_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    terms = (
        "football",
        "schedule",
        "alexander.gomes-coelho@trusd.net",
        "gomes",
    )
    out: list[dict[str, str]] = []
    for link in links:
        text = _clean(link.get("text", ""))
        href = _clean(link.get("href", ""))
        blob = f"{text} {href}".lower()
        if any(term in blob for term in terms):
            out.append({"text": text, "href": href})
    return _dedupe_links(out)


async def scrape_school() -> dict[str, Any]:
    """Scrape Foothill High's public football page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    home = {"url": HOME_URL, "title": "", "text": "", "links": [], "blocked": False}
    football = {"url": FOOTBALL_URL, "title": "", "text": "", "links": [], "blocked": False}
    coach_heading = ""
    coach_bio = ""
    schedule_link: dict[str, str] | None = None

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            user_agent=USER_AGENT,
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(1_200)
            home = await _snapshot(page)
            source_pages.append(home["url"])

            football_link = _find_link(
                home["links"],
                text="Football",
                href_contains="/ATHLETICS/FALL-SPORTS/Football/",
            )
            football_url = football_link["href"] if football_link else FOOTBALL_URL
            if not football_link:
                errors.append("football_link_not_found_on_home_page")

            await page.goto(football_url, wait_until="domcontentloaded", timeout=90_000)
            await page.wait_for_timeout(1_200)
            football = await _snapshot(page)
            source_pages.append(football["url"])

            heading_locator = page.locator("main .page-title a.title-link")
            if not await heading_locator.count():
                heading_locator = page.locator("main .page-title h3")
            if await heading_locator.count():
                coach_heading = _clean(await heading_locator.first.inner_text(timeout=20_000))

            bio_locator = page.locator("main .FW_EDITOR_STYLE")
            if await bio_locator.count():
                coach_bio = _clean(await bio_locator.first.inner_text(timeout=20_000))

            printable_locator = page.get_by_role("link", name="Printable Schedule")
            if await printable_locator.count():
                schedule_href = _normalize_href(await printable_locator.first.get_attribute("href") or "")
                schedule_text = _clean(await printable_locator.first.inner_text(timeout=20_000))
                if schedule_href:
                    schedule_link = {
                        "text": schedule_text or "Printable Schedule",
                        "href": schedule_href,
                    }
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    coach_name = ""
    coach_email = ""
    football_links = _extract_relevant_links(football["links"])

    # Use page-local selectors in addition to body text so the scraper is stable
    # when the CMS repeats content in multiple wrappers.
    if football["text"]:
        emails = _extract_emails(football["text"])
        coach_email = emails[0] if emails else ""

    if coach_heading.startswith("Head Coach -"):
        coach_name = _clean(coach_heading.removeprefix("Head Coach -"))
    elif "head coach -" in coach_heading.lower():
        coach_name = _clean(re.sub(r"(?i)^head coach\s*-\s*", "", coach_heading))

    if not coach_name and football["text"]:
        heading_match = re.search(r"Head Coach\s*-\s*(?P<name>[A-Z][A-Za-z'. -]+)", football["text"])
        if heading_match:
            coach_name = _clean(heading_match.group("name"))

    if not coach_bio and football["text"]:
        bio_candidates: list[str] = []
        for block in football["text"].splitlines():
            cleaned = _clean(block)
            if cleaned and (
                "coach gomes" in cleaned.lower()
                or "head coaching" in cleaned.lower()
                or "johnson high" in cleaned.lower()
                or "river valley high" in cleaned.lower()
            ):
                bio_candidates.append(cleaned)
        bio_candidates = _dedupe_keep_order(bio_candidates)
        coach_bio = bio_candidates[0] if bio_candidates else ""

    if not schedule_link and football["links"]:
        schedule_link = _find_link(football["links"], text="Printable Schedule")

    if not coach_name:
        errors.append("head_coach_name_not_found")
    if not coach_email:
        errors.append("head_coach_email_not_found")
    if not schedule_link:
        errors.append("printable_schedule_link_not_found")

    athletics_hub_url = _find_link(
        home["links"],
        text="FHS ATHLETICS",
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": True,
        "football_team_page_url": football["url"] or FOOTBALL_URL,
        "home_page_football_link": _find_link(home["links"], text="Football"),
        "athletics_hub_url": athletics_hub_url["href"] if athletics_hub_url else "",
        "coach_heading": coach_heading,
        "head_coach": {
            "name": coach_name,
            "bio": coach_bio,
        },
        "head_coach_email": coach_email,
        "printable_schedule": schedule_link,
        "football_links": football_links,
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": PROXY_PROFILE,
            "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "navigation_steps": [
                "goto_home",
                "derive_football_link_from_home",
                "goto_football_page",
                "extract_coach_and_schedule_fields",
            ],
            "page_titles": [home["title"], football["title"]],
            "home_link_count": len(home["links"]),
            "football_link_count": len(football["links"]),
        },
        "errors": errors,
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
