"""Deterministic football scraper for Hueneme High School (CA)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062927004519"
SCHOOL_NAME = "Hueneme High School"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.huenemehigh.us"
ATHLETICS_URL = "https://www.huenemehigh.us/athletics"
TARGET_URLS = [HOME_URL, ATHLETICS_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


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


def _normalize_href(href: str) -> str:
    href = _clean(href)
    if href.startswith("//"):
        return f"https:{href}"
    return href


def _text(element: Any) -> str:
    if element is None:
        return ""
    return _clean(element.get_text(" ", strip=True))


def _parse_role_contact(name: str, role: str, email: str) -> dict[str, str]:
    return {
        "name": name,
        "role": role,
        "email": email,
    }


def _extract_athletics_contacts(soup: BeautifulSoup) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    for paragraph in soup.select("p"):
        role_el = paragraph.find("em")
        if not role_el:
            continue

        role = _text(role_el)
        if not role:
            continue

        role_lower = role.lower()
        if "athletics director" not in role_lower and "athletics trainer" not in role_lower:
            continue

        strong = paragraph.find("strong")
        name = _text(strong)
        if not name:
            continue

        email_anchor = paragraph.find("a", href=lambda href: isinstance(href, str) and href.lower().startswith("mailto:"))
        email = _clean(str(email_anchor.text)) if email_anchor else ""
        if not email and email_anchor and isinstance(email_anchor.get("href"), str):
            email = _clean(email_anchor.get("href")[7:])

        contacts.append(_parse_role_contact(name=name, role=role, email=email))

    return contacts


def _extract_sports_table(soup: BeautifulSoup) -> list[dict[str, Any]]:
    sports: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    current_season = ""

    for row in soup.select("table tr"):
        columns = [
            _clean(col.get_text(" ", strip=True))
            for col in row.find_all(["th", "td"])
        ]
        if not columns:
            continue

        first = columns[0].lower()
        if not first:
            continue

        if "fall sports" in first:
            current_season = "fall"
            continue
        if "winter sports" in first:
            current_season = "winter"
            continue
        if "spring sports" in first:
            current_season = "spring"
            continue

        if len(columns) < 2:
            continue
        if first in {"sports", "coach", "contact information", "sport"}:
            continue
        if "coach" in first and "contact" in (columns[1].lower() if len(columns) > 1 else ""):
            continue

        sport = _clean(columns[0])
        coach = _clean(columns[1]) if len(columns) > 1 else ""
        contact = _clean(columns[2]) if len(columns) > 2 else ""

        row_cells = row.find_all(["th", "td"])
        if len(row_cells) >= 3:
            email_anchor = row_cells[2].find("a", href=lambda href: isinstance(href, str) and href.lower().startswith("mailto:"))
            if email_anchor:
                contact_email = _clean(email_anchor.get_text(" ", strip=True))
                if not contact_email:
                    contact_email = _clean(str(email_anchor.get("href") or "")[7:])
                if contact_email:
                    contact = contact_email

        key = (sport.lower(), coach.lower())
        if key in seen:
            continue
        seen.add(key)

        sports.append(
            {
                "sport": sport,
                "season": current_season,
                "coach": coach,
                "contact": contact,
            }
        )

    return sports


def _extract_resource_links(soup: BeautifulSoup) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        href = _normalize_href(anchor.get("href") or "")
        if not href:
            continue

        text = _text(anchor)
        if not text and href:
            text = href

        if any(token in (href + " " + text).lower() for token in ("maxpreps", "sports roster", "athletics", "schedule")):
            links.append({"text": text, "url": href})

    return _dedupe_keep_order([f"{item['text']}|{item['url']}" for item in links])


def _extract_football_rows(sports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in sports if row.get("sport", "").lower() == "football"]


def _contains_football(text: str) -> bool:
    return bool(re.search(r"\bfootball\b", text, flags=re.I))


def _find_football_evidence(text: str) -> list[str]:
    output: list[str] = []
    lines = text.splitlines()
    for line in lines:
        clean = _clean(line)
        if not clean:
            continue
        if re.search(r"\bfootball\b", clean, flags=re.I):
            output.append(clean)
    return _dedupe_keep_order(output)


async def _collect_page(page: Any, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1200)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    body_text = _clean(await page.inner_text("body"))

    return {
        "requested_url": url,
        "url": page.url,
        "title": _clean(await page.title()),
        "html": html,
        "soup": soup,
        "text": body_text,
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    home_data: dict[str, Any] = {}
    athletics_data: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            home_data = await _collect_page(page, HOME_URL)
            source_pages.append(home_data["url"])

            athletics_data = await _collect_page(page, ATHLETICS_URL)
            source_pages.append(athletics_data["url"])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await context.close()
            await browser.close()

    if not athletics_data:
        errors.append("blocked:no_public_athletics_page_loaded")
        return {
            "nces_id": NCES_ID,
            "school_name": SCHOOL_NAME,
            "state": STATE,
            "source_pages": source_pages,
            "extracted_items": {
                "football_program_available": False,
                "athletics_page_url": ATHLETICS_URL,
            },
            "scrape_meta": {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "script_version": "1.0.0",
                "focus": "football_only",
                "target_urls": TARGET_URLS,
            },
            "errors": errors,
        }

    athletics_soup = athletics_data["soup"]
    athletics_text = str(athletics_data["text"])
    sports = _extract_sports_table(athletics_soup)
    football_sport_rows = _extract_football_rows(sports)
    football_evidence = _find_football_evidence(athletics_text)
    contacts = _extract_athletics_contacts(athletics_soup)
    resource_links = _extract_resource_links(athletics_soup)

    football_available = bool(football_sport_rows or _contains_football(athletics_text))
    if not football_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_team = football_sport_rows[0] if football_sport_rows else {
        "sport": "Football",
        "season": "fall",
        "coach": "",
        "contact": "",
    }

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "focus": "football_only",
            "navigation_steps": ["home", "athletics"],
        }
    )

    extracted_items: dict[str, Any] = {
        "school_profile": {
            "website": ATHLETICS_URL,
            "home_title": home_data.get("title", ""),
            "athletics_title": athletics_data.get("title", ""),
        },
        "football_program_available": football_available,
        "athletics_page_url": athletics_data.get("url", ATHLETICS_URL),
        "home_page_url": home_data.get("url", HOME_URL),
        "sports_offered_count": len(sports),
        "sports": sports,
        "football_team": {
            "sport": football_team.get("sport", "Football"),
            "season": football_team.get("season", ""),
            "coach": football_team.get("coach", ""),
            "contact": football_team.get("contact", ""),
        },
        "football_evidence": football_evidence[:20],
        "athletics_contacts": contacts,
        "related_links": resource_links,
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


async def main() -> None:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
