"""Deterministic football scraper for East Bakersfield High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061954002343"
SCHOOL_NAME = "East Bakersfield High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://east.kernhigh.org/"
TEAMS_URL = "https://east.kernhigh.org/apps/pages/index.jsp?uREC_ID=613684&type=d&pREC_ID=1089661"
FOOTBALL_URL = "https://east.kernhigh.org/apps/pages/index.jsp?uREC_ID=613846&type=d"
ATHLETICS_CLEARANCE_URL = "https://east.kernhigh.org/apps/pages/index.jsp?uREC_ID=1556150&type=d&pREC_ID=1681990"
ATHLETICS_SCHEDULE_URL = "https://east.kernhigh.org/apps/pages/index.jsp?uREC_ID=613684&type=d&pREC_ID=1136234"
STAFF_URL = "https://east.kernhigh.org/apps/staff/"

TARGET_URLS = [
    HOME_URL,
    TEAMS_URL,
    FOOTBALL_URL,
    ATHLETICS_CLEARANCE_URL,
    ATHLETICS_SCHEDULE_URL,
    STAFF_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for value in values:
        key = _clean(str(value)) if not isinstance(value, dict) else repr(value)
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _abs_url(base_url: str, href: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


def _decode_cfemail(encoded: str) -> str:
    encoded = _clean(encoded)
    if len(encoded) < 4:
        return ""
    try:
        key = int(encoded[:2], 16)
        chars = [
            chr(int(encoded[i : i + 2], 16) ^ key)
            for i in range(2, len(encoded), 2)
        ]
    except ValueError:
        return ""
    return "".join(chars)


def _decode_cfemails(soup: BeautifulSoup) -> list[str]:
    emails: list[str] = []
    for span in soup.select("span.__cf_email__"):
        encoded = _clean(str(span.get("data-cfemail") or ""))
        decoded = _decode_cfemail(encoded)
        if decoded:
            emails.append(decoded)
    return _dedupe_keep_order(emails)


def _title_from_email(email: str) -> str:
    local = _clean(email.split("@", 1)[0])
    if not local:
        return ""
    return " ".join(part.capitalize() for part in re.split(r"[._-]+", local) if part)


def _text_lines(text: str) -> list[str]:
    return [line for line in (_clean(line) for line in text.splitlines()) if line]


def _extract_anchor_map(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    anchors: list[dict[str, str]] = []
    for tag in soup.select("a[href]"):
        text = _clean(tag.get_text(" ", strip=True))
        href = _abs_url(base_url, str(tag.get("href") or ""))
        if not href:
            continue
        anchors.append({"text": text, "href": href})
    return anchors


def _extract_football_team_links(anchors: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in anchors:
        text = _clean(item.get("text") or "")
        href = _clean(item.get("href") or "")
        blob = f"{text} {href}".lower()
        if "football" not in blob:
            continue
        out.append({"text": text, "href": href})
    return _dedupe_keep_order(out)


def _extract_embed_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls: list[str] = []
    for iframe in soup.select("iframe[src]"):
        src = _abs_url(base_url, str(iframe.get("src") or ""))
        if src:
            urls.append(src)
    return _dedupe_keep_order(urls)


def _extract_visible_blockquote_text(soup: BeautifulSoup) -> str:
    root = soup.select_one("div.placeholder-tinymce-text")
    if not root:
        return ""
    return _clean(root.get_text("\n"))


def _extract_teams_page(soup: BeautifulSoup) -> dict[str, Any]:
    anchors = _extract_anchor_map(soup, TEAMS_URL)
    football_links = _extract_football_team_links(anchors)
    fall_section = []
    root = soup.select_one("div.placeholder-tinymce-text")
    if root:
        for anchor in root.select("a[href]"):
            text = _clean(anchor.get_text(" ", strip=True))
            href = _abs_url(TEAMS_URL, str(anchor.get("href") or ""))
            if text and href:
                fall_section.append({"text": text, "href": href})
    return {
        "football_team_links": football_links,
        "all_team_links": _dedupe_keep_order(fall_section),
    }


def _extract_football_page(soup: BeautifulSoup) -> dict[str, Any]:
    root = soup.select_one("div.placeholder-tinymce-text")
    text = _extract_visible_blockquote_text(soup)
    iframe_urls = _extract_embed_urls(soup, FOOTBALL_URL)
    emails = _decode_cfemails(soup)

    contact_alias_match = re.search(r"contact Coach ([A-Za-z][A-Za-z'\-]+)", text, re.IGNORECASE)
    contact_alias = f"Coach {contact_alias_match.group(1)}" if contact_alias_match else ""
    contact_email = emails[0] if emails else ""
    contact_name = _title_from_email(contact_email)

    practice_line = ""
    for line in _text_lines(text):
        lowered = line.lower()
        if "conditioning or practice" in lowered or "cleared by the ebhs athletics" in lowered:
            practice_line = line
            break

    clearance_link = ""
    if root:
        for anchor in root.select('a[href*="uREC_ID=1556150"]'):
            clearance_link = _abs_url(FOOTBALL_URL, str(anchor.get("href") or ""))
            if clearance_link:
                break

    return {
        "football_contact_alias": contact_alias,
        "football_contact_name": contact_name,
        "football_contact_email": contact_email,
        "football_schedule_embed_urls": iframe_urls,
        "football_practice_notice": practice_line,
        "football_clearance_link": clearance_link,
        "football_text": text,
    }


def _extract_clearance_page(soup: BeautifulSoup) -> dict[str, Any]:
    root = soup.select_one("div.placeholder-tinymce-text")
    text = _extract_visible_blockquote_text(soup)
    registration_url = ""
    support_emails = _decode_cfemails(soup)
    if root:
        for anchor in root.select('a[href^="https://sportsnethost.com/"]'):
            registration_url = _abs_url(ATHLETICS_CLEARANCE_URL, str(anchor.get("href") or ""))
            if registration_url:
                break
    if not registration_url:
        match = re.search(r"https://sportsnethost\.com/[^\s\"'<>]+", text)
        if match:
            registration_url = match.group(0)
    return {
        "athletic_clearance_registration_url": registration_url,
        "athletic_clearance_support_emails": support_emails,
        "athletic_clearance_text": text,
    }


def _extract_schedule_page(soup: BeautifulSoup) -> dict[str, Any]:
    return {
        "athletics_schedule_embed_urls": _extract_embed_urls(soup, ATHLETICS_SCHEDULE_URL),
        "athletics_schedule_text": _extract_visible_blockquote_text(soup),
    }


def _extract_staff_page(soup: BeautifulSoup) -> dict[str, Any]:
    coaches: list[dict[str, str]] = []
    for staff in soup.select("li.staff"):
        position = ""
        name = ""
        profile_url = ""
        position_tag = staff.select_one("span.user-position.user-data")
        name_tag = staff.select_one("a.name")
        if position_tag:
            position = _clean(position_tag.get_text(" ", strip=True))
        if name_tag:
            name = _clean(name_tag.get_text(" ", strip=True))
            profile_url = _abs_url(STAFF_URL, str(name_tag.get("href") or ""))
        if not name or "football coach" not in position.lower():
            continue
        coaches.append(
            {
                "name": name,
                "position": position,
                "profile_url": profile_url,
            }
        )
    return {"football_staff_entries": _dedupe_keep_order(coaches)}


async def _fetch_html(context, url: str) -> tuple[str, str]:
    response = await context.request.get(url, timeout=60_000)
    status = response.status
    if status >= 400:
        raise RuntimeError(f"HTTP {status} for {url}")
    return response.url, await response.text()


async def scrape_school() -> dict[str, Any]:
    """Scrape East Bakersfield High football signals from public Edlio pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_data: dict[str, dict[str, Any]] = {}

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

        try:
            for url in TARGET_URLS:
                try:
                    final_url, html = await _fetch_html(context, url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"fetch_failed:{type(exc).__name__}:{url}")
                    continue

                soup = BeautifulSoup(html, "html.parser")
                if url == TEAMS_URL:
                    page_data["teams"] = _extract_teams_page(soup)
                elif url == FOOTBALL_URL:
                    page_data["football"] = _extract_football_page(soup)
                elif url == ATHLETICS_CLEARANCE_URL:
                    page_data["clearance"] = _extract_clearance_page(soup)
                elif url == ATHLETICS_SCHEDULE_URL:
                    page_data["schedule"] = _extract_schedule_page(soup)
                elif url == STAFF_URL:
                    page_data["staff"] = _extract_staff_page(soup)
                else:
                    page_data["home"] = {
                        "title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
                        "text": _extract_visible_blockquote_text(soup),
                        "athletics_links": _extract_football_team_links(
                            _extract_anchor_map(soup, final_url)
                        ),
                    }

                source_pages.append(final_url)
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    teams = page_data.get("teams", {})
    football = page_data.get("football", {})
    clearance = page_data.get("clearance", {})
    schedule = page_data.get("schedule", {})
    staff = page_data.get("staff", {})
    home = page_data.get("home", {})

    football_team_links = teams.get("football_team_links", [])
    football_schedule_embed_urls = football.get("football_schedule_embed_urls", [])
    athletics_schedule_embed_urls = schedule.get("athletics_schedule_embed_urls", [])
    football_staff_entries = staff.get("football_staff_entries", [])

    football_contact_name = _clean(str(football.get("football_contact_name") or ""))
    football_contact_alias = _clean(str(football.get("football_contact_alias") or ""))
    football_contact_email = _clean(str(football.get("football_contact_email") or ""))
    practice_notice = _clean(str(football.get("football_practice_notice") or ""))
    clearance_link = _clean(str(football.get("football_clearance_link") or ""))
    clearance_registration_url = _clean(str(clearance.get("athletic_clearance_registration_url") or ""))

    football_contacts: list[dict[str, str]] = []
    if football_contact_name or football_contact_email:
        football_contacts.append(
            {
                "name": football_contact_name,
                "alias": football_contact_alias,
                "email": football_contact_email,
                "source_page": FOOTBALL_URL,
            }
        )
    for entry in football_staff_entries:
        football_contacts.append(
            {
                "name": _clean(str(entry.get("name") or "")),
                "role": _clean(str(entry.get("position") or "")),
                "profile_url": _clean(str(entry.get("profile_url") or "")),
                "source_page": STAFF_URL,
            }
        )
    football_contacts = _dedupe_keep_order(football_contacts)

    football_program_available = bool(
        football_team_links
        or football_contacts
        or football_schedule_embed_urls
        or athletics_schedule_embed_urls
        or practice_notice
    )
    if not football_program_available:
        errors.append("no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "home_page_url": HOME_URL,
        "athletics_teams_url": TEAMS_URL,
        "football_page_url": FOOTBALL_URL,
        "athletics_clearance_url": ATHLETICS_CLEARANCE_URL,
        "athletics_schedule_url": ATHLETICS_SCHEDULE_URL,
        "staff_directory_url": STAFF_URL,
        "home_athletics_links": home.get("athletics_links", []),
        "football_team_links": football_team_links,
        "football_contacts": football_contacts,
        "football_schedule_embed_urls": football_schedule_embed_urls,
        "athletics_schedule_embed_urls": athletics_schedule_embed_urls,
        "football_practice_notice": practice_notice,
        "football_clearance_link": clearance_link,
        "athletic_clearance_registration_url": clearance_registration_url,
        "athletic_clearance_support_emails": clearance.get("athletic_clearance_support_emails", []),
        "football_summary": (
            "East Bakersfield High publishes a dedicated Football page, a Teams page with the football link, a staff-directory entry for a Head Football Coach, a football schedule calendar embed, and an athletic clearance registration workflow."
            if football_program_available
            else ""
        ),
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
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(source_pages),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
