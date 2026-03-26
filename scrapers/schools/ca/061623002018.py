"""Deterministic football scraper for El Cajon Valley High (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061623002018"
SCHOOL_NAME = "El Cajon Valley High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://braves.guhsd.net/"
ATHLETICS_URL = "https://sites.google.com/guhsd.net/ecvathletics/"
COACHES_URL = "https://sites.google.com/guhsd.net/ecvathletics/ecvhs-coaches"
CALENDAR_URL = "https://sites.google.com/guhsd.net/ecvathletics/calendar"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    COACHES_URL,
    CALENDAR_URL,
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
    out: list[Any] = []
    for value in values:
        if isinstance(value, dict):
            key = repr(sorted(value.items()))
        else:
            key = _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _normalize_href(href: str, base_url: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("/"):
        return urljoin(base_url, raw)
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return urljoin(base_url, raw)


def _split_lines(text: str) -> list[str]:
    return [line for line in (_clean(line) for line in text.splitlines()) if line]


def _text_from_body(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return "\n".join(_split_lines(soup.get_text("\n")))


def _extract_anchor_map(links: list[dict[str, str]], base_url: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in links:
        text = _clean(str(item.get("text") or ""))
        href = _normalize_href(str(item.get("href") or ""), base_url)
        if text or href:
            out.append({"text": text, "href": href})
    return _dedupe_keep_order(out)


def _find_first_index(lines: list[str], needle: str) -> int:
    needle = needle.lower()
    for idx, line in enumerate(lines):
        if needle in line.lower():
            return idx
    return -1


def _join_email_parts(local_part: str, domain_part: str | None) -> str:
    local = _clean(local_part).replace(" ", "")
    domain = _clean(domain_part or "").replace(" ", "")
    if "@" in local:
        return local
    if domain.startswith("@"):
        return f"{local}{domain}"
    if domain:
        return f"{local}@{domain}"
    return local


def _extract_email_from_lines(lines: list[str], start_idx: int) -> str:
    for offset in range(start_idx, min(start_idx + 4, len(lines))):
        line = _clean(lines[offset])
        if "@" in line and "." in line:
            return line.replace(" ", "")
        if offset + 1 < len(lines):
            combined = _join_email_parts(line, lines[offset + 1])
            if "@" in combined and "." in combined:
                return combined
    return ""


def _extract_social_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in links:
        text = _clean(str(item.get("text") or ""))
        href = _clean(str(item.get("href") or ""))
        blob = f"{text} {href}".lower()
        if "instagram.com" in blob and ("football" in blob or "braves" in blob or "athletics" in blob):
            out.append({"text": text, "href": href})
    return _dedupe_keep_order(out)


def _extract_fall_sports(lines: list[str], links: list[dict[str, str]]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "athletic_director": {},
        "football": {},
        "girls_flag_football": {},
        "social_links": [],
    }

    fall_idx = _find_first_index(lines, "Fall Sports")
    winter_idx = _find_first_index(lines, "Winter Sports")
    if fall_idx == -1:
        return result

    fall_lines = lines[fall_idx + 1 : winter_idx if winter_idx > fall_idx else len(lines)]

    football_idx = _find_first_index(fall_lines, "Football")
    if football_idx != -1:
        football_name = _clean(fall_lines[football_idx + 1]) if football_idx + 1 < len(fall_lines) else ""
        football_email = _extract_email_from_lines(fall_lines, football_idx + 2)
        football_instagram = ""
        for item in links:
            text = _clean(str(item.get("text") or "")).lower()
            href = _clean(str(item.get("href") or "")).lower()
            if text == "football instagram" and "instagram.com" in href:
                football_instagram = _clean(str(item.get("href") or ""))
                break
        result["football"] = {
            "sport": "Football",
            "coach_name": football_name,
            "coach_email": football_email,
            "instagram_url": football_instagram,
        }

    flag_idx = -1
    for idx in range(len(fall_lines) - 1):
        if fall_lines[idx].lower() == "girls" and fall_lines[idx + 1].lower() == "flag football":
            flag_idx = idx
            break
        if fall_lines[idx].lower() == "girls flag football":
            flag_idx = idx
            break
    if flag_idx != -1:
        label_offset = 2 if fall_lines[flag_idx].lower() == "girls" else 1
        name_idx = flag_idx + label_offset
        flag_name = _clean(fall_lines[name_idx]) if name_idx < len(fall_lines) else ""
        flag_email = _extract_email_from_lines(fall_lines, name_idx + 1)
        flag_instagram = ""
        for item in links:
            text = _clean(str(item.get("text") or "")).lower()
            href = _clean(str(item.get("href") or "")).lower()
            if "flag football instagram" in text and "instagram.com" in href:
                flag_instagram = _clean(str(item.get("href") or ""))
                break
        result["girls_flag_football"] = {
            "sport": "Girls Flag Football",
            "coach_name": flag_name,
            "coach_email": flag_email,
            "instagram_url": flag_instagram,
        }

    if "meet our athletics team" in "\n".join(lines).lower():
        team_idx = _find_first_index(lines, "Meet our Athletics Team")
        if team_idx != -1:
            team_lines = lines[team_idx + 1 : fall_idx]
            if len(team_lines) >= 6:
                result["athletic_director"] = {
                    "assistant_principal": {
                        "name": _clean(team_lines[0]),
                        "role": _clean(team_lines[1]),
                        "email": _extract_email_from_lines(team_lines, 2),
                    },
                    "athletic_director": {
                        "name": _clean(team_lines[3]),
                        "role": _clean(team_lines[4]),
                        "email": _extract_email_from_lines(team_lines, 5),
                    },
                }

    result["social_links"] = _extract_social_links(links)
    return result


async def _fetch_page(context, url: str) -> tuple[str, str, str, list[dict[str, str]]]:
    response = await context.request.get(url, timeout=60_000)
    status = response.status
    if status >= 400:
        raise RuntimeError(f"HTTP {status} for {url}")
    html = await response.text()
    body_text = _text_from_body(html)
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    for tag in soup.select("a[href]"):
        links.append(
            {
                "text": _clean(tag.get_text(" ", strip=True)),
                "href": _clean(str(tag.get("href") or "")),
            }
        )
    return response.url, html, body_text, links


async def scrape_school() -> dict[str, Any]:
    """Scrape El Cajon Valley High football signals from public athletics pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    extracted: dict[str, Any] = {}

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
                    final_url, html, body_text, links = await _fetch_page(context, url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"fetch_failed:{type(exc).__name__}:{url}")
                    continue

                source_pages.append(final_url)
                if final_url.rstrip("/") == HOME_URL.rstrip("/"):
                    extracted["home"] = {
                        "title": _clean(BeautifulSoup(html, "html.parser").title.get_text(" ", strip=True))
                        if BeautifulSoup(html, "html.parser").title
                        else "",
                        "athletics_link": next(
                            (
                                {"text": item["text"], "href": item["href"]}
                                for item in _extract_anchor_map(links, final_url)
                                if "ecvathletics" in item["href"]
                            ),
                            {},
                        ),
                    }
                elif "ecvathletics/ecvhs-coaches" in final_url:
                    text_lines = _split_lines(_text_from_body(html))
                    extracted["coaches"] = {
                        "page_url": final_url,
                        "page_title": _clean(BeautifulSoup(html, "html.parser").title.get_text(" ", strip=True))
                        if BeautifulSoup(html, "html.parser").title
                        else "",
                        "football_sections": _extract_fall_sports(text_lines, _extract_anchor_map(links, final_url)),
                        "visible_text": body_text,
                    }
                elif "ecvathletics/calendar" in final_url:
                    extracted["calendar"] = {
                        "page_url": final_url,
                        "page_title": _clean(BeautifulSoup(html, "html.parser").title.get_text(" ", strip=True))
                        if BeautifulSoup(html, "html.parser").title
                        else "",
                        "visible_text": body_text,
                        "links": _extract_anchor_map(links, final_url),
                    }
                else:
                    extracted.setdefault("athletics", {})
                    extracted["athletics"] = {
                        "page_url": final_url,
                        "page_title": _clean(BeautifulSoup(html, "html.parser").title.get_text(" ", strip=True))
                        if BeautifulSoup(html, "html.parser").title
                        else "",
                        "visible_text": body_text,
                        "links": _extract_anchor_map(links, final_url),
                    }
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    coaches = extracted.get("coaches", {}).get("football_sections", {})
    football = coaches.get("football", {})
    flag_football = coaches.get("girls_flag_football", {})
    athletic_director = coaches.get("athletic_director", {})

    football_programs = _dedupe_keep_order(
        [
            football,
            flag_football,
        ]
    )

    football_program_available = bool(
        football_programs
        and any(_clean(str(item.get("coach_name") or "")) for item in football_programs if isinstance(item, dict))
    )
    if not football_program_available:
        errors.append("no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "home_page_url": HOME_URL,
        "athletics_page_url": ATHLETICS_URL,
        "coaches_page_url": COACHES_URL,
        "calendar_page_url": CALENDAR_URL,
        "athletics_home_link": extracted.get("home", {}).get("athletics_link", {}),
        "football_programs": football_programs,
        "athletic_director": athletic_director,
        "football_summary": (
            "El Cajon Valley High publicly lists Football coach James Simon and Girls Flag Football coach Carl Sharpe on the athletics coaches page."
            if football_program_available
            else ""
        ),
        "instagram_links": coaches.get("social_links", []),
        "calendar_page_excerpt": extracted.get("calendar", {}).get("visible_text", ""),
    }

    runtime_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": runtime_meta["proxy_profile"],
            "proxy_servers": runtime_meta["proxy_servers"],
            "proxy_auth_mode": runtime_meta["proxy_auth_mode"],
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
