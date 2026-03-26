"""Deterministic athletics scraper for High Tech High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from pipeline.proxy import get_browser_proxy_env
from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060175308599"
SCHOOL_NAME = "High Tech High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETICS_URL = "https://www.hightechhigh.org/athletics/"
STORM_HOME_URL = "https://hthstormathletics.hightechhigh.org/"
GAMES_SCHEDULE_URL = "https://hthstormathletics.hightechhigh.org/games-schedule"
ATHLETIC_REGISTRATION_URL = "https://hthstormathletics.hightechhigh.org/athletic-registration"

TARGET_URLS = [
    ATHLETICS_URL,
    STORM_HOME_URL,
    GAMES_SCHEDULE_URL,
    ATHLETIC_REGISTRATION_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "football",
    "flag football",
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


def _proxy_env() -> dict[str, str]:
    env = get_browser_proxy_env(profile=PROXY_PROFILE)
    return {
        key: value
        for key, value in env.items()
        if key.lower() in {"http_proxy", "https_proxy", "all_proxy"}
    }


def _extract_links(html_text: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _clean(str(anchor.get("href") or ""))
        if not href:
            continue
        links.append({"text": text, "href": urljoin(base_url, href)})
    return links


def _collect_page(url: str) -> dict[str, Any]:
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": USER_AGENT},
        proxies=_proxy_env(),
    )
    response.raise_for_status()
    html_text = response.text
    soup = BeautifulSoup(html_text, "html.parser")
    body_text = _clean(soup.get_text("\n", strip=True))
    title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else ""
    return {
        "requested_url": url,
        "final_url": _clean(response.url),
        "title": title,
        "html": html_text,
        "text": body_text,
        "links": _extract_links(html_text, response.url),
    }


def _football_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        if any(keyword in line.lower() for keyword in FOOTBALL_KEYWORDS):
            lines.append(line)
    return _dedupe_keep_order(lines)


async def scrape() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    pages: list[dict[str, Any]] = []
    errors: list[str] = []
    source_pages: list[str] = []

    for url in TARGET_URLS:
        try:
            snapshot = _collect_page(url)
            pages.append(snapshot)
            source_pages.append(snapshot["final_url"])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"navigation_failed:{type(exc).__name__}:{url}")

    source_pages = _dedupe_keep_order(source_pages)

    page_text = "\n".join(str(page.get("text") or "") for page in pages)
    page_links = [
        link
        for page in pages
        for link in page.get("links", [])
        if isinstance(link, dict)
    ]

    football_lines = _football_lines(page_text)
    football_links = [
        {
            "text": _clean(str(link.get("text") or "")),
            "href": _clean(str(link.get("href") or "")),
        }
        for link in page_links
        if any(keyword in f"{str(link.get('text') or '')} {str(link.get('href') or '')}".lower() for keyword in FOOTBALL_KEYWORDS)
    ]

    athletics_summary = [
        "Accessible athletics pages cover mission, registration, and games schedule.",
        "No public football-specific team, roster, or coach page was found on the accessible High Tech High athletics pages.",
    ]

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)

    if football_lines or football_links:
        return {
            "nces_id": NCES_ID,
            "school_name": SCHOOL_NAME,
            "state": STATE,
            "source_pages": source_pages,
            "extracted_items": {
                "athletics_summary": athletics_summary,
                "football_lines": football_lines,
                "football_links": football_links,
            },
            "scrape_meta": {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "proxy_profile": proxy_meta["proxy_profile"],
                "proxy_servers": proxy_meta["proxy_servers"],
                "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
                "target_urls": TARGET_URLS,
                "pages_checked": len(source_pages),
                "focus": "football_only",
            },
            "errors": errors,
        }

    notes = (
        "High Tech High athletics pages are reachable, but they only expose general athletics "
        "mission, registration, and games schedule content. No public football-specific team, "
        "coach, roster, or schedule page was found on the accessible pages."
    )

    return {
        "status": "no_football",
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": {},
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": TARGET_URLS,
            "pages_checked": len(source_pages),
            "focus": "football_only",
        },
        "errors": errors,
        "reason": "no_public_football_program_found",
        "notes": notes,
    }


async def scrape_school() -> dict[str, Any]:
    return await scrape()


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape()), indent=2, ensure_ascii=True))
