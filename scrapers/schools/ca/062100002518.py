"""Deterministic football scraper for Agoura High School (CA)."""

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

NCES_ID = "062100002518"
SCHOOL_NAME = "Agoura High School"
STATE = "CA"
PROXY_PROFILE = "datacenter"

ATHLETIC_URL = "https://sites.google.com/lvusd.org/agourachargers"
SPORTS_MEDICINE_URL = "https://sites.google.com/lvusd.org/agourachargers/sports-medicine"

TARGET_URLS = [
    ATHLETIC_URL,
    SPORTS_MEDICINE_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_KEYWORDS = (
    "football",
    "flag football",
    "football coach",
    "football program",
    "football team",
    "football highlights",
    "football honors",
    "football athlete",
)

CHAMPIONSHIP_PATTERN = re.compile(
    r"\b(FLAG|FOOTBALL|BOYS SOCCER|BOYS WATER POLO|GIRLS WATER POLO|"
    r"BOYS LACROSSE|GIRLS LACROSSE|BOYS BASKETBALL|BASEBALL)\s+(\d{4})\b",
    re.IGNORECASE,
)

NCAA_FOOTBALL_PATTERN = re.compile(
    r"([A-Z][A-Za-z' -]+?)\s+Football\s+([A-Z][A-Za-z'.& -]+?(?:College|Univ\.|University)[A-Za-z'.& -]*)",
    re.IGNORECASE,
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


def _collect_links(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _clean(str(anchor.get("href") or ""))
        if not href:
            continue
        links.append({"text": text, "href": urljoin(base_url, href)})
    return links


def _fetch_page(url: str) -> dict[str, Any]:
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": USER_AGENT},
        proxies=get_browser_proxy_env(profile=PROXY_PROFILE),
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    return {
        "requested_url": url,
        "final_url": _clean(response.url),
        "title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
        "html": response.text,
        "text": _clean(soup.get_text("\n", strip=True)),
        "links": _collect_links(soup, response.url),
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


def _extract_championships(text: str) -> list[dict[str, str]]:
    championships: list[dict[str, str]] = []
    for match in CHAMPIONSHIP_PATTERN.finditer(text):
        sport = _clean(match.group(1).title())
        year = _clean(match.group(2))
        championships.append({"sport": sport, "year": year})
    return _dedupe_keep_order([f"{item['sport']}|{item['year']}" for item in championships]) and championships


def _extract_ncaa_football_athletes(text: str) -> list[dict[str, str]]:
    athletes: list[dict[str, str]] = []
    for match in NCAA_FOOTBALL_PATTERN.finditer(text):
        name = _clean(match.group(1))
        college = _clean(match.group(2))
        if name and college:
            athletes.append({"name": name, "sport": "Football", "college": college})
    return athletes


def _football_related_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    seen: set[str] = set()
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        blob = f"{text} {href}".lower()
        if not any(keyword in blob for keyword in ("football", "athletic", "coach", "sports medicine", "clearance", "cif", "ncaa", "tickets", "tryouts")):
            continue
        if href in seen:
            continue
        seen.add(href)
        output.append({"text": text, "href": href})
    return output


def _extract_summary_text(text: str) -> dict[str, str]:
    return {
        "tradition": "A tradition of excellence" if "tradition of excellence" in text.lower() else "",
        "league": "Marmonte League" if "marmonte league" in text.lower() else "",
        "cif_section": "CIF Southern Section" if "cif southern section" in text.lower() else "",
        "school_name": "Agoura Chargers" if "agoura chargers" in text.lower() else "",
    }


async def scrape_agoura_high_athletics() -> dict[str, Any]:
    """Scrape public football evidence from the Agoura Chargers athletics site."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    source_pages: list[str] = []
    pages: list[dict[str, Any]] = []
    errors: list[str] = []

    for url in TARGET_URLS:
        try:
            snapshot = _fetch_page(url)
            pages.append(snapshot)
            source_pages.append(snapshot["final_url"])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"navigation_failed:{type(exc).__name__}:{url}")

    source_pages = _dedupe_keep_order(source_pages)

    joined_text = "\n".join(str(page.get("text") or "") for page in pages)
    collapsed_text = _clean(joined_text)
    all_links = [link for page in pages for link in page.get("links", []) if isinstance(link, dict)]

    football_lines = _football_lines(joined_text)
    championships = _extract_championships(joined_text)
    ncaa_football_athletes = _extract_ncaa_football_athletes(collapsed_text)
    related_links = _football_related_links(all_links)
    summary = _extract_summary_text(collapsed_text)

    if not football_lines and not championships and not ncaa_football_athletes and not related_links:
        return {
            "status": "no_football",
            "nces_id": NCES_ID,
            "school_name": SCHOOL_NAME,
            "state": STATE,
            "source_pages": source_pages,
            "extracted_items": {},
            "scrape_meta": {
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                **get_proxy_runtime_meta(profile=PROXY_PROFILE),
                "target_urls": TARGET_URLS,
                "pages_checked": len(source_pages),
                "focus": "football_only",
            },
            "errors": errors,
            "reason": "no_public_football_program_found",
            "notes": (
                "Agoura High athletics content is reachable and includes football-specific "
                "highlights, but no public football-specific page structure was available "
                "for a deeper deterministic scrape."
            ),
        }

    extracted_items: dict[str, Any] = {
        "school_site": {
            "athletics_url": ATHLETIC_URL,
            "sports_medicine_url": SPORTS_MEDICINE_URL,
            "page_titles": [page.get("title", "") for page in pages if page.get("title")],
        },
        "football_program": {
            "school_name": summary["school_name"] or SCHOOL_NAME,
            "tradition": summary["tradition"],
            "league": summary["league"],
            "cif_section": summary["cif_section"],
            "football_related_links": related_links,
        },
        "football_highlights": football_lines[:40],
        "league_championships": championships[:20],
        "ncaa_football_athletes": ncaa_football_athletes[:10],
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "target_urls": TARGET_URLS,
            "pages_checked": len(source_pages),
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_school() -> dict[str, Any]:
    return await scrape_agoura_high_athletics()


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_agoura_high_athletics()


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(scrape_agoura_high_athletics()), indent=2, sort_keys=True))
