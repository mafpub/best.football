"""Deterministic football scraper for Colusa High School (CA)."""

from __future__ import annotations

import io
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from pypdf import PdfReader

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060957000966"
SCHOOL_NAME = "Colusa High"
STATE = "CA"

PROXY_PROFILE = "datacenter"
SCHOOL_HOME_URL = "https://colusahigh.colusa.k12.ca.us/"
ATHLETICS_URL = "https://colusahigh.colusa.k12.ca.us/Athletics/index.html"
HANDBOOK_URL = (
    "https://d16k74nzx9emoe.cloudfront.net/e9485daf-30e7-48a8-973f-c5a92a036816/"
    "Colusa%20HS%20Athletic%20Handbook%202025-2026.pdf"
)
MAXPREPS_SCHOOL_URL = "https://www.maxpreps.com/ca/colusa/colusa-redhawks/"
MAXPREPS_FOOTBALL_URL = "https://www.maxpreps.com/ca/colusa/colusa-redhawks/football/"

TARGET_URLS = [
    SCHOOL_HOME_URL,
    ATHLETICS_URL,
    HANDBOOK_URL,
    MAXPREPS_SCHOOL_URL,
    MAXPREPS_FOOTBALL_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _extract_pdf_text(payload: bytes) -> str:
    reader = PdfReader(io.BytesIO(payload))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _extract_links_from_html(html: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = urljoin(base_url, anchor.get("href", "").strip())
        if href:
            links.append({"text": text, "href": href})
    return links


def _dedupe_link_dicts(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    output: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        key = (text, href)
        if not href or key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


def _extract_keyword_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        lowered = line.lower()
        if line and any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _parse_maxpreps_record(text: str) -> dict[str, str]:
    match = re.search(
        r"Overall\s+([0-9-]+)\s+League\s+([0-9-]+(?:\s+\([^)]+\))?)\s+NAT Rank\s+([0-9]+)\s+CA Rank\s+([0-9]+)",
        text,
    )
    if not match:
        return {}
    return {
        "overall_record": _clean(match.group(1)),
        "league_record": _clean(match.group(2)),
        "national_rank": _clean(match.group(3)),
        "california_rank": _clean(match.group(4)),
    }


def _parse_schedule_glance(text: str) -> list[str]:
    match = re.search(
        r"Schedule at a Glance\s+(.*?)\s+Full Schedule\s+Rankings",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return []
    chunk = match.group(1)
    parts = [
        _clean(part)
        for part in re.split(r"(?=(?:Fri|Sat|Thu|Mon|Tue|Wed),\s+\d{1,2}/\d{1,2})", chunk)
        if _clean(part)
    ]
    return _dedupe_keep_order(parts)


def _parse_team_leaders(text: str) -> list[str]:
    match = re.search(
        r"Team Leaders\s+(.*?)\s+All Player Stats\s+Meet the Team",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return []
    chunk = match.group(1)
    patterns = [
        r"Receiving Yards Per Game\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+•\s+([^0-9]+?)\s+([0-9.]+)\s+Y/G",
        r"Rushing Yards Per Game\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+•\s+([^0-9]+?)\s+([0-9.]+)\s+Y/G",
        r"Total TDs\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+•\s+([^0-9]+?)\s+([0-9.]+)\s+TOT",
        r"Tackles Per Game\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+•\s+([^0-9]+?)\s+([0-9.]+)\s+TCKL/G",
        r"Sacks\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+•\s+([^0-9]+?)\s+([0-9.]+)\s+SAK",
        r"Interceptions\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+•\s+([^0-9]+?)\s+([0-9.]+)\s+INT",
    ]
    lines: list[str] = []
    for pattern in patterns:
        match = re.search(pattern, chunk)
        if match:
            name = _clean(f"{match.group(1)} {match.group(2)}.")
            position = _clean(match.group(3))
            value = _clean(match.group(4))
            label = pattern.split(r"\s+")[0].replace("\\", " ")
            lines.append(f"{label}: {name} ({position}) {value}")
    return _dedupe_keep_order(lines)


def _parse_coaches(text: str) -> list[dict[str, str]]:
    match = re.search(
        r"Meet the Team\s+(.*?)\s+Full Roster",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return []
    tokens = [
        _clean(part)
        for part in match.group(1).splitlines()
        if _clean(part) and "team photo" not in part.lower() and "upload it here" not in part.lower()
    ]
    coaches: list[dict[str, str]] = []
    i = 0
    while i + 1 < len(tokens):
        role = tokens[i + 1]
        if role in {"Head Coach", "Assistant Coach"}:
            coaches.append({"name": tokens[i], "role": role})
            i += 2
            continue
        i += 1
    return coaches


def _parse_stat_leaders(text: str) -> list[str]:
    match = re.search(
        r"Stat Leaders\s+(.*?)\s+More Stat Leaders",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return []
    chunk = match.group(1)
    labels = ("Passing Yards", "Rushing Yards", "Receiving Yards")
    lines: list[str] = []
    for label in labels:
        stat_match = re.search(
            rf"{re.escape(label)}\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+([0-9]+)(?:\s+([A-Za-z.'\- ]+)\s+([A-Za-z]+)\.\s+([0-9]+))?",
            chunk,
        )
        if not stat_match:
            continue
        first = f"{_clean(stat_match.group(1))} {_clean(stat_match.group(2))}. {_clean(stat_match.group(3))}"
        line = f"{label}: {first}"
        if stat_match.group(4) and stat_match.group(5) and stat_match.group(6):
            second = f"{_clean(stat_match.group(4))} {_clean(stat_match.group(5))}. {_clean(stat_match.group(6))}"
            line = f"{line}; {second}"
        lines.append(line)
    return _dedupe_keep_order(lines)


async def scrape_school() -> dict[str, Any]:
    """Scrape public football data for Colusa High from school-hosted pages and MaxPreps."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    athletics_text = ""
    athletics_links: list[dict[str, str]] = []
    handbook_text = ""
    maxpreps_school_text = ""
    maxpreps_football_text = ""
    maxpreps_football_links: list[dict[str, str]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            ignore_https_errors=True,
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            try:
                response = await context.request.get(ATHLETICS_URL, timeout=90000)
                source_pages.append(str(response.url))
                athletics_html = await response.text()
                athletics_text = _clean(BeautifulSoup(athletics_html, "html.parser").get_text("\n", strip=True))
                athletics_links = _extract_links_from_html(athletics_html, ATHLETICS_URL)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"athletics_fetch_failed:{type(exc).__name__}")

            try:
                response = await context.request.get(HANDBOOK_URL, timeout=90000)
                source_pages.append(HANDBOOK_URL)
                handbook_text = _extract_pdf_text(await response.body())
            except Exception as exc:  # noqa: BLE001
                errors.append(f"handbook_fetch_failed:{type(exc).__name__}")

            try:
                response = await page.goto(MAXPREPS_SCHOOL_URL, wait_until="domcontentloaded", timeout=90000)
                if response:
                    source_pages.append(page.url)
                await page.wait_for_timeout(5000)
                maxpreps_school_text = await page.locator("body").inner_text()
            except Exception as exc:  # noqa: BLE001
                errors.append(f"maxpreps_school_fetch_failed:{type(exc).__name__}")

            try:
                response = await page.goto(MAXPREPS_FOOTBALL_URL, wait_until="domcontentloaded", timeout=90000)
                if response:
                    source_pages.append(page.url)
                await page.wait_for_timeout(7000)
                maxpreps_football_text = await page.locator("body").inner_text()
                raw_links = await page.locator("a[href]").evaluate_all(
                    """els => els.map(e => ({
                        text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
                        href: e.href || ""
                    }))"""
                )
                maxpreps_football_links = _dedupe_link_dicts(
                    [
                        {
                            "text": str(entry.get("text") or ""),
                            "href": str(entry.get("href") or ""),
                        }
                        for entry in raw_links
                    ]
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"maxpreps_football_fetch_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    athletics_summary_lines = _extract_keyword_lines(
        athletics_text,
        (
            "athletics",
            "athletic excellence",
            "maxpreps",
            "athletic handbook",
            "athletic registration",
            "sports schedules",
        ),
    )
    handbook_football_lines = _extract_keyword_lines(
        handbook_text,
        (
            "football",
            "concussion",
            "physical",
            "coach",
        ),
    )

    school_athletics_links = _dedupe_link_dicts(
        [
            link
            for link in athletics_links
            if any(
                keyword in f"{link.get('text', '')} {link.get('href', '')}".lower()
                for keyword in (
                    "maxpreps",
                    "athletic handbook",
                    "physical form",
                    "athletic registration",
                )
            )
        ]
    )

    football_links = _dedupe_link_dicts(
        [
            link
            for link in maxpreps_football_links
            if any(
                keyword in f"{link.get('text', '')} {link.get('href', '')}".lower()
                for keyword in (
                    "/football/",
                    "schedule",
                    "roster",
                    "stats",
                    "rankings",
                    "video",
                    "box score",
                )
            )
        ]
    )

    maxpreps_record = _parse_maxpreps_record(maxpreps_football_text)
    schedule_glance = _parse_schedule_glance(maxpreps_football_text)
    coaches = _parse_coaches(maxpreps_football_text)
    team_leader_lines = _parse_team_leaders(maxpreps_football_text)
    stat_leader_lines = _parse_stat_leaders(maxpreps_football_text)

    football_program_available = "varsity football" in maxpreps_football_text.lower()

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_athletics_page": ATHLETICS_URL,
        "school_athletics_summary_lines": athletics_summary_lines[:10],
        "school_athletics_links": school_athletics_links[:12],
        "handbook_url": HANDBOOK_URL,
        "handbook_football_lines": handbook_football_lines[:12],
        "maxpreps_school_url": MAXPREPS_SCHOOL_URL,
        "maxpreps_football_url": MAXPREPS_FOOTBALL_URL,
        "maxpreps_school_summary_lines": _extract_keyword_lines(
            maxpreps_school_text,
            (
                "school sports",
                "football",
                "athletic director",
                "mascot",
                "redhawks",
            ),
        )[:10],
        "football_team_name": "Colusa RedHawks Varsity Football" if football_program_available else "",
        "football_team_level": "Varsity" if football_program_available else "",
        "football_record": maxpreps_record,
        "football_schedule_glance": schedule_glance[:5],
        "football_coaches": coaches,
        "football_team_leaders": team_leader_lines,
        "football_stat_leaders": stat_leader_lines,
        "football_links": football_links[:20],
        "athletic_director": {
            "name": "Eric Lay" if "Athletic Director Eric Lay" in maxpreps_school_text else "",
            "source": MAXPREPS_SCHOOL_URL,
        },
        "mascot": "RedHawks" if "Mascot RedHawks" in maxpreps_school_text else "",
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
            "proxy": get_proxy_runtime_meta(PROXY_PROFILE),
        },
        "errors": errors,
    }
