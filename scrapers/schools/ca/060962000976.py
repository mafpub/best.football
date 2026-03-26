"""Deterministic football scraper for Compton High (CA)."""

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

NCES_ID = "060962000976"
SCHOOL_NAME = "Compton High"
STATE = "CA"

PROXY_PROFILE = "datacenter"
TEAM_URL = "https://www.maxpreps.com/ca/compton/compton-tarbabes/football/"
STAFF_URL = "https://www.maxpreps.com/ca/compton/compton-tarbabes/football/staff/"
TARGET_URLS = [TEAM_URL, STAFF_URL]

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
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


async def _collect_snapshot(page) -> dict[str, Any]:
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: e.href || ''
        }))""",
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

    return {
        "title": _clean(await page.title() or ""),
        "url": page.url,
        "text": _clean(await page.inner_text("body")),
        "links": normalized_links,
    }


def _extract_team_name(text: str) -> str:
    match = re.search(r"C\s+(.+?Varsity Football)\s+Compton,\s*CA", text)
    if match:
        return _clean(match.group(1))
    return "Compton Tarbabes Varsity Football"


def _extract_record(text: str) -> dict[str, str]:
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


def _extract_address(text: str) -> str:
    match = re.search(r"601\s+S\s+Acacia\s+Ave\s+Compton,\s*CA\s+90220-3702", text)
    if match:
        return _clean(match.group(0))
    return ""


def _extract_staff(text: str) -> list[dict[str, str]]:
    match = re.search(
        r"Staff Position\s+(.*?)\s+Roster last updated",
        text,
        flags=re.DOTALL,
    )
    if not match:
        return []

    chunk = _clean(match.group(1))
    tokens = chunk.split()
    coaches: list[dict[str, str]] = []
    i = 0
    current_name: list[str] = []
    while i < len(tokens):
        if i + 1 < len(tokens) and tokens[i] == "Head" and tokens[i + 1] == "Coach":
            if current_name:
                coaches.append({"name": " ".join(current_name), "role": "Head Coach"})
            current_name = []
            i += 2
            continue
        if i + 1 < len(tokens) and tokens[i] == "Assistant" and tokens[i + 1] == "Coach":
            if current_name:
                coaches.append({"name": " ".join(current_name), "role": "Assistant Coach"})
            current_name = []
            i += 2
            continue

        current_name.append(tokens[i])
        i += 1

    if current_name:
        trailing_name = " ".join(current_name).strip()
        if trailing_name and trailing_name not in {"Staff", "Position"}:
            coaches.append({"name": trailing_name, "role": "Assistant Coach"})

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for coach in coaches:
        key = (coach["name"], coach["role"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(coach)
    return deduped


def _extract_current_schedule_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    exact = [
        link
        for link in links
        if re.search(r"/football/schedule/?$", link["href"])
    ]
    if exact:
        return exact

    fallback = [
        link
        for link in links
        if "/football/" in link["href"].lower() and "/schedule/" in link["href"].lower()
    ]
    return fallback


def _dedupe_link_dicts(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        key = (text, href)
        if not href or key in seen:
            continue
        seen.add(key)
        out.append({"text": text, "href": href})
    return out


def _extract_links(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    links = snapshot.get("links")
    if not isinstance(links, list):
        return []
    out: list[dict[str, str]] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        href = _clean(str(link.get("href") or ""))
        if not href:
            continue
        out.append(
            {
                "text": _clean(str(link.get("text") or "")),
                "href": href,
            }
        )
    return out


async def scrape_school() -> dict[str, Any]:
    """Scrape public football data for Compton High from MaxPreps."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []
    snapshots: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 920},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(TEAM_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            navigation_steps.append("visit_team")
            snapshots.append(await _collect_snapshot(page))

            await page.goto(STAFF_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            navigation_steps.append("visit_staff")
            snapshots.append(await _collect_snapshot(page))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    team_snapshot = snapshots[0] if snapshots else {}
    staff_snapshot = snapshots[-1] if snapshots else {}
    team_text = str(team_snapshot.get("text") or "")
    staff_text = str(staff_snapshot.get("text") or "")
    combined_text = "\n".join([team_text, staff_text])
    combined_links = _extract_links(team_snapshot) + _extract_links(staff_snapshot)

    team_name = _extract_team_name(team_text or staff_text)
    record = _extract_record(combined_text)
    schedule_links = _dedupe_link_dicts(_extract_current_schedule_links(combined_links))
    team_links = [
        link
        for link in combined_links
        if link["href"].rstrip("/").endswith("/football")
        or "/compton-tarbabes/football/" in link["href"].lower()
    ]
    team_links = _dedupe_link_dicts(team_links)

    coaches = _extract_staff(staff_text)
    if not coaches:
        errors.append("blocked:no_public_football_staff_found")

    football_mentions = _dedupe_keep_order(
        [
            line
            for line in combined_text.splitlines()
            if "football" in line.lower()
            or "compton tarbabes" in line.lower()
            or "head coach" in line.lower()
            or "assistant coach" in line.lower()
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(coaches or record or team_links),
        "football_team_name": team_name,
        "football_team_home_url": TEAM_URL,
        "football_staff_url": STAFF_URL,
        "football_schedule_urls": schedule_links,
        "football_team_links": team_links,
        "team_record": record,
        "football_coaches": coaches,
        "school_address": _extract_address(combined_text),
        "football_mentions": football_mentions,
        "navigation_steps": navigation_steps,
        "source_line_count": len(combined_text.splitlines()),
    }

    if not extracted_items["football_program_available"]:
        errors.append("blocked:no_public_football_content_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_profile"],
            "proxy_servers": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_servers"],
            "proxy_auth_mode": get_proxy_runtime_meta(PROXY_PROFILE)["proxy_auth_mode"],
            "focus": "football_only",
            "pages_requested": TARGET_URLS,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_steps,
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
