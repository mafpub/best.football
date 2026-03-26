"""Deterministic football scraper for Borrego Springs High School (CA)."""

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

NCES_ID = "060570000518"
SCHOOL_NAME = "Borrego Springs High School"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://bshs.bsusd.net"
ATHLETICS_URL = "https://bshs.bsusd.net/athletics"
MAXPREPS_SPORTS_URL = "https://www.maxpreps.com/high-schools/borrego-springs-rams-(borrego-springs,ca)/sports.htm"
MAXPREPS_FOOTBALL_URL = (
    "https://www.maxpreps.com/ca/borrego-springs/borrego-springs-rams/football/"
)
MAXPREPS_FOOTBALL_SCHEDULE_URL = (
    "https://www.maxpreps.com/ca/borrego-springs/borrego-springs-rams/football/schedule/"
)

TARGET_PAGES = [
    HOME_URL,
    ATHLETICS_URL,
    MAXPREPS_SPORTS_URL,
    MAXPREPS_FOOTBALL_URL,
    MAXPREPS_FOOTBALL_SCHEDULE_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

SPORT_KEYWORDS = (
    "athletics",
    "athletic",
    "football",
    "schedule",
    "coach",
    "record",
    "rank",
    "team",
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


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


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        if line in lines:
            continue
        lines.append(line)
    return lines


def _is_blocked(title: str, text: str) -> bool:
    lowered_title = _clean(title).lower()
    lowered_text = _clean(text).lower()
    return (
        "403" in lowered_text
        or "forbidden" in lowered_text
        or "cloudflare" in lowered_text
        or "blocked" in lowered_text
        or "attention required" in lowered_text
        or "403" in lowered_title
        or "forbidden" in lowered_title
        or "attention required" in lowered_title
    )


def _find_link(links: list[dict[str, str]], *, text_sub: str | None = None, href_sub: str | None = None) -> str | None:
    for link in links:
        text = (link.get("text") or "").lower()
        href = (link.get("href") or "").lower()
        if text_sub and text_sub.lower() in text:
            return link.get("href")
        if href_sub and href_sub.lower() in href:
            return link.get("href")
    return None


async def _snapshot(page) -> dict[str, Any]:
    body_text = await page.inner_text("body")
    text = _clean(body_text)
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(e => ({
            text: (e.textContent || "").replace(/\\s+/g, " ").trim(),
            href: (e.getAttribute("href") || "").trim(),
            absolute: (e.href || "").trim()
        }))""",
    )
    if not isinstance(links, list):
        links = []
    normalized_links: list[dict[str, str]] = []
    for raw_link in links:
        if not isinstance(raw_link, dict):
            continue
        href = str(raw_link.get("href") or "").strip()
        if not href:
            continue
        normalized_links.append(
            {
                "text": _clean(str(raw_link.get("text") or "")),
                "href": href,
            }
        )
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "text": text,
        "links": normalized_links,
        "blocked": _is_blocked(_clean(await page.title()), text),
    }


def _extract_matches(lines: list[str]) -> dict[str, list[str]]:
    football_lines = _dedupe_keep_order([line for line in lines if "football" in line.lower()])
    record_candidates = [
        line for line in lines if re.search(r"\d{1,2}-\d{1,2}", line) and ("record" in line.lower() or "rank" in line.lower())
    ]
    coach_candidates = [
        line
        for line in lines
        if "coach" in line.lower()
        and any(token in line.lower() for token in ("head coach", "assistant coach", "coaches"))
    ]
    team_candidates = [
        line
        for line in lines
        if "borrego springs rams" in line.lower() or "borrego springs" in line.lower()
    ]
    schedule_candidates = [
        line for line in lines if re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", line.lower())
    ]
    return {
        "football_lines": football_lines,
        "record_lines": _dedupe_keep_order(record_candidates),
        "coach_lines": _dedupe_keep_order(coach_candidates),
        "team_lines": _dedupe_keep_order(team_candidates),
        "schedule_lines": _dedupe_keep_order(schedule_candidates),
    }


async def _safe_click(page, label_pattern: str) -> bool:
    locator = page.get_by_role("link", name=re.compile(label_pattern, re.I)).first
    if await locator.count() == 0:
        return False
    await locator.click(timeout=12000)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(1200)
    return True


async def scrape_school() -> dict[str, Any]:
    """Scrape football-specific public evidence from Borrego Springs High School."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_PAGES, profile=PROXY_PROFILE)

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
            viewport={"width": 1400, "height": 900},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            home_snapshot = await _snapshot(page)
            snapshots.append(home_snapshot)
            source_pages.append(home_snapshot["url"])
            navigation_steps.append("visit_school_home")

            # Human-like route through a school subpage
            if not await _safe_click(page, r"\bAthletics\b"):
                await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                navigation_steps.append("fallback_direct_athletics")
            else:
                navigation_steps.append("click_home_athletics")

            athletics_snapshot = await _snapshot(page)
            snapshots.append(athletics_snapshot)
            source_pages.append(athletics_snapshot["url"])
            navigation_steps.append("visit_athletics_page")

            football_link = _find_link(
                athletics_snapshot["links"],
                text_sub="football",
            )
            if football_link:
                if "http" in football_link:
                    try:
                        await page.goto(football_link, wait_until="domcontentloaded", timeout=90000)
                        await page.wait_for_timeout(1200)
                        navigation_steps.append("open_athletics_football_link")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"goto_football_link_failed:{type(exc).__name__}")
                        football_link = None
                else:
                    errors.append("athletics_football_link_not_absolute")
                    football_link = None

            if not football_link:
                await page.goto(MAXPREPS_FOOTBALL_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1200)
                navigation_steps.append("fallback_direct_maxpreps_football")

            football_snapshot = await _snapshot(page)
            snapshots.append(football_snapshot)
            source_pages.append(football_snapshot["url"])

            # Capture schedule page for stronger evidence.
            clicked_schedule = await _safe_click(page, r"\bSchedule\b")
            if clicked_schedule:
                schedule_snapshot = await _snapshot(page)
                snapshots.append(schedule_snapshot)
                source_pages.append(schedule_snapshot["url"])
                navigation_steps.append("click_maxpreps_schedule_tab")
            else:
                try:
                    await page.goto(MAXPREPS_FOOTBALL_SCHEDULE_URL, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1200)
                    schedule_snapshot = await _snapshot(page)
                    snapshots.append(schedule_snapshot)
                    source_pages.append(schedule_snapshot["url"])
                    navigation_steps.append("fallback_direct_maxpreps_schedule")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"schedule_navigation_failed:{type(exc).__name__}")
                    schedule_snapshot = None
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_flow_failed:{type(exc).__name__}")
            schedule_snapshot = None
            football_snapshot = snapshots[-1] if snapshots else None
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    all_lines: list[str] = []
    for snapshot in snapshots:
        all_lines.extend(_extract_lines(snapshot.get("text", "")))

    home_lines = all_lines
    matches = _extract_matches(home_lines)

    football_links = []
    for snapshot in snapshots:
        for link in snapshot.get("links", []):
            text = str(link.get("text", "")).lower()
            href = str(link.get("href", "")).lower()
            if "football" in text or "football" in href:
                if href:
                    football_links.append({
                        "text": str(link.get("text") or ""),
                        "href": str(link.get("href") or ""),
                    })

    # Include direct and fallback URLs discovered from known selectors.
    if not any("maxpreps.com" in str(link.get("href", "")) for link in football_links):
        football_links.extend(
            [
                {"text": "MaxPreps football fallback", "href": MAXPREPS_FOOTBALL_URL},
                {"text": "MaxPreps football schedule fallback", "href": MAXPREPS_FOOTBALL_SCHEDULE_URL},
            ]
        )

    blocked_detected = any(snapshot.get("blocked") for snapshot in snapshots)
    if blocked_detected:
        errors.append("blocked:site_or_provider_access_block_detected")

    if not matches["football_lines"]:
        errors.append("no_explicit_football_evidence_found")

    extracted_items = {
        "football_program_available": bool(matches["football_lines"]),
        "school_home_pages": [HOME_URL],
        "athletics_page_url": ATHLETICS_URL,
        "maxpreps_sports_url": MAXPREPS_SPORTS_URL,
        "football_team_lines": _dedupe_keep_order(matches["team_lines"]),
        "football_record_lines": _dedupe_keep_order(matches["record_lines"]),
        "football_coach_lines": _dedupe_keep_order(matches["coach_lines"]),
        "football_schedule_lines": _dedupe_keep_order(matches["schedule_lines"])[:18],
        "football_links": _dedupe_keep_order([f"{item['text']}|{item['href']}" for item in football_links]),
        "football_keywords_present": _dedupe_keep_order(
            [line for line in _extract_lines("\n".join(matches["football_lines"])) if "football" in line.lower()]
        ),
        "navigation_log": navigation_steps,
        "block_signature": [
            {
                "url": snapshot.get("url", ""),
                "title": snapshot.get("title", ""),
                "blocked": snapshot.get("blocked"),
            }
            for snapshot in snapshots
        ],
        "source_line_count": len(all_lines),
    }

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "pages_requested": TARGET_PAGES,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_steps,
            "verification_focus": "school_athletics_navigation_then_maxpreps_football",
            "targeted_football_evidence_count": len(matches["football_lines"]),
        }
    )

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": scrape_meta,
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
