"""Deterministic football scraper for Leigh High School (CA)."""

from __future__ import annotations

import asyncio
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060723008122"
SCHOOL_NAME = "Leigh High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://leigh.cuhsd.org/"
ATHLETICS_URL = "https://leigh.cuhsd.org/apps/pages/index.jsp?uREC_ID=2154447&type=d&pREC_ID=2170412"
HOW_TO_PARTICIPATE_URL = "https://leigh.cuhsd.org/apps/pages/AthleticsHowToParticipate"
FOOTBALL_URL = "https://leigh.cuhsd.org/apps/pages/Football"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, HOW_TO_PARTICIPATE_URL, FOOTBALL_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DAY_PREFIX_RE = re.compile(
    r"^(?P<day>Mon|Tue|Wed|Thu|Thur|Fri|Sat|Sun)\s+(?P<date>\d{1,2}/\d{1,2})\s+(?P<rest>.+)$",
    re.IGNORECASE,
)
BYE_WEEK_RE = re.compile(r"^(?P<date>\d{1,2}/\d{1,2})\s+BYE WEEK$", re.IGNORECASE)


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


def _decode_cfemail(raw_value: str) -> str:
    value = _clean(raw_value)
    if not value:
        return ""
    try:
        key = int(value[:2], 16)
        return "".join(
            chr(int(value[index : index + 2], 16) ^ key)
            for index in range(2, len(value), 2)
        )
    except (ValueError, TypeError):
        return ""


def _collect_lines(text: str) -> list[str]:
    return [line for line in (_clean(raw) for raw in text.splitlines()) if line]


def _find_section(lines: list[str], start_marker: str, end_marker: str | None = None) -> list[str]:
    start_index = None
    start_lower = start_marker.lower()
    for index, line in enumerate(lines):
        if start_lower in line.lower():
            start_index = index
            break
    if start_index is None:
        return []

    end_index = len(lines)
    if end_marker:
        end_lower = end_marker.lower()
        for index in range(start_index + 1, len(lines)):
            if end_lower in lines[index].lower():
                end_index = index
                break

    return lines[start_index + 1 : end_index]


def _decode_anchor_email(anchor) -> str:
    candidates: list[str] = []
    if anchor.has_attr("data-cfemail"):
        candidates.append(str(anchor.get("data-cfemail") or ""))
    if anchor.has_attr("data-email"):
        candidates.append(str(anchor.get("data-email") or ""))
    for node in anchor.select("[data-cfemail]"):
        candidates.append(str(node.get("data-cfemail") or ""))

    for candidate in candidates:
        decoded = _decode_cfemail(candidate)
        if decoded:
            return decoded
    return ""


def _emails_from_text(text: str) -> list[str]:
    return _dedupe_keep_order(
        re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    )


def _page_snapshot(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main") or soup.body or soup
    raw_text = main.get_text("\n")
    text = _clean(raw_text)
    title = _clean(soup.title.get_text(" ")) if soup.title else ""

    links: list[dict[str, str]] = []
    for anchor in main.find_all("a", href=True):
        raw_href = _clean(str(anchor.get("href") or ""))
        href = urljoin(url, raw_href)
        text_value = _clean(anchor.get_text(" ", strip=True))
        decoded_email = _decode_anchor_email(anchor)
        if decoded_email:
            href = f"mailto:{decoded_email}"
            if not text_value or text_value.startswith("[email"):
                text_value = decoded_email
        links.append({"text": text_value, "href": href})

    return {
        "url": url,
        "title": title,
        "body_text": text,
        "lines": [line for line in (_clean(raw) for raw in raw_text.splitlines()) if line],
        "links": links,
    }


async def _collect_page(page, url: str) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1000)
    snapshot = _page_snapshot(await page.content(), page.url)
    snapshot["requested_url"] = url
    return snapshot


def _parse_schedule(lines: list[str]) -> list[dict[str, str]]:
    schedule: list[dict[str, str]] = []
    for line in lines:
        if not line:
            continue
        bye_match = BYE_WEEK_RE.match(line)
        if bye_match:
            schedule.append(
                {
                    "raw_line": line,
                    "kind": "bye_week",
                    "date": bye_match.group("date"),
                }
            )
            continue

        match = DAY_PREFIX_RE.match(line)
        if not match:
            continue

        rest = _clean(match.group("rest"))
        kind = "game"
        if rest.lower().startswith("scrimmage "):
            kind = "scrimmage"
            rest = _clean(rest[len("Scrimmage ") :])

        site = ""
        if rest.lower().startswith("vs. "):
            site = "home"
            rest = _clean(rest[4:])
        elif rest.startswith("@ "):
            site = "away"
            rest = _clean(rest[2:])

        levels = ""
        if "(JV & Varsity)" in rest:
            levels = "JV & Varsity"
            rest = _clean(rest.replace("(JV & Varsity)", ""))

        time = ""
        if "JV @" in rest:
            time_index = rest.index("JV @")
            time = _clean(rest[time_index:])
            rest = _clean(rest[:time_index])
        elif rest.endswith("TBD"):
            time = "TBD"
            rest = _clean(rest[: -len("TBD")])

        opponent = rest
        schedule.append(
            {
                "raw_line": line,
                "kind": kind,
                "day": _clean(match.group("day")),
                "date": _clean(match.group("date")),
                "site": site,
                "opponent": opponent,
                "levels": levels,
                "time": time,
            }
        )

    return schedule


def _parse_coaches(lines: list[str]) -> list[dict[str, str]]:
    coaches: list[dict[str, str]] = []
    for line in lines:
        match = re.match(r"^(Head Coach|Asst Coach|Head JV Coach|Coach Contact):\s*(.+)$", line, re.IGNORECASE)
        if match:
            coaches.append(
                {
                    "role": _clean(match.group(1)),
                    "name": _clean(match.group(2)),
                }
            )
    return coaches


def _first_line_emails(lines: list[str], marker: str) -> list[str]:
    marker_lower = marker.lower()
    for line in lines:
        if marker_lower in line.lower():
            emails = _emails_from_text(line)
            if emails:
                return emails
    return []


def _email_addresses_from_links(links: list[dict[str, str]]) -> list[str]:
    emails: list[str] = []
    for link in links:
        href = _clean(link.get("href", ""))
        text = _clean(link.get("text", ""))
        if href.startswith("mailto:"):
            email = _clean(href.removeprefix("mailto:"))
            if email:
                emails.append(email)
        href_emails = _emails_from_text(href)
        text_emails = _emails_from_text(text)
        if href_emails:
            emails.extend(href_emails)
        if text_emails:
            emails.extend(text_emails)
    return _dedupe_keep_order(emails)


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []

    home_page: dict[str, Any] = {}
    athletics_page: dict[str, Any] = {}
    participation_page: dict[str, Any] = {}
    football_page: dict[str, Any] = {}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            ignore_https_errors=True,
        )
        page = await context.new_page()

        try:
            home_page = await _collect_page(page, HOME_URL)
            athletics_page = await _collect_page(page, ATHLETICS_URL)
            participation_page = await _collect_page(page, HOW_TO_PARTICIPATE_URL)
            football_page = await _collect_page(page, FOOTBALL_URL)

            source_pages.extend(
                [
                    home_page["url"],
                    athletics_page["url"],
                    participation_page["url"],
                    football_page["url"],
                ]
            )
            navigation_steps.extend(
                [
                    "visit_home",
                    "visit_athletics",
                    "visit_how_to_participate",
                    "visit_football",
                ]
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_flow_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    source_titles = _dedupe_keep_order(
        [
            home_page.get("title", ""),
            athletics_page.get("title", ""),
            participation_page.get("title", ""),
            football_page.get("title", ""),
        ]
    )

    football_lines = football_page.get("lines", [])
    participation_lines = participation_page.get("lines", [])
    athletics_lines = athletics_page.get("lines", [])

    schedule_heading = _find_section(football_lines, "2025 game schedule", "coaches")
    coach_section = _find_section(football_lines, "coaches", "how to participate")
    if not coach_section:
        coach_section = _find_section(football_lines, "coaches")

    schedule_rows = _parse_schedule(schedule_heading)
    coach_rows = _parse_coaches(coach_section)
    football_links = football_page.get("links", [])
    participation_links = participation_page.get("links", [])
    athletics_links = athletics_page.get("links", [])

    live_stream_urls = _dedupe_keep_order(
        [
            link["href"]
            for link in football_links
            if "nfhsnetwork.com" in link.get("href", "").lower()
        ]
    )
    athletic_clearance_urls = _dedupe_keep_order(
        [
            link["href"]
            for link in participation_links
            if "athleticclearance.com" in link.get("href", "").lower()
        ]
    )
    participation_pdf_urls = _dedupe_keep_order(
        [
            link["href"]
            for link in participation_links
            if link.get("href", "").lower().endswith(".pdf")
        ]
    )

    if not football_page:
        errors.append("no_football_page_loaded")
    if not schedule_rows:
        errors.append("no_public_schedule_found")
    if not coach_rows:
        errors.append("no_public_coach_lines_found")

    coach_contact_email = ""
    athletic_director_email = ""
    general_contact_email = ""

    coach_contact_candidates = _first_line_emails(football_lines, "Coach Contact:")
    if coach_contact_candidates:
        coach_contact_email = coach_contact_candidates[0]

    athletic_director_candidates = _first_line_emails(football_lines, "Athletic Director")
    if athletic_director_candidates:
        athletic_director_email = athletic_director_candidates[-1]

    team_snap_candidates = _first_line_emails(football_lines, "TeamSnap")
    if len(team_snap_candidates) >= 2:
        if not coach_contact_email:
            coach_contact_email = team_snap_candidates[0]
        if not athletic_director_email:
            athletic_director_email = team_snap_candidates[1]

    page_link_emails = _email_addresses_from_links(football_page.get("links", []))
    if not athletic_director_email:
        for candidate in page_link_emails:
            if candidate and candidate != coach_contact_email:
                athletic_director_email = candidate
                break

    if not general_contact_email:
        other_candidates = _first_line_emails(football_lines, "Coach Contact")
        if other_candidates:
            general_contact_email = other_candidates[0]
    if not general_contact_email and athletic_director_email:
        general_contact_email = athletic_director_email
    if not general_contact_email and page_link_emails:
        general_contact_email = page_link_emails[0]

    evidence_lines = _dedupe_keep_order(
        [
            line
            for line in football_lines
            if any(
                marker in line.lower()
                for marker in (
                    "football important dates",
                    "current football workouts",
                    "links to live streams",
                    "2025 game schedule",
                    "coaches",
                    "team snap",
                    "practice",
                    "coach contact",
                )
            )
        ]
    )

    extracted_items: dict[str, Any] = {
        "football_program_available": bool(football_page and schedule_rows and coach_rows),
        "program_name": "Leigh Football",
        "football_team_names": ["Football"],
        "home_url": HOME_URL,
        "athletics_url": ATHLETICS_URL,
        "how_to_participate_url": HOW_TO_PARTICIPATE_URL,
        "football_page_url": FOOTBALL_URL,
        "source_page_titles": source_titles,
        "manual_navigation_path": [
            "home",
            "athletics",
            "how_to_participate",
            "football",
        ],
        "football_lines": evidence_lines[:20],
        "football_schedule": schedule_rows,
        "football_schedule_count": len(schedule_rows),
        "live_stream_urls": live_stream_urls,
        "athletic_clearance_urls": athletic_clearance_urls,
        "participation_pdf_urls": participation_pdf_urls,
        "coaching_staff": coach_rows,
        "football_contacts": [
            {
                "name": "Kyle Padia",
                "role": "Head Coach",
                "email": coach_contact_email,
            },
            {
                "name": "Drew Marino",
                "role": "Athletic Director / TeamSnap contact",
                "email": athletic_director_email,
            },
            {
                "role": "Coach Contact",
                "email": general_contact_email,
            },
        ],
        "team_snap_note": "Coach Padia and Athletic Director Drew Marino are listed for TeamSnap enrollment on the football page.",
    }

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "pages_visited": len(source_pages),
            "manual_navigation_steps": navigation_steps,
            "verification_focus": "football_only",
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


async def main() -> None:
    result = await scrape_school()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
