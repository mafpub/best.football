"""Deterministic football scraper for Livingston High (CA)."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote, urlparse

import requests
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062466003700"
SCHOOL_NAME = "Livingston High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://lhs.muhsd.org/"
ATHLETICS_URL = "https://lhs.muhsd.org/11296_1"
ATHLETIC_DIRECTOR_URL = "https://lhs.muhsd.org/40965_2"
TEAMS_AND_SCHEDULES_URL = "https://lhs.muhsd.org/41358_2"
TARGET_URLS = [BASE_URL, ATHLETICS_URL, ATHLETIC_DIRECTOR_URL, TEAMS_AND_SCHEDULES_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[\w.\-]+\.\w+")
DAY_DATE_RE = re.compile(r"^(Mon|Tue|Tues|Wed|Thu|Thur|Thurs|Fri|Sat|Sun)\.?\s+\d{1,2}/\d{1,2}", re.I)
PHONE_RE = re.compile(r"(\(?\d{3}\)?[-\s]\d{3}[-\s]\d{4})")


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    ordered: list[Any] = []
    for value in values:
        marker = repr(sorted(value.items())) if isinstance(value, dict) else _clean(str(value))
        if not marker or marker in seen:
            continue
        seen.add(marker)
        ordered.append(value)
    return ordered


def _split_columns(line: str) -> list[str]:
    return [_clean(part) for part in re.split(r"\t+|\s{2,}", line) if _clean(part)]


def _export_url(doc_url: str) -> str:
    if "/edit" in doc_url:
        return doc_url.split("/edit", 1)[0] + "/export?format=txt"
    return doc_url.rstrip("/") + "/export?format=txt"


def _proxy_requests_url() -> str:
    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)
    parsed = urlparse(proxy["server"])
    username = quote(str(proxy["username"]), safe="")
    password = quote(str(proxy["password"]), safe="")
    return f"{parsed.scheme}://{username}:{password}@{parsed.hostname}:{parsed.port}"


def _requests_proxy_map() -> dict[str, str]:
    proxy_url = _proxy_requests_url()
    return {"http": proxy_url, "https": proxy_url}


def _parse_standard_schedule(raw_lines: list[str]) -> list[dict[str, str]]:
    header_index = next((i for i, line in enumerate(raw_lines) if _clean(line).startswith("Day/Date")), -1)
    if header_index == -1:
        return []

    rows: list[dict[str, str]] = []
    for line in raw_lines[header_index + 1 :]:
        cleaned = _clean(line)
        if not cleaned:
            continue
        if cleaned == "Contacts" or EMAIL_RE.search(cleaned):
            break
        if not DAY_DATE_RE.match(cleaned):
            continue

        parts = _split_columns(line)
        if len(parts) < 2:
            continue

        day_date = parts[0]
        opponent = parts[1]
        location = ""
        depart_time = ""
        game_time = ""

        if len(parts) >= 5:
            location = parts[2]
            depart_time = parts[3]
            game_time = parts[4]
        elif len(parts) == 4:
            if parts[2].upper() == "BYE":
                opponent = "BYE"
                location = ""
            else:
                location = parts[2]
                game_time = parts[3]
        elif len(parts) == 3 and parts[2].upper() == "BYE":
            opponent = "BYE"

        rows.append(
            {
                "day_date": day_date,
                "opponent": opponent,
                "location": location,
                "depart_time": depart_time,
                "game_time": game_time,
                "is_home_game": location.lower().startswith("livingston"),
            }
        )

    return rows


def _parse_flag_schedule(raw_lines: list[str]) -> tuple[list[dict[str, str]], list[str]]:
    header_index = next((i for i, line in enumerate(raw_lines) if _clean(line).startswith("Day/Date")), -1)
    if header_index == -1:
        return [], []

    rows: list[dict[str, str]] = []
    notes: list[str] = []
    i = header_index + 1

    while i < len(raw_lines):
        current = _clean(raw_lines[i])
        if not current:
            i += 1
            continue
        if EMAIL_RE.search(current):
            break
        if current.startswith("All home game"):
            notes.append(current)
            i += 1
            continue
        if not DAY_DATE_RE.match(current):
            i += 1
            continue

        day_date = current
        i += 1
        bucket: list[str] = []
        while i < len(raw_lines):
            candidate = _clean(raw_lines[i])
            if EMAIL_RE.search(candidate) or candidate.startswith("All home game") or DAY_DATE_RE.match(candidate):
                break
            if candidate:
                bucket.append(candidate)
            i += 1

        if not bucket:
            continue

        opponent = bucket[0] if len(bucket) > 0 else ""
        level = bucket[1] if len(bucket) > 1 else ""
        location = bucket[2] if len(bucket) > 2 else ""
        if len(bucket) >= 5:
            depart_time = bucket[3]
            game_time = bucket[4]
        elif len(bucket) == 4:
            depart_time = ""
            game_time = bucket[3]
        else:
            depart_time = ""
            game_time = ""

        rows.append(
            {
                "day_date": day_date,
                "opponent": opponent,
                "level": level,
                "location": location,
                "depart_time": depart_time,
                "game_time": game_time,
                "is_home_game": location.lower().startswith("livingston"),
            }
        )

    return rows, _dedupe_keep_order(notes)


def _parse_contacts(raw_lines: list[str]) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    for line in raw_lines:
        cleaned = _clean(line)
        if not EMAIL_RE.search(cleaned):
            continue
        parts = _split_columns(line)
        if len(parts) < 3:
            continue
        email = next((part for part in parts if EMAIL_RE.fullmatch(part)), "")
        if not email:
            continue
        name = parts[0]
        role = parts[1]
        contacts.append(
            {
                "name": name,
                "role": role,
                "email": email,
            }
        )
    return _dedupe_keep_order(contacts)


def _extract_department_contact(raw_lines: list[str]) -> tuple[str, str]:
    for line in raw_lines:
        if not _clean(line).startswith("Athletic Department"):
            continue
        parts = _split_columns(line)
        address = parts[1] if len(parts) > 1 else ""
        phone = parts[2] if len(parts) > 2 else ""
        return address, phone
    return "", ""


def _parse_doc(text: str, *, team_label: str, doc_url: str) -> dict[str, Any]:
    raw_lines = [line.rstrip() for line in text.replace("\ufeff", "").splitlines()]
    contacts = _parse_contacts(raw_lines)
    department_address, department_phone = _extract_department_contact(raw_lines)
    season_match = re.search(r"\b(20\d{2})\b", raw_lines[0] if raw_lines else "")
    season = season_match.group(1) if season_match else ""

    if "flag football" in team_label.lower():
        schedule_rows, notes = _parse_flag_schedule(raw_lines)
    else:
        schedule_rows = _parse_standard_schedule(raw_lines)
        notes = []

    return {
        "team_label": team_label,
        "doc_url": doc_url,
        "export_url": _export_url(doc_url),
        "season": season,
        "row_count": len(schedule_rows),
        "schedule_rows": schedule_rows,
        "contacts": contacts,
        "department_address": department_address,
        "department_phone": department_phone,
        "notes": notes,
    }


def _extract_athletic_director(text: str) -> dict[str, str]:
    lines = [_clean(line) for line in text.splitlines() if _clean(line)]
    for index, line in enumerate(lines):
        if line.lower() != "athletic director":
            continue
        name = ""
        for candidate in lines[index + 1 : index + 5]:
            if candidate.lower() != "athletic director":
                name = candidate
                break
        phone = ""
        for follow in lines[index + 1 : index + 7]:
            match = PHONE_RE.search(follow)
            if match:
                phone = _clean(match.group(1))
                break
        if name:
            return {
                "name": name,
                "role": "Athletic Director",
                "phone": phone,
            }
    return {}


def _download_text(url: str) -> str:
    response = requests.get(
        url,
        timeout=30,
        proxies=_requests_proxy_map(),
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    return response.text.replace("\ufeff", "")


async def _collect_page(page, requested_url: str) -> dict[str, Any]:
    body_raw = await page.locator("body").inner_text()
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map((anchor) => ({
            text: (anchor.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: anchor.href || anchor.getAttribute('href') || ''
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
        normalized_links.append({"text": _clean(str(item.get("text") or "")), "href": href})

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body_raw": body_raw,
        "body_text": _clean(body_raw),
        "links": normalized_links,
    }


def _extract_football_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    football_links: list[dict[str, str]] = []
    for item in links:
        text = _clean(item.get("text", ""))
        href = _clean(item.get("href", ""))
        blob = f"{text} {href}".lower()
        if "football" not in blob:
            continue
        football_links.append({"text": text, "href": href})
    return _dedupe_keep_order(football_links)


async def scrape_school() -> dict[str, Any]:
    """Collect public Livingston football pages and schedule documents."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_log: list[str] = []
    page_signals: list[dict[str, Any]] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            navigation_log.append("visit_school_home")
            page_signals.append(await _collect_page(page, BASE_URL))

            athletics_link = page.locator("a", has_text="Athletics").first
            if await athletics_link.count():
                try:
                    await athletics_link.click(timeout=10000)
                    await page.wait_for_timeout(1500)
                    navigation_log.append("click_athletics_from_home")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"athletics_click_failed:{type(exc).__name__}")
                    await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    navigation_log.append("goto_athletics_fallback")
            else:
                await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1500)
                navigation_log.append("goto_athletics_direct")

            source_pages.append(page.url)
            page_signals.append(await _collect_page(page, ATHLETICS_URL))

            teams_link = page.locator("a", has_text="Teams and Schedules").first
            if await teams_link.count():
                try:
                    await teams_link.click(timeout=10000)
                    await page.wait_for_timeout(1500)
                    navigation_log.append("click_teams_and_schedules")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"teams_and_schedules_click_failed:{type(exc).__name__}")
                    await page.goto(TEAMS_AND_SCHEDULES_URL, wait_until="domcontentloaded", timeout=90000)
                    await page.wait_for_timeout(1500)
                    navigation_log.append("goto_teams_and_schedules_fallback")
            else:
                await page.goto(TEAMS_AND_SCHEDULES_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1500)
                navigation_log.append("goto_teams_and_schedules_direct")

            source_pages.append(page.url)
            teams_signal = await _collect_page(page, TEAMS_AND_SCHEDULES_URL)
            page_signals.append(teams_signal)

            await page.goto(ATHLETIC_DIRECTOR_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1500)
            source_pages.append(page.url)
            navigation_log.append("visit_athletic_director_page")
            athletic_director_signal = await _collect_page(page, ATHLETIC_DIRECTOR_URL)
            page_signals.append(athletic_director_signal)
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    football_links = _extract_football_links(teams_signal.get("links", []))
    doc_links = [
        item
        for item in football_links
        if "docs.google.com/document/d/" in item.get("href", "")
        and any(
            marker in item.get("text", "").lower()
            for marker in ["f/s football", "jv/v football", "girls flag football"]
        )
    ]

    football_docs: list[dict[str, Any]] = []
    for link in doc_links:
        doc_url = link["href"]
        try:
            parsed_doc = _parse_doc(
                _download_text(_export_url(doc_url)),
                team_label=link["text"],
                doc_url=doc_url,
            )
            football_docs.append(parsed_doc)
            source_pages.append(parsed_doc["export_url"])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"doc_fetch_failed:{type(exc).__name__}:{doc_url}")

    source_pages = _dedupe_keep_order(source_pages)

    football_contacts: list[dict[str, str]] = []
    football_team_names: list[str] = []
    for doc in football_docs:
        football_team_names.append(doc["team_label"])
        football_contacts.extend(doc.get("contacts", []))
    football_contacts = _dedupe_keep_order(football_contacts)

    athletic_director = _extract_athletic_director(athletic_director_signal.get("body_raw", ""))
    if athletic_director:
        matching_doc_contact = next(
            (
                contact
                for contact in football_contacts
                if contact.get("role") == "Athletic Director"
                and contact.get("name") == athletic_director.get("name")
            ),
            None,
        )
        if matching_doc_contact:
            athletic_director["email"] = matching_doc_contact.get("email", "")

    football_program_available = bool(football_docs)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_url": ATHLETICS_URL,
        "teams_and_schedules_url": TEAMS_AND_SCHEDULES_URL,
        "athletic_director_url": ATHLETIC_DIRECTOR_URL,
        "athletic_director": athletic_director,
        "football_team_names": _dedupe_keep_order(football_team_names),
        "football_links": football_links,
        "football_contacts": football_contacts,
        "football_schedule_docs": [
            {
                "team_label": doc["team_label"],
                "doc_url": doc["doc_url"],
                "export_url": doc["export_url"],
                "season": doc["season"],
                "row_count": doc["row_count"],
                "notes": doc["notes"],
            }
            for doc in football_docs
        ],
        "football_schedules": football_docs,
        "department_contact": {
            "address": next(
                (doc.get("department_address", "") for doc in football_docs if doc.get("department_address")),
                "",
            ),
            "phone": next(
                (doc.get("department_phone", "") for doc in football_docs if doc.get("department_phone")),
                "",
            ),
        },
        "summary": (
            "Livingston High publishes public F/S Football, JV/V Football, and Girls Flag Football schedule documents on its Teams and Schedules page, with coach and athletic director contact details."
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
            "proxy_profile": PROXY_PROFILE,
            "proxy_server": proxy["server"],
            "focus": "football_only",
            "pages_checked": len(page_signals),
            "docs_checked": len(football_docs),
            "navigation_log": navigation_log,
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import asyncio

    print(asyncio.run(scrape_school()))
