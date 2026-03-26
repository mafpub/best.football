"""Deterministic football scraper for Hilltop Senior High (CA)."""

from __future__ import annotations

import io
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from playwright.async_api import async_playwright
from pypdf import PdfReader

from pipeline.proxy import get_httpx_proxy_url
from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "063864006484"
SCHOOL_NAME = "Hilltop Senior High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://hth.sweetwaterschools.org"
HOME_URL = BASE_URL
ATHLETICS_URL = f"{BASE_URL}/calendarr/athletics-schedules"
SUHSD_ATHLETICS_URL = f"{HOME_URL}/calendarr/athletics-schedules/suhsd-athletics-website"
DISTRICT_ATHLETICS_URL = "https://athletics.sweetwaterschools.org/"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    SUHSD_ATHLETICS_URL,
    DISTRICT_ATHLETICS_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_RE = re.compile(r"football", re.IGNORECASE)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\(\d{3}\)\s*\d{3}[- ]\d{4}")
SCHEDULE_ROW_RE = re.compile(
    r"^(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+(?P<day>[A-Za-z]{3,9})\s+(?P<sport>.+?)\s+(?P<opponent>.+?)\s+(?P<site>Home|Away|Home/Away|Away/Home)\s+(?P<time>\d{1,2}:\d{2}\s+(?:AM|PM))(?P<notes>.*)$",
    re.IGNORECASE,
)
SCHEDULE_BYE_RE = re.compile(
    r"^(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+(?P<day>[A-Za-z]{3,9})\s+(?P<sport>.+?)\s+(?P<status>Bye)(?P<notes>.*)$",
    re.IGNORECASE,
)



def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\u00a0", " ")).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in values:
        value = _clean(item)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _dedupe_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        key = (text.lower(), href.lower())
        if not href or key in seen:
            continue
        seen.add(key)
        out.append({"text": text, "href": href})
    return out


def _collect_lines(text: str, *, limit: int | None = None) -> list[str]:
    lines: list[str] = []
    for raw in (text or "").splitlines():
        line = _clean(raw)
        if not line:
            continue
        lines.append(line)
        if limit and len(lines) >= limit:
            break
    return lines


def _normalize_href(href: str, base_url: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("//"):
        return f"https:{href}"
    if href.startswith("/"):
        return urljoin(base_url, href)
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return urljoin(base_url, href)


def _collect_links(raw_links: list[dict[str, str]], base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for raw in raw_links:
        if not isinstance(raw, dict):
            continue
        text = _clean(str(raw.get("text") or ""))
        href = _normalize_href(str(raw.get("href") or ""), base_url)
        if href:
            links.append({"text": text, "href": href})
    return _dedupe_links(links)


async def _collect_page_signal(page, url: str) -> dict[str, Any]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(1500)
    except Exception as exc:  # noqa: BLE001
        return {
            "requested_url": url,
            "final_url": url,
            "title": "",
            "body_text": "",
            "links": [],
            "navigation_error": f"{type(exc).__name__}:{exc}",
        }

    title = _clean(await page.title())
    try:
        body_text = _clean(await page.locator("body").inner_text(timeout=15000))
    except Exception:  # noqa: BLE001
        body_text = ""

    raw_links = await page.eval_on_selector_all(
        "a[href]",
        "() => Array.from(document.querySelectorAll('a[href]')).map(a => ({text: (a.textContent || '').replace(/\\s+/g, ' ').trim(), href: a.getAttribute('href') || a.href || ''}))",
    )
    if not isinstance(raw_links, list):
        raw_links = []

    links = _collect_links([entry for entry in raw_links if isinstance(entry, dict)], page.url)

    return {
        "requested_url": url,
        "final_url": page.url,
        "title": title,
        "body_text": body_text,
        "links": links,
    }


def _extract_football_team_contacts(body_text: str) -> list[dict[str, str]]:
    lines = _collect_lines(body_text)
    teams: list[dict[str, str]] = []
    for idx, line in enumerate(lines):
        lower = line.lower()
        if not lower.startswith("football varsity") and not lower.startswith("football jv"):
            continue

        team_name = "Football Varsity" if "varsity" in lower else "Football JV"
        coach_name = ""
        coach_email = ""

        forward = idx + 1
        while forward < len(lines):
            candidate = lines[forward]
            forward += 1
            if not candidate or candidate == "(Schedule)":
                continue
            if "(" in candidate and "schedule" in candidate.lower():
                continue
            email_match = EMAIL_RE.search(candidate)
            if email_match:
                coach_email = email_match.group(0).lower()
                maybe_name = _clean(candidate[: email_match.start()]).strip("-–—").strip()
                if maybe_name:
                    coach_name = maybe_name
            break

        team: dict[str, str] = {
            "team_name": team_name,
            "coach_name": coach_name,
            "coach_email": coach_email,
        }
        if team not in teams:
            teams.append(team)

    return teams


def _extract_contacts_from_text(body_text: str) -> list[dict[str, str]]:
    lines = _collect_lines(body_text)
    contacts: list[dict[str, str]] = []

    for i, line in enumerate(lines):
        phone_match = PHONE_RE.search(line)
        if not phone_match:
            continue

        phone = phone_match.group(0)
        if not phone:
            continue

        name = ""
        role = ""

        back_index = i - 1
        while back_index >= 0 and not name:
            candidate = _clean(lines[back_index])
            back_index -= 1
            if not candidate or "@" in candidate.lower():
                continue
            if "phone" in candidate.lower() or "email" in candidate.lower():
                continue
            if PHONE_RE.search(candidate):
                continue

            if not role and candidate not in {"contact", "contacts"}:
                role = candidate
                continue

            if role and candidate and not name:
                if candidate.lower().startswith("coach") or "athletic" in candidate.lower():
                    role = f"{candidate} ({role})".strip()
                else:
                    name = candidate
                break

        if not name and role:
            name = role
            role = ""

        email = ""
        for forward_index in range(i + 1, min(len(lines), i + 6)):
            candidate = lines[forward_index]
            candidate_match = EMAIL_RE.search(candidate)
            if candidate_match:
                email = candidate_match.group(0).lower()
                break

        if not name and not email and not role:
            continue

        contact = {
            "name": _clean(name),
            "role": _clean(role),
            "phone": _clean(phone),
            "email": _clean(email),
        }
        if not any(contact.values()):
            continue
        contacts.append(contact)

    # Remove duplicate contacts while preserving order.
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for contact in contacts:
        key = f"{contact.get('name')}|{contact.get('email')}|{contact.get('phone')}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(contact)
    return deduped


def _extract_schedule_urls(links: list[dict[str, str]]) -> list[str]:
    urls: list[str] = []
    for link in links:
        text = _clean(str(link.get("text") or "")).lower()
        href = _clean(str(link.get("href") or ""))
        if not href:
            continue
        if "football" not in href.lower() and "football" not in text:
            continue
        if "/resource-manager/" in href:
            continue
        if ".pdf" in href.lower() or "finalsite.net" in href.lower():
            urls.append(href)
    return _dedupe_keep_order(urls)


def _proxy_url() -> str:
    return get_httpx_proxy_url(profile=PROXY_PROFILE)


def _parse_schedule_pdf(url: str) -> dict[str, Any]:
    response = requests.get(
        url,
        proxies={"http": _proxy_url(), "https": _proxy_url()},
        timeout=60,
        allow_redirects=True,
    )
    response.raise_for_status()

    reader = PdfReader(io.BytesIO(response.content))
    extracted = "\n".join([_clean(page.extract_text() or "") for page in reader.pages])

    rows: list[dict[str, str]] = []
    for line in _collect_lines(extracted, limit=120):
        if not line or line.lower().startswith("date day"):
            continue

        m_bye = SCHEDULE_BYE_RE.match(line)
        if m_bye:
            rows.append(
                {
                    "date": m_bye.group("date"),
                    "day": m_bye.group("day"),
                    "level": _clean(m_bye.group("sport")),
                    "opponent": "Bye",
                    "site": "",
                    "time": "",
                    "notes": _clean(m_bye.group("notes")),
                }
            )
            continue

        m = SCHEDULE_ROW_RE.match(line)
        if not m:
            continue

        site = m.group("site").strip()
        rows.append(
            {
                "date": m.group("date"),
                "day": m.group("day"),
                "level": _clean(m.group("sport")),
                "opponent": _clean(m.group("opponent")),
                "site": site,
                "time": m.group("time"),
                "notes": _clean(m.group("notes")),
            }
        )

    return {
        "source_url": url,
        "final_url": response.url,
        "filename": Path(response.url).name,
        "content_type": response.headers.get("content-type", ""),
        "page_count": len(reader.pages),
        "rows": rows,
        "row_count": len(rows),
    }


def _extract_practice_locations(body_text: str) -> list[str]:
    lines = _collect_lines(body_text, limit=140)
    return [
        line
        for line in lines
        if "practice" in line.lower()
        and "football" in line.lower()
    ][:6]


async def scrape_school() -> dict[str, Any]:
    """Scrape Hilltop Senior High football and return deterministic extraction payload."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                signal = await _collect_page_signal(page, url)
                source_pages.append(signal["final_url"])
                page_signals.append(signal)
                if signal.get("navigation_error"):
                    errors.append(f"navigation_error:{signal['navigation_error']}")
        finally:
            await browser.close()

    page_by_url = {signal["final_url"]: signal for signal in page_signals}
    athletics_signal = page_by_url.get(ATHLETICS_URL, {})
    district_signal = page_by_url.get(DISTRICT_ATHLETICS_URL, {})
    home_signal = page_by_url.get(HOME_URL, {})

    athletics_text = _clean(athletics_signal.get("body_text", ""))
    home_text = _clean(home_signal.get("body_text", ""))
    district_text = _clean(district_signal.get("body_text", ""))
    all_text = " \n ".join(
        part for part in [home_text, athletics_text, district_text] if part
    )

    links: list[dict[str, str]] = []
    for signal in page_signals:
        links.extend(signal.get("links", []))

    football_urls = _extract_schedule_urls(links)
    football_schedule_rows: list[dict[str, Any]] = []
    football_schedule_meta: list[dict[str, Any]] = []

    for url in football_urls:
        if "finalsite" not in url.lower():
            continue
        try:
            parsed = _parse_schedule_pdf(url)
            football_schedule_meta.append(
                {
                    "source_url": parsed["source_url"],
                    "final_url": parsed["final_url"],
                    "filename": parsed["filename"],
                    "content_type": parsed["content_type"],
                    "row_count": parsed["row_count"],
                    "page_count": parsed["page_count"],
                }
            )
            football_schedule_rows.extend(parsed["rows"])
        except Exception as exc:  # noqa: BLE001
            errors.append(f"football_schedule_pdf_failed:{type(exc).__name__}:{exc}")

    football_teams = _extract_football_team_contacts(athletics_text)
    football_contacts = _extract_contacts_from_text(athletics_text)
    practice_notes = _extract_practice_locations(all_text)

    football_program_available = bool(
        FOOTBALL_RE.search(athletics_text)
        or any(
            "football" in link.get("text", "").lower() or "football" in link.get("href", "").lower()
            for link in links
        )
    )

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": {
            "football_program_available": football_program_available,
            "football_program_pages": [
                ATHLETICS_URL,
                SUHSD_ATHLETICS_URL,
                DISTRICT_ATHLETICS_URL,
            ],
            "football_teams": football_teams,
            "football_contacts": football_contacts,
            "football_schedule_urls": football_urls,
            "football_schedule_metadata": football_schedule_meta,
            "football_schedule_rows": football_schedule_rows,
            "football_practice_lines": practice_notes,
            "district_administration": {
                "contact_page": district_text[:160],
            },
            "home_page_url": HOME_URL,
        },
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "navigation_steps": [
                "visit_home_page",
                "visit_athletics_page",
                "visit_suhsd_athletics",
                "visit_district_athletics",
            ],
            "proxy_profile": PROXY_PROFILE,
            "proxy_server_count": len(get_proxy_runtime_meta(profile=PROXY_PROFILE)["proxy_servers"]),
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()
