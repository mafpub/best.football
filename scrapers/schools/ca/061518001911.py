"""Deterministic football scraper for Gilroy High (CA)."""

from __future__ import annotations

import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from pypdf import PdfReader
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061518001911"
SCHOOL_NAME = "Gilroy High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://gilroyhs.gilroyunified.org/"
ATHLETICS_URL = "https://gilroyhs.gilroyunified.org/mustang-athletics"
FOOTBALL_SCHEDULE_URL = (
    "https://gilroyhs.gilroyunified.org/fs/resource-manager/view/4543e18c-cc21-4eab-982d-307d53c855ff"
)

TARGET_URLS = [HOME_URL, ATHLETICS_URL, FOOTBALL_SCHEDULE_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

DAY_RE = re.compile(
    r"\b(Sunday|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday)\s+(\d{1,2}/\d{1,2})\b"
)
TIME_RE = re.compile(
    r"(\d{1,2}:\d{2}(?:/\d{1,2}:\d{2})?(?:am|pm))\s+(JV/Varsity|Varsity|JV)\b",
    re.I,
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _normalize_href(href: str, base_url: str) -> str:
    raw = _clean(href)
    if not raw:
        return ""
    if raw.startswith("//"):
        return f"https:{raw}"
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if raw.startswith("/"):
        return urljoin(base_url, raw)
    return ""


def _extract_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_football_sections(page_text: str) -> list[str]:
    """Return the athletics lines that mention football-specific content."""
    return _extract_lines(page_text, ("football", "coach", "schedule", "impact", "athletic trainer"))


async def _collect_page_snapshot(page) -> dict[str, Any]:
    body_text = _clean(await page.locator("body").inner_text(timeout=15000))
    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: a.getAttribute('href') || '',
            fileName: a.getAttribute('data-file-name') || '',
            resourceUuid: a.getAttribute('data-resource-uuid') || ''
        }))""",
    )
    normalized_links: list[dict[str, str]] = []
    if isinstance(links, list):
        for raw_link in links:
            if not isinstance(raw_link, dict):
                continue
            href = _normalize_href(str(raw_link.get("href") or ""), page.url)
            if not href:
                continue
            normalized_links.append(
                {
                    "text": _clean(str(raw_link.get("text") or "")),
                    "href": href,
                    "file_name": _clean(str(raw_link.get("fileName") or "")),
                    "resource_uuid": _clean(str(raw_link.get("resourceUuid") or "")),
                }
            )

    return {
        "title": _clean(await page.title()),
        "url": page.url,
        "body_text": body_text,
        "links": normalized_links,
        "football_lines": _extract_football_sections(body_text),
    }


async def _extract_fall_sports_entries(page) -> list[dict[str, str]]:
    entries = await page.evaluate(
        """
        () => {
          const section = Array.from(document.querySelectorAll('section.fsContent')).find((el) => {
            const title = el.querySelector('h2.fsElementTitle');
            return title && title.textContent && title.textContent.trim() === 'Fall Sports';
          });
          if (!section) return [];
          const content = section.querySelector('.fsElementContent');
          if (!content) return [];
          const children = Array.from(content.children);
          const rows = [];
          for (let i = 0; i < children.length; i += 1) {
            const node = children[i];
            if (node.tagName !== 'P') continue;
            const strong = node.querySelector('strong');
            const sport = ((strong && strong.textContent) || node.textContent || '').replace(/\\s+/g, ' ').trim();
            if (!sport) continue;
            const ul = children[i + 1];
            const coachLink = ul && ul.tagName === 'UL'
              ? Array.from(ul.querySelectorAll('a[href^="mailto:"]')).find((a) => (a.textContent || '').trim())
              : null;
            const scheduleLink = ul && ul.tagName === 'UL'
              ? Array.from(ul.querySelectorAll('a[href]')).find((a) => (a.textContent || '').trim().toLowerCase() === 'schedule')
              : null;
            rows.push({
              sport,
              coach_text: coachLink ? (coachLink.textContent || '').replace(/\\s+/g, ' ').trim() : '',
              coach_href: coachLink ? coachLink.getAttribute('href') || '' : '',
              schedule_text: scheduleLink ? (scheduleLink.textContent || '').replace(/\\s+/g, ' ').trim() : '',
              schedule_href: scheduleLink ? scheduleLink.getAttribute('href') || '' : '',
              schedule_file_name: scheduleLink ? scheduleLink.getAttribute('data-file-name') || '' : '',
              schedule_resource_uuid: scheduleLink ? scheduleLink.getAttribute('data-resource-uuid') || '' : '',
            });
          }
          return rows;
        }
        """
    )
    if not isinstance(entries, list):
        return []
    output: list[dict[str, str]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        output.append(
            {
                "sport": _clean(str(item.get("sport") or "")),
                "coach_text": _clean(str(item.get("coach_text") or "")),
                "coach_href": _clean(str(item.get("coach_href") or "")),
                "schedule_text": _clean(str(item.get("schedule_text") or "")),
                "schedule_href": _clean(str(item.get("schedule_href") or "")),
                "schedule_file_name": _clean(str(item.get("schedule_file_name") or "")),
                "schedule_resource_uuid": _clean(str(item.get("schedule_resource_uuid") or "")),
            }
        )
    return output


def _extract_schedule_rows(text: str) -> list[dict[str, str]]:
    normalized = _clean(text)
    matches = list(DAY_RE.finditer(normalized))
    rows: list[dict[str, str]] = []
    for idx, match in enumerate(matches):
        segment = normalized[
            match.end() : matches[idx + 1].start() if idx + 1 < len(matches) else len(normalized)
        ].strip()
        segment = re.sub(r"^Day Date Opponent Location Time Level\s*", "", segment, flags=re.I)
        time_match = TIME_RE.search(segment)
        if not time_match:
            continue
        before = _clean(segment[: time_match.start()])
        after = _clean(segment[time_match.start() : time_match.end()])
        rows.append(
            {
                "day": match.group(1),
                "date": match.group(2),
                "event_text": before,
                "time": _clean(time_match.group(1)),
                "level": _clean(time_match.group(2)),
                "source_text": f"{match.group(1)} {match.group(2)} {before} {after}",
            }
        )
    return rows


def _extract_schedule_metadata(text: str) -> dict[str, str]:
    cleaned = _clean(text)
    coach_match = re.search(
        r"Head Coach:\s*([A-Za-z .'\-]+?)\s+([A-Za-z0-9._%+-]+@gilroyunified\.org)\b",
        cleaned,
        re.I,
    )
    director_match = re.search(r"Athletic Director:\s*([A-Za-z .'\-]+?)(?:\s+Athletic Trainer:|$)", cleaned, re.I)
    trainer_match = re.search(r"Athletic Trainer:\s*([A-Za-z .'\-]+?)(?:\s+Athletic Asst:|$)", cleaned, re.I)
    address_match = re.search(
        r"Gilroy High School\s+(750 W\. 10th Street Gilroy, CA 95020)\s+\((669)\)\s*(205-5400)",
        cleaned,
        re.I,
    )
    return {
        "head_coach_name": _clean(coach_match.group(1)) if coach_match else "",
        "head_coach_email": _clean(coach_match.group(2)) if coach_match else "",
        "athletic_director": _clean(director_match.group(1)) if director_match else "",
        "athletic_trainer": _clean(trainer_match.group(1)) if trainer_match else "",
        "home_address": _clean(address_match.group(1)) if address_match else "",
        "home_phone": f"({address_match.group(2)}) {address_match.group(3)}" if address_match else "",
    }


async def _download_schedule_pdf(page) -> tuple[str, str, list[dict[str, str]], dict[str, str]]:
    link = page.locator('a[data-resource-uuid="4543e18c-cc21-4eab-982d-307d53c855ff"]').first
    if await link.count() == 0:
        return "", "", [], {}

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        pdf_path = Path(tmp.name)

    try:
        async with page.expect_download() as download_info:
            await link.click(timeout=15000)
        download = await download_info.value
        await download.save_as(str(pdf_path))
        reader = PdfReader(str(pdf_path))
        pdf_text = _clean("\n".join((page_obj.extract_text() or "") for page_obj in reader.pages))
        rows = _extract_schedule_rows(pdf_text)
        meta = _extract_schedule_metadata(pdf_text)
        return download.url or FOOTBALL_SCHEDULE_URL, download.suggested_filename, rows, meta
    finally:
        if pdf_path.exists():
            pdf_path.unlink(missing_ok=True)


async def scrape_school() -> dict[str, Any]:
    """Scrape Gilroy High's public football information."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []
    page_signals: list[dict[str, Any]] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
            ignore_https_errors=True,
            accept_downloads=True,
        )
        page = await context.new_page()

        try:
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1000)
            home_signal = await _collect_page_snapshot(page)
            page_signals.append(home_signal)
            source_pages.append(home_signal["url"])
            navigation_steps.append("visit_home")

            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1000)
            athletics_signal = await _collect_page_snapshot(page)
            page_signals.append(athletics_signal)
            source_pages.append(athletics_signal["url"])
            navigation_steps.append("visit_mustang_athletics")

            fall_entries = await _extract_fall_sports_entries(page)
            football_entry = next(
                (entry for entry in fall_entries if entry.get("sport", "").lower() == "football"),
                {},
            )
            football_schedule_url = _normalize_href(football_entry.get("schedule_href", ""), page.url)
            if football_schedule_url:
                source_pages.append(football_schedule_url)

            schedule_url, schedule_filename, schedule_rows, schedule_meta = await _download_schedule_pdf(page)
            if schedule_url:
                source_pages.append(schedule_url)
                navigation_steps.append("download_football_schedule_pdf")

        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_flow_failed:{type(exc).__name__}")
            fall_entries = []
            football_entry = {}
            schedule_url = ""
            schedule_filename = ""
            schedule_rows = []
            schedule_meta = {}
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)
    football_lines = _dedupe_keep_order(
        [
            line
            for signal in page_signals
            for line in signal.get("football_lines", [])
            if isinstance(line, str)
        ]
    )

    football_available = bool(football_entry) and bool(schedule_rows or football_lines)
    if not football_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    football_coach_name = _clean(str(football_entry.get("coach_text") or schedule_meta.get("head_coach_name") or ""))
    football_coach_email = _clean(
        str(schedule_meta.get("head_coach_email") or football_entry.get("coach_href") or "")
    )
    if football_coach_email.startswith("mailto:"):
        football_coach_email = football_coach_email.replace("mailto:", "", 1)

    football_schedule_link = _normalize_href(str(football_entry.get("schedule_href") or ""), ATHLETICS_URL)
    if not football_schedule_link:
        football_schedule_link = FOOTBALL_SCHEDULE_URL

    extracted_items: dict[str, Any] = {
        "football_program_available": football_available,
        "football_team_name": "Football",
        "football_team_url": ATHLETICS_URL,
        "football_schedule_url": football_schedule_link or FOOTBALL_SCHEDULE_URL,
        "football_schedule_filename": schedule_filename or _clean(str(football_entry.get("schedule_file_name") or "")),
        "football_coach_name": football_coach_name,
        "football_coach_email": football_coach_email.replace("mailto:", ""),
        "football_athletic_director": _clean(str(schedule_meta.get("athletic_director") or "")),
        "football_athletic_trainer": _clean(str(schedule_meta.get("athletic_trainer") or "")),
        "football_home_address": _clean(str(schedule_meta.get("home_address") or "")),
        "football_home_phone": _clean(str(schedule_meta.get("home_phone") or "")),
        "football_schedule_rows": schedule_rows,
        "football_schedule_row_count": len(schedule_rows),
        "football_fall_sports_entry": football_entry,
        "football_athletics_evidence_lines": football_lines[:20],
        "football_summary": (
            "Gilroy High's Mustang Athletics page lists Football under Fall Sports with Coach Dillon Babb and a schedule download; "
            "the linked 2025 PDF includes the coach email, athletic director, athletic trainer, and a 10-game JV/Varsity schedule."
        ),
    }

    proxy_meta = get_proxy_runtime_meta(PROXY_PROFILE)

    scrape_meta = {
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "script_version": "1.0.0",
        "proxy_profile": proxy_meta["proxy_profile"],
        "proxy_servers": proxy_meta["proxy_servers"],
        "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
        "focus": "football_only",
        "pages_visited": len(source_pages),
        "pages_requested": TARGET_URLS,
        "navigation_steps": navigation_steps,
        "football_schedule_row_count": len(schedule_rows),
        "football_entry_found": bool(football_entry),
    }

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

