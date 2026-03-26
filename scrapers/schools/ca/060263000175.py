"""Deterministic football scraper for Hope High (CA)."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060263000175"
SCHOOL_NAME = "Hope High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://hope.auhsd.us/"
ATHLETICS_URL = "https://hope.auhsd.us/Hope/department/11856-ATHLETICS"
COACHES_URL = "https://hope.auhsd.us/Hope/Department/11856-ATHLETICS/31327-Coaches.html"
FOOTBALL_SEARCH_URL = "https://hope.auhsd.us/Hope/Search/?text=football"

TARGET_URLS = [HOME_URL, ATHLETICS_URL, COACHES_URL, FOOTBALL_SEARCH_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for item in values:
        if isinstance(item, dict):
            key = repr(item)
        else:
            key = _clean(str(item))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _abs_url(base_url: str, href: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


def _collect_lines(text: str) -> list[str]:
    return [line for line in (_clean(line) for line in (text or "").splitlines()) if line]


def _looks_like_date(value: str) -> bool:
    lowered = value.lower()
    month_like = any(month in lowered for month in ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec"])
    has_digit = any(ch.isdigit() for ch in value)
    return bool(month_like and has_digit)


def _contains_football(value: str) -> bool:
    return "football" in _clean(value).lower()


def _looks_like_person_name(value: str) -> bool:
    value = _clean(value)
    if not value:
        return False
    parts = value.split()
    if not 2 <= len(parts) <= 4:
        return False
    for token in parts:
        if not re.fullmatch(r"[A-Za-z][A-Za-z.'-]*", token):
            return False
    banned = {
        "football",
        "athletics",
        "coach",
        "coaches",
        "staff",
        "director",
        "athletic",
        "schedule",
        "roster",
        "homepage",
        "search",
        "edlio",
        "school",
        "academy",
    }
    if value.lower() in banned:
        return False
    return True


def _extract_anchor_map(soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
    anchors: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _abs_url(base_url, str(anchor.get("href") or ""))
        if not href:
            continue
        anchors.append({"text": text, "href": href})
    return anchors


def _extract_page_snapshot(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = _extract_anchor_map(soup, url)
    title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else ""
    body_text = ""
    if soup.body:
        body_text = _clean(soup.body.get_text("\n", strip=True))
    return {
        "url": url,
        "title": title,
        "text": body_text,
        "html": html,
        "anchors": anchors,
        "soup": soup,
    }


def _extract_athletics_links(anchors: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for anchor in anchors:
        text = _clean(str(anchor.get("text") or ""))
        href = _clean(str(anchor.get("href") or ""))
        haystack = f"{text} {href}".lower()
        if not ( "athletics" in haystack or "coach" in haystack ):
            continue
        if "search" in href.lower():
            continue
        out.append({"text": text, "href": href})
    return _dedupe_keep_order(out)


def _extract_football_links(anchors: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for anchor in anchors:
        text = _clean(str(anchor.get("text") or ""))
        href = _clean(str(anchor.get("href") or ""))
        if not text and not href:
            continue
        if not _contains_football(f"{text} {href}"):
            continue
        out.append({"text": text, "href": href})
    return _dedupe_keep_order(out)


def _extract_football_events(soup: BeautifulSoup, page_url: str, anchors: list[dict[str, str]]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen: set[str] = set()

    for table in soup.select("table"):
        for row in table.select("tr"):
            cells = [_clean(cell.get_text(" ", strip=True)) for cell in row.select("td,th")]
            if not cells:
                continue
            if row.find("th"):
                continue
            row_blob = " ".join(cells).lower()
            if not _contains_football(row_blob):
                continue

            row_href = ""
            anchor = row.select_one("a[href]")
            if anchor:
                row_href = _abs_url(page_url, str(anchor.get("href") or ""))

            event: dict[str, Any] = {
                "title": "",
                "type": "",
                "date": "",
                "path": "",
                "url": row_href,
                "raw_columns": cells,
            }
            for col in cells:
                if _contains_football(col) and not event["title"]:
                    event["title"] = col
                if not event["date"] and _looks_like_date(col):
                    event["date"] = col
                if not event["type"] and col and not _looks_like_date(col) and not _contains_football(col):
                    event["type"] = col
            if not event["title"] and anchor:
                event["title"] = _clean(anchor.get_text(" ", strip=True))
            event["path"] = _clean(event["url"].replace("https://hope.auhsd.us", "")) if event["url"] else ""
            key = f"{event['title']}|{event['date']}|{event['url']}"
            if key in seen:
                continue
            seen.add(key)
            events.append(event)

    # Fallback from football anchors if table parse returns no rows.
    if not events:
        for anchor in anchors:
            text = _clean(str(anchor.get("text") or ""))
            href = _clean(str(anchor.get("href") or ""))
            if not _contains_football(f"{text} {href}"):
                continue
            if "/Hope/Search/" in href and "text=football" in href.lower():
                continue
            event = {
                "title": text,
                "type": "",
                "date": "",
                "path": href.replace("https://hope.auhsd.us", ""),
                "url": href,
                "raw_columns": [text],
            }
            key = f"{event['title']}|{event['path']}"
            if key in seen:
                continue
            seen.add(key)
            events.append(event)

    return events


def _extract_coach_lines(lines: list[str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for line in lines:
        if not _contains_football(line):
            continue
        lowered = line.lower()
        if not any(word in lowered for word in ("coach", "staff", "director", "head", "assist", "coach.")):
            continue
        # Common pattern: "Football Name" or "... Coach Name ..."
        for fragment in re.split(r"[;|]\s*", line):
            fragment = _clean(fragment)
            if not fragment:
                continue
            # remove prefix words like "Football", "Head Coach", etc.
            cleaned = re.sub(r"(?i)^.*?football\s*:?\s*", "", fragment)
            cleaned = re.sub(r"(?i)^coach(es)?\s*", "", cleaned).strip()
            cleaned = re.sub(r"\s{2,}", " ", cleaned)
            if _looks_like_person_name(cleaned):
                entries.append({"role_line": fragment, "name": cleaned})
                continue

            # Also capture embedded two-word names in the fragment.
            for match in re.finditer(r"\b([A-Z][A-Za-z'-]+\s+[A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+)?)\b", fragment):
                candidate = _clean(match.group(1))
                if _looks_like_person_name(candidate):
                    entries.append({"role_line": fragment, "name": candidate})

    # Deduplicate by name+role_line.
    return _dedupe_keep_order(entries)


async def _collect_pages() -> tuple[list[dict[str, Any]], list[str]]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    pages: list[dict[str, Any]] = []
    errors: list[str] = []

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
        )
        page = await context.new_page()

        try:
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await page.wait_for_timeout(1200)
                    html = await page.content()
                    pages.append(_extract_page_snapshot(html, page.url))
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:{url}")
        finally:
            await browser.close()

    return pages, errors


async def scrape_school() -> dict[str, Any]:
    """Scrape public football information for Hope High from fixed football-relevant pages."""
    pages, errors = await _collect_pages()

    if not pages:
        errors.append("blocked:all_targets_unreachable")

    page_by_url: dict[str, dict[str, Any]] = {}
    for item in pages:
        page_by_url[item.get("url", "")] = item

    home_page = page_by_url.get(HOME_URL, {})
    athletics_page = page_by_url.get(ATHLETICS_URL, {})
    coaches_page = page_by_url.get(COACHES_URL, {})
    search_page = page_by_url.get(FOOTBALL_SEARCH_URL, {})

    home_anchors = home_page.get("anchors", []) if isinstance(home_page.get("anchors"), list) else []
    athletics_anchors = athletics_page.get("anchors", []) if isinstance(athletics_page.get("anchors"), list) else []
    coaches_anchors = coaches_page.get("anchors", []) if isinstance(coaches_page.get("anchors"), list) else []
    search_anchors = search_page.get("anchors", []) if isinstance(search_page.get("anchors"), list) else []

    home_lines = _collect_lines(str(home_page.get("text") or ""))
    athletics_lines = _collect_lines(str(athletics_page.get("text") or ""))
    coaches_lines = _collect_lines(str(coaches_page.get("text") or ""))
    search_lines = _collect_lines(str(search_page.get("text") or ""))

    athletics_links = _extract_athletics_links([item for item in athletics_anchors if isinstance(item, dict)])
    football_links = _extract_football_links([item for item in athletics_anchors if isinstance(item, dict)])
    football_coach_links = _extract_football_links([item for item in coaches_anchors if isinstance(item, dict)])
    search_events = _extract_football_events(
        search_page.get("soup") if isinstance(search_page.get("soup"), BeautifulSoup) else BeautifulSoup("", "html.parser"),
        FOOTBALL_SEARCH_URL,
        [item for item in search_anchors if isinstance(item, dict)],
    )
    coach_entries = _extract_coach_lines(coaches_lines)
    football_mentions = _dedupe_keep_order([line for line in _collect_lines("\n".join(home_lines + athletics_lines + coaches_lines + search_lines)) if _contains_football(line)])

    football_coach_names = _dedupe_keep_order(
        [entry.get("name", "") for entry in coach_entries if isinstance(entry, dict)]
    )

    football_program_available = bool(
        football_links
        or search_events
        or football_coach_names
        or _contains_football(" ".join(home_lines + athletics_lines + coaches_lines + search_lines))
    )
    if not football_program_available:
        errors.append("no_public_football_content_found")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "home_url": HOME_URL,
        "athletics_department_url": ATHLETICS_URL,
        "coaches_url": COACHES_URL,
        "football_search_url": FOOTBALL_SEARCH_URL,
        "home_title": _clean(str(home_page.get("title") or "")),
        "athletics_department_title": _clean(str(athletics_page.get("title") or "")),
        "coaches_title": _clean(str(coaches_page.get("title") or "")),
        "football_department_links": [item for item in athletics_links if isinstance(item, dict)],
        "football_links_from_department": football_links,
        "coach_links_from_coaches_page": football_coach_links,
        "coach_lines": football_mentions,
        "coach_entries": coach_entries,
        "football_coach_names": football_coach_names,
        "football_event_count": len(search_events),
        "football_events": search_events,
        "football_search_anchor_count": len(search_anchors),
        "football_page_evidence": football_mentions[:20],
        "football_summary": (
            "Hope High has football references on the athletics page and coach page, with football-labelled search results and coaching staff text."
            if football_program_available
            else ""
        ),
    }

    pages_visited = _dedupe_keep_order([item.get("url", "") for item in pages if isinstance(item, dict) and item.get("url")])

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": pages_visited,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": get_proxy_runtime_meta(PROXY_PROFILE).get("proxy_profile"),
            "proxy_servers": get_proxy_runtime_meta(PROXY_PROFILE).get("proxy_servers"),
            "proxy_auth_mode": get_proxy_runtime_meta(PROXY_PROFILE).get("proxy_auth_mode"),
            "focus": "football_only",
            "target_urls": TARGET_URLS,
            "pages_visited": len(pages_visited),
            "navigation_errors": errors,
            "football_evidence_count": len(football_mentions) + len(search_events) + len(football_coach_names),
        },
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias for runtime compatibility."""
    return await scrape_school()
