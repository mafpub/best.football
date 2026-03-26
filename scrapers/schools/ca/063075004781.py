"""Deterministic football scraper for Colfax High (CA)."""

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

NCES_ID = "063075004781"
SCHOOL_NAME = "Colfax High"
STATE = "CA"

PROXY_PROFILE = "datacenter"
BASE_URL = "https://sites.google.com/puhsd.k12.ca.us/colfax-high-school-2021-22"
HOME_URL = f"{BASE_URL}/home"
ATHLETICS_URL = f"{BASE_URL}/athletics-home"
SUMMER_CAMPS_URL = f"{BASE_URL}/athletics-home/summer-camps"
HANDBOOK_VIEW_URL = "https://drive.google.com/file/d/16LXFlA_Hu8EPRhWDeeImiTRh3PKvxyM-/view?usp=sharing"
HANDBOOK_DOWNLOAD_URL = "https://drive.google.com/uc?export=download&id=16LXFlA_Hu8EPRhWDeeImiTRh3PKvxyM-"
CIF_SURVEY_VIEW_URL = "https://drive.google.com/file/d/1PaRkNwiw9VgxgWlum4XFUSS-6Q6sMjgi/view?usp=sharing"
CIF_SURVEY_DOWNLOAD_URL = "https://drive.google.com/uc?export=download&id=1PaRkNwiw9VgxgWlum4XFUSS-6Q6sMjgi"

TARGET_URLS = [
    HOME_URL,
    ATHLETICS_URL,
    SUMMER_CAMPS_URL,
    HANDBOOK_VIEW_URL,
    CIF_SURVEY_VIEW_URL,
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
    out: list[str] = []
    for value in values:
        item = _clean(str(value))
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _extract_emails(text: str) -> list[str]:
    return _dedupe_keep_order(re.findall(r"[\w.\-+]+@[\w.\-]+\.\w+", text))


def _html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return "\n".join(
        _clean(line)
        for line in soup.get_text("\n", strip=True).splitlines()
        if _clean(line)
    )


def _extract_links(html: str, base_url: str) -> list[dict[str, str]]:
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
    out: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = str(link.get("href") or "").strip()
        key = (text, href)
        if not href or key in seen:
            continue
        seen.add(key)
        out.append({"text": text, "href": href})
    return out


def _extract_keyword_lines(text: str, keywords: tuple[str, ...]) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        lowered = line.lower()
        if line and any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)


def _extract_pdf_text(payload: bytes) -> str:
    reader = PdfReader(io.BytesIO(payload))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _parse_athletic_director(cif_text: str) -> dict[str, str]:
    match = re.search(
        r"Athletic Director\s+([A-Za-z.\- ]+)\s+Athletic Director Email\s+([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        cif_text,
    )
    if not match:
        return {}
    return {
        "name": _clean(match.group(1)),
        "email": _clean(match.group(2)),
    }


def _extract_cif_rows(cif_text: str) -> list[str]:
    rows: list[str] = []
    for raw_line in cif_text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if lowered.startswith("flag football ") or lowered.startswith("football - 11 player "):
            rows.append(line)
    return _dedupe_keep_order(rows)


def _team_names_from_cif_rows(rows: list[str]) -> list[str]:
    names: list[str] = []
    for row in rows:
        match = re.match(r"^(Flag Football|Football - 11 player)\b", row)
        if match:
            names.append(match.group(1))
    return _dedupe_keep_order(names)


async def scrape_school() -> dict[str, Any]:
    """Scrape publicly available Colfax football signals from school-hosted pages and linked PDFs."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    athletics_text = ""
    athletics_links: list[dict[str, str]] = []
    summer_camps_text = ""
    handbook_text = ""
    cif_text = ""

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

        try:
            try:
                response = await context.request.get(ATHLETICS_URL, timeout=90000)
                source_pages.append(str(response.url))
                athletics_html = await response.text()
                athletics_text = _html_to_text(athletics_html)
                athletics_links = _extract_links(athletics_html, str(response.url))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"athletics_fetch_failed:{type(exc).__name__}")

            try:
                response = await context.request.get(SUMMER_CAMPS_URL, timeout=90000)
                source_pages.append(str(response.url))
                summer_camps_text = _html_to_text(await response.text())
            except Exception as exc:  # noqa: BLE001
                errors.append(f"summer_camps_fetch_failed:{type(exc).__name__}")

            try:
                response = await context.request.get(HANDBOOK_DOWNLOAD_URL, timeout=90000)
                source_pages.append(HANDBOOK_VIEW_URL)
                handbook_text = _extract_pdf_text(await response.body())
            except Exception as exc:  # noqa: BLE001
                errors.append(f"handbook_fetch_failed:{type(exc).__name__}")

            try:
                response = await context.request.get(CIF_SURVEY_DOWNLOAD_URL, timeout=90000)
                source_pages.append(CIF_SURVEY_VIEW_URL)
                cif_text = _extract_pdf_text(await response.body())
            except Exception as exc:  # noqa: BLE001
                errors.append(f"cif_survey_fetch_failed:{type(exc).__name__}")
        finally:
            await browser.close()

    athletics_lines = _extract_keyword_lines(
        athletics_text,
        (
            "athletics",
            "21 different sports",
            "varsity level",
            "fall sports",
            "athletic handbook",
            "registration",
        ),
    )
    summer_camp_lines = _extract_keyword_lines(
        summer_camps_text,
        (
            "colfax summer camps",
            "flag football",
            "summer camp",
            "pre-register",
        ),
    )
    handbook_lines = _extract_keyword_lines(
        handbook_text,
        (
            "athletic clearance",
            "football techniques",
            "football players only",
        ),
    )
    cif_rows = _extract_cif_rows(cif_text)
    athletic_director = _parse_athletic_director(cif_text)

    football_team_names = _team_names_from_cif_rows(cif_rows)
    football_program_available = bool(cif_rows or any("flag football" in line.lower() for line in summer_camp_lines))

    football_links = _dedupe_link_dicts([
        entry
        for entry in athletics_links
        if any(
            keyword in f"{entry.get('text', '')} {entry.get('href', '')}".lower()
            for keyword in ("athletic handbook", "fall sports", "summer camps")
        )
    ])

    athletic_director_email = athletic_director.get("email")
    program_contact_emails = [athletic_director_email] if athletic_director_email else []

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_home_url": ATHLETICS_URL,
        "summer_camps_url": SUMMER_CAMPS_URL,
        "athletic_handbook_url": HANDBOOK_VIEW_URL,
        "cif_participation_survey_url": CIF_SURVEY_VIEW_URL,
        "football_team_names": football_team_names,
        "football_cif_participation_rows": cif_rows,
        "athletic_director_contact": athletic_director,
        "football_handbook_lines": handbook_lines,
        "flag_football_camp_lines": summer_camp_lines,
        "athletics_page_lines": athletics_lines,
        "football_related_links": football_links,
        "program_contact_emails": program_contact_emails,
        "summary": (
            "Colfax High publicly exposes football evidence through its athletics site, a linked school CIF participation survey with football and flag football rows, a summer camps page listing flag football summer camp, and the athletic handbook's football-techniques clearance requirement."
            if football_program_available
            else ""
        ),
        "notes": (
            "No public football schedule, roster, or dedicated football coach page was exposed through proxied retrieval. The strongest football-specific signals are the school-linked CIF participation survey rows and the summer-camp and handbook references."
            if football_program_available
            else ""
        ),
    }

    if not football_program_available:
        errors.append("no_public_football_content_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": PROXY_PROFILE,
            "proxy_runtime": get_proxy_runtime_meta(PROXY_PROFILE),
            "fetch_mode": "playwright_context_request",
            "focus": "football_only",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
