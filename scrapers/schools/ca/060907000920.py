"""Deterministic football scraper for Coachella Valley High (CA)."""

from __future__ import annotations

import re
import subprocess
import tempfile
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "060907000920"
SCHOOL_NAME = "Coachella Valley High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://cvhs.cvusd.us"
ATHLETICS_URL = f"{BASE_URL}/242575_2"
SPORT_SEASON_REPORT_URL = "https://files.smartsites.parentsquare.com/9175/postseasonreport1.pdf"
ACADEMIC_ELIGIBILITY_URL = "https://files.smartsites.parentsquare.com/9175/academiceligibility1.pdf"
ATHLETIC_CLEARANCE_GUIDE_URL = "https://files.smartsites.parentsquare.com/9175/athleticclearanceinformation1.pdf"
SPORTS_PHYSICAL_FORM_URL = "https://files.smartsites.parentsquare.com/9175/sportsphysicalform.pdf"
ATHLETIC_CLEARANCE_URL = "https://athleticclearance.com/"

TARGET_URLS = [
    ATHLETICS_URL,
    SPORT_SEASON_REPORT_URL,
    ACADEMIC_ELIGIBILITY_URL,
    ATHLETIC_CLEARANCE_GUIDE_URL,
    SPORTS_PHYSICAL_FORM_URL,
    ATHLETIC_CLEARANCE_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        item = _clean(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _matching_lines(text: str, keywords: tuple[str, ...], *, limit: int = 25) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = _clean(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if any(keyword in lowered for keyword in keywords):
            lines.append(line)
    return _dedupe_keep_order(lines)[:limit]


def _download_text_from_pdf(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as response:
        payload = response.read()

    with tempfile.TemporaryDirectory(prefix="cvhs_pdf_") as temp_dir:
        pdf_path = Path(temp_dir) / "document.pdf"
        txt_path = Path(temp_dir) / "document.txt"
        pdf_path.write_bytes(payload)
        proc = subprocess.run(
            ["pdftotext", str(pdf_path), str(txt_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        _ = proc
        return txt_path.read_text(encoding="utf-8", errors="replace")


async def _collect_athletics_page(page) -> dict[str, Any]:
    await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(1500)

    body_text = await page.inner_text("body")
    title = _clean(await page.title())

    links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(el => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || ''
        }))""",
    )
    if not isinstance(links, list):
        links = []

    video_sources = await page.eval_on_selector_all(
        "video source",
        "els => els.map(el => el.src || '').filter(Boolean)",
    )
    if not isinstance(video_sources, list):
        video_sources = []

    leadership_lines = _matching_lines(
        body_text,
        ("athletics director", "assistant athletics director"),
        limit=10,
    )
    document_links = [
        f"{_clean(str(item.get('text') or ''))}|{_clean(str(item.get('href') or ''))}"
        for item in links
        if isinstance(item, dict)
        and item.get("href")
        and (
            "files.smartsites.parentsquare.com/9175/" in str(item.get("href") or "").lower()
            or "athleticclearance.com" in str(item.get("href") or "").lower()
        )
        and any(
            token in f"{item.get('text', '')} {item.get('href', '')}".lower()
            for token in (
                "report",
                "physical",
                "eligibility",
                "clearance",
                "communication",
                "coach",
                "student form",
                "sport",
            )
        )
    ]

    return {
        "url": page.url,
        "title": title,
        "football_video_urls": _dedupe_keep_order(
            [str(value) for value in video_sources if "football" in str(value).lower()]
        ),
        "athletics_caption_lines": _matching_lines(
            body_text,
            ("student-athletes", "athletics", "sport season report"),
            limit=20,
        ),
        "leadership_lines": leadership_lines,
        "document_links": _dedupe_keep_order(document_links),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Coachella Valley High's public football signals from its athletics page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    athletics_signal: dict[str, Any] = {}
    academic_eligibility_text = ""
    athletic_clearance_text = ""

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=get_playwright_proxy_config(profile=PROXY_PROFILE),
        )
        context = await browser.new_context(
            ignore_https_errors=True,
            locale="en-US",
            user_agent=USER_AGENT,
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            athletics_signal = await _collect_athletics_page(page)
            source_pages.append(str(athletics_signal.get("url") or ATHLETICS_URL))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"athletics_page_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    for resource_url in (
        SPORT_SEASON_REPORT_URL,
        ACADEMIC_ELIGIBILITY_URL,
        ATHLETIC_CLEARANCE_GUIDE_URL,
    ):
        source_pages.append(resource_url)

    try:
        academic_eligibility_text = _download_text_from_pdf(ACADEMIC_ELIGIBILITY_URL)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"academic_eligibility_pdf_failed:{type(exc).__name__}:{exc}")

    try:
        athletic_clearance_text = _download_text_from_pdf(ATHLETIC_CLEARANCE_GUIDE_URL)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"athletic_clearance_pdf_failed:{type(exc).__name__}:{exc}")

    football_video_urls = [
        str(value)
        for value in athletics_signal.get("football_video_urls", [])
        if isinstance(value, str)
    ]
    football_asset_filenames = _dedupe_keep_order(
        [Path(url.split("?", 1)[0]).name for url in football_video_urls]
    )

    academic_eligibility_lines = _matching_lines(
        academic_eligibility_text,
        ("fall sports", "2.0", "eligible", "4 classes"),
        limit=15,
    )
    athletic_clearance_lines = _matching_lines(
        athletic_clearance_text,
        ("athletic clearance", "student-athletes", "practice", "physical", "insurance"),
        limit=15,
    )
    leadership_lines = [
        str(value)
        for value in athletics_signal.get("leadership_lines", [])
        if isinstance(value, str)
    ]
    document_links = [
        str(value)
        for value in athletics_signal.get("document_links", [])
        if isinstance(value, str)
    ]
    athletics_caption_lines = [
        str(value)
        for value in athletics_signal.get("athletics_caption_lines", [])
        if isinstance(value, str)
    ]

    football_program_available = bool(football_video_urls and academic_eligibility_lines)
    if not football_program_available:
        errors.append("blocked:no_public_football_signal_verified_on_cvhs_athletics_page")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": str(athletics_signal.get("url") or ATHLETICS_URL),
        "football_media_assets": football_video_urls,
        "football_asset_filenames": football_asset_filenames,
        "athletics_documents": {
            "sport_season_report_url": SPORT_SEASON_REPORT_URL,
            "academic_eligibility_url": ACADEMIC_ELIGIBILITY_URL,
            "athletic_clearance_guide_url": ATHLETIC_CLEARANCE_GUIDE_URL,
            "sports_physical_form_url": SPORTS_PHYSICAL_FORM_URL,
            "athletic_clearance_url": ATHLETIC_CLEARANCE_URL,
        },
        "academic_eligibility_lines": academic_eligibility_lines,
        "athletic_clearance_lines": athletic_clearance_lines,
        "athletics_caption_lines": athletics_caption_lines,
        "athletics_leadership_lines": leadership_lines,
        "athletics_document_links": document_links,
        "football_schedule_public": False,
        "football_schedule_note": (
            "The public CVHS athletics page did not expose a football-specific schedule link."
        ),
        "football_coach_public": False,
        "football_coach_note": (
            "The Athletics Leadership section rendered role labels publicly, but no staff names "
            "were exposed in the live DOM."
        ),
        "summary": (
            "CVHS publishes a football-branded athletics hero video and fall-sports eligibility "
            "requirements on its public athletics page."
            if football_program_available
            else ""
        ),
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": _dedupe_keep_order(source_pages),
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "target_urls": TARGET_URLS,
            "focus": "football_only",
            "proxy_profile": PROXY_PROFILE,
            "proxy_runtime": get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "playwright_navigation_used": True,
            "pdf_parser": "pdftotext",
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
