"""Deterministic football scraper for Desert Mirage High (CA)."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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

NCES_ID = "060907011175"
SCHOOL_NAME = "Desert Mirage High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

BASE_URL = "https://www.cifsshome.org"
DIRECTORY_URL = f"{BASE_URL}/widget/school/directory?section=1"
DETAILS_URL_TEMPLATE = f"{BASE_URL}/widget/get-school-details/{{school_id}}/details"
SCHOOL_BUTTON_LABELS = ("Desert Mirage", "Desert Mirage High")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


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


def _normalize_website(value: str) -> str:
    clean = _clean(value)
    if not clean:
        return ""
    if clean.startswith("http://") or clean.startswith("https://"):
        return clean
    return f"https://{clean}"


def _extract_school_id(directory_html: str) -> str:
    soup = BeautifulSoup(directory_html, "html.parser")
    for button in soup.select("button.school-btn[data-id]"):
        text = _clean(button.get_text(" ", strip=True))
        if text.lower() not in {label.lower() for label in SCHOOL_BUTTON_LABELS}:
            continue
        school_id = _clean(str(button.get("data-id") or ""))
        if school_id:
            return school_id
    raise RuntimeError("Could not find Desert Mirage in the CIFSS school directory")


def _extract_football_coach(coaches: list[dict[str, Any]]) -> dict[str, str]:
    for coach in coaches:
        if coach.get("sport") != "Football (11 person)":
            continue
        first = _clean(str(coach.get("firstname") or ""))
        last = _clean(str(coach.get("lastname") or ""))
        return {
            "name": _clean(f"{first} {last}"),
            "sport": "Football (11 person)",
            "title": _clean(str(coach.get("aft_name") or "")),
            "email": _clean(str(coach.get("email") or "")),
            "level": _clean(str(coach.get("level_name") or "")),
        }
    return {}


def _extract_athletic_contacts(athletic_faculties: list[dict[str, Any]]) -> list[dict[str, str]]:
    contacts: list[dict[str, str]] = []
    wanted_titles = {
        "Activities Director",
        "Athletic Director",
        "Athletic Trainer",
        "Principal",
    }
    for contact in athletic_faculties:
        title = _clean(str(contact.get("aft_name") or ""))
        if title not in wanted_titles:
            continue
        first = _clean(str(contact.get("firstname") or ""))
        last = _clean(str(contact.get("lastname") or ""))
        name = _clean(f"{first} {last}")
        if not name:
            continue
        contacts.append(
            {
                "name": name,
                "title": title,
                "email": _clean(str(contact.get("email") or "")),
                "phone": _clean(str(contact.get("work_phone") or "")),
            }
        )
    return contacts


async def scrape_school() -> dict[str, Any]:
    """Scrape Desert Mirage High football signals from the public CIFSS widget."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    directory_html = ""
    details_payload: dict[str, Any] = {}

    assert_not_blocklisted([DIRECTORY_URL], profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True, proxy=proxy)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1440, "height": 900},
        )
        page = await context.new_page()

        try:
            await page.goto(DIRECTORY_URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector("button.school-btn[data-id]", timeout=60000)
            await page.wait_for_timeout(500)
            directory_html = await page.content()
            source_pages.append(page.url)
        except Exception as exc:  # noqa: BLE001
            await context.close()
            await browser.close()
            raise RuntimeError(f"Failed to load CIFSS directory: {type(exc).__name__}: {exc}") from exc

        school_id = _extract_school_id(directory_html)
        details_url = DETAILS_URL_TEMPLATE.format(school_id=school_id)
        assert_not_blocklisted([details_url], profile=PROXY_PROFILE)

        try:
            payload_text = await page.evaluate(
                """async (schoolId) => {
                    const resp = await fetch(`/widget/get-school-details/${schoolId}/details`, {
                        headers: {
                            'Accept': 'application/json, text/javascript, */*; q=0.01',
                            'X-Requested-With': 'XMLHttpRequest',
                        },
                    });
                    return await resp.text();
                }""",
                school_id,
            )
            details_payload = json.loads(payload_text)
            source_pages.append(details_url)
        except Exception as exc:  # noqa: BLE001
            await context.close()
            await browser.close()
            raise RuntimeError(f"Failed to fetch CIFSS details: {type(exc).__name__}: {exc}") from exc

        await context.close()
        await browser.close()

    school = details_payload.get("school", {})
    coaches = details_payload.get("coaches", [])
    athletic_faculties = details_payload.get("athleticFaculties", [])

    football_coach = _extract_football_coach(coaches if isinstance(coaches, list) else [])
    athletic_contacts = _extract_athletic_contacts(
        athletic_faculties if isinstance(athletic_faculties, list) else []
    )

    football_program_available = bool(football_coach)
    if not football_program_available:
        errors.append("no_public_football_coach_listed_in_cifss_school_details")

    source_school_name = _clean(str(school.get("full_name") or school.get("name") or ""))
    school_address = _clean(str(school.get("address_line_1") or ""))
    school_city = _clean(str(school.get("city") or ""))
    school_state = _clean(str(school.get("physical_state") or STATE))
    school_zip = _clean(str(school.get("physical_zip") or ""))
    school_phone = _clean(str(school.get("phone") or ""))
    school_website = _normalize_website(str(school.get("website") or ""))
    mascot = _clean(str(school.get("mascot") or ""))

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "cifss_school_id": int(school_id),
        "school_directory_button_label": source_school_name or "Desert Mirage",
        "school_directory_url": DIRECTORY_URL,
        "details_url": details_url,
        "source_school_name": source_school_name,
        "school_profile": {
            "name": source_school_name,
            "address": school_address,
            "city": school_city,
            "state": school_state,
            "zip": school_zip,
            "phone": school_phone,
            "website": school_website,
            "mascot": mascot,
            "enrollment": school.get("enrollment"),
            "grades": details_payload.get("grades"),
        },
        "football_team": {
            "sport": "Football (11 person)",
            "coach": football_coach,
        },
        "athletic_contacts": athletic_contacts,
        "football_signals": _dedupe_keep_order(
            [
                f"Directory entry: {source_school_name or 'Desert Mirage'} (CIFSS school id {school_id})",
                (
                    f"Football coach listed: {football_coach.get('name')} "
                    f"({football_coach.get('email')})"
                    if football_coach
                    else ""
                ),
                f"School mascot: {mascot}",
                f"School website: {school_website}",
            ]
        ),
    }

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "target_urls": [DIRECTORY_URL, details_url],
            "pages_checked": 2,
            "focus": "football_only",
            "cifss_school_id": int(school_id),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
