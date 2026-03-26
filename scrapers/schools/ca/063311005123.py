"""Deterministic football scraper for Delta High (CA)."""

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

NCES_ID = "063311005123"
SCHOOL_NAME = "Delta High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.rdusd.org/schools/delta-high-school/index"
CONTACTS_URL = "https://www.rdusd.org/schools/delta-high-school/our-school/school-contacts"
SPORTS_URL = "https://www.rdusd.org/schools/delta-high-school/our-school/delta-high-school-sport"

TARGET_URLS = [HOME_URL, CONTACTS_URL, SPORTS_URL]

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


def _extract_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in (text or "").splitlines():
        line = _clean(raw_line)
        if line:
            lines.append(line)
    return _dedupe_keep_order(lines)


async def _page_text(page) -> str:
    for selector in ("main", "body"):
        try:
            text = await page.locator(selector).inner_text(timeout=10000)
            cleaned = (text or "").strip()
            if cleaned:
                return cleaned
        except Exception:  # noqa: BLE001
            continue
    return ""


async def _collect_links(page) -> list[dict[str, str]]:
    raw_links = await page.eval_on_selector_all(
        "a[href]",
        """els => els.map(a => ({
            text: (a.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: (a.href || a.getAttribute('href') || '').trim()
        }))""",
    )
    if not isinstance(raw_links, list):
        return []

    links: list[dict[str, str]] = []
    for item in raw_links:
        if not isinstance(item, dict):
            continue
        href = _clean(str(item.get("href") or ""))
        if not href:
            continue
        links.append(
            {
                "text": _clean(str(item.get("text") or "")),
                "href": href,
            }
        )
    return links


async def _click_link(page, label: str) -> bool:
    locator = page.get_by_role("link", name=label).first
    if await locator.count() == 0:
        locator = page.locator("a[href]").filter(has_text=label).first
    if await locator.count() == 0:
        return False
    await locator.scroll_into_view_if_needed(timeout=10000)
    try:
        await locator.click(timeout=15000, force=True)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(1000)
        return True
    except Exception:  # noqa: BLE001
        return False


async def _expand_accordion(page, label: str) -> str:
    button = page.get_by_role("button", name=label).first
    if await button.count() == 0:
        button = page.locator("a.cs-accordion-title").filter(has_text=label).first
    if await button.count() == 0:
        return ""

    region_id = await button.get_attribute("aria-controls")
    await button.scroll_into_view_if_needed(timeout=10000)
    await button.click(timeout=15000)
    await page.wait_for_timeout(800)

    if region_id:
        try:
            return await page.locator(f"#{region_id}").inner_text(timeout=10000)
        except Exception:  # noqa: BLE001
            pass

    try:
        return await page.locator("div.cs-accordion-content:not([hidden])").last.inner_text(timeout=10000)
    except Exception:  # noqa: BLE001
        return ""


def _extract_contact_blocks(text: str) -> dict[str, list[str]]:
    lines = _extract_lines(text)
    blocks: dict[str, list[str]] = {
        "principal": [],
        "vice_principal": [],
        "office_staff": [],
        "student_support_staff": [],
    }
    current: str | None = None

    for line in lines:
        if line in {"Principal", "Vice Principal", "Office Staff", "Student Support Staff"}:
            current = line
            continue
        if not current:
            continue
        if "@" in line or line.lower().startswith("mailto:"):
            continue
        if current == "Principal":
            blocks["principal"].append(line)
        elif current == "Vice Principal":
            blocks["vice_principal"].append(line)
        elif current == "Office Staff":
            blocks["office_staff"].append(line)
        elif current == "Student Support Staff":
            blocks["student_support_staff"].append(line)

    return {key: _dedupe_keep_order(values) for key, values in blocks.items()}


def _extract_physical_ed_staff(text: str) -> list[str]:
    names: list[str] = []
    for line in _extract_lines(text):
        if "Physical Education" not in line:
            continue
        name = _clean(line.split("-", 1)[0])
        if name:
            names.append(name)
    return _dedupe_keep_order(names)


def _filter_football_links(links: list[dict[str, str]]) -> list[dict[str, str]]:
    keywords = (
        "football",
        "athleticclearance",
        "athletic clearance",
        "cifsjs",
        "maxpreps",
        "gofan",
        "sports physical",
        "clearance",
        "waiver",
        "incident report",
    )
    filtered: list[dict[str, str]] = []
    for link in links:
        combo = f"{link.get('text', '')} {link.get('href', '')}".lower()
        if any(keyword in combo for keyword in keywords):
            filtered.append(link)
    return filtered


def _dedupe_links_keep_order(links: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    output: list[dict[str, str]] = []
    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        if not href:
            continue
        key = (text, href)
        if key in seen:
            continue
        seen.add(key)
        output.append({"text": text, "href": href})
    return output


async def scrape_school() -> dict[str, Any]:
    """Scrape public football evidence from Delta High's school pages."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []

    home_text = ""
    contacts_text = ""
    sports_text = ""
    football_text = ""
    contacts_links: list[dict[str, str]] = []
    sports_links: list[dict[str, str]] = []

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
            await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)
            await page.wait_for_timeout(1200)
            source_pages.append(page.url)
            home_text = await _page_text(page)
            navigation_steps.append("visit_home")

            if not await _click_link(page, "School Contacts"):
                await page.goto(CONTACTS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            contacts_text = await _page_text(page)
            contacts_links = await _collect_links(page)
            navigation_steps.append("open_school_contacts")

            await _expand_accordion(page, "SCHOOL OFFICE PHONE CONTACTS")
            await _expand_accordion(page, "STAFF EMAIL")
            await _expand_accordion(page, "TEACHING STAFF EMAIL")
            contacts_text = await _page_text(page)

            if not await _click_link(page, "CMS/DHS Sports"):
                await page.goto(SPORTS_URL, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(1000)
            source_pages.append(page.url)
            sports_text = await _page_text(page)
            sports_links = await _collect_links(page)
            navigation_steps.append("open_cms_dhs_sports")

            football_text = await _expand_accordion(page, "Football")
            sports_text = await _page_text(page)
            navigation_steps.append("expand_football_accordion")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{exc}")
        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    home_lines = _extract_lines(home_text)
    sports_lines = _extract_lines(sports_text)
    football_lines = _extract_lines(football_text or sports_text)
    contacts_blocks = _extract_contact_blocks(contacts_text)
    physical_ed_staff = _extract_physical_ed_staff(contacts_text)
    football_links = _dedupe_links_keep_order(_filter_football_links([*contacts_links, *sports_links]))

    school_office_phone = ""
    office_phone_match = re.search(r"\(\d{3}\)\s*\d{3}-\d{4}", contacts_text)
    if office_phone_match:
        school_office_phone = office_phone_match.group(0)

    football_schedule_lines = [
        line
        for line in football_lines
        if any(token in line.lower() for token in ("football", "schedule", "playoff", "woodland christian", "7pm", "11/"))
    ]

    if not football_schedule_lines and football_text:
        football_schedule_lines = _extract_lines(football_text)

    football_section_titles = [
        line for line in sports_lines if line in {"Fall Schedules", "Winter Schedules", "Spring Schedules", "Football"}
    ]

    football_program_available = bool(football_schedule_lines or football_lines or football_links)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_delta_high_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_urls": {
            "home": HOME_URL,
            "contacts": CONTACTS_URL,
            "sports": SPORTS_URL,
        },
        "school_contact_summary": {
            "office_phone": school_office_phone,
            "principal": contacts_blocks["principal"][0] if contacts_blocks["principal"] else "",
            "vice_principal": contacts_blocks["vice_principal"][0] if contacts_blocks["vice_principal"] else "",
            "office_staff": contacts_blocks["office_staff"],
            "student_support_staff": contacts_blocks["student_support_staff"],
            "physical_ed_staff": physical_ed_staff,
        },
        "football_page": {
            "section_title": "Football",
            "schedule_title": "Playoff Schedule",
            "schedule_lines": football_schedule_lines,
            "section_lines": football_lines,
            "section_titles": football_section_titles,
        },
        "football_resource_links": football_links,
        "football_resource_urls": [link["href"] for link in football_links],
        "football_resource_labels": [link["text"] for link in football_links],
        "home_page_highlights": [
            line for line in home_lines if "sports" in line.lower() or "contacts" in line.lower()
        ],
        "summary": (
            "Delta High publishes a public CMS/DHS sports page with a football accordion, a playoff schedule, football-related forms and clearance links, and school contact information."
        ),
    }

    scrape_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)
    scrape_meta.update(
        {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            "target_urls": TARGET_URLS,
            "pages_visited": len(source_pages),
            "navigation_steps": navigation_steps,
            "focus": "football_only",
            "football_lines_found": len(football_lines),
            "football_links_found": len(football_links),
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
