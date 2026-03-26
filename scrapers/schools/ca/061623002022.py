"""Deterministic football scraper for Grossmont High (CA)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "061623002022"
SCHOOL_NAME = "Grossmont High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://www.foothillers.com/"
ATHLETICS_URL = "https://www.foothillers.com/Athletics/index.html"

TARGET_URLS = [HOME_URL, ATHLETICS_URL]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return " ".join((value or "").split()).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        key = _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


async def _collect_athletics_snapshot(page) -> tuple[list[dict[str, str]], str | None]:
    """Extract football schedule links and coach-contact URL from the athletics page."""

    js = '''() => {
        const normalize = (value) => (value || "").replace(/\s+/g, " ").trim();
        const isSchedule = (value) => normalize(value).toLowerCase() === "schedule";

        const extractLabel = (anchor) => {
            const candidates = [];
            const container = anchor.closest("li, p, td, tr, div, section") || anchor.parentElement || anchor;
            const rowText = normalize(container ? container.textContent : "");
            const rowTokens = rowText.split(" ").filter(Boolean);

            let node = anchor.previousSibling;
            while (node) {
                const candidate = normalize(node.textContent || "");
                if (candidate && !isSchedule(candidate)) {
                    candidates.push(candidate);
                    break;
                }
                node = node.previousSibling;
            }

            if (candidates.length) {
                return candidates[0];
            }

            node = anchor.previousElementSibling;
            while (node) {
                const candidate = normalize(node.textContent || "");
                if (candidate && !isSchedule(candidate)) {
                    candidates.push(candidate);
                    break;
                }
                node = node.previousElementSibling;
            }

            if (candidates.length) {
                return candidates[0];
            }

            const lastScheduleIndex = rowTokens.lastIndexOf("Schedule");
            if (lastScheduleIndex > 0) {
                return normalize(rowTokens.slice(Math.max(0, lastScheduleIndex - 3), lastScheduleIndex).join(" "));
            }

            if (rowTokens.length) {
                return rowTokens[rowTokens.length - 1];
            }

            return "";
        };

        const footballLinks = [];
        let coachContact = null;
        const anchors = Array.from(document.querySelectorAll("a[href]"));

        for (const anchor of anchors) {
            const text = normalize(anchor.textContent);
            if (!text) {
                continue;
            }

            const href = normalize(anchor.href || "");
            const lower = text.toLowerCase();

            const isCoachContact =
                lower.includes("coach") &&
                lower.includes("contact") &&
                href.includes("docs.google") &&
                !coachContact;

            if (isCoachContact) {
                coachContact = href;
            }

            if (!href.startsWith("http") || !href.includes("docs.google")) {
                continue;
            }

            if (lower === "schedule") {
                const label = normalize(extractLabel(anchor));
                if (!label) {
                    continue;
                }

                const labelLower = label.toLowerCase();
                const isFootball = /football/.test(labelLower);
                if (!isFootball) {
                    continue;
                }

                footballLinks.push({
                    "label": label,
                    "schedule_label": text,
                    "schedule_url": href,
                });
            }
        }

        return {footballLinks, coachContact};
    }'''

    body_text = _clean(await page.locator("body").inner_text())
    snapshot = await page.evaluate(js)

    football_schedule_rows = snapshot.get("footballLinks", []) if isinstance(snapshot, dict) else []
    if not isinstance(football_schedule_rows, list):
        football_schedule_rows = []

    football_schedules: list[dict[str, str]] = []
    for row in football_schedule_rows:
        if not isinstance(row, dict):
            continue
        row_label = _clean(str(row.get("label", "")))
        row_url = _clean(str(row.get("schedule_url", "")))
        if not row_label or not row_url:
            continue
        row_item = {
            "label": row_label,
            "url": row_url,
        }
        if row_item not in football_schedules:
            football_schedules.append(row_item)

    coach_contact_url = _clean(str(snapshot.get("coachContact", ""))) if isinstance(snapshot, dict) else ""
    return football_schedules, coach_contact_url, body_text


async def scrape_school() -> dict[str, Any]:
    """Scrape football program signals from Grossmont High's public athletics page."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []

    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True,
            proxy=proxy,
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1400, "height": 900},
        )
        page = await context.new_page()

        football_program_available = False
        football_schedules: list[dict[str, str]] = []
        coach_contact_url = ""
        football_snippets: list[str] = []

        try:
            await page.goto(ATHLETICS_URL, wait_until="domcontentloaded", timeout=70_000)
            await page.wait_for_timeout(1500)

            source_pages.append(page.url)
            football_schedules, coach_contact_url, football_snippets_text = await _collect_athletics_snapshot(page)
            if football_snippets_text:
                football_snippets = [line for line in football_snippets_text.split(" | ") if "Football" in line or "Flag Football" in line]

            if football_schedules:
                football_program_available = True
        except Exception as exc:  # noqa: BLE001
            errors.append(f"navigation_failed:{type(exc).__name__}:{ATHLETICS_URL}")
            football_program_available = False

        await browser.close()

    football_program_available = bool(football_schedules)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "athletics_page_url": ATHLETICS_URL,
        "coach_contact_url": coach_contact_url,
        "football_schedule_links": football_schedules,
        "football_schedule_count": len(football_schedules),
        "football_snippets": _dedupe_keep_order(football_snippets),
        "source_football_labels": [item.get("label", "") for item in football_schedules],
    }

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "proxy_profile": get_proxy_runtime_meta(profile=PROXY_PROFILE).get("proxy_profile"),
            "proxy_servers": get_proxy_runtime_meta(profile=PROXY_PROFILE).get("proxy_servers"),
            "proxy_auth_mode": get_proxy_runtime_meta(profile=PROXY_PROFILE).get("proxy_auth_mode"),
            "focus": "football_only",
            "target_urls": TARGET_URLS,
            "pages_checked": len(source_pages),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Entry point expected by runtime wrappers."""
    return await scrape_school()

