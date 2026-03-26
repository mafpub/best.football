"""Deterministic football scraper for Hilmar High School (CA)."""

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

NCES_ID = "061722002177"
SCHOOL_NAME = "Hilmar High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

DISTRICT_HOME_URL = "https://www.hilmarusd.org"
HHS_HOME_URL = "https://hhs.hilmarusd.org"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

FOOTBALL_TERMS = {"football", "flag football"}


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        text = _clean(value)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _contains_football_term(value: str) -> bool:
    lowered = value.lower()
    return any(term in lowered for term in FOOTBALL_TERMS)


def _looks_like_heading(value: str, *, allow_single_word: bool = False) -> bool:
    text = _clean(value)
    if not text:
        return False
    if text.upper() != text:
        return False
    if any(ch.isdigit() for ch in text):
        return False
    if not re.fullmatch(r"[A-Z0-9 &'\"()\\-./]+", text):
        return False
    words = text.split()
    if len(words) > 6:
        return False
    if len(words) == 1 and not allow_single_word:
        return False
    return True


def _looks_like_team_heading(value: str) -> str:
    text = _clean(value).upper()
    if text in {"VARSITY", "JV", "FRESHMAN", "FROSH"}:
        if text == "FROSH":
            return "Freshman"
        return text.title()
    return ""


def _parse_name_and_role(line: str) -> tuple[str, str]:
    text = _clean(line)
    if not text:
        return "", ""
    role_patterns = [
        r"(?i)\bhead coach\b",
        r"(?i)\bassistant coach\b",
        r"(?i)\bstatistician\b",
        r"(?i)\bcoach\b",
    ]
    for pattern in role_patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        role = _clean(match.group(0))
        name = _clean(text[: match.start()])
        return name, role.title()
    return text, ""


def _extract_staff_sections(lines: list[str]) -> list[dict[str, str]]:
    staff: list[dict[str, str]] = []
    current_team = "Football"

    for line in lines:
        if not _clean(line):
            continue

        team = _looks_like_team_heading(line)
        if team:
            current_team = team
            continue

        if _contains_football_term(line):
            # skip pure sport labels already represented by section headers
            continue

        lowered = line.lower()
        if any(token in lowered for token in ("head coach", "assistant coach", "coach", "statistician")):
            name, role = _parse_name_and_role(line)
            if not name and not role:
                continue

            staff.append(
                {
                    "team": current_team,
                    "raw_line": _clean(line),
                }
            )
            if name:
                staff[-1]["name"] = name
            if role:
                staff[-1]["role"] = role
    return staff


def _extract_coach_names(lines: list[str]) -> list[str]:
    names: list[str] = []
    for item in _extract_staff_sections(lines):
        name = _clean(str(item.get("name") or ""))
        if name:
            names.append(name)
    return _dedupe_keep_order(names)


def _extract_team_names(lines: list[str]) -> list[str]:
    teams = {"Football"}
    for line in lines:
        team = _looks_like_team_heading(line)
        if team:
            teams.add(team)
        if "GIRLS FLAG FOOTBALL" in _clean(line).upper():
            teams.add("Girls Flag Football")
    return _dedupe_keep_order(list(teams))


def _extract_schedule_lines(lines: list[str], max_items: int = 30) -> list[str]:
    out: list[str] = []
    for line in lines:
        line_clean = _clean(line)
        if not line_clean:
            continue
        if not _contains_football_term(line_clean):
            continue
        if not re.search(r"\b(mon|tue|wed|thu|fri|sat|sun|\d{1,2}/\d{1,2})\b", line_clean, re.IGNORECASE):
            continue
        out.append(line_clean)
    return _dedupe_keep_order(out[:max_items])


def _extract_football_sections(lines: list[str]) -> list[tuple[str, list[str]]]:
    heading_indexes = [
        i
        for i, line in enumerate(lines)
        if _contains_football_term(line) and _looks_like_heading(line, allow_single_word=True)
    ]
    if not heading_indexes:
        return []

    sections: list[tuple[str, list[str]]] = []
    for idx, start in enumerate(heading_indexes):
        heading = _clean(lines[start])
        end = heading_indexes[idx + 1] if idx + 1 < len(heading_indexes) else len(lines)
        section_lines = [lines[i] for i in range(start + 1, end)]
        if not section_lines:
            continue
        sections.append((heading, section_lines))
    return sections


def _extract_league_links(links: list[dict[str, str]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    league_pages: list[dict[str, str]] = []
    football_documents: list[dict[str, str]] = []

    for link in links:
        text = _clean(str(link.get("text") or ""))
        href = _clean(str(link.get("href") or ""))
        if not text or not href:
            continue
        lower_text = text.lower()
        lower_href = href.lower()

        if "transvalley" in lower_href or "trans valley" in lower_text:
            league_pages.append({"text": text, "url": href})

        is_football_pdf = href.endswith(".pdf") and _contains_football_term(lower_text)
        is_football_wsimg = "img1.wsimg.com" in lower_href and _contains_football_term(lower_text)
        if is_football_pdf or is_football_wsimg:
            football_documents.append({"text": text, "url": href})

    return _dedupe_dicts(league_pages), _dedupe_dicts(football_documents)


def _dedupe_dicts(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        text = _clean(str(row.get("text") or ""))
        url = _clean(str(row.get("url") or ""))
        if not url or url in seen:
            continue
        seen.add(url)
        out.append({"text": text, "url": url})
    return out


def _find_first_matching_link(
    links: list[dict[str, str]],
    *,
    text_contains: str | None = None,
    href_contains: str | None = None,
    href_startswith: str | None = None,
) -> str:
    t_hint = (text_contains or "").strip().lower()
    h_hint = (href_contains or "").strip().lower()
    h_start = (href_startswith or "").strip().lower()
    for item in links:
        text = _clean(str(item.get("text") or "")).lower()
        href = _clean(str(item.get("href") or "")).lower()
        if t_hint and t_hint in text:
            return _clean(str(item.get("href") or ""))
        if h_hint and h_hint in href:
            return _clean(str(item.get("href") or ""))
        if h_start and href.startswith(h_start):
            return _clean(str(item.get("href") or ""))
    return ""


async def _collect_snapshot(page, requested_url: str) -> dict[str, Any]:
    body = await page.locator("body").inner_text()
    raw_links = await page.eval_on_selector_all(
        "a[href]",
        """
        els => els.map((el) => ({
            text: (el.textContent || '').replace(/\\s+/g, ' ').trim(),
            href: el.href || '',
        }))
        """,
    )
    if not isinstance(raw_links, list):
        raw_links = []

    links = [
        {
            "text": _clean(str(item.get("text") or "")),
            "href": _clean(str(item.get("href") or "")),
        }
        for item in raw_links
        if _clean(str(item.get("href") or ""))
    ]
    lines = _dedupe_keep_order([_clean(line) for line in (body or "").splitlines() if _clean(line)])

    return {
        "requested_url": requested_url,
        "final_url": page.url,
        "title": _clean(await page.title()),
        "body": _clean(body or ""),
        "lines": lines,
        "links": links,
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape public Hilmar High football data from athletics pages and league links."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted([DISTRICT_HOME_URL, HHS_HOME_URL], profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    navigation_steps: list[str] = []
    snapshots: list[dict[str, Any]] = []

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
            # 1) Discover the district home and the Hilmar High subdomain link.
            district_snapshot: dict[str, Any] = {}
            try:
                await page.goto(DISTRICT_HOME_URL, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1200)
                district_snapshot = await _collect_snapshot(page, DISTRICT_HOME_URL)
                source_pages.append(district_snapshot["final_url"])
                navigation_steps.append("district_home")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:district_home")

            district_links = district_snapshot.get("links", [])
            school_url = _find_first_matching_link(
                district_links,
                text_contains="hilmar high school",
                href_contains="hhs.hilmarusd.org",
            )
            if not school_url:
                school_url = _find_first_matching_link(
                    district_links,
                    href_startswith=HHS_HOME_URL,
                ) or HHS_HOME_URL

            # 2) School homepage
            school_snapshot: dict[str, Any] = {}
            try:
                await page.goto(school_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1200)
                school_snapshot = await _collect_snapshot(page, school_url)
                source_pages.append(school_snapshot["final_url"])
                navigation_steps.append("school_home")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:school_home")
                school_snapshot = {"links": []}

            school_links = school_snapshot.get("links", [])
            athletics_url = _find_first_matching_link(
                school_links,
                text_contains="athletics",
                href_contains="/athletics",
            )
            if not athletics_url:
                athletics_url = f"{HHS_HOME_URL}/athletics"

            # 3) Athletics landing page
            athletics_snapshot: dict[str, Any] = {}
            try:
                await page.goto(athletics_url, wait_until="domcontentloaded", timeout=60000)
                await page.wait_for_timeout(1200)
                athletics_snapshot = await _collect_snapshot(page, athletics_url)
                source_pages.append(athletics_snapshot["final_url"])
                navigation_steps.append("athletics")
            except Exception as exc:  # noqa: BLE001
                errors.append(f"navigation_failed:{type(exc).__name__}:athletics")
                athletics_snapshot = {"links": []}

            athletics_links = athletics_snapshot.get("links", [])
            fall_url = _find_first_matching_link(
                athletics_links,
                text_contains="fall sports",
                href_contains="12714_2",
            )
            trans_valley_url = _find_first_matching_link(
                athletics_links,
                text_contains="trans valley league",
            )

            # 4) Fall sports page includes direct football coach/schedule lines.
            fall_snapshot: dict[str, Any] = {}
            if fall_url:
                try:
                    await page.goto(fall_url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(1200)
                    fall_snapshot = await _collect_snapshot(page, fall_url)
                    source_pages.append(fall_snapshot["final_url"])
                    navigation_steps.append("fall_sports")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:fall_sports")
            else:
                errors.append("missing_url:fall_sports_link")

            # 5) Optional league pages for football schedule documents.
            league_page_snapshot: dict[str, Any] = {}
            if trans_valley_url:
                try:
                    await page.goto(trans_valley_url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(1200)
                    league_page_snapshot = await _collect_snapshot(page, trans_valley_url)
                    source_pages.append(league_page_snapshot["final_url"])
                    navigation_steps.append("transvalley_league")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"navigation_failed:{type(exc).__name__}:transvalley_league")

            schedules_snapshot: dict[str, Any] = {}
            if league_page_snapshot:
                league_links = league_page_snapshot.get("links", [])
                schedules_url = _find_first_matching_link(
                    league_links,
                    text_contains="schedules",
                    href_contains="transvalleyleague.org/schedules",
                )
                if schedules_url:
                    try:
                        await page.goto(schedules_url, wait_until="domcontentloaded", timeout=60000)
                        await page.wait_for_timeout(1200)
                        schedules_snapshot = await _collect_snapshot(page, schedules_url)
                        source_pages.append(schedules_snapshot["final_url"])
                        navigation_steps.append("transvalley_schedules")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"navigation_failed:{type(exc).__name__}:transvalley_schedules")

            snapshots = [
                district_snapshot,
                school_snapshot,
                athletics_snapshot,
                fall_snapshot,
                league_page_snapshot,
                schedules_snapshot,
            ]
        finally:
            await browser.close()

    snapshots = [snapshot for snapshot in snapshots if isinstance(snapshot, dict) and snapshot]
    source_pages = _dedupe_keep_order(source_pages)
    football_sections: list[tuple[str, list[str]]] = []
    for snapshot in snapshots:
        if not snapshot:
            continue
        if snapshot.get("requested_url") and "fall" in str(snapshot.get("requested_url")):
            football_sections.extend(_extract_football_sections(snapshot.get("lines", [])))

    fall_section_lines: list[str] = []
    fall_section_heads: list[str] = []
    for heading, lines in football_sections:
        fall_section_heads.append(heading)
        fall_section_lines.extend(lines)

    football_coach_lines = [item.get("raw_line", "") for item in _extract_staff_sections(fall_section_lines)]
    football_coach_sections = _extract_staff_sections(fall_section_lines)
    football_coach_names = _extract_coach_names(fall_section_lines)
    football_team_names = _extract_team_names(fall_section_lines)
    football_schedule_lines = _extract_schedule_lines(fall_section_lines)

    league_pages: list[dict[str, str]] = []
    league_documents: list[dict[str, str]] = []
    for snapshot in snapshots:
        links = snapshot.get("links", [])
        if not isinstance(links, list):
            continue
        page_leagues, page_docs = _extract_league_links(links)
        league_pages.extend(page_leagues)
        league_documents.extend(page_docs)
    league_pages = _dedupe_dicts(league_pages)
    league_documents = _dedupe_dicts(league_documents)

    football_program_available = bool(
        football_sections or football_coach_lines or football_schedule_lines or league_documents
    )
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found")

    proxy_meta = get_proxy_runtime_meta(profile=PROXY_PROFILE)

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_homepage": school_snapshot.get("final_url") or HHS_HOME_URL,
        "athletics_page": athletics_snapshot.get("final_url") if athletics_snapshot else "",
        "fall_sports_page": fall_snapshot.get("final_url") if fall_snapshot else "",
        "trans_valley_league_page": league_page_snapshot.get("final_url") if league_page_snapshot else "",
        "trans_valley_schedules_page": schedules_snapshot.get("final_url") if schedules_snapshot else "",
        "football_section_headings": fall_section_heads,
        "football_team_names": football_team_names,
        "football_coach_sections": football_coach_sections,
        "football_coach_lines": football_coach_lines,
        "football_coach_names": football_coach_names,
        "football_schedule_lines": football_schedule_lines,
        "trans_valley_league_pages": league_pages,
        "trans_valley_football_documents": league_documents,
        "source_lines_count": (len(fall_section_lines)),
        "summary": (
            "Hilmar High athletics publishes football staff and schedule signals on the Fall Sports page, including Varsity/JV football and Girls Flag Football."
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
            "proxy_profile": proxy_meta["proxy_profile"],
            "proxy_servers": proxy_meta["proxy_servers"],
            "proxy_auth_mode": proxy_meta["proxy_auth_mode"],
            "focus": "football_only",
            "navigation_steps": navigation_steps,
            "pages_visited": len(source_pages),
            "football_sections_found": len(football_sections),
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()
