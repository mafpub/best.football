"""Deterministic football scraper for Canyon High (CA)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_playwright_proxy_config,
    get_proxy_runtime_meta,
    require_proxy_credentials,
)

NCES_ID = "062865004421"
SCHOOL_NAME = "Canyon High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

SCHOOL_HOME_URL = "https://www.canyonhighschool.org/"
ATHLETICS_HOME_URL = "https://www.canyonathletics.org/"
FOOTBALL_HOME_URL = "https://www.canyonathletics.org/varsity/football/"
FOOTBALL_COACHES_URL = "https://www.canyonathletics.org/varsity/football/coaches"
FOOTBALL_ROSTER_URL = "https://www.canyonathletics.org/varsity/football/roster"
FOOTBALL_NEWS_URL = "https://www.canyonathletics.org/varsity/football/news"
FOOTBALL_SCHEDULE_RESULTS_URL = "https://www.canyonathletics.org/varsity/football/schedule-results"

TARGET_URLS = [
    SCHOOL_HOME_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_HOME_URL,
    FOOTBALL_COACHES_URL,
    FOOTBALL_ROSTER_URL,
    FOOTBALL_NEWS_URL,
    FOOTBALL_SCHEDULE_RESULTS_URL,
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


def _absolute_url(href: str, base_url: str) -> str:
    href = _clean(href)
    if not href:
        return ""
    return urljoin(base_url, href)


def _extract_footer_contact(text: str) -> dict[str, str]:
    phone_match = re.search(r"\((\d{3})\)\s*(\d{3})\s*(\d{4})", text)
    address_match = re.search(r"(\d+\s+S\.\s+Imperial Hwy\.)", text)
    city_state_zip_match = re.search(r"Anaheim,\s+California\s+(\d{5})", text)
    return {
        "address": _clean(address_match.group(1)) if address_match else "",
        "city_state_zip": f"Anaheim, California {city_state_zip_match.group(1)}"
        if city_state_zip_match
        else "",
        "phone": f"({phone_match.group(1)}) {phone_match.group(2)} {phone_match.group(3)}"
        if phone_match
        else "",
    }


def _parse_roster(soup: BeautifulSoup, base_url: str) -> dict[str, Any]:
    selected_option = soup.select_one("#yearDropdown option[selected]")
    selected_year = _clean(selected_option.get_text(" ", strip=True)) if selected_option else ""
    selected_value = selected_option.get("value", "") if selected_option else ""
    roster_div = soup.select_one(f"#coach_div_{selected_value}") if selected_value else None
    table = roster_div.select_one("table.rostertableforoster") if roster_div else None

    roster: list[dict[str, str]] = []
    if table:
        for tr in table.select("tbody tr"):
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue
            number = _clean(cells[1].get_text(" ", strip=True))
            name_link = cells[2].select_one("a[href]")
            name = _clean(name_link.get_text(" ", strip=True)) if name_link else _clean(cells[2].get_text(" ", strip=True))
            position = _clean(cells[3].get_text(" ", strip=True))
            profile_url = _absolute_url(name_link.get("href", ""), base_url) if name_link else ""
            if number or name or position:
                roster.append(
                    {
                        "number": number,
                        "name": name,
                        "position": position,
                        "profile_url": profile_url,
                    }
                )

    return {
        "season": selected_year,
        "roster_div_id": f"coach_div_{selected_value}" if selected_value else "",
        "players": roster,
    }


def _parse_coaches(soup: BeautifulSoup, base_url: str) -> dict[str, Any]:
    selected_option = soup.select_one("#yearDropdown option[selected]")
    selected_year = _clean(selected_option.get_text(" ", strip=True)) if selected_option else ""
    selected_value = selected_option.get("value", "") if selected_option else ""
    coach_div = soup.select_one(f"#coach_div_{selected_value}") if selected_value else None
    cards = coach_div.select("ul.staff-lineup > li") if coach_div else []

    coaches: list[dict[str, str]] = []
    for li in cards:
        name_node = li.select_one("h3")
        role_node = li.select_one("p.mt-2.mb-3")
        contact_link = li.select_one("a.focusable-link[id^='modalBtns']")
        name = _clean(name_node.get_text(" ", strip=True)) if name_node else ""
        role = _clean(role_node.get_text(" ", strip=True)) if role_node else ""
        name_link = li.select_one("h3 a[href]")
        profile_url = _absolute_url(name_link.get("href", ""), base_url) if name_link else ""
        contact_id = _clean(contact_link.get("id", "")) if contact_link else ""
        modal = li.find("div", id=re.compile(r"^myModal[s]?\d+-\d+$"))
        team_role = ""
        if modal:
            team_line = modal.select_one("li.box-shadow")
            if team_line:
                team_role = _clean(team_line.get_text(" ", strip=True))
        if name or role:
            coaches.append(
                {
                    "name": name,
                    "role": role,
                    "profile_url": profile_url,
                    "contact_button_id": contact_id,
                    "team_assignment": team_role,
                }
            )

    return {
        "season": selected_year,
        "coach_div_id": f"coach_div_{selected_value}" if selected_value else "",
        "coaches": coaches,
    }


def _parse_news(soup: BeautifulSoup, base_url: str) -> dict[str, Any]:
    articles: list[dict[str, str]] = []
    for article in soup.select("article"):
        title_node = article.select_one("h3")
        link_node = article.select_one("a[href]")
        title = _clean(title_node.get_text(" ", strip=True)) if title_node else ""
        url = _absolute_url(link_node.get("href", ""), base_url) if link_node else ""
        if title:
            articles.append({"title": title, "url": url})
    return {"articles": articles}


def _parse_schedule_ajax_url(page_html: str) -> tuple[str, dict[str, str]]:
    def _extract(pattern: str, default: str = "") -> str:
        match = re.search(pattern, page_html, flags=re.DOTALL)
        return _clean(match.group(1)) if match else default

    ajaxurl = _extract(r'var\s+ajaxurl\s*=\s*"([^"]+)"')
    level = _extract(r"'&level='\s*\+\s*\"([^\"]+)\"")
    sport_id = _extract(r"'&sportID='\s*\+\s*\"([^\"]+)\"")
    show_record = _extract(r"'&showRecord='\s*\+\s*\"([^\"]+)\"", "yes") or "yes"
    school_id = _extract(r"'&school_id='\s*\+\s*\"([^\"]+)\"")
    year = _extract(r"'&year='\s*\+\s*\"([^\"]+)\"")
    endpoint = _extract(r"'&endpoint='\s*\+\s*\"([^\"]+)\"")
    nonce = _extract(r"'&_ajax_nonce=([a-z0-9]+)'")

    params = {
        "action": "load_schedule_results_full",
        "level": level,
        "sportID": sport_id,
        "showRecord": show_record,
        "school_id": school_id,
        "year": year,
        "endpoint": endpoint,
        "_ajax_nonce": nonce,
    }
    return ajaxurl, params


def _parse_schedule_results(soup: BeautifulSoup, base_url: str) -> dict[str, Any]:
    text = _clean(soup.get_text(" ", strip=True))
    record_match = re.search(
        r"Overall Record:\s*([0-9-]+),\s*League Record:\s*([0-9-]+)",
        text,
    )
    overall_record = record_match.group(1) if record_match else ""
    league_record = record_match.group(2) if record_match else ""

    rows: list[dict[str, Any]] = []
    for li in soup.select("li.schedule-and-results-list-item"):
        sport = li.select_one("div.sport")
        date_node = li.select_one("div.date")
        match_type_node = li.select_one("div.vs")
        school_node = li.select_one("div.school")
        outcome_node = li.select_one("div.outcome")
        score_node = outcome_node.select_one("div.score") if outcome_node else None
        result_node = outcome_node.select_one("strong") if outcome_node else None
        time_node = li.select_one("div.time strong")
        location_link = li.select_one("a.location-link")
        modal = li.select_one("div.modal")

        dismissal_time = ""
        departure_time = ""
        return_time = ""
        if modal:
            for paragraph in modal.select("p"):
                line = _clean(paragraph.get_text(" ", strip=True))
                if line.startswith("Dismissal Time:"):
                    dismissal_time = _clean(line.split(":", 1)[1])
                elif line.startswith("Departure Time:"):
                    departure_time = _clean(line.split(":", 1)[1])
                elif line.startswith("Return Time:"):
                    return_time = _clean(line.split(":", 1)[1])

        score_text = _clean(score_node.get_text(" ", strip=True)) if score_node else ""
        score_numbers = [part.strip() for part in score_text.split("-")]
        score_for = score_numbers[0] if len(score_numbers) > 0 else ""
        score_against = score_numbers[1] if len(score_numbers) > 1 else ""
        opponent = ""
        venue = ""
        if school_node:
            opponent = _clean((school_node.select_one("p").get_text(" ", strip=True) if school_node.select_one("p") else school_node.get_text(" ", strip=True)))
        if location_link:
            venue = _clean(location_link.get_text(" ", strip=True))

        rows.append(
            {
                "date": _clean(date_node.get_text(" ", strip=True)) if date_node else "",
                "date_iso": _clean(li.get("data-date", "")),
                "match_type": _clean(match_type_node.get_text(" ", strip=True)) if match_type_node else "",
                "opponent": opponent,
                "venue": venue,
                "result": _clean(result_node.get_text(" ", strip=True)) if result_node else "",
                "score": score_text,
                "score_for": score_for,
                "score_against": score_against,
                "start_time": _clean(time_node.get_text(" ", strip=True)) if time_node else "",
                "dismissal_time": dismissal_time,
                "departure_time": departure_time,
                "return_time": return_time,
                "sport": _clean(sport.get_text(" ", strip=True)) if sport else "",
                "event_id": _clean((sport.get("data-event-id") if sport else "") or ""),
                "game_type": _clean((sport.get("data-game-type") if sport else "") or ""),
            }
        )

    return {
        "overall_record": overall_record,
        "league_record": league_record,
        "rows": rows,
        "row_count": len(rows),
        "home_games": sum(1 for row in rows if row.get("match_type", "").lower() == "vs"),
        "away_games": sum(1 for row in rows if row.get("match_type", "").lower() == "at"),
    }


async def _load_page(page, url: str, *, wait_ms: int = 2200) -> dict[str, Any]:
    await page.goto(url, wait_until="domcontentloaded", timeout=90000)
    await page.wait_for_timeout(wait_ms)
    html = await page.content()
    return {
        "url": page.url,
        "title": _clean(await page.title()),
        "html": html,
        "text": _clean(await page.locator("body").inner_text(timeout=15000)),
    }


async def scrape_school() -> dict[str, Any]:
    """Scrape Canyon High's public football-facing athletics content."""
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(TARGET_URLS, profile=PROXY_PROFILE)

    errors: list[str] = []
    source_pages: list[str] = []
    page_signals: dict[str, dict[str, Any]] = {}

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
            for url in [SCHOOL_HOME_URL, ATHLETICS_HOME_URL, FOOTBALL_HOME_URL]:
                try:
                    signal = await _load_page(page, url)
                    page_signals[url] = signal
                    source_pages.append(signal["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{url}")

            football_home_signal = page_signals.get(FOOTBALL_HOME_URL, {})
            football_coaches_signal: dict[str, Any] = {}
            football_roster_signal: dict[str, Any] = {}
            football_schedule_signal: dict[str, Any] = {}
            football_news_signal: dict[str, Any] = {}
            schedule_ajax_signal: dict[str, Any] = {}

            if football_home_signal:
                try:
                    signal = await _load_page(page, FOOTBALL_COACHES_URL)
                    football_coaches_signal = signal
                    page_signals[FOOTBALL_COACHES_URL] = signal
                    source_pages.append(signal["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{FOOTBALL_COACHES_URL}")

                try:
                    signal = await _load_page(page, FOOTBALL_ROSTER_URL)
                    football_roster_signal = signal
                    page_signals[FOOTBALL_ROSTER_URL] = signal
                    source_pages.append(signal["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{FOOTBALL_ROSTER_URL}")

                try:
                    signal = await _load_page(page, FOOTBALL_SCHEDULE_RESULTS_URL)
                    football_schedule_signal = signal
                    page_signals[FOOTBALL_SCHEDULE_RESULTS_URL] = signal
                    source_pages.append(signal["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(
                        f"playwright_navigation_failed:{type(exc).__name__}:{FOOTBALL_SCHEDULE_RESULTS_URL}"
                    )

                schedule_page_html = str(football_schedule_signal.get("html") or "")
                ajaxurl, ajax_params = _parse_schedule_ajax_url(schedule_page_html)
                schedule_ajax_url = f"{ajaxurl}?{urlencode(ajax_params)}"

                try:
                    ajax_text = await page.evaluate(
                        """async url => {
                            const response = await fetch(url, { credentials: 'same-origin' });
                            return await response.text();
                        }""",
                        schedule_ajax_url,
                    )
                    ajax_html = str(ajax_text or "")
                    ajax_text_clean = _clean(BeautifulSoup(ajax_html, "html.parser").get_text(" ", strip=True))
                    schedule_ajax_signal = {
                        "url": schedule_ajax_url,
                        "title": "",
                        "html": ajax_html,
                        "text": ajax_text_clean,
                    }
                    page_signals[schedule_ajax_url] = schedule_ajax_signal
                    source_pages.append(schedule_ajax_url)
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_fetch_failed:{type(exc).__name__}:{schedule_ajax_url}")
                    try:
                        signal = await _load_page(page, schedule_ajax_url)
                        schedule_ajax_signal = signal
                        page_signals[schedule_ajax_url] = signal
                        source_pages.append(signal["url"])
                    except Exception as inner_exc:  # noqa: BLE001
                        errors.append(
                            f"playwright_navigation_failed:{type(inner_exc).__name__}:{schedule_ajax_url}"
                        )

                try:
                    signal = await _load_page(page, FOOTBALL_NEWS_URL)
                    football_news_signal = signal
                    page_signals[FOOTBALL_NEWS_URL] = signal
                    source_pages.append(signal["url"])
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"playwright_navigation_failed:{type(exc).__name__}:{FOOTBALL_NEWS_URL}")

        finally:
            await browser.close()

    source_pages = _dedupe_keep_order(source_pages)

    school_home_signal = page_signals.get(SCHOOL_HOME_URL, {})
    athletics_home_signal = page_signals.get(ATHLETICS_HOME_URL, {})
    football_home_signal = page_signals.get(FOOTBALL_HOME_URL, {})
    football_coaches_signal = page_signals.get(FOOTBALL_COACHES_URL, {})
    football_roster_signal = page_signals.get(FOOTBALL_ROSTER_URL, {})
    football_schedule_signal = page_signals.get(FOOTBALL_SCHEDULE_RESULTS_URL, {})
    football_news_signal = page_signals.get(FOOTBALL_NEWS_URL, {})
    schedule_ajax_signal = next(
        (
            signal
            for url, signal in page_signals.items()
            if url.startswith("https://www.canyonathletics.org/wp-admin/admin-ajax.php")
        ),
        {},
    )

    school_home_text = str(school_home_signal.get("text") or "")
    athletics_home_text = str(athletics_home_signal.get("text") or "")
    football_home_text = str(football_home_signal.get("text") or "")
    football_coaches_html = str(football_coaches_signal.get("html") or "")
    football_roster_html = str(football_roster_signal.get("html") or "")
    football_schedule_html = str(football_schedule_signal.get("html") or "")
    football_news_html = str(football_news_signal.get("html") or "")
    schedule_ajax_html = str(schedule_ajax_signal.get("html") or "")

    football_home_soup = BeautifulSoup(football_home_signal.get("html") or "", "html.parser")
    coaches_soup = BeautifulSoup(football_coaches_html, "html.parser")
    roster_soup = BeautifulSoup(football_roster_html, "html.parser")
    schedule_soup = BeautifulSoup(schedule_ajax_html or football_schedule_html, "html.parser")
    news_soup = BeautifulSoup(football_news_html, "html.parser")

    school_contact = _extract_footer_contact(school_home_text or athletics_home_text or football_home_text)

    football_home_links = _dedupe_keep_order(
        [
            f"{_clean(link.get_text(' ', strip=True))}|{_absolute_url(link.get('href', ''), FOOTBALL_HOME_URL)}"
            for link in football_home_soup.select("a[href]")
            if any(
                token in (_clean(link.get_text(" ", strip=True)) + " " + _clean(link.get("href", ""))).lower()
                for token in ("schedule", "coach", "roster", "news", "football")
            )
        ]
    )

    coaches_data = _parse_coaches(coaches_soup, FOOTBALL_COACHES_URL)
    roster_data = _parse_roster(roster_soup, FOOTBALL_ROSTER_URL)
    schedule_data = _parse_schedule_results(schedule_soup, FOOTBALL_SCHEDULE_RESULTS_URL)
    news_data = _parse_news(news_soup, FOOTBALL_NEWS_URL)

    football_coaches = coaches_data["coaches"]
    football_roster = roster_data["players"]
    football_schedule_rows = schedule_data["rows"]
    football_news_headlines = [item["title"] for item in news_data["articles"]]

    football_head_coach = next(
        (coach["name"] for coach in football_coaches if coach.get("role") == "Head Coach"),
        "",
    )
    football_assistant_coaches = _dedupe_keep_order(
        [coach["name"] for coach in football_coaches if coach.get("role") == "Assistant Coach"]
    )
    football_roster_names = _dedupe_keep_order([player["name"] for player in football_roster])
    football_roster_positions = _dedupe_keep_order([player["position"] for player in football_roster])
    football_schedule_opponents = _dedupe_keep_order([row["opponent"] for row in football_schedule_rows])

    football_program_available = bool(
        football_home_links
        or football_coaches
        or football_roster
        or football_schedule_rows
        or football_news_headlines
    )

    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "school_homepage_url": SCHOOL_HOME_URL,
        "athletics_homepage_url": ATHLETICS_HOME_URL,
        "football_homepage_url": FOOTBALL_HOME_URL,
        "football_coaches_url": FOOTBALL_COACHES_URL,
        "football_roster_url": FOOTBALL_ROSTER_URL,
        "football_news_url": FOOTBALL_NEWS_URL,
        "football_schedule_url": FOOTBALL_SCHEDULE_RESULTS_URL,
        "football_schedule_ajax_url": str(schedule_ajax_signal.get("url") or ""),
        "athletics_contact": school_contact,
        "football_home_links": football_home_links,
        "football_head_coach": football_head_coach,
        "football_coaches": football_coaches,
        "football_assistant_coaches": football_assistant_coaches,
        "football_roster_season": roster_data["season"],
        "football_roster": football_roster,
        "football_roster_names": football_roster_names,
        "football_roster_positions": football_roster_positions,
        "football_schedule_season": roster_data["season"],
        "football_schedule_record": {
            "overall": schedule_data["overall_record"],
            "league": schedule_data["league_record"],
        },
        "football_schedule_rows": football_schedule_rows,
        "football_schedule_opponents": football_schedule_opponents,
        "football_news_headlines": football_news_headlines,
        "football_news_articles": news_data["articles"],
        "summary": (
            "Canyon High has a public football portal on Canyon Athletics with dedicated coaches, roster, schedule/results, and football news pages."
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
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "pages_checked": len(source_pages),
            "manual_navigation_steps": [
                "school_homepage",
                "athletics_homepage",
                "football_homepage",
                "football_coaches",
                "football_roster",
                "football_schedule_results",
                "football_schedule_ajax",
                "football_news",
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    """Alias entrypoint for runtime compatibility."""
    return await scrape_school()


if __name__ == "__main__":
    import asyncio

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
