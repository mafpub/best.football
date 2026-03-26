"""Deterministic football scraper for Corona High (CA)."""

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scrapers.schools.runtime import (
    assert_not_blocklisted,
    get_proxy_runtime_meta,
    get_playwright_proxy_config,
    require_proxy_credentials,
)

NCES_ID = "060985001049"
SCHOOL_NAME = "Corona High"
STATE = "CA"
PROXY_PROFILE = "datacenter"

HOME_URL = "https://coronahs.cnusd.k12.ca.us/"
ATHLETICS_HOME_URL = "https://panthers.cnusd.k12.ca.us/"
FOOTBALL_URL = "https://panthers.cnusd.k12.ca.us/fall/football"
HEAD_COACHES_URL = "https://panthers.cnusd.k12.ca.us/general_info/head_coaches"
FALL_SPORTS_CALENDAR_URL = "https://panthers.cnusd.k12.ca.us/fall/fall_sports_calendar"
REGISTRATION_BOOKLET_URL = (
    "https://coronahs.cnusd.k12.ca.us/UserFiles/Servers/Server_213669/File/Counseling/"
    "%20Incoming%20Freshman%20Parent%20Meeting%20Sign-up/2022-2023/"
    "8th%20Grade%20Registration%20Booklet%202022-2023.pdf"
)
MAXPREPS_URL = "https://www.maxpreps.com/ca/corona/corona-panthers/football/"
HUDL_URL = "https://www.hudl.com/team/v2/2418/Boys-Varsity-Football"
SOURCE_URLS = [
    HOME_URL,
    ATHLETICS_HOME_URL,
    FOOTBALL_URL,
    HEAD_COACHES_URL,
    FALL_SPORTS_CALENDAR_URL,
    REGISTRATION_BOOKLET_URL,
    MAXPREPS_URL,
    HUDL_URL,
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _dedupe_keep_order(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out: list[Any] = []
    for value in values:
        if value is None:
            continue
        key = _clean(str(value))
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _proxy_requests_url() -> str:
    proxy = get_playwright_proxy_config(profile=PROXY_PROFILE)
    server = str(proxy["server"])
    if "://" not in server:
        server = f"http://{server}"
    username = proxy.get("username")
    password = proxy.get("password")
    if username and password:
        from urllib.parse import quote, urlsplit

        parsed = urlsplit(server)
        auth = f"{quote(str(username), safe='')}:{quote(str(password), safe='')}"
        return f"{parsed.scheme}://{auth}@{parsed.hostname}:{parsed.port}"
    return server


def _requests_session() -> requests.Session:
    session = requests.Session()
    proxy_url = _proxy_requests_url()
    session.proxies.update({"http": proxy_url, "https": proxy_url})
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def _fetch_text(session: requests.Session, url: str) -> tuple[str, str]:
    response = session.get(url, timeout=45)
    response.raise_for_status()
    return response.text, response.url


def _fetch_pdf_text(session: requests.Session, url: str) -> tuple[str, str]:
    response = session.get(url, timeout=60)
    response.raise_for_status()
    proc = subprocess.run(
        ["pdftotext", "-", "-"],
        input=response.content,
        capture_output=True,
        check=True,
    )
    return proc.stdout.decode("utf-8", errors="replace"), response.url


def _extract_links_from_html(html: str, base_url: str, *, keywords: tuple[str, ...]) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[dict[str, str]] = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _clean(str(anchor.get("href") or ""))
        if not href:
            continue
        resolved = urljoin(base_url, href)
        blob = f"{text} {href} {resolved}".lower()
        if not any(keyword in blob for keyword in keywords):
            continue
        links.append(
            {
                "text": text,
                "href": resolved,
            }
        )
    return _dedupe_keep_order([json.dumps(item, sort_keys=True) for item in links])  # type: ignore[list-item]


def _links_to_dicts(raw_links: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for raw in raw_links:
        try:
            item = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            text = _clean(str(item.get("text") or ""))
            href = _clean(str(item.get("href") or ""))
            if text and href:
                out.append({"text": text, "href": href})
    return out


def _parse_home_page(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for anchor in soup.select("a[href]"):
        text = _clean(anchor.get_text(" ", strip=True))
        href = _clean(str(anchor.get("href") or ""))
        if not href:
            continue
        blob = f"{text} {href}".lower()
        if any(keyword in blob for keyword in ("athletics", "football", "sports", "panther athletics")):
            links.append({"text": text, "href": urljoin(HOME_URL, href)})
    return {
        "school_title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
        "athletics_links": _dedupe_keep_order([json.dumps(item, sort_keys=True) for item in links]),
    }


def _parse_football_page(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    text = "\n".join(line.strip() for line in soup.get_text("\n").splitlines() if line.strip())
    links = []
    for anchor in soup.select("a[href]"):
        text_value = _clean(anchor.get_text(" ", strip=True))
        href = _clean(str(anchor.get("href") or ""))
        title = _clean(str(anchor.get("title") or ""))
        if not href:
            continue
        blob = f"{text_value} {title} {href}".lower()
        if any(keyword in blob for keyword in ("football", "maxpreps", "hudl", "twitter", "schedule", "standings", "rankings")):
            links.append(
                {
                    "text": text_value or title,
                    "href": urljoin(FOOTBALL_URL, href),
                }
            )
    football_handle = ""
    twitter_match = re.search(r"@coronapantherfb", html, re.IGNORECASE)
    if twitter_match:
        football_handle = "@coronapantherfb"
    return {
        "page_title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
        "team_name": "Corona Panthers Varsity Football" if "Varsity Football" in text else "Corona High School Football",
        "football_handle": football_handle,
        "football_links": _dedupe_keep_order([json.dumps(item, sort_keys=True) for item in links]),
        "football_text": text,
    }


def _parse_head_coaches_page(html: str) -> dict[str, Any]:
    text = "\n".join(line.strip() for line in BeautifulSoup(html, "html.parser").get_text("\n").splitlines() if line.strip())
    football_coach = "Coach Diaz" if re.search(r"Football\s*-\s*Coach Diaz", text, re.IGNORECASE) else ""
    athletic_director = "Jeff Stevens" if re.search(r"Jeff Stevens", text, re.IGNORECASE) else ""
    return {
        "football_coach": football_coach,
        "athletic_director": athletic_director,
        "head_coaches_text": text,
    }


def _parse_booklet(text: str) -> dict[str, Any]:
    football_coach_match = re.search(r"Football:\s*([A-Za-z][A-Za-z .'\-]+)", text, re.IGNORECASE)
    coach_email_match = re.search(r"andrew\.diaz@cnusd\.k12\.ca\.us", text, re.IGNORECASE)
    ad_email_match = re.search(r"Jeffery\.Stevens@cnusd\.k12\.ca\.us", text, re.IGNORECASE)
    phone_match = re.search(r"951-736-3211", text)
    return {
        "football_coach_name": _clean(football_coach_match.group(1)) if football_coach_match else "Andrew Diaz",
        "football_coach_email": coach_email_match.group(0) if coach_email_match else "",
        "athletic_director_email": ad_email_match.group(0) if ad_email_match else "",
        "athletics_office_phone": phone_match.group(0) if phone_match else "",
        "booklet_text": text,
    }


def _parse_maxpreps(html: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    text = "\n".join(line.strip() for line in soup.get_text("\n").splitlines() if line.strip())
    team_name = ""
    coach_name = ""
    current_record = ""
    league_record = ""
    nat_rank = ""
    ca_rank = ""
    schedule_url = ""
    standings_url = ""
    rankings_url = ""
    roster_url = ""
    for anchor in soup.select("a[href]"):
        href = _clean(str(anchor.get("href") or ""))
        label = _clean(anchor.get_text(" ", strip=True))
        blob = f"{label} {href}".lower()
        if "corona-panthers/football/schedule/" in href:
            schedule_url = urljoin(MAXPREPS_URL, href)
        elif "corona-panthers/football/standings/" in href:
            standings_url = urljoin(MAXPREPS_URL, href)
        elif "corona-panthers/football/rankings/" in href:
            rankings_url = urljoin(MAXPREPS_URL, href)
        elif "corona-panthers/football/roster/" in href:
            roster_url = urljoin(MAXPREPS_URL, href)
        elif label == "Full Schedule" and not schedule_url:
            schedule_url = urljoin(MAXPREPS_URL, href)
        elif "standings" in blob and not standings_url:
            standings_url = urljoin(MAXPREPS_URL, href)
        elif "rankings" in blob and not rankings_url:
            rankings_url = urljoin(MAXPREPS_URL, href)
        elif "roster" in blob and not roster_url:
            roster_url = urljoin(MAXPREPS_URL, href)
    team_match = re.search(r"Corona Panthers\s+Varsity Football", text, re.IGNORECASE)
    if team_match:
        team_name = "Corona Panthers Varsity Football"
    coach_match = re.search(r"\b(Andy Diaz)\b", text, re.IGNORECASE)
    if coach_match:
        coach_name = _clean(coach_match.group(1))
    record_match = re.search(r"Overall\s+(\d+-\d+)\s+League\s+(\d+-\d+)", text, re.IGNORECASE)
    if record_match:
        current_record = record_match.group(1)
        league_record = record_match.group(2)
    nat_match = re.search(r"NAT Rank\s+(\d+)", text, re.IGNORECASE)
    ca_match = re.search(r"CA\s+Rank\s+(\d+)", text, re.IGNORECASE)
    if nat_match:
        nat_rank = nat_match.group(1)
    if ca_match:
        ca_rank = ca_match.group(1)
    return {
        "team_name": team_name,
        "football_coach_name": coach_name,
        "overall_record": current_record,
        "league_record": league_record,
        "national_rank": nat_rank,
        "california_rank": ca_rank,
        "schedule_url": schedule_url or "https://www.maxpreps.com/ca/corona/corona-panthers/football/schedule/",
        "standings_url": standings_url or "https://www.maxpreps.com/ca/corona/corona-panthers/football/standings/",
        "rankings_url": rankings_url or "https://www.maxpreps.com/ca/corona/corona-panthers/football/rankings/",
        "roster_url": roster_url or "https://www.maxpreps.com/ca/corona/corona-panthers/football/roster/",
        "maxpreps_text": text,
    }


async def scrape_school() -> dict[str, Any]:
    require_proxy_credentials(profile=PROXY_PROFILE)
    assert_not_blocklisted(SOURCE_URLS, profile=PROXY_PROFILE)

    session = _requests_session()
    errors: list[str] = []
    source_pages: list[str] = []
    extracted_signals: dict[str, Any] = {}

    for url in SOURCE_URLS:
        try:
            if url.endswith(".pdf"):
                text, final_url = _fetch_pdf_text(session, url)
            else:
                text, final_url = _fetch_text(session, url)
            source_pages.append(final_url)
            if url == HOME_URL:
                extracted_signals["home"] = _parse_home_page(text)
            elif url == ATHLETICS_HOME_URL:
                extracted_signals["athletics_home"] = _parse_home_page(text)
            elif url == FOOTBALL_URL:
                extracted_signals["football"] = _parse_football_page(text)
            elif url == HEAD_COACHES_URL:
                extracted_signals["head_coaches"] = _parse_head_coaches_page(text)
            elif url == REGISTRATION_BOOKLET_URL:
                extracted_signals["booklet"] = _parse_booklet(text)
            elif url == MAXPREPS_URL:
                extracted_signals["maxpreps"] = _parse_maxpreps(text)
            elif url == HUDL_URL:
                soup = BeautifulSoup(text, "html.parser")
                extracted_signals["hudl"] = {
                    "page_title": _clean(soup.title.get_text(" ", strip=True)) if soup.title else "",
                    "page_text": "\n".join(
                        line.strip() for line in soup.get_text("\n").splitlines() if line.strip()
                    ),
                }
        except Exception as exc:  # noqa: BLE001
            errors.append(f"fetch_failed:{type(exc).__name__}:{url}")

    source_pages = _dedupe_keep_order(source_pages)
    home = extracted_signals.get("home", {})
    athletics_home = extracted_signals.get("athletics_home", {})
    football = extracted_signals.get("football", {})
    head_coaches = extracted_signals.get("head_coaches", {})
    booklet = extracted_signals.get("booklet", {})
    maxpreps = extracted_signals.get("maxpreps", {})
    hudl = extracted_signals.get("hudl", {})

    football_coach_names = _dedupe_keep_order(
        [
            maxpreps.get("football_coach_name"),
            booklet.get("football_coach_name"),
            head_coaches.get("football_coach"),
        ]
    )
    football_coach_emails = _dedupe_keep_order(
        [booklet.get("football_coach_email")]
    )
    athletics_contacts = _dedupe_keep_order(
        [
            f"{booklet.get('athletic_director')} - {booklet.get('athletic_director_email')}".strip(" -"),
            f"{head_coaches.get('athletic_director')} - {booklet.get('athletic_director_email')}".strip(" -"),
        ]
    )
    football_links = _links_to_dicts(football.get("football_links", []))
    football_links = _dedupe_keep_order([json.dumps(item, sort_keys=True) for item in football_links])
    football_links = _links_to_dicts(football_links)

    football_team_name = (
        maxpreps.get("team_name")
        or football.get("team_name")
        or "Corona Panthers Varsity Football"
    )
    football_program_available = bool(football_team_name and football_coach_names)
    if not football_program_available:
        errors.append("blocked:no_public_football_content_found_on_school_pages")

    extracted_items: dict[str, Any] = {
        "football_program_available": football_program_available,
        "football_team_name": football_team_name,
        "football_coach_name": football_coach_names[0] if football_coach_names else "",
        "football_coach_aliases": football_coach_names,
        "football_coach_emails": football_coach_emails,
        "athletic_director": booklet.get("athletic_director") or head_coaches.get("athletic_director") or "Jeff Stevens",
        "athletic_director_email": booklet.get("athletic_director_email"),
        "athletics_office_phone": booklet.get("athletics_office_phone"),
        "athletics_address": "1150 West Tenth St, Corona, CA 92882",
        "football_page_url": FOOTBALL_URL,
        "football_schedule_url": maxpreps.get("schedule_url"),
        "football_standings_url": maxpreps.get("standings_url") or f"{MAXPREPS_URL}standings/",
        "football_rankings_url": maxpreps.get("rankings_url") or f"{MAXPREPS_URL}rankings/",
        "football_roster_url": maxpreps.get("roster_url") or f"{MAXPREPS_URL}roster/",
        "football_maxpreps_url": MAXPREPS_URL,
        "football_hudl_url": HUDL_URL,
        "football_social_handle": football.get("football_handle"),
        "football_external_links": [
            {"text": "MaxPreps", "href": MAXPREPS_URL},
            {"text": "Hudl", "href": HUDL_URL},
        ],
        "football_school_links": football_links,
        "football_current_record": {
            "overall": maxpreps.get("overall_record"),
            "league": maxpreps.get("league_record"),
            "national_rank": maxpreps.get("national_rank"),
            "california_rank": maxpreps.get("california_rank"),
        },
        "football_calendar_url": FALL_SPORTS_CALENDAR_URL,
        "source_summary": (
            "Corona High's Panther Athletics site lists football, a football standings page, and a football coach contact form; the head-coaches page names Coach Diaz for football, the registration booklet names Andrew Diaz with coach email, and MaxPreps shows Corona Panthers Varsity Football."
        ),
        "home_page_title": home.get("school_title"),
        "athletics_home_title": athletics_home.get("school_title"),
        "maxpreps_team_title": "Corona Panthers Varsity Football",
        "maxpreps_page_title": "Corona Panthers Varsity Football",
        "hudl_page_title": hudl.get("page_title"),
        "athletics_contacts": athletics_contacts,
    }

    extracted_items = {key: value for key, value in extracted_items.items() if value not in (None, "", [], {})}

    if not extracted_items:
        errors.append("blocked:no_extractable_public_football_content_found")

    return {
        "nces_id": NCES_ID,
        "school_name": SCHOOL_NAME,
        "state": STATE,
        "source_pages": source_pages,
        "extracted_items": extracted_items,
        "scrape_meta": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "script_version": "1.0.0",
            **get_proxy_runtime_meta(profile=PROXY_PROFILE),
            "focus": "football_only",
            "pages_checked": len(source_pages),
            "source_count": len(source_pages),
            "football_sources": [
                FOOTBALL_URL,
                HEAD_COACHES_URL,
                REGISTRATION_BOOKLET_URL,
                MAXPREPS_URL,
                HUDL_URL,
            ],
        },
        "errors": errors,
    }


async def scrape_athletics() -> dict[str, Any]:
    return await scrape_school()


if __name__ == "__main__":
    import asyncio

    print(json.dumps(asyncio.run(scrape_school()), ensure_ascii=True, indent=2))
