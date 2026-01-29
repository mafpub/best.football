"""California CIF Southern Section (CIFSS) scraper.

Data source: https://www.cifsshome.org/widget/school/directory
Format: JSON API via widget endpoints with AJAX headers required.

CIFSS is the largest of California's 10 CIF sections, covering Southern California.
This scraper focuses on CIFSS only for MVP scope.

Note: Football playoff divisions (1-16) are determined annually via competitive equity
formula and published as PDFs. This scraper fetches school/program data; division
assignments require separate PDF parsing or are available via MaxPreps.
"""

import json
import logging
import re
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from pipeline.cache import CacheManager
from pipeline.database import get_db
from .scraper_base import ProxiedScraper

# Module logger
logger = logging.getLogger(__name__)

# CIFSS Home Campus API endpoints
BASE_URL = "https://www.cifsshome.org"
DIRECTORY_URL = f"{BASE_URL}/widget/school/directory"
SCHOOL_DETAILS_URL = f"{BASE_URL}/widget/get-school-details"
SCHOOLS_SEARCH_URL = f"{BASE_URL}/widget/schools/get"

# CIFSS section ID in the Home Campus system
CIFSS_SECTION_ID = 1

# Sport IDs from the Home Campus system
FOOTBALL_11_SPORT_ID = 1

# Cache directory
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "state_athletics" / "california_cifss"

# AJAX headers required for API access
AJAX_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{DIRECTORY_URL}?section={CIFSS_SECTION_ID}",
}


class CIFSSScraper(ProxiedScraper):
    """Scraper for California CIFSS school data from Home Campus API."""

    def __init__(self):
        super().__init__(cache_dir=CACHE_DIR, respect_delay=1.0)
        self.processed_cache = CacheManager(CACHE_DIR)

    def _fetch_json(
        self,
        url: str,
        cache_hours: int = 24,
        use_proxy: bool = True,
    ) -> Optional[dict]:
        """
        Fetch JSON from API with required AJAX headers.

        Args:
            url: API endpoint URL
            cache_hours: Cache validity in hours
            use_proxy: Whether to use proxy

        Returns:
            Parsed JSON response or None
        """
        import httpx
        import random
        import time

        # Check cache first
        cache_path = self._get_cache_path(url)
        if cache_hours > 0:
            cached = self._get_cached(url, cache_hours)
            if cached:
                try:
                    return json.loads(cached)
                except json.JSONDecodeError:
                    pass  # Cached content not valid JSON, refetch

        # Respect rate limit
        self._respect_rate_limit()

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            **AJAX_HEADERS,
        }

        for attempt in range(3):
            try:
                if use_proxy:
                    proxy_url = self._get_proxy_url()
                    transport = httpx.HTTPTransport(proxy=proxy_url)
                else:
                    transport = None

                with httpx.Client(transport=transport, timeout=30.0) as client:
                    logger.debug("Fetching JSON: %s (attempt %d)", url, attempt + 1)
                    response = client.get(url, headers=headers, follow_redirects=True)
                    response.raise_for_status()

                    # Verify we got JSON, not an error page
                    content_type = response.headers.get("content-type", "")
                    if "application/json" not in content_type:
                        logger.warning(
                            "Non-JSON response for %s: %s", url, content_type
                        )
                        return None

                    data = response.json()

                    # Cache the successful response as JSON string
                    if cache_hours > 0:
                        self._save_cache(url, json.dumps(data))

                    return data

            except (httpx.HTTPStatusError, httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning("Fetch error for %s: %s", url, e)
                if attempt < 2:
                    wait = random.uniform(2, 5)
                    logger.debug("Retrying in %.1fs...", wait)
                    time.sleep(wait)

            except json.JSONDecodeError as e:
                logger.warning("JSON decode error for %s: %s", url, e)
                return None

        return None

    def fetch_school_ids(self) -> list[tuple[int, str]]:
        """
        Fetch all CIFSS school IDs and names from the directory widget.

        Returns:
            List of (school_id, school_name) tuples
        """
        logger.info("Fetching CIFSS school directory...")

        # Fetch the directory HTML (not JSON - it's a rendered page)
        url = f"{DIRECTORY_URL}?section={CIFSS_SECTION_ID}"
        html = self.fetch(url, cache_hours=24)

        if not html:
            logger.error("Failed to fetch school directory")
            return []

        # Parse school buttons from HTML
        soup = BeautifulSoup(html, "html.parser")
        school_buttons = soup.find_all("button", class_="school-btn")

        schools = []
        for btn in school_buttons:
            school_id = btn.get("data-id")
            if school_id:
                name = btn.get_text(strip=True)
                schools.append((int(school_id), name))

        logger.info("Found %d schools in CIFSS directory", len(schools))
        return schools

    def fetch_school_details(self, school_id: int) -> Optional[dict]:
        """
        Fetch detailed information for a single school.

        Args:
            school_id: CIFSS Home Campus school ID

        Returns:
            School details dict or None
        """
        url = f"{SCHOOL_DETAILS_URL}/{school_id}/details"
        return self._fetch_json(url, cache_hours=168)  # Cache for 1 week

    def _parse_school_data(self, school_id: int, details: dict) -> Optional[dict]:
        """
        Parse API response into normalized school dict.

        Args:
            school_id: CIFSS school ID
            details: Raw API response

        Returns:
            Normalized school dict or None
        """
        school_data = details.get("school", {})
        if not school_data:
            return None

        # Check if school is hidden from directory
        if school_data.get("hide_from_directory") == 1:
            logger.debug("Skipping hidden school: %s", school_data.get("name"))
            return None

        # Check if school has football program by looking at coaches
        has_football = False
        coaches = details.get("coaches", [])
        for coach in coaches:
            if coach.get("sport") == "Football (11 person)":
                has_football = True
                break

        # Extract geographic/administrative info
        geo_groups = details.get("geoGroups", {})
        district = None
        county = None
        conference = None
        area = None

        for key, group in geo_groups.items():
            group_type = group.get("type", "")
            value = group.get("value")
            if value:
                if group_type == "District":
                    district = value.get("name")
                elif group_type == "County":
                    county = value.get("name")
                elif group_type == "Conference":
                    conference = value.get("name")
                elif group_type == "Area":
                    area = value.get("name")

        return {
            "cifss_id": school_id,
            "name": school_data.get("name"),
            "full_name": school_data.get("full_name"),
            "state": "CA",
            "nces_id": None,  # CIFSS doesn't provide NCES IDs
            "enrollment": school_data.get("enrollment"),
            "address": school_data.get("address_line_1"),
            "city": school_data.get("city"),
            "county": county,
            "zip": school_data.get("physical_zip"),
            "phone": school_data.get("phone"),
            "website": school_data.get("website"),
            "mascot": school_data.get("mascot"),
            "colors": school_data.get("color"),
            "year_established": school_data.get("year_established"),
            "is_private": bool(school_data.get("is_private")),
            "district": district,
            "conference": conference,
            "area": area,
            "section": "Southern Section",
            "grades": details.get("grades"),
            "has_football": has_football,
            # Division info is not in the API - it's determined annually via playoff assignments
            "division": None,
        }

    def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all CIFSS schools with their details.

        Returns:
            List of school dictionaries
        """
        # Check for processed cache
        cache_key = "cifss_schools_all"
        cached = self.processed_cache.get(cache_key, max_age_days=7)
        if cached:
            logger.info("Using cached CIFSS data (%d schools)", len(cached))
            return cached

        logger.info("Fetching all CIFSS school data...")

        # Get list of all school IDs
        school_ids = self.fetch_school_ids()

        all_schools = []
        failed_count = 0

        for i, (school_id, name) in enumerate(school_ids):
            if (i + 1) % 50 == 0:
                logger.info("Progress: %d/%d schools", i + 1, len(school_ids))

            details = self.fetch_school_details(school_id)
            if not details:
                logger.warning("Failed to fetch details for school %d: %s", school_id, name)
                failed_count += 1
                continue

            school = self._parse_school_data(school_id, details)
            if school:
                all_schools.append(school)

        logger.info(
            "Total: %d CIFSS schools fetched (%d failed)",
            len(all_schools),
            failed_count,
        )

        # Cache processed results
        self.processed_cache.set(cache_key, all_schools, DIRECTORY_URL)
        return all_schools

    def fetch_football_schools(self) -> list[dict]:
        """
        Fetch only schools with football programs.

        Returns:
            List of school dicts with has_football=True
        """
        all_schools = self.fetch_all_schools()
        football_schools = [s for s in all_schools if s.get("has_football")]

        logger.info(
            "Football schools: %d/%d",
            len(football_schools),
            len(all_schools),
        )
        return football_schools

    def _normalize_name_for_matching(self, name: str) -> str:
        """
        Normalize school name for database matching.

        Handles CIFSS naming quirks:
        - "Alhambra/Alhambra" -> "Alhambra"
        - Adds "High" suffix for matching NCES names
        """
        # Handle "City/Name" format - take the second part
        if "/" in name:
            parts = name.split("/")
            name = parts[-1].strip()

        return name

    def _find_school_match(self, conn, name: str, city: str) -> Optional[str]:
        """
        Find matching NCES school ID using multiple strategies.

        Args:
            conn: Database connection
            name: CIFSS school name
            city: School city

        Returns:
            NCES ID if found, None otherwise
        """
        # Normalize name
        normalized = self._normalize_name_for_matching(name)

        # Clean city (remove state abbreviation if present)
        clean_city = re.sub(r',?\s*(ca\.?|california)$', '', city, flags=re.IGNORECASE).strip()

        def escape_like(s: str) -> str:
            return (
                s.lower()
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )

        # Strategy 1: Exact city + name with "High" suffix
        escaped_name = escape_like(normalized)
        row = conn.execute(
            """
            SELECT nces_id FROM schools
            WHERE state = 'CA'
            AND LOWER(city) = LOWER(?)
            AND (LOWER(name) LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\')
            LIMIT 1
            """,
            (clean_city, f"{escaped_name} high%", f"{escaped_name}%"),
        ).fetchone()
        if row:
            return row["nces_id"]

        # Strategy 2: Fuzzy city match (first word) + name
        city_first = clean_city.split()[0] if clean_city else ""
        if city_first:
            row = conn.execute(
                """
                SELECT nces_id FROM schools
                WHERE state = 'CA'
                AND LOWER(city) LIKE ? ESCAPE '\\'
                AND (LOWER(name) LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\')
                LIMIT 1
                """,
                (f"{escape_like(city_first)}%", f"{escaped_name} high%", f"{escaped_name}%"),
            ).fetchone()
            if row:
                return row["nces_id"]

        # Strategy 3: Name only (for unique names)
        row = conn.execute(
            """
            SELECT nces_id FROM schools
            WHERE state = 'CA'
            AND (LOWER(name) LIKE ? ESCAPE '\\' OR LOWER(name) LIKE ? ESCAPE '\\')
            LIMIT 1
            """,
            (f"{escaped_name} high%", f"% {escaped_name} %"),
        ).fetchone()
        if row:
            return row["nces_id"]

        return None

    def load_to_db(self, schools: list[dict]) -> int:
        """
        Load CIFSS data into athletic_programs table.

        Matches schools by name and city since CIFSS doesn't provide NCES IDs.

        Args:
            schools: List of school dicts from fetch_all_schools()

        Returns:
            Number of schools successfully matched and loaded
        """
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                if not school.get("has_football"):
                    continue

                # Try to find matching school in our database by name + city
                # CIFSS doesn't provide NCES IDs, so we match on location
                name = school.get("name", "")
                city = school.get("city", "")

                if not name or not city:
                    continue

                school_nces_id = self._find_school_match(conn, name, city)

                if school_nces_id:
                    # Insert/update athletic program
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO athletic_programs
                        (school_id, sport, classification, conference, division, state_association_id)
                        VALUES (?, 'football', ?, ?, ?, ?)
                        """,
                        (
                            school_nces_id,
                            "CIFSS",  # Classification = section name
                            school.get("conference"),
                            school.get("division"),  # Usually None - requires PDF parsing
                            str(school.get("cifss_id")),  # CIFSS internal ID
                        ),
                    )
                    matched += 1
                else:
                    unmatched.append(f"{name} ({city})")

            football_count = len([s for s in schools if s.get("has_football")])
            logger.info("Matched %d/%d football schools to database", matched, football_count)

            if unmatched:
                if len(unmatched) <= 10:
                    logger.info("Unmatched: %s", unmatched)
                else:
                    logger.info("Unmatched: %d schools (first 5: %s)", len(unmatched), unmatched[:5])

            return matched


def fetch_and_load() -> int:
    """Convenience function to fetch and load CIFSS data."""
    scraper = CIFSSScraper()
    schools = scraper.fetch_all_schools()
    return scraper.load_to_db(schools)


if __name__ == "__main__":
    # Configure logging for CLI usage
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    fetch_and_load()
