"""Florida FHSAA (Florida High School Athletic Association) scraper.

Data source: https://www.fhsaahome.org/widget/school-directory-locations
Format: HTML page with embedded JavaScript geocodeAddress() calls containing school data.
"""

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

# FHSAA Widget URLs
BASE_URL = "https://www.fhsaahome.org/widget/school-directory-locations"
FOOTBALL_URL = f"{BASE_URL}?sport_id=1"

# Cache directory
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "state_athletics" / "florida"

# Florida classification structure (from STATE_SITES.md)
# Classes: 1A (smallest) through 7A (largest), plus Rural
CLASSIFICATIONS = ["1A", "2A", "3A", "4A", "5A", "6A", "7A", "Rural"]

# FHSAA Sections (geographic divisions)
SECTIONS = ["Section 1", "Section 2", "Section 3", "Section 4"]

# FHSAA Districts
DISTRICTS = [f"D{i}" for i in range(1, 18)]  # D1 through D17

# Regex pattern to extract geocodeAddress calls
# Note: School info HTML may contain escaped quotes (\") for nicknames like "Don"
GEOCODE_PATTERN = re.compile(
    r'geocodeAddress\s*\(\s*'
    r'"([^"]*)",\s*'  # latitude
    r'"([^"]*)",\s*'  # longitude
    r'geocoder,\s*map,\s*'
    r'"([^"]*)",\s*'  # color
    r'"((?:[^"\\]|\\.)*)"\s*\)',  # school info HTML (handles escaped quotes)
    re.DOTALL
)


class FHSAAScraper(ProxiedScraper):
    """Scraper for Florida FHSAA school data from the member directory widget."""

    def __init__(self):
        super().__init__(cache_dir=CACHE_DIR, respect_delay=1.5)
        self.processed_cache = CacheManager(CACHE_DIR)

    def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all schools with football programs from FHSAA widget.

        Returns:
            List of school dictionaries with athletic program data
        """
        # Check for processed cache
        cache_key = "fhsaa_schools_all"
        cached = self.processed_cache.get(cache_key, max_age_days=7)
        if cached:
            logger.info("Using cached FHSAA data (%d schools)", len(cached))
            return cached

        logger.info("Fetching FHSAA school data from widget...")

        html = self.fetch(FOOTBALL_URL, cache_hours=24)
        if not html:
            logger.error("Failed to fetch FHSAA widget")
            return []

        schools = self._parse_widget(html)
        logger.info("Total: %d FHSAA football schools", len(schools))

        self.processed_cache.set(cache_key, schools, FOOTBALL_URL)
        return schools

    def fetch_schools_by_class(self, class_id: int) -> list[dict]:
        """
        Fetch schools for a specific classification.

        Args:
            class_id: Classification ID (1-7 for 1A-7A, or 8 for Rural)

        Returns:
            List of school dictionaries
        """
        url = f"{FOOTBALL_URL}&class_id={class_id}"
        html = self.fetch(url, cache_hours=24)

        if not html:
            logger.error("Failed to fetch FHSAA class %d", class_id)
            return []

        return self._parse_widget(html)

    def _parse_widget(self, html: str) -> list[dict]:
        """
        Parse the FHSAA widget HTML to extract school data.

        The widget embeds school data as JavaScript geocodeAddress() calls
        with HTML-encoded school information.

        Args:
            html: Raw HTML from the widget

        Returns:
            List of school dictionaries
        """
        schools = []

        # Find all geocodeAddress calls
        matches = GEOCODE_PATTERN.findall(html)

        for lat, lng, color, school_html in matches:
            school = self._parse_school_info(school_html, lat, lng)
            if school:
                schools.append(school)

        logger.debug("Parsed %d schools from widget", len(schools))
        return schools

    def _parse_school_info(
        self, school_html: str, lat: str, lng: str
    ) -> Optional[dict]:
        """
        Parse a single school's HTML snippet from geocodeAddress call.

        Args:
            school_html: HTML snippet containing school info (may be escaped)
            lat: Latitude string (may be empty)
            lng: Longitude string (may be empty)

        Returns:
            School dictionary or None if parsing fails
        """
        try:
            # Unescape the HTML (single quotes are escaped with backslash)
            school_html = school_html.replace("\\'", "'")

            # Parse the HTML snippet
            soup = BeautifulSoup(school_html, "html.parser")

            # Extract school name from h1
            h1 = soup.find("h1")
            if not h1:
                return None

            display_name = h1.get_text(strip=True)
            if not display_name:
                return None

            # Initialize school record
            school = {
                "name": display_name,
                "full_name": None,
                "state": "FL",
                "nces_id": None,  # FHSAA doesn't provide NCES IDs
                "enrollment": None,
                "classification": None,  # Will be derived from division
                "division": None,
                "district": None,
                "section": None,
                "region": None,
                "conference": None,  # FHSAA doesn't use conferences like OHSAA
                "athletic_director": None,
                "address": None,
                "zip": None,
                "lat": None,
                "lng": None,
            }

            # Parse coordinates
            if lat and lat.strip():
                try:
                    school["lat"] = float(lat)
                except ValueError:
                    pass
            if lng and lng.strip():
                try:
                    school["lng"] = float(lng)
                except ValueError:
                    pass

            # Extract fields from <p> tags with <span> labels
            for p in soup.find_all("p"):
                text = p.get_text(" ", strip=True)

                if "School Full Name:" in text:
                    school["full_name"] = self._extract_field(text, "School Full Name:")
                elif "Athletic Director:" in text:
                    school["athletic_director"] = self._extract_field(text, "Athletic Director:")
                elif "Section:" in text:
                    school["section"] = self._extract_field(text, "Section:")
                elif "FIAAA District:" in text:
                    district_num = self._extract_field(text, "FIAAA District:")
                    if district_num:
                        school["district"] = f"D{district_num}"
                elif "Division:" in text:
                    school["division"] = self._extract_field(text, "Division:")
                elif "Street Address:" in text:
                    school["address"] = self._extract_field(text, "Street Address:")
                elif "Physical Zip:" in text:
                    school["zip"] = self._extract_field(text, "Physical Zip:")
                elif "Entrollment:" in text:  # Note: typo in source ("Entrollment")
                    enrollment_str = self._extract_field(text, "Entrollment:")
                    if enrollment_str and enrollment_str.isdigit():
                        school["enrollment"] = int(enrollment_str)

            # Derive classification from division (Div1-Div32 maps to 1A-7A/Rural)
            school["classification"] = self._division_to_classification(school["division"])

            # Use full name if display name is abbreviated
            if school["full_name"] and len(school["full_name"]) > len(school["name"]):
                # Keep display_name as "name" for consistency with how schools are referenced
                pass

            return school

        except Exception as e:
            logger.warning("Parse error for school: %s", e)
            return None

    def _extract_field(self, text: str, label: str) -> Optional[str]:
        """Extract a field value after a label."""
        if label not in text:
            return None
        value = text.split(label, 1)[1].strip()
        return value if value else None

    def _division_to_classification(self, division: Optional[str]) -> Optional[str]:
        """
        Map FHSAA division (Div1-Div32) to classification (1A-7A, Rural).

        FHSAA uses divisions for playoff brackets, not traditional classifications.
        Divisions are roughly correlated with enrollment:
        - Div1-Div4: Rural/1A (smallest)
        - Div5-Div8: 2A
        - Div9-Div12: 3A
        - Div13-Div16: 4A
        - Div17-Div20: 5A
        - Div21-Div24: 6A
        - Div25-Div32: 7A (largest)

        Note: This is an approximation. FHSAA's classification system is complex
        and changes year to year based on competitive balance calculations.
        """
        if not division:
            return None

        # Extract division number
        match = re.match(r"Div(\d+)", division)
        if not match:
            return division

        div_num = int(match.group(1))

        # Approximate mapping (may need adjustment based on current FHSAA alignment)
        if div_num <= 4:
            return "1A"
        elif div_num <= 8:
            return "2A"
        elif div_num <= 12:
            return "3A"
        elif div_num <= 16:
            return "4A"
        elif div_num <= 20:
            return "5A"
        elif div_num <= 24:
            return "6A"
        else:
            return "7A"

    def fetch_football_schools(self) -> list[dict]:
        """
        Fetch all schools with football programs.

        This is the same as fetch_all_schools() since we use sport_id=1 filter.
        """
        return self.fetch_all_schools()

    def load_to_db(self, schools: list[dict]) -> int:
        """
        Load FHSAA data into athletic_programs table.

        Matches schools by name since FHSAA doesn't provide NCES IDs.

        Args:
            schools: List of school dictionaries from fetch_all_schools()

        Returns:
            Number of schools successfully matched and loaded
        """
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                # Try to find matching school in our database by name
                # Use both the display name and full name for matching
                school_row = None

                names_to_try = [school["name"]]
                if school["full_name"]:
                    names_to_try.append(school["full_name"])

                for name in names_to_try:
                    if school_row:
                        break

                    # Escape LIKE special characters
                    escaped_name = (
                        name.lower()
                        .replace("\\", "\\\\")
                        .replace("%", "\\%")
                        .replace("_", "\\_")
                    )

                    # Try exact match first
                    school_row = conn.execute(
                        """
                        SELECT nces_id FROM schools
                        WHERE state = 'FL' AND LOWER(name) = ? ESCAPE '\\'
                        LIMIT 1
                        """,
                        (escaped_name,)
                    ).fetchone()

                    if not school_row:
                        # Try partial match
                        school_row = conn.execute(
                            """
                            SELECT nces_id FROM schools
                            WHERE state = 'FL' AND LOWER(name) LIKE ? ESCAPE '\\'
                            LIMIT 1
                            """,
                            (f"%{escaped_name}%",)
                        ).fetchone()

                if school_row:
                    # Insert/update athletic program
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO athletic_programs
                        (school_id, sport, classification, conference, division, state_association_id)
                        VALUES (?, 'football', ?, ?, ?, ?)
                        """,
                        (
                            school_row["nces_id"],
                            school["classification"],
                            school["section"],  # Use section as "conference" equivalent
                            school["division"],
                            school["district"],  # FIAAA district as state association ID
                        )
                    )
                    matched += 1
                else:
                    unmatched.append(school["name"])

            logger.info("Matched %d/%d schools to database", matched, len(schools))
            if unmatched and len(unmatched) <= 10:
                logger.info("Unmatched: %s", unmatched)
            elif unmatched:
                logger.info("Unmatched: %d schools (first 5: %s)", len(unmatched), unmatched[:5])

            return matched

    def get_school_details(self, school_name: str) -> Optional[dict]:
        """
        Get detailed information for a specific school.

        Args:
            school_name: School name to search for

        Returns:
            School dictionary or None if not found
        """
        schools = self.fetch_all_schools()
        school_name_lower = school_name.lower()

        for school in schools:
            if school_name_lower in school["name"].lower():
                return school
            if school["full_name"] and school_name_lower in school["full_name"].lower():
                return school

        return None

    def get_schools_by_section(self, section: str) -> list[dict]:
        """
        Get all schools in a specific FHSAA section.

        Args:
            section: Section name (e.g., "Section 1", "Section 2")

        Returns:
            List of schools in the section
        """
        schools = self.fetch_all_schools()
        return [s for s in schools if s.get("section") == section]

    def get_schools_by_district(self, district: str) -> list[dict]:
        """
        Get all schools in a specific FIAAA district.

        Args:
            district: District identifier (e.g., "D1", "D5")

        Returns:
            List of schools in the district
        """
        schools = self.fetch_all_schools()
        return [s for s in schools if s.get("district") == district]


def fetch_and_load() -> int:
    """Convenience function to fetch and load FHSAA data."""
    scraper = FHSAAScraper()
    schools = scraper.fetch_all_schools()
    return scraper.load_to_db(schools)


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Test the scraper
    scraper = FHSAAScraper()
    schools = scraper.fetch_all_schools()

    print(f"\nFetched {len(schools)} Florida football schools")

    if schools:
        # Show sample
        print("\nSample schools:")
        for school in schools[:5]:
            print(f"  - {school['name']} ({school['full_name']})")
            print(f"    Division: {school['division']}, Classification: {school['classification']}")
            print(f"    Section: {school['section']}, District: {school['district']}")
            print(f"    Enrollment: {school['enrollment']}")
            print()

        # Show stats by section
        print("\nSchools by Section:")
        for section in SECTIONS:
            count = len([s for s in schools if s.get("section") == section])
            print(f"  {section}: {count}")

        # Show stats by classification
        print("\nSchools by Classification:")
        for classification in CLASSIFICATIONS:
            count = len([s for s in schools if s.get("classification") == classification])
            print(f"  {classification}: {count}")
