"""Ohio OHSAA (Ohio High School Athletic Association) scraper.

Data source: https://ohsaa.finalforms.com/state_schools
Format: Paginated HTML tables with comprehensive school data including NCES IDs.
"""

import re
from pathlib import Path

from bs4 import BeautifulSoup

from pipeline.cache import CacheManager
from pipeline.database import get_db
from .scraper_base import ProxiedScraper

# OHSAA FinalForms base URL
BASE_URL = "https://ohsaa.finalforms.com/state_schools"

# Cache directory
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "state_athletics" / "ohio"


class OHSAAScraper(ProxiedScraper):
    """Scraper for Ohio OHSAA school data from FinalForms."""

    def __init__(self):
        super().__init__(cache_dir=CACHE_DIR, respect_delay=1.5)
        self.processed_cache = CacheManager(CACHE_DIR)

    def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all schools from OHSAA FinalForms directory.

        Returns:
            List of school dictionaries with athletic program data
        """
        # Check for processed cache
        cache_key = "ohsaa_schools_all"
        cached = self.processed_cache.get(cache_key, max_age_days=7)
        if cached:
            print(f"Using cached OHSAA data ({len(cached)} schools)")
            return cached

        print("Fetching OHSAA school data from FinalForms...")

        all_schools = []
        page = 1
        total_pages = None

        while True:
            url = f"{BASE_URL}?page={page}&direction=asc&sort=state_schools.full_name"
            html = self.fetch(url, cache_hours=24)

            if not html:
                print(f"  [error] Failed to fetch page {page}")
                break

            schools, total_pages = self._parse_page(html)
            all_schools.extend(schools)

            print(f"  Page {page}/{total_pages or '?'}: found {len(schools)} schools")

            if not schools or (total_pages and page >= total_pages):
                break

            page += 1

        print(f"Total: {len(all_schools)} OHSAA schools")
        self.processed_cache.set(cache_key, all_schools, BASE_URL)
        return all_schools

    def _parse_page(self, html: str) -> tuple[list[dict], int | None]:
        """
        Parse a page of school results.

        Returns:
            (list of school dicts, total page count or None)
        """
        soup = BeautifulSoup(html, "html.parser")
        schools = []

        # Find the table
        table = soup.find("table")
        if not table:
            return [], None

        # Parse pagination to get total pages
        total_pages = self._parse_pagination(soup)

        # Find all school rows
        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            school = self._parse_row(row, cells)
            if school:
                schools.append(school)

        return schools, total_pages

    def _parse_pagination(self, soup: BeautifulSoup) -> int | None:
        """Extract total page count from pagination."""
        # Look for pagination links
        pagination = soup.find("nav", {"aria-label": "Pagination"})
        if not pagination:
            pagination = soup.find("ul", class_=re.compile(r"pagination"))

        if pagination:
            # Find last page number
            page_links = pagination.find_all("a")
            max_page = 1
            for link in page_links:
                text = link.get_text(strip=True)
                if text.isdigit():
                    max_page = max(max_page, int(text))
            return max_page

        return None

    def _parse_row(self, row, cells) -> dict | None:
        """Parse a single school row from FinalForms table."""
        try:
            row_text = row.get_text(" ", strip=True)

            # Skip header/filter rows
            if "School Level" in row_text or row_text.startswith("Name |"):
                return None

            # Need at least 5 cells for valid data row
            if len(cells) < 5:
                return None

            # FinalForms actual structure:
            # Cell 0: Empty (checkbox column)
            # Cell 1: Enrollment number
            # Cell 2: School name with [grades] and District info
            # Cell 3: Conference, Athletic District, Classes info
            # Cell 4: Address info

            # Get enrollment from cell 1
            enrollment = None
            enrollment_text = cells[1].get_text(strip=True)
            if enrollment_text.isdigit():
                enrollment = int(enrollment_text)

            # Get school name from cell 2
            name_cell = cells[2]
            school_name = None

            # Find the school name link (usually has the main name)
            links = name_cell.find_all("a")
            for link in links:
                text = link.get_text(strip=True)
                # Skip grade level text like [9th - 12th]
                if text and not text.startswith("["):
                    school_name = text
                    break

            if not school_name:
                # Fallback: get text before the grade brackets
                cell_text = name_cell.get_text(" ", strip=True)
                school_name = cell_text.split("[")[0].strip()
                # Remove any leading letter prefix (like "C " for charter)
                if len(school_name) > 2 and school_name[1] == " ":
                    school_name = school_name[2:].strip()

            if not school_name or len(school_name) < 3:
                return None

            school = {
                "name": school_name,
                "state": "OH",
                "nces_id": None,
                "enrollment": enrollment,
                "division": None,
                "district": None,
                "conference": None,
                "athletic_district": None,
                "classes": None,
            }

            # Extract NCES ID (12-digit number after "NCES ID:")
            nces_match = re.search(r"NCES ID:\s*(\d{12})", row_text)
            if nces_match:
                school["nces_id"] = nces_match.group(1)

            # Parse cell 3 using HTML structure (small tags with titles)
            cell3 = cells[3]

            # Extract conference from <small title="Conference">
            conf_small = cell3.find("small", title="Conference")
            if conf_small:
                conf_link = conf_small.find("a")
                if conf_link:
                    conf = conf_link.get_text(strip=True)
                    if conf and conf not in ("--", "Primary Athletic"):
                        school["conference"] = conf

            # Extract athletic district from <small title="District">
            dist_small = cell3.find("small", title="District")
            if dist_small:
                dist_link = dist_small.find("a")
                if dist_link:
                    district = dist_link.get_text(strip=True)
                    if district and district != "--":
                        school["athletic_district"] = district

            # Extract classes from <small title="Class">
            class_small = cell3.find("small", title="Class")
            if class_small:
                # First dropdown shows primary class
                dropdown = class_small.find("a", class_="dropdown-toggle")
                if dropdown:
                    classes = dropdown.get_text(strip=True)
                    if classes and classes not in ("--", ""):
                        school["classes"] = classes

            # Extract football division from <small title="Division">
            # Look for dropdown menu item containing "Boys Football"
            div_small = cell3.find("small", title="Division")
            if div_small:
                for link in div_small.find_all("a"):
                    text = link.get_text(strip=True)
                    if "Boys Football" in text:
                        # Extract roman numeral prefix (I, II, III, IV, V, VI, VII)
                        div_match = re.match(r"^(I{1,3}|IV|VI{0,2}|VII)\s", text)
                        if div_match:
                            school["division"] = div_match.group(1)
                        break

            return school

        except Exception as e:
            print(f"  [parse error] {e}")
            return None

    def fetch_football_schools(self) -> list[dict]:
        """
        Fetch only schools with football programs.

        Note: FinalForms doesn't have a direct football filter,
        so we fetch all and can filter by division later.
        """
        all_schools = self.fetch_all_schools()

        # Schools with a football division are football schools
        football_schools = [s for s in all_schools if s.get("division")]

        print(f"Football schools: {len(football_schools)}/{len(all_schools)}")
        return football_schools

    def load_to_db(self, schools: list[dict]) -> int:
        """
        Load OHSAA data into athletic_programs table.

        Matches schools by NCES ID when available, otherwise by name.
        """
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                # Try to find matching school in our database
                nces_id = school.get("nces_id")
                school_row = None

                if nces_id:
                    school_row = conn.execute(
                        "SELECT nces_id FROM schools WHERE nces_id = ?",
                        (nces_id,)
                    ).fetchone()

                if not school_row:
                    # Try name match
                    school_row = conn.execute(
                        """
                        SELECT nces_id FROM schools
                        WHERE state = 'OH' AND LOWER(name) LIKE ?
                        LIMIT 1
                        """,
                        (f"%{school['name'].lower()}%",)
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
                            school.get("division"),  # Division I-VII
                            school.get("conference"),
                            school.get("district"),  # Athletic district
                            school.get("nces_id"),  # Use as state association ID
                        )
                    )
                    matched += 1
                else:
                    unmatched.append(school["name"])

            print(f"Matched {matched}/{len(schools)} schools to database")
            if unmatched and len(unmatched) <= 10:
                print(f"Unmatched: {unmatched}")
            elif unmatched:
                print(f"Unmatched: {len(unmatched)} schools (first 5: {unmatched[:5]})")

            return matched


def fetch_and_load() -> int:
    """Convenience function to fetch and load OHSAA data."""
    scraper = OHSAAScraper()
    schools = scraper.fetch_all_schools()
    return scraper.load_to_db(schools)


if __name__ == "__main__":
    fetch_and_load()
