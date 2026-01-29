"""NCES (National Center for Education Statistics) data client.

Fetches school data from the NCES Common Core of Data (CCD).
Data available at: https://nces.ed.gov/ccd/files.asp
"""

import csv
import io
import zipfile
from pathlib import Path

import httpx

from pipeline.cache import CacheManager
from pipeline.database import get_db

# MVP states
MVP_STATES = {"TX", "CA", "FL", "OH"}

# NCES CCD data URLs (2023-2024 school year)
# Directory listing: https://nces.ed.gov/ccd/files.asp
CCD_SCHOOL_URL = "https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_2324_w_1a_073124.zip"

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache" / "nces"


class NCESClient:
    """Client for fetching NCES school data."""

    def __init__(self):
        self.cache = CacheManager(CACHE_DIR)
        self.client = httpx.Client(timeout=120.0)

    def fetch_schools(self, states: set[str] | None = None) -> list[dict]:
        """
        Fetch school data for specified states.

        Args:
            states: Set of state abbreviations (e.g., {"TX", "CA"}).
                   Defaults to MVP_STATES.

        Returns:
            List of school dictionaries
        """
        states = states or MVP_STATES

        cache_key = f"ccd_schools_{'_'.join(sorted(states))}"
        cached = self.cache.get(cache_key, max_age_days=365)
        if cached:
            print(f"Using cached NCES data ({len(cached)} schools)")
            return cached

        print(f"Downloading NCES CCD data from {CCD_SCHOOL_URL}...")
        response = self.client.get(CCD_SCHOOL_URL)
        response.raise_for_status()

        schools = []
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Find the CSV file in the archive
            csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            print(f"Processing {csv_name}...")

            with zf.open(csv_name) as f:
                # NCES uses ISO-8859-1 encoding
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="iso-8859-1"))

                for row in reader:
                    state = row.get("MSTATE", "").strip()
                    if state not in states:
                        continue

                    # Filter to schools with high school grades
                    grades = row.get("GSLO", ""), row.get("GSHI", "")
                    if not self._has_high_school_grades(grades):
                        continue

                    school = self._parse_school(row)
                    if school:
                        schools.append(school)

        print(f"Parsed {len(schools)} high schools in {states}")
        self.cache.set(cache_key, schools, CCD_SCHOOL_URL)
        return schools

    def _has_high_school_grades(self, grades: tuple[str, str]) -> bool:
        """Check if school serves high school grades (9-12)."""
        lo, hi = grades
        # Grade codes: 09, 10, 11, 12 or UG (ungraded)
        high_school_grades = {"09", "10", "11", "12"}
        try:
            if hi in high_school_grades:
                return True
            # Check if range includes high school
            if lo.isdigit() and hi.isdigit():
                return int(hi) >= 9
        except (ValueError, AttributeError):
            pass
        return False

    def _parse_school(self, row: dict) -> dict | None:
        """Parse a CCD row into our schema."""
        nces_id = row.get("NCESSCH", "").strip()
        if not nces_id:
            return None

        # Parse enrollment
        enrollment = row.get("TOTAL", "")
        try:
            enrollment = int(enrollment) if enrollment and enrollment != "." else None
        except ValueError:
            enrollment = None

        # Parse coordinates
        lat = row.get("LAT", "")
        lng = row.get("LON", "")
        try:
            lat = float(lat) if lat and lat not in (".", "M", "N") else None
            lng = float(lng) if lng and lng not in (".", "M", "N") else None
        except ValueError:
            lat, lng = None, None

        # School type mapping
        charter = row.get("CHARESSION", "").strip()
        sch_type = row.get("SCH_TYPE", "").strip()
        if charter == "1":
            school_type = "charter"
        elif sch_type == "1":
            school_type = "public"
        elif sch_type == "2":
            school_type = "private"
        else:
            school_type = "public"

        return {
            "nces_id": nces_id,
            "name": row.get("SCH_NAME", "").strip(),
            "address": row.get("LSTREET1", "").strip(),
            "city": row.get("LCITY", "").strip(),
            "county": row.get("CONAME", "").strip(),
            "state": row.get("MSTATE", "").strip(),
            "zip": row.get("LZIP", "").strip()[:5],
            "lat": lat,
            "lng": lng,
            "enrollment": enrollment,
            "grades": f"{row.get('GSLO', '')}-{row.get('GSHI', '')}",
            "school_type": school_type,
            "title_i": row.get("TITLEI_STATUS", "") == "1",
            "urban_locale": row.get("ULOCALE", "").strip(),
        }

    def load_to_db(self, schools: list[dict]) -> int:
        """Load schools into the database."""
        with get_db() as conn:
            inserted = 0
            for school in schools:
                try:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO schools
                        (nces_id, name, address, city, county, state, zip,
                         lat, lng, enrollment, grades, school_type, title_i,
                         urban_locale, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                CURRENT_TIMESTAMP)
                        """,
                        (
                            school["nces_id"],
                            school["name"],
                            school["address"],
                            school["city"],
                            school["county"],
                            school["state"],
                            school["zip"],
                            school["lat"],
                            school["lng"],
                            school["enrollment"],
                            school["grades"],
                            school["school_type"],
                            school["title_i"],
                            school["urban_locale"],
                        ),
                    )
                    inserted += 1
                except Exception as e:
                    print(f"Error inserting {school['nces_id']}: {e}")

            print(f"Loaded {inserted} schools into database")
            return inserted


def fetch_and_load(states: set[str] | None = None) -> int:
    """Convenience function to fetch and load NCES data."""
    client = NCESClient()
    schools = client.fetch_schools(states)
    return client.load_to_db(schools)


if __name__ == "__main__":
    fetch_and_load()
