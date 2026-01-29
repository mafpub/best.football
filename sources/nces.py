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
CCD_DIRECTORY_URL = "https://nces.ed.gov/ccd/Data/zip/ccd_sch_029_2324_w_1a_073124.zip"
CCD_MEMBERSHIP_URL = "https://nces.ed.gov/ccd/Data/zip/ccd_sch_052_2324_l_1a_073124.zip"

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache" / "nces"
RAW_CACHE_DIR = CACHE_DIR / "raw"


class NCESClient:
    """Client for fetching NCES school data."""

    def __init__(self):
        self.cache = CacheManager(CACHE_DIR)
        self.client = httpx.Client(timeout=300.0)
        RAW_CACHE_DIR.mkdir(parents=True, exist_ok=True)

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

        cache_key = f"ccd_schools_full_{'_'.join(sorted(states))}"
        cached = self.cache.get(cache_key, max_age_days=365)
        if cached:
            print(f"Using cached NCES data ({len(cached)} schools)")
            return cached

        # Fetch directory data (school info) - uses raw file cache
        schools = self._fetch_directory(states)

        # Fetch and merge enrollment data - uses raw file cache
        enrollment_data = self._fetch_enrollment(states)
        schools = self._merge_enrollment(schools, enrollment_data)

        print(f"Total: {len(schools)} high schools with enrollment data")
        self.cache.set(cache_key, schools, CCD_DIRECTORY_URL)
        return schools

    def _download_or_cache(self, url: str, filename: str) -> bytes:
        """Download a file or return cached version."""
        cache_path = RAW_CACHE_DIR / filename
        if cache_path.exists():
            print(f"Using cached {filename}")
            return cache_path.read_bytes()

        print(f"Downloading {url}...")
        if "052" in url:  # Membership file is large
            print("(This is a large file ~200MB, please wait...)")
        response = self.client.get(url)
        response.raise_for_status()

        cache_path.write_bytes(response.content)
        print(f"Cached to {cache_path}")
        return response.content

    def _fetch_directory(self, states: set[str]) -> list[dict]:
        """Fetch school directory data."""
        content = self._download_or_cache(CCD_DIRECTORY_URL, "ccd_directory_2324.zip")

        schools = []
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            print(f"Processing {csv_name}...")

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="iso-8859-1"))

                for row in reader:
                    state = row.get("MSTATE", "").strip()
                    if state not in states:
                        continue

                    grades = row.get("GSLO", ""), row.get("GSHI", "")
                    if not self._has_high_school_grades(grades):
                        continue

                    school = self._parse_school(row)
                    if school:
                        schools.append(school)

        print(f"Parsed {len(schools)} high schools from directory")
        return schools

    def _fetch_enrollment(self, states: set[str]) -> dict[str, int]:
        """Fetch enrollment data from membership file."""
        content = self._download_or_cache(CCD_MEMBERSHIP_URL, "ccd_membership_2324.zip")

        enrollment = {}
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
            print(f"Processing {csv_name}...")

            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="iso-8859-1"))
                for row in reader:
                    self._process_membership_row(row, states, enrollment)

        print(f"Found enrollment data for {len(enrollment)} schools")
        return enrollment

    def _process_membership_row(self, row: dict, states: set[str], enrollment: dict):
        """Process a single membership row to extract enrollment."""
        state = row.get("ST", "").strip()
        if state not in states:
            return

        nces_id = row.get("NCESSCH", "").strip()
        if not nces_id:
            return

        # The membership file has one row per school/grade/race/sex combination
        # We want "Education Unit Total" rows which have total enrollment
        total_ind = row.get("TOTAL_INDICATOR", "").strip()

        if total_ind == "Education Unit Total":
            student_count = row.get("STUDENT_COUNT", "")
            try:
                if student_count and student_count not in (".", "-1", "-2", "-9", ""):
                    val = int(float(student_count))
                    if val > 0:
                        enrollment[nces_id] = val
            except ValueError:
                pass

    def _merge_enrollment(self, schools: list[dict], enrollment: dict[str, int]) -> list[dict]:
        """Merge enrollment data into school records."""
        matched = 0
        for school in schools:
            nces_id = school["nces_id"]
            if nces_id in enrollment:
                school["enrollment"] = enrollment[nces_id]
                matched += 1
        print(f"Matched enrollment for {matched}/{len(schools)} schools")
        return schools

    def _has_high_school_grades(self, grades: tuple[str, str]) -> bool:
        """Check if school serves high school grades (9-12)."""
        lo, hi = grades
        high_school_grades = {"09", "10", "11", "12"}
        try:
            if hi in high_school_grades:
                return True
            if lo.isdigit() and hi.isdigit():
                return int(hi) >= 9
        except (ValueError, AttributeError):
            pass
        return False

    def _parse_school(self, row: dict) -> dict | None:
        """Parse a CCD directory row into our schema."""
        nces_id = row.get("NCESSCH", "").strip()
        if not nces_id:
            return None

        # Parse coordinates
        lat = row.get("LAT", "")
        lng = row.get("LON", "")
        try:
            lat = float(lat) if lat and lat not in (".", "M", "N") else None
            lng = float(lng) if lng and lng not in (".", "M", "N") else None
        except ValueError:
            lat, lng = None, None

        # School type mapping
        charter = row.get("CHARTER_TEXT", "").strip()
        if charter == "Yes":
            school_type = "charter"
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
            "enrollment": None,  # Will be filled from membership data
            "grades": f"{row.get('GSLO', '')}-{row.get('GSHI', '')}",
            "school_type": school_type,
            "title_i": row.get("TITLEI_STATUS_TEXT", "") == "Yes",
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
