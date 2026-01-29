"""Census API client for county demographics."""

from pathlib import Path

import httpx

from pipeline.cache import CacheManager
from pipeline.database import get_db

# Census API base URL
CENSUS_API_BASE = "https://api.census.gov/data"

# ACS 5-year estimates (most stable county-level data)
ACS_YEAR = "2022"
ACS_DATASET = "acs/acs5"

# MVP state FIPS codes
STATE_FIPS = {
    "TX": "48",
    "CA": "06",
    "FL": "12",
    "OH": "39",
}

CACHE_DIR = Path(__file__).parent.parent / "data" / "cache" / "census"


class CensusClient:
    """Client for fetching Census county demographics."""

    def __init__(self, api_key: str | None = None):
        self.cache = CacheManager(CACHE_DIR)
        self.client = httpx.Client(timeout=60.0)
        self.api_key = api_key  # Census API works without key for basic queries

    def fetch_counties(self, states: list[str] | None = None) -> list[dict]:
        """
        Fetch county demographics for specified states.

        Args:
            states: List of state abbreviations (e.g., ["TX", "CA"])

        Returns:
            List of county dictionaries
        """
        states = states or list(STATE_FIPS.keys())

        all_counties = []
        for state in states:
            fips = STATE_FIPS.get(state)
            if not fips:
                print(f"Unknown state: {state}")
                continue

            cache_key = f"census_counties_{state}"
            cached = self.cache.get(cache_key, max_age_days=365)
            if cached:
                print(f"Using cached Census data for {state} ({len(cached)} counties)")
                all_counties.extend(cached)
                continue

            print(f"Fetching Census data for {state}...")
            counties = self._fetch_state_counties(fips, state)
            self.cache.set(cache_key, counties)
            all_counties.extend(counties)

        return all_counties

    def _fetch_state_counties(self, state_fips: str, state_abbr: str) -> list[dict]:
        """Fetch county data for a single state."""
        # Variables:
        # B01003_001E - Total population
        # B19013_001E - Median household income
        # NAME - County name
        variables = "NAME,B01003_001E,B19013_001E"

        url = (
            f"{CENSUS_API_BASE}/{ACS_YEAR}/{ACS_DATASET}"
            f"?get={variables}&for=county:*&in=state:{state_fips}"
        )
        if self.api_key:
            url += f"&key={self.api_key}"

        response = self.client.get(url)
        response.raise_for_status()

        data = response.json()
        headers = data[0]
        counties = []

        for row in data[1:]:
            row_dict = dict(zip(headers, row))

            # Parse population and income
            pop = row_dict.get("B01003_001E")
            income = row_dict.get("B19013_001E")

            try:
                population = int(pop) if pop and pop not in ("-", "null") else None
            except ValueError:
                population = None

            try:
                median_income = int(income) if income and income not in ("-", "null") else None
            except ValueError:
                median_income = None

            # Build FIPS code
            county_fips = f"{row_dict['state']}{row_dict['county']}"

            # Parse name (format: "County Name, State")
            name = row_dict.get("NAME", "").split(",")[0].strip()
            # Remove " County" suffix if present
            if name.endswith(" County"):
                name = name[:-7]

            counties.append({
                "fips": county_fips,
                "name": name,
                "state": state_abbr,
                "population": population,
                "median_income": median_income,
                "area_sq_mi": None,  # Would need TIGER data
            })

        print(f"  Found {len(counties)} counties in {state_abbr}")
        return counties

    def load_to_db(self, counties: list[dict]) -> int:
        """Load counties into the database."""
        with get_db() as conn:
            inserted = 0
            for county in counties:
                try:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO counties
                        (fips, name, state, population, median_income, area_sq_mi)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            county["fips"],
                            county["name"],
                            county["state"],
                            county["population"],
                            county["median_income"],
                            county["area_sq_mi"],
                        ),
                    )
                    inserted += 1
                except Exception as e:
                    print(f"Error inserting {county['fips']}: {e}")

            print(f"Loaded {inserted} counties into database")
            return inserted


def fetch_and_load(states: list[str] | None = None) -> int:
    """Convenience function to fetch and load Census data."""
    client = CensusClient()
    counties = client.fetch_counties(states)
    return client.load_to_db(counties)


if __name__ == "__main__":
    fetch_and_load()
