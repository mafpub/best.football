"""Florida FHSAA (Florida High School Athletic Association) Playwright scraper.

Data source: https://www.fhsaahome.org/widget/school-directory-locations
Format: Dynamic widget with school directory and locations

Uses Playwright to handle dynamic content rendering.
"""

import logging
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.base import PlaywrightScraper
from pipeline.database import get_db

# Module logger
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "scrapers" / "florida"

# URLs
BASE_URL = "https://www.fhsaahome.org"
DIRECTORY_URL = "https://www.fhsaahome.org/widget/school-directory-locations"


class FloridaFhsaaScraper(PlaywrightScraper):
    """Scraper for Florida FHSAA school data using Playwright."""

    def __init__(self):
        """Initialize Florida FHSAA scraper."""
        selector_yaml = PROJECT_ROOT / "scrapers" / "selectors" / "florida_fhsaa.yaml"
        super().__init__(
            selector_yaml=selector_yaml,
            state="FL",
            association_name="FHSAA",
        )

        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    async def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all schools from FHSAA directory using Playwright.

        Returns:
            List of school dictionaries with athletic program data
        """
        logger.info("Fetching FHSAA school data from directory...")

        all_schools = []

        try:
            browser = await self._get_browser()
            context = await self._new_context(browser)
            page = await context.new_page()

            logger.debug("Navigating to: %s", DIRECTORY_URL)
            await page.goto(DIRECTORY_URL, wait_until="domcontentloaded", timeout=30000)

            # Wait for table to load
            await page.wait_for_selector("table, .school-directory, .widget", timeout=15000)

            # Check for pagination
            has_more_pages = True
            page_num = 1

            while has_more_pages:
                logger.debug("Processing page %d", page_num)

                # Get page content
                content = await page.content()
                soup = BeautifulSoup(content, "html.parser")

                # Parse schools from current page
                schools = self._parse_schools(soup)
                all_schools.extend(schools)
                logger.info("Page %d: found %d schools", page_num, len(schools))

                # Check for next page
                next_button = await page.query_selector("a.next, .pagination-next, [aria-label='Next']")

                if next_button:
                    try:
                        is_disabled = await next_button.is_disabled()
                        if is_disabled:
                            has_more_pages = False
                        else:
                            await next_button.click()
                            await page.wait_for_load_state("domcontentloaded")
                            page_num += 1
                    except Exception:
                        has_more_pages = False
                else:
                    has_more_pages = False

            await browser.close()

        except Exception as e:
            logger.error("Failed to fetch FHSAA directory: %s", e)

        logger.info("Total: %d FHSAA schools", len(all_schools))
        return all_schools

    def _parse_schools(self, soup) -> list[dict]:
        """Parse schools from BeautifulSoup HTML."""
        schools = []

        # Try table structure
        table = soup.find("table")
        if table:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    school = self._parse_row(cells)
                    if school:
                        schools.append(school)

        # Try alternative structures
        if not schools:
            for item in soup.select(".school-row, .directory-item"):
                school = self._parse_div_item(item)
                if school:
                    schools.append(school)

        return schools

    def _parse_row(self, cells) -> dict | None:
        """Parse a table row into school data."""
        if len(cells) < 2:
            return None

        try:
            # Structure: name, classification, conference, city, county
            name = cells[0].get_text(strip=True)
            classification = cells[1].get_text(strip=True) if len(cells) > 1 else None
            conference = cells[2].get_text(strip=True) if len(cells) > 2 else None
            city = cells[3].get_text(strip=True) if len(cells) > 3 else None
            county = cells[4].get_text(strip=True) if len(cells) > 4 else None

            # Skip header rows
            if "school" in name.lower() or "name" in name.lower():
                return None

            if not name or len(name) < 2:
                return None

            return {
                "name": name,
                "state": "FL",
                "nces_id": None,
                "classification": classification,
                "conference": conference,
                "city": city,
                "county": county,
                "division": None,
            }

        except Exception as e:
            logger.warning("Error parsing row: %s", e)
            return None

    def _parse_div_item(self, item) -> dict | None:
        """Parse a div-based directory item."""
        try:
            name_elem = item.select_one(".school-name, .name")
            class_elem = item.select_one(".classification, .class")
            conf_elem = item.select_one(".conference, .district")
            city_elem = item.select_one(".city")
            county_elem = item.select_one(".county")

            if not name_elem:
                return None

            name = name_elem.get_text(strip=True)
            if not name or len(name) < 2:
                return None

            return {
                "name": name,
                "state": "FL",
                "nces_id": None,
                "classification": class_elem.get_text(strip=True) if class_elem else None,
                "conference": conf_elem.get_text(strip=True) if conf_elem else None,
                "city": city_elem.get_text(strip=True) if city_elem else None,
                "county": county_elem.get_text(strip=True) if county_elem else None,
                "division": None,
            }

        except Exception as e:
            logger.warning("Error parsing div item: %s", e)
            return None

    def load_to_db(self, schools: list[dict]) -> int:
        """Load FHSAA data into athletic_programs table."""
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                school_row = self._find_school_in_db(
                    conn, school["name"], school.get("city"), school.get("county")
                )

                if school_row:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO athletic_programs
                        (school_id, sport, classification, conference, division, state_association_id)
                        VALUES (?, 'football', ?, ?, ?, ?)
                        """,
                        (
                            school_row["nces_id"],
                            school.get("classification"),
                            school.get("conference"),
                            school.get("division"),
                            None,
                        ),
                    )
                    matched += 1
                else:
                    unmatched.append(school["name"])

            logger.info("Matched %d/%d schools to database", matched, len(schools))
            return matched

    def _find_school_in_db(self, conn, school_name: str, city: str = None, county: str = None) -> Optional[dict]:
        """Find a school in the database by name, city, and county."""
        normalized = school_name.lower().strip()

        # Try with city and county first
        if city and county:
            row = conn.execute(
                """
                SELECT nces_id, name FROM schools
                WHERE state = 'FL' AND LOWER(name) = ? AND LOWER(city) = ? AND LOWER(county) = ?
                LIMIT 1
                """,
                (normalized, city.lower(), county.lower()),
            ).fetchone()

            if row:
                return row

        # Try with city
        if city:
            row = conn.execute(
                """
                SELECT nces_id, name FROM schools
                WHERE state = 'FL' AND LOWER(name) = ? AND LOWER(city) = ?
                LIMIT 1
                """,
                (normalized, city.lower()),
            ).fetchone()

            if row:
                return row

        # Try exact name match
        row = conn.execute(
            "SELECT nces_id, name FROM schools WHERE state = 'FL' AND LOWER(name) = ? LIMIT 1",
            (normalized,),
        ).fetchone()

        if row:
            return row

        # Try name contains match
        escaped_name = (
            normalized.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        row = conn.execute(
            "SELECT nces_id, name FROM schools WHERE state = 'FL' AND LOWER(name) LIKE ? ESCAPE '\\' LIMIT 1",
            (f"%{escaped_name}%",),
        ).fetchone()

        return row


async def scrape_florida() -> list[dict]:
    """Convenience function to scrape Florida schools."""
    scraper = FloridaFhsaaScraper()
    return await scraper.fetch_all_schools()


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    async def main():
        scraper = FloridaFhsaaScraper()
        schools = await scraper.fetch_all_schools()
        print(f"Total schools found: {len(schools)}")

    asyncio.run(main())
