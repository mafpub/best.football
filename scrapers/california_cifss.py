"""California CIF-SS (CIF Southern Section) Playwright scraper.

Data source: https://www.cifsshome.org/widget/school/directory
Format: Dynamic widget/table with school directory

Uses Playwright to handle dynamic content rendering.
"""

import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup

from scrapers.base import PlaywrightScraper
from pipeline.database import get_db

# Module logger
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "scrapers" / "california"

# URLs
BASE_URL = "https://www.cifsshome.org"
DIRECTORY_URL = "https://www.cifsshome.org/widget/school/directory"


class CaliforniaCIFSSScraper(PlaywrightScraper):
    """Scraper for California CIF-SS school data using Playwright."""

    def __init__(self):
        """Initialize California CIF-SS scraper."""
        selector_yaml = PROJECT_ROOT / "scrapers" / "selectors" / "california_cifss.yaml"
        super().__init__(
            selector_yaml=selector_yaml,
            state="CA",
            association_name="CIF-SS",
        )

        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    async def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all schools from CIF-SS directory using Playwright.

        Returns:
            List of school dictionaries with athletic program data
        """
        logger.info("Fetching CIF-SS school data from directory...")

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
                        # Check if next button is disabled
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
            logger.error("Failed to fetch CIF-SS directory: %s", e)

        logger.info("Total: %d CIF-SS schools", len(all_schools))
        return all_schools

    def _parse_schools(self, soup: BeautifulSoup) -> list[dict]:
        """
        Parse schools from BeautifulSoup HTML.

        Returns:
            List of school dictionaries
        """
        schools = []

        # Try different table structures
        table = soup.find("table")
        if table:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    school = self._parse_row(cells)
                    if school:
                        schools.append(school)

        # Try alternative structures (div-based)
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
            # Assume structure: name, classification, conference, city
            name = cells[0].get_text(strip=True)
            classification = cells[1].get_text(strip=True) if len(cells) > 1 else None
            conference = cells[2].get_text(strip=True) if len(cells) > 2 else None
            city = cells[3].get_text(strip=True) if len(cells) > 3 else None

            # Skip header rows
            if "school" in name.lower() or "name" in name.lower():
                return None

            if not name or len(name) < 2:
                return None

            return {
                "name": name,
                "state": "CA",
                "nces_id": None,
                "classification": classification,
                "conference": conference,
                "city": city,
                "division": None,
            }

        except Exception as e:
            logger.warning("Error parsing row: %s", e)
            return None

    def _parse_div_item(self, item) -> dict | None:
        """Parse a div-based directory item."""
        try:
            name_elem = item.select_one(".school-name, .name")
            class_elem = item.select_one(".classification, .division")
            conf_elem = item.select_one(".conference, .league")
            city_elem = item.select_one(".city")

            if not name_elem:
                return None

            name = name_elem.get_text(strip=True)
            if not name or len(name) < 2:
                return None

            return {
                "name": name,
                "state": "CA",
                "nces_id": None,
                "classification": class_elem.get_text(strip=True) if class_elem else None,
                "conference": conf_elem.get_text(strip=True) if conf_elem else None,
                "city": city_elem.get_text(strip=True) if city_elem else None,
                "division": None,
            }

        except Exception as e:
            logger.warning("Error parsing div item: %s", e)
            return None

    def load_to_db(self, schools: list[dict]) -> int:
        """Load CIF-SS data into athletic_programs table."""
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                school_row = self._find_school_in_db(conn, school["name"], school.get("city"))

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

    def _find_school_in_db(self, conn, school_name: str, city: str = None) -> Optional[dict]:
        """Find a school in the database by name and city."""
        normalized = school_name.lower().strip()

        # Try exact match with city if provided
        if city:
            row = conn.execute(
                """
                SELECT nces_id, name FROM schools
                WHERE state = 'CA' AND LOWER(name) = ? AND LOWER(city) = ?
                LIMIT 1
                """,
                (normalized, city.lower()),
            ).fetchone()

            if row:
                return row

        # Try exact name match
        row = conn.execute(
            "SELECT nces_id, name FROM schools WHERE state = 'CA' AND LOWER(name) = ? LIMIT 1",
            (normalized,),
        ).fetchone()

        if row:
            return row

        # Try name contains match
        escaped_name = (
            normalized.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        row = conn.execute(
            "SELECT nces_id, name FROM schools WHERE state = 'CA' AND LOWER(name) LIKE ? ESCAPE '\\' LIMIT 1",
            (f"%{escaped_name}%",),
        ).fetchone()

        return row


async def scrape_california() -> list[dict]:
    """Convenience function to scrape California schools."""
    scraper = CaliforniaCIFSSScraper()
    return await scraper.fetch_all_schools()


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    async def main():
        scraper = CaliforniaCIFSSScraper()
        schools = await scraper.fetch_all_schools()
        print(f"Total schools found: {len(schools)}")

    asyncio.run(main())
