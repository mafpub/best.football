"""Ohio OHSAA (Ohio High School Athletic Association) Playwright scraper.

Data source: https://ohsaa.finalforms.com/state_schools
Format: Paginated HTML tables with comprehensive school data including NCES IDs

Migrated from httpx + BeautifulSoup to Playwright for better reliability.
"""

import logging
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from scrapers.base import PlaywrightScraper
from pipeline.database import get_db

# Module logger
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "scrapers" / "ohio"

# URLs
BASE_URL = "https://ohsaa.finalforms.com"
DIRECTORY_URL = "https://ohsaa.finalforms.com/state_schools"


class OhioOhsaaScraper(PlaywrightScraper):
    """Scraper for Ohio OHSAA school data using Playwright."""

    def __init__(self):
        """Initialize Ohio OHSAA scraper."""
        selector_yaml = PROJECT_ROOT / "scrapers" / "selectors" / "ohio_ohsaa.yaml"
        super().__init__(
            selector_yaml=selector_yaml,
            state="OH",
            association_name="OHSAA",
        )

        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    async def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all schools from OHSAA FinalForms directory using Playwright.

        Returns:
            List of school dictionaries with athletic program data
        """
        logger.info("Fetching OHSAA school data from FinalForms...")

        all_schools = []

        try:
            browser = await self._get_browser()
            context = await self._new_context(browser)
            page = await context.new_page()

            logger.debug("Navigating to: %s", DIRECTORY_URL)
            await page.goto(DIRECTORY_URL, wait_until="domcontentloaded", timeout=30000)

            # Wait for table to load
            await page.wait_for_selector("table", timeout=15000)

            # Check for pagination
            has_more_pages = True
            page_num = 1

            while has_more_pages:
                logger.debug("Processing page %d", page_num)

                # Get page content
                content = await page.content()
                schools, total_pages = self._parse_page(content)
                all_schools.extend(schools)

                logger.info("Page %d/%s: found %d schools", page_num, total_pages or "?", len(schools))

                if not schools or (total_pages and page_num >= total_pages):
                    has_more_pages = False
                else:
                    # Try to go to next page
                    try:
                        next_button = await page.query_selector("a[aria-label='Next'], .pagination-next")
                        if next_button:
                            is_disabled = await next_button.is_disabled()
                            if is_disabled:
                                has_more_pages = False
                            else:
                                await next_button.click()
                                await page.wait_for_load_state("domcontentloaded")
                                page_num += 1
                        else:
                            has_more_pages = False
                    except Exception:
                        has_more_pages = False

            await browser.close()

        except Exception as e:
            logger.error("Failed to fetch OHSAA directory: %s", e)

        logger.info("Total: %d OHSAA schools", len(all_schools))
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
        pagination = soup.find("nav", {"aria-label": "Pagination"})
        if not pagination:
            pagination = soup.find("ul", class_=re.compile(r"pagination"))

        if pagination:
            page_links = pagination.find_all("a")
            max_page = 1
            for link in page_links:
                text = link.get_text(strip=True)
                if text.isdigit():
                    max_page = max(max_page, int(text))
            return max_page

        return None

    def _extract_small_value(
        self,
        cell: Tag,
        title: str,
        exclude: Optional[set[str]] = None,
        use_dropdown: bool = False,
    ) -> Optional[str]:
        """
        Extract a value from a <small title="..."> element within a cell.

        Args:
            cell: BeautifulSoup Tag for the table cell
            title: The title attribute to search for
            exclude: Set of values to treat as empty/invalid
            use_dropdown: If True, look for dropdown-toggle class instead of first link

        Returns:
            Extracted text value or None
        """
        if exclude is None:
            exclude = {"--", ""}

        small_tag = cell.find("small", title=title)
        if not small_tag:
            return None

        if use_dropdown:
            link = small_tag.find("a", class_="dropdown-toggle")
        else:
            link = small_tag.find("a")

        if not link:
            return None

        value = link.get_text(strip=True)
        if value and value not in exclude:
            return value
        return None

    def _parse_row(self, row, cells) -> dict | None:
        """Parse a single school row from FinalForms table."""
        try:
            row_text = row.get_text(" ", strip=True)

            # Skip header/filter rows
            if "School Level" in row_text or row_text.startswith("Name |"):
                return None

            if len(cells) < 5:
                return None

            # Get enrollment from cell 1
            enrollment = None
            enrollment_text = cells[1].get_text(strip=True)
            if enrollment_text.isdigit():
                enrollment = int(enrollment_text)

            # Get school name from cell 2
            name_cell = cells[2]
            school_name = None

            links = name_cell.find_all("a")
            for link in links:
                text = link.get_text(strip=True)
                if text and not text.startswith("["):
                    school_name = text
                    break

            if not school_name:
                cell_text = name_cell.get_text(" ", strip=True)
                school_name = cell_text.split("[")[0].strip()
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

            # Extract NCES ID
            nces_match = re.search(r"NCES ID:\s*(\d{12})", row_text)
            if nces_match:
                school["nces_id"] = nces_match.group(1)

            # Parse cell 3 using HTML structure
            cell3 = cells[3]

            school["conference"] = self._extract_small_value(
                cell3, "Conference", exclude={"--", "", "Primary Athletic"}
            )

            school["athletic_district"] = self._extract_small_value(cell3, "District")
            school["classes"] = self._extract_small_value(
                cell3, "Class", use_dropdown=True
            )

            # Extract football division
            div_small = cell3.find("small", title="Division")
            if div_small:
                for link in div_small.find_all("a"):
                    text = link.get_text(strip=True)
                    if "Boys Football" in text:
                        div_match = re.match(r"^(I{1,3}|IV|VI{0,2}|VII)\s", text)
                        if div_match:
                            school["division"] = div_match.group(1)
                        break

            return school

        except Exception as e:
            logger.warning("Parse error for row: %s - %s", row.get_text(" ", strip=True)[:100], e)
            return None

    def load_to_db(self, schools: list[dict]) -> int:
        """Load OHSAA data into athletic_programs table."""
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                nces_id = school["nces_id"]
                school_row = None

                if nces_id:
                    school_row = conn.execute(
                        "SELECT nces_id FROM schools WHERE nces_id = ?",
                        (nces_id,)
                    ).fetchone()

                if not school_row:
                    escaped_name = (
                        school["name"]
                        .lower()
                        .replace("\\", "\\\\")
                        .replace("%", "\\%")
                        .replace("_", "\\_")
                    )
                    school_row = conn.execute(
                        """
                        SELECT nces_id FROM schools
                        WHERE state = 'OH' AND LOWER(name) LIKE ? ESCAPE '\\'
                        LIMIT 1
                        """,
                        (f"%{escaped_name}%",)
                    ).fetchone()

                if school_row:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO athletic_programs
                        (school_id, sport, classification, conference, division, state_association_id)
                        VALUES (?, 'football', ?, ?, ?, ?)
                        """,
                        (
                            school_row["nces_id"],
                            school["division"],
                            school["conference"],
                            school["athletic_district"],
                            school["nces_id"],
                        )
                    )
                    matched += 1
                else:
                    unmatched.append(school["name"])

            logger.info("Matched %d/%d schools to database", matched, len(schools))
            return matched


async def scrape_ohio() -> list[dict]:
    """Convenience function to scrape Ohio schools."""
    scraper = OhioOhsaaScraper()
    return await scraper.fetch_all_schools()


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    async def main():
        scraper = OhioOhsaaScraper()
        schools = await scraper.fetch_all_schools()
        print(f"Total schools found: {len(schools)}")

    asyncio.run(main())
