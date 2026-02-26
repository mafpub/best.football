"""Texas UIL (University Interscholastic League) Playwright scraper.

Data source: https://www.uiltexas.org/football/alignments
Format: PDF documents containing district alignments by classification.

Migrated from httpx + BeautifulSoup to Playwright for better reliability
with dynamic content and PDF downloads.
"""

import logging
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import pypdf

from scrapers.base import PlaywrightScraper, SelectorConfig
from pipeline.database import get_db

# Module logger
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "cache" / "scrapers" / "texas"
PDF_CACHE_DIR = CACHE_DIR / "pdfs"

# Classification structure
CLASSIFICATIONS = ["1A", "2A", "3A", "4A", "5A", "6A"]
DIVISIONS = ["D1", "D2"]

# URLs
BASE_URL = "https://www.uiltexas.org"
ALIGNMENTS_URL = "https://www.uiltexas.org/football/alignments"


class TexasUILScraper(PlaywrightScraper):
    """Scraper for Texas UIL school data using Playwright."""

    def __init__(self):
        """Initialize Texas UIL scraper."""
        selector_yaml = PROJECT_ROOT / "scrapers" / "selectors" / "texas_uil.yaml"
        super().__init__(
            selector_yaml=selector_yaml,
            state="TX",
            association_name="UIL",
        )

        # Create cache directories
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    async def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all schools from UIL alignment PDFs using Playwright.

        Returns:
            List of school dictionaries with athletic program data
        """
        logger.info("Fetching UIL school data from alignment PDFs...")

        # First, get the actual PDF links from the alignments page
        pdf_links = await self._fetch_pdf_links()
        if not pdf_links:
            logger.error("No PDF links found on alignments page")
            return []

        all_schools = []

        for classification, division, url in pdf_links:
            logger.info("Processing %s %s: %s", classification, division or "", url)
            schools = await self._process_pdf(url, classification, division)
            all_schools.extend(schools)
            logger.info("Found %d schools in %s %s", len(schools), classification, division or "")

        logger.info("Total: %d UIL schools", len(all_schools))
        return all_schools

    async def _fetch_pdf_links(self) -> list[tuple[str, Optional[str], str]]:
        """
        Fetch actual PDF links from the alignments page using Playwright.

        Returns:
            List of (classification, division, url) tuples
        """
        from scrapers.base import async_playwright

        pdf_links = []

        try:
            browser = await self._get_browser()
            context = await self._new_context(browser)
            page = await context.new_page()

            logger.debug("Navigating to alignments page: %s", ALIGNMENTS_URL)
            await page.goto(ALIGNMENTS_URL, wait_until="domcontentloaded", timeout=30000)

            # Wait for links to load
            await page.wait_for_selector("a[href$='.pdf']", timeout=10000)

            # Get all PDF links
            links = await page.query_selector_all("a[href$='.pdf']")

            for link in links:
                href = await link.get_attribute("href")
                text = await link.inner_text()

                if not href:
                    continue

                text_lower = text.lower()
                href_lower = href.lower()

                # Skip non-football PDFs
                if "football" not in text_lower and "alignment" not in text_lower and "fb" not in href_lower:
                    if "/alignments/" not in href_lower:
                        continue

                # Skip non-alignment PDFs
                if "organizing" in text_lower or "chair" in text_lower:
                    continue

                # Parse classification and division
                classification, division = self._parse_classification(text, href)
                if classification:
                    # Ensure full URL
                    if href.startswith("/"):
                        href = urljoin(BASE_URL, href)
                    elif not href.startswith("http"):
                        href = f"http://www.uiltexas.org/files/alignments/{href}"

                    pdf_links.append((classification, division, href))
                    logger.debug("Found PDF: %s %s - %s", classification, division or "", href)

            await browser.close()

        except Exception as e:
            logger.error("Failed to fetch PDF links: %s", e)

        # Sort by classification and division
        class_order = {c: i for i, c in enumerate(CLASSIFICATIONS)}
        pdf_links.sort(key=lambda x: (class_order.get(x[0], 99), x[1] or ""))

        return pdf_links

    def _parse_classification(
        self, text: str, url: str
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Parse classification and division from link text or URL.

        Returns:
            (classification, division) tuple, e.g., ("3A", "D1")
        """
        text = text.lower()
        url_lower = url.lower()

        classification = None
        division = None

        # Try to extract classification (1A through 6A)
        for cls in CLASSIFICATIONS:
            if cls.lower() in text or cls.lower() in url_lower:
                classification = cls
                break

        if not classification:
            # Try patterns like "1ad1" or "6abb"
            match = re.search(r"(\d)a", url_lower)
            if match:
                classification = f"{match.group(1)}A"

        if not classification:
            return None, None

        # Try to extract division
        if "division i" in text and "division ii" not in text:
            division = "D1"
        elif "division ii" in text:
            division = "D2"
        elif "d1" in text or "d1" in url_lower:
            if "d2" not in text and "d2" not in url_lower:
                division = "D1"
            elif "d2" in url_lower:
                division = "D2"
            else:
                division = "D1"
        elif "d2" in text or "d2" in url_lower:
            division = "D2"

        return classification, division

    async def _process_pdf(
        self, url: str, classification: str, division: Optional[str]
    ) -> list[dict]:
        """
        Download and parse a UIL alignment PDF.

        Args:
            url: PDF URL
            classification: School classification (e.g., "5A")
            division: Division (D1, D2, or None)

        Returns:
            List of school dictionaries
        """
        pdf_content = await self._fetch_pdf(url)
        if not pdf_content:
            return []

        try:
            import io

            reader = pypdf.PdfReader(io.BytesIO(pdf_content))

            all_text = ""
            for page_obj in reader.pages:
                text = page_obj.extract_text()
                if text:
                    all_text += text + "\n"

            return self._parse_alignment_text(all_text, classification, division)

        except Exception as e:
            logger.error("Failed to parse PDF %s: %s", url, e)
            return []

    async def _fetch_pdf(self, url: str) -> Optional[bytes]:
        """
        Fetch PDF content with caching using Playwright.

        Returns:
            PDF bytes or None if failed
        """
        import hashlib
        import time

        # Create cache filename from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        cache_path = PDF_CACHE_DIR / f"{url_hash}.pdf"

        # Check cache (PDFs valid for 30 days)
        if cache_path.exists():
            age_days = (time.time() - cache_path.stat().st_mtime) / 86400
            if age_days < 30:
                logger.debug("Using cached PDF: %s", url)
                return cache_path.read_bytes()

        # Fetch PDF with Playwright
        from scrapers.base import async_playwright

        try:
            browser = await self._get_browser()
            context = await self._new_context(browser)
            page = await context.new_page()

            logger.debug("Downloading PDF: %s", url)
            response = await page.goto(url, timeout=60000)

            if response and response.ok:
                # Download as bytes
                pdf_bytes = await context.request.get(url)

                # Verify it's a PDF
                content = await pdf_bytes.body()
                if content.startswith(b"%PDF"):
                    # Cache the PDF
                    cache_path.write_bytes(content)
                    logger.debug("Cached PDF: %s", url)

                    await browser.close()
                    return content

            await browser.close()

        except Exception as e:
            logger.warning("Failed to fetch PDF %s: %s", url, e)

        return None

    def _parse_alignment_text(
        self, text: str, classification: str, division: Optional[str]
    ) -> list[dict]:
        """
        Parse the text content from a UIL alignment PDF.

        Returns:
            List of school dictionaries
        """
        schools = []

        # Clean up the text
        text = re.sub(r"Region \d(\s+Region \d)+", "", text)
        text = re.sub(r"\+ Boys Only", "", text)
        text = re.sub(r"\* Girls Only", "", text)
        text = re.sub(r"FOOTBALL.*?ALIGNMENT", "", text, flags=re.DOTALL | re.IGNORECASE)

        is_six_man = classification == "1A"

        # Split by district markers
        district_pattern = re.compile(r"(\d+)\s*District")
        district_matches = list(district_pattern.finditer(text))

        if not district_matches:
            logger.warning("No district markers found in text for %s %s", classification, division)
            return self._parse_flat_text(text, classification, division)

        for i, match in enumerate(district_matches):
            district_num = match.group(1)

            start = match.end()
            if i + 1 < len(district_matches):
                end = district_matches[i + 1].start()
            else:
                end = len(text)

            district_text = text[start:end].strip()

            district_schools = self._parse_district_schools(
                district_text, district_num, classification, division, is_six_man
            )
            schools.extend(district_schools)

        return schools

    def _parse_district_schools(
        self,
        text: str,
        district_num: str,
        classification: str,
        division: Optional[str],
        is_six_man: bool,
    ) -> list[dict]:
        """Parse school names from district text."""
        schools = []
        text = text.strip()

        raw_lines = [line.strip() for line in text.split("\n") if line.strip()]
        merged_lines = self._merge_wrapped_names(raw_lines)

        for line in merged_lines:
            school_name = self._clean_school_name(line)
            if school_name and len(school_name) >= 2:
                school = {
                    "name": school_name,
                    "state": "TX",
                    "nces_id": None,
                    "enrollment": None,
                    "division": division,
                    "district": district_num,
                    "classification": classification,
                    "conference": None,
                    "is_six_man": is_six_man,
                }
                schools.append(school)

        return schools

    def _merge_wrapped_names(self, lines: list[str]) -> list[str]:
        """Merge lines that appear to be continuations of previous lines."""
        if not lines:
            return []

        suffix_only_words = {
            "Leadership", "Prep", "Charter", "Academy",
            "Park", "Heights", "Ridge", "Hills", "Springs", "Mound",
            "Junction", "Crossing", "Landing",
            "Northeast", "Northwest", "Southeast", "Southwest",
            "County", "Co.",
        }

        merged = []
        i = 0

        while i < len(lines):
            current = lines[i].strip()

            if len(current) < 2:
                i += 1
                continue

            while i + 1 < len(lines):
                next_line = lines[i + 1].strip()

                if self._is_suffix_continuation(next_line, suffix_only_words):
                    current = current + " " + next_line
                    i += 1
                else:
                    break

            merged.append(current)
            i += 1

        return merged

    def _is_suffix_continuation(self, line: str, suffix_only_words: set) -> bool:
        """Check if a line is definitely a continuation suffix."""
        if not line or len(line) < 2:
            return False

        if re.match(r"^\d+\s*District", line):
            return False

        if line[0].islower():
            return True

        words = line.split()
        if len(words) == 1 and words[0] in suffix_only_words:
            return True

        return False

    def _clean_school_name(self, name: str) -> Optional[str]:
        """Clean up a school name extracted from PDF text."""
        if not name:
            return None

        name = re.sub(r"[+*]$", "", name).strip()
        name = " ".join(name.split())

        if len(name) < 2:
            return None

        skip_patterns = [
            r"^District$", r"^Region\s*\d", r"^Conference",
            r"^Football", r"^Division", r"^\d+$",
        ]
        for pattern in skip_patterns:
            if re.match(pattern, name, re.I):
                return None

        # Expand abbreviations
        abbreviations = {
            "Ft Worth": "Fort Worth",
            "Ft. Worth": "Fort Worth",
        }
        for abbr, full in abbreviations.items():
            if name.startswith(abbr):
                name = full + name[len(abbr):]
            name = name.replace(f" {abbr}", f" {full}")

        return name.strip()

    def _parse_flat_text(
        self, text: str, classification: str, division: Optional[str]
    ) -> list[dict]:
        """Fallback parser for text without clear district markers."""
        schools = []
        is_six_man = classification == "1A"

        lines = [line.strip() for line in text.split("\n") if line.strip()]

        for line in lines:
            school_name = self._clean_school_name(line)
            if school_name:
                school = {
                    "name": school_name,
                    "state": "TX",
                    "nces_id": None,
                    "enrollment": None,
                    "division": division,
                    "district": None,
                    "classification": classification,
                    "conference": None,
                    "is_six_man": is_six_man,
                }
                schools.append(school)

        return schools

    def load_to_db(self, schools: list[dict]) -> int:
        """Load UIL data into athletic_programs table."""
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                school_row = self._find_school_in_db(conn, school["name"])

                if school_row:
                    classification = school.get("classification", "")
                    if school.get("division"):
                        classification = f"{classification}-{school['division']}"

                    conn.execute(
                        """
                        INSERT OR REPLACE INTO athletic_programs
                        (school_id, sport, classification, conference, division, state_association_id)
                        VALUES (?, 'football', ?, ?, ?, ?)
                        """,
                        (
                            school_row["nces_id"],
                            classification,
                            school.get("conference"),
                            school.get("district"),
                            None,
                        ),
                    )
                    matched += 1
                else:
                    unmatched.append(school["name"])

            logger.info("Matched %d/%d schools to database", matched, len(schools))
            return matched

    def _find_school_in_db(self, conn, school_name: str) -> Optional[dict]:
        """Find a school in the database by name."""
        normalized = school_name.lower().strip()

        # Exact match
        row = conn.execute(
            "SELECT nces_id, name FROM schools WHERE state = 'TX' AND LOWER(name) = ? LIMIT 1",
            (normalized,),
        ).fetchone()

        if row:
            return row

        # Name contains match
        escaped_name = (
            normalized.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )
        row = conn.execute(
            "SELECT nces_id, name FROM schools WHERE state = 'TX' AND LOWER(name) LIKE ? ESCAPE '\\' LIMIT 1",
            (f"%{escaped_name}%",),
        ).fetchone()

        if row:
            return row

        return None


async def scrape_texas() -> list[dict]:
    """Convenience function to scrape Texas schools."""
    scraper = TexasUILScraper()
    return await scraper.fetch_all_schools()


if __name__ == "__main__":
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    async def main():
        scraper = TexasUILScraper()
        schools = await scraper.fetch_all_schools()
        print(f"Total schools found: {len(schools)}")

        for classification in CLASSIFICATIONS:
            class_schools = [s for s in schools if s.get("classification") == classification]
            print(f"{classification}: {len(class_schools)} schools")

    asyncio.run(main())
