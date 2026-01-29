"""Texas UIL (University Interscholastic League) scraper.

Data source: https://www.uiltexas.org/football/alignments
Format: PDF documents containing district alignments by classification.

The UIL organizes Texas high school football into:
- Classes 1A through 6A (based on enrollment)
- Each class has Division I and Division II (except 6A)
- 1A is six-man football, 2A-6A are eleven-man
- Schools are organized into regions and districts
"""

import logging
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import pypdf

from pipeline.cache import CacheManager
from pipeline.database import get_db
from .scraper_base import ProxiedScraper

# Module logger
logger = logging.getLogger(__name__)

# UIL URLs
BASE_URL = "https://www.uiltexas.org"
ALIGNMENTS_URL = "https://www.uiltexas.org/football/alignments"
PDF_BASE_URL = "http://www.uiltexas.org/files/alignments/"

# Cache directory
CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "state_athletics" / "texas"

# Classification structure
CLASSIFICATIONS = ["1A", "2A", "3A", "4A", "5A", "6A"]
DIVISIONS = ["D1", "D2"]  # Division I and Division II


class TexasUILScraper(ProxiedScraper):
    """Scraper for Texas UIL school data from alignment PDFs."""

    def __init__(self):
        super().__init__(cache_dir=CACHE_DIR, respect_delay=1.5)
        self.processed_cache = CacheManager(CACHE_DIR)
        self.pdf_cache_dir = CACHE_DIR / "pdfs"
        self.pdf_cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_all_schools(self) -> list[dict]:
        """
        Fetch all schools from UIL alignment PDFs.

        Returns:
            List of school dictionaries with athletic program data
        """
        # Check for processed cache
        cache_key = "uil_schools_all"
        cached = self.processed_cache.get(cache_key, max_age_days=7)
        if cached:
            logger.info("Using cached UIL data (%d schools)", len(cached))
            return cached

        logger.info("Fetching UIL school data from alignment PDFs...")

        # First, get the actual PDF links from the alignments page
        pdf_links = self._fetch_pdf_links()
        if not pdf_links:
            logger.error("No PDF links found on alignments page")
            return []

        all_schools = []

        for classification, division, url in pdf_links:
            logger.info("Processing %s %s: %s", classification, division or "", url)
            schools = self._process_pdf(url, classification, division)
            all_schools.extend(schools)
            logger.info("Found %d schools in %s %s", len(schools), classification, division or "")

        logger.info("Total: %d UIL schools", len(all_schools))
        self.processed_cache.set(cache_key, all_schools, ALIGNMENTS_URL)
        return all_schools

    def _fetch_pdf_links(self) -> list[tuple[str, Optional[str], str]]:
        """
        Fetch actual PDF links from the alignments page.

        Returns:
            List of (classification, division, url) tuples
        """
        html = self.fetch(ALIGNMENTS_URL, cache_hours=168)  # Cache for 1 week
        if not html:
            logger.error("Failed to fetch alignments page")
            return []

        soup = BeautifulSoup(html, "html.parser")
        pdf_links = []

        # Find all PDF links on the page
        for link in soup.find_all("a", href=re.compile(r"\.pdf$", re.I)):
            href = link.get("href", "")
            text = link.get_text(strip=True).lower()

            # Skip non-football PDFs
            if "football" not in text and "alignment" not in text and "fb" not in href.lower():
                # Check if it's in the alignments directory
                if "/alignments/" not in href.lower():
                    continue

            # Skip non-alignment PDFs (organizing chairs, etc.)
            if "organizing" in text or "chair" in text:
                continue

            # Parse classification and division from text or URL
            classification, division = self._parse_classification(text, href)
            if classification:
                # Ensure full URL
                if href.startswith("/"):
                    href = urljoin(BASE_URL, href)
                elif not href.startswith("http"):
                    href = PDF_BASE_URL + href

                pdf_links.append((classification, division, href))
                logger.debug("Found PDF: %s %s - %s", classification, division or "", href)

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

        # 6A typically doesn't have divisions in the same way
        # but may have D1/D2 in URL
        if classification == "6A" and division is None:
            # Check URL patterns
            if "d1" in url_lower or "d2" in url_lower:
                if "d1" in url_lower:
                    division = "D1"
                else:
                    division = "D2"
            # If 6A has no explicit division, it might be combined
            # Leave division as None for "all 6A"

        return classification, division

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
        """
        Fetch PDF content with caching.

        Returns:
            PDF bytes or None if failed
        """
        import hashlib

        # Create cache filename from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        cache_path = self.pdf_cache_dir / f"{url_hash}.pdf"

        # Check cache (PDFs valid for 30 days since alignments are biennial)
        if cache_path.exists():
            import time

            age_days = (time.time() - cache_path.stat().st_mtime) / 86400
            if age_days < 30:
                logger.debug("Using cached PDF: %s", url)
                return cache_path.read_bytes()

        # Fetch PDF
        self._respect_rate_limit()

        import httpx

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"}

        try:
            # Try without proxy first for direct PDF downloads
            with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                response = client.get(url, headers=headers)
                response.raise_for_status()

                content = response.content

                # Verify it's a PDF
                if not content.startswith(b"%PDF"):
                    logger.warning("Invalid PDF content from %s", url)
                    return None

                # Cache the PDF
                cache_path.write_bytes(content)
                logger.debug("Cached PDF: %s", url)

                return content

        except (httpx.HTTPStatusError, httpx.RequestError, httpx.TimeoutException) as e:
            logger.warning("Failed to fetch PDF %s: %s", url, e)
            return None

    def _process_pdf(
        self, url: str, classification: str, division: Optional[str]
    ) -> list[dict]:
        """
        Download and parse a UIL alignment PDF.

        Uses default extraction mode (not layout) as it produces cleaner
        line-by-line output. Post-processing merges wrapped school names.

        Returns:
            List of school dictionaries
        """
        pdf_content = self._fetch_pdf(url)
        if not pdf_content:
            return []

        try:
            import io

            reader = pypdf.PdfReader(io.BytesIO(pdf_content))

            # Use default extraction mode (cleaner line breaks)
            all_text = ""
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    all_text += text + "\n"

            return self._parse_alignment_text(all_text, classification, division)

        except Exception as e:
            logger.error("Failed to parse PDF %s: %s", url, e)
            return []

    def _parse_alignment_text(
        self, text: str, classification: str, division: Optional[str]
    ) -> list[dict]:
        """
        Parse the text content from a UIL alignment PDF.

        The PDFs have a structure like:
        - Region headers (Region 1, Region 2, etc.)
        - District numbers followed by school names
        - Some schools have location prefixes (e.g., "El Paso Coronado")

        Returns:
            List of school dictionaries
        """
        schools = []

        # Clean up the text
        # Remove header info like "Region 1 Region 2 Region 3 Region 4"
        text = re.sub(r"Region \d(\s+Region \d)+", "", text)

        # Remove legend markers
        text = re.sub(r"\+ Boys Only", "", text)
        text = re.sub(r"\* Girls Only", "", text)

        # Remove title lines
        text = re.sub(r"FOOTBALL.*?ALIGNMENT", "", text, flags=re.DOTALL | re.IGNORECASE)

        # Determine if this is six-man football
        is_six_man = classification == "1A"

        # Current region and district tracking
        current_district = None
        current_region = None

        # Split by district markers
        # Districts appear as "1District", "2District", etc.
        district_pattern = re.compile(r"(\d+)\s*District")

        # Find all district markers and their positions
        district_matches = list(district_pattern.finditer(text))

        if not district_matches:
            logger.warning("No district markers found in text for %s %s", classification, division)
            # Try alternative parsing
            return self._parse_flat_text(text, classification, division)

        for i, match in enumerate(district_matches):
            district_num = match.group(1)

            # Get text between this district and the next (or end)
            start = match.end()
            if i + 1 < len(district_matches):
                end = district_matches[i + 1].start()
            else:
                end = len(text)

            district_text = text[start:end].strip()

            # Determine region from district number
            # UIL typically has 4 regions, districts are distributed across them
            # Region assignment varies by classification, so we estimate
            region = self._estimate_region(int(district_num), classification)

            # Parse schools from district text
            district_schools = self._parse_district_schools(
                district_text, district_num, region, classification, division, is_six_man
            )
            schools.extend(district_schools)

        return schools

    def _estimate_region(self, district_num: int, classification: str) -> int:
        """
        Estimate region based on district number.

        UIL divides Texas into 4 regions. District numbers are roughly
        sequential within regions, but the exact mapping varies by year.

        This is an approximation - the actual region would need to be
        extracted from the PDF layout (regions appear in columns).
        """
        # For now, we'll leave region as derived from district
        # A more accurate implementation would parse the PDF columns
        # Most classes have districts 1-4 in Region 1, 5-8 in Region 2, etc.
        # but the distribution varies

        # Return 0 to indicate we haven't determined the region
        # The region info is in the PDF but hard to extract from text alone
        return 0

    def _parse_district_schools(
        self,
        text: str,
        district_num: str,
        region: int,
        classification: str,
        division: Optional[str],
        is_six_man: bool,
    ) -> list[dict]:
        """
        Parse school names from district text.

        Schools are listed one per line or sometimes run together.
        Some have location prefixes like "El Paso" or "Houston".
        Handles wrapped school names that span multiple lines.
        """
        schools = []

        # Clean up text
        text = text.strip()

        # Split by newlines
        raw_lines = [line.strip() for line in text.split("\n") if line.strip()]

        # Merge wrapped school names
        # A line is likely a continuation if:
        # 1. It's a single short word that could be a suffix (Park, Hill, etc.)
        # 2. It doesn't look like a standalone school name
        merged_lines = self._merge_wrapped_names(raw_lines)

        for line in merged_lines:
            school_name = self._clean_school_name(line)
            if school_name and len(school_name) >= 2:
                school = {
                    "name": school_name,
                    "state": "TX",
                    "nces_id": None,  # UIL doesn't provide NCES IDs
                    "enrollment": None,
                    "division": division,
                    "district": district_num,
                    "region": region if region > 0 else None,
                    "classification": classification,
                    "conference": None,
                    "is_six_man": is_six_man,
                }
                schools.append(school)

        return schools

    def _merge_wrapped_names(self, lines: list[str]) -> list[str]:
        """
        Merge lines that appear to be continuations of previous lines.

        In UIL PDFs, some school names wrap across lines due to column width.
        Examples:
        - "Amarillo Highland" + "Park" -> "Amarillo Highland Park"
        - "Abilene Texas" + "Leadership" -> "Abilene Texas Leadership"

        We're very conservative - only merge when the next line is clearly
        a continuation word (not a standalone place name).
        """
        if not lines:
            return []

        # Words that are ONLY suffixes, never standalone school names
        # These are words that only make sense as part of a longer name
        suffix_only_words = {
            # School suffixes
            "Leadership", "Prep", "Charter", "Academy",
            # Geographic suffixes that rarely stand alone
            "Park", "Heights", "Ridge", "Hills", "Springs", "Mound",
            "Junction", "Crossing", "Landing",
            # Directional suffixes
            "Northeast", "Northwest", "Southeast", "Southwest",
            # Texas specific
            "County", "Co.",
        }

        merged = []
        i = 0

        while i < len(lines):
            current = lines[i].strip()

            # Skip empty or very short lines
            if len(current) < 2:
                i += 1
                continue

            # Check if next line should be merged
            while i + 1 < len(lines):
                next_line = lines[i + 1].strip()

                # Only merge if next_line is a single suffix-only word
                if self._is_suffix_continuation(next_line, suffix_only_words):
                    current = current + " " + next_line
                    i += 1
                else:
                    break

            merged.append(current)
            i += 1

        return merged

    def _is_suffix_continuation(self, line: str, suffix_only_words: set) -> bool:
        """
        Check if a line is definitely a continuation suffix.

        Very conservative - only returns True for words that cannot
        stand alone as school names.
        """
        if not line or len(line) < 2:
            return False

        # Skip if it looks like a district marker
        if re.match(r"^\d+\s*District", line):
            return False

        # If the first word is lowercase, it's likely a continuation
        if line[0].islower():
            return True

        # Only merge if it's a single suffix-only word
        words = line.split()
        if len(words) == 1 and words[0] in suffix_only_words:
            return True

        return False

    def _clean_school_name(self, name: str) -> Optional[str]:
        """
        Clean up a school name extracted from PDF text.

        Handles:
        - Extra whitespace
        - Trailing markers (+, *)
        - Common abbreviations
        """
        if not name:
            return None

        # Remove markers
        name = re.sub(r"[+*]$", "", name).strip()

        # Remove leading/trailing whitespace and collapse internal whitespace
        name = " ".join(name.split())

        # Skip if too short or looks like a header
        if len(name) < 2:
            return None

        skip_patterns = [
            r"^District$",
            r"^Region\s*\d",
            r"^Conference",
            r"^Football",
            r"^Division",
            r"^\d+$",
        ]
        for pattern in skip_patterns:
            if re.match(pattern, name, re.I):
                return None

        # Expand common abbreviations for consistency
        abbreviations = {
            "Ft Worth": "Fort Worth",
            "Ft. Worth": "Fort Worth",
            "H ": "Houston ",  # At start of name
            "SA ": "San Antonio ",
            "Cyp.": "Cypress",
            "Cyp ": "Cypress ",
            "LC ": "League City ",
            "RR ": "Round Rock ",
        }
        for abbr, full in abbreviations.items():
            if name.startswith(abbr):
                name = full + name[len(abbr) :]
            name = name.replace(f" {abbr}", f" {full}")

        return name.strip()

    def _parse_flat_text(
        self, text: str, classification: str, division: Optional[str]
    ) -> list[dict]:
        """
        Fallback parser for text without clear district markers.

        Returns:
            List of school dictionaries with unknown district
        """
        schools = []
        is_six_man = classification == "1A"

        # Try to split on capital letters or newlines
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
                    "region": None,
                    "classification": classification,
                    "conference": None,
                    "is_six_man": is_six_man,
                }
                schools.append(school)

        return schools

    def fetch_football_schools(self) -> list[dict]:
        """
        Fetch only schools with football programs.

        For UIL, all schools in the alignment PDFs have football programs.
        """
        return self.fetch_all_schools()

    def load_to_db(self, schools: list[dict]) -> int:
        """
        Load UIL data into athletic_programs table.

        Matches schools by name since UIL doesn't provide NCES IDs.
        Uses fuzzy matching to handle name variations.
        """
        with get_db() as conn:
            matched = 0
            unmatched = []

            for school in schools:
                # Try to find matching school in our database
                school_row = self._find_school_in_db(conn, school["name"])

                if school_row:
                    # Build classification string (e.g., "3A-D1" or "6A")
                    classification = school.get("classification", "")
                    if school.get("division"):
                        classification = f"{classification}-{school['division']}"

                    # Insert/update athletic program
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
                            school.get("district"),  # UIL district as division field
                            None,  # UIL doesn't provide state association IDs
                        ),
                    )
                    matched += 1
                else:
                    unmatched.append(school["name"])

            logger.info("Matched %d/%d schools to database", matched, len(schools))
            if unmatched and len(unmatched) <= 10:
                logger.info("Unmatched: %s", unmatched)
            elif unmatched:
                logger.info(
                    "Unmatched: %d schools (first 10: %s)", len(unmatched), unmatched[:10]
                )

            return matched

    def _find_school_in_db(self, conn, school_name: str) -> Optional[dict]:
        """
        Find a school in the database by name.

        Uses multiple matching strategies:
        1. Exact name match
        2. Name contains match
        3. Fuzzy match with common variations
        """
        # Normalize for matching
        normalized = school_name.lower().strip()

        # Strategy 1: Exact match
        # Escape LIKE special characters
        escaped_name = (
            normalized.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        )

        row = conn.execute(
            """
            SELECT nces_id, name FROM schools
            WHERE state = 'TX' AND LOWER(name) = ?
            LIMIT 1
            """,
            (normalized,),
        ).fetchone()

        if row:
            return row

        # Strategy 2: Name contains match (handles prefix variations)
        # For names like "El Paso Coronado" try to match "Coronado"
        # and for "Houston Memorial" try "Memorial"
        row = conn.execute(
            """
            SELECT nces_id, name FROM schools
            WHERE state = 'TX' AND LOWER(name) LIKE ? ESCAPE '\\'
            LIMIT 1
            """,
            (f"%{escaped_name}%",),
        ).fetchone()

        if row:
            return row

        # Strategy 3: Try variations
        variations = self._get_name_variations(school_name)
        for variation in variations:
            escaped_var = (
                variation.lower()
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )
            row = conn.execute(
                """
                SELECT nces_id, name FROM schools
                WHERE state = 'TX' AND LOWER(name) LIKE ? ESCAPE '\\'
                LIMIT 1
                """,
                (f"%{escaped_var}%",),
            ).fetchone()
            if row:
                return row

        return None

    def _get_name_variations(self, name: str) -> list[str]:
        """
        Generate common name variations for matching.

        Handles cases like:
        - "San Antonio LEE" -> "Robert E. Lee"
        - "Houston Bellaire" -> "Bellaire"
        - City prefixes being removed/added
        """
        variations = []

        # Extract the school name without city prefix
        # Common city prefixes in Texas
        city_prefixes = [
            "Houston",
            "Dallas",
            "San Antonio",
            "Fort Worth",
            "Austin",
            "El Paso",
            "Arlington",
            "Corpus Christi",
            "Plano",
            "Laredo",
            "Lubbock",
            "Garland",
            "Irving",
            "Amarillo",
            "Grand Prairie",
            "Brownsville",
            "McKinney",
            "Frisco",
            "Pasadena",
            "Mesquite",
            "Killeen",
            "McAllen",
            "Waco",
            "Denton",
            "Midland",
            "Abilene",
            "Odessa",
            "Beaumont",
            "Round Rock",
            "Carrollton",
            "Richardson",
            "Lewisville",
            "Tyler",
            "College Station",
            "Pearland",
            "San Angelo",
            "Allen",
            "Wichita Falls",
            "League City",
            "Sugar Land",
            "Bryan",
            "Temple",
            "Edinburg",
            "Mission",
            "Conroe",
            "New Braunfels",
            "Mansfield",
            "Cedar Hill",
            "Pflugerville",
            "Harlingen",
            "Longview",
        ]

        for prefix in city_prefixes:
            if name.startswith(prefix + " "):
                # Add the name without the city prefix
                school_only = name[len(prefix) + 1 :].strip()
                if school_only:
                    variations.append(school_only)
                break

        # Handle "ISD" suffix variations
        if " ISD" not in name:
            variations.append(f"{name} ISD")
            variations.append(f"{name} Independent School District")

        # Handle "High School" suffix
        if "High School" not in name and "HS" not in name:
            variations.append(f"{name} High School")
            variations.append(f"{name} H S")

        return variations


def fetch_and_load() -> int:
    """Convenience function to fetch and load UIL data."""
    scraper = TexasUILScraper()
    schools = scraper.fetch_all_schools()
    return scraper.load_to_db(schools)


if __name__ == "__main__":
    # Set up logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Run the scraper
    scraper = TexasUILScraper()
    schools = scraper.fetch_all_schools()

    print(f"\nTotal schools found: {len(schools)}")

    # Print sample by classification
    for classification in CLASSIFICATIONS:
        class_schools = [s for s in schools if s.get("classification") == classification]
        print(f"\n{classification}: {len(class_schools)} schools")
        if class_schools:
            print(f"  Sample: {[s['name'] for s in class_schools[:5]]}")
