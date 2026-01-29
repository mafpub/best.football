"""Base class for state athletics association scrapers."""

from abc import ABC, abstractmethod
from pathlib import Path

import httpx

from pipeline.cache import CacheManager

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache" / "state_athletics"


class BaseAthleticsScraper(ABC):
    """Abstract base class for state athletics scrapers."""

    state: str  # Two-letter state code
    association_name: str  # e.g., "UIL", "CIF"
    base_url: str

    def __init__(self):
        self.cache = CacheManager(CACHE_DIR / self.state.lower())
        self.client = httpx.Client(
            timeout=60.0,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; best.football/1.0)"
            }
        )

    @abstractmethod
    def fetch_schools(self) -> list[dict]:
        """
        Fetch football programs from the state association.

        Returns:
            List of dicts with keys:
            - state_association_id: str
            - school_name: str
            - classification: str (e.g., "6A", "5A")
            - conference: str
            - division: str | None
        """
        pass

    @abstractmethod
    def fetch_classifications(self) -> list[dict]:
        """
        Fetch classification/conference structure.

        Returns:
            List of dicts with keys:
            - id: str
            - name: str
            - classification: str
            - region: str | None
        """
        pass

    def match_to_nces(self, association_schools: list[dict]) -> list[tuple[dict, str | None]]:
        """
        Attempt to match association schools to NCES IDs.

        This is a fuzzy matching process that should be reviewed manually.

        Returns:
            List of (association_school, nces_id or None) tuples
        """
        from pipeline.database import get_db

        results = []

        with get_db() as conn:
            for school in association_schools:
                # Try exact name match first
                row = conn.execute(
                    """
                    SELECT nces_id FROM schools
                    WHERE state = ? AND LOWER(name) = LOWER(?)
                    """,
                    (self.state, school["school_name"])
                ).fetchone()

                if row:
                    results.append((school, row["nces_id"]))
                else:
                    # Try partial match
                    row = conn.execute(
                        """
                        SELECT nces_id FROM schools
                        WHERE state = ? AND LOWER(name) LIKE ?
                        LIMIT 1
                        """,
                        (self.state, f"%{school['school_name'].lower()}%")
                    ).fetchone()

                    results.append((school, row["nces_id"] if row else None))

        matched = sum(1 for _, nces_id in results if nces_id)
        print(f"Matched {matched}/{len(results)} schools to NCES IDs")
        return results
