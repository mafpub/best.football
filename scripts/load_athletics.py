#!/usr/bin/env python3
"""Load state athletics data from all configured scrapers.

Runs scrapers for:
- Ohio (OHSAA)
- Texas (UIL)
- Florida (FHSAA)
- California (CIFSS - Southern Section)

Each scraper fetches data from state association websites and matches
schools to our NCES database by NCES ID or name.
"""

import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sources.state_athletics.ohio_ohsaa import fetch_and_load as fetch_ohio
from sources.state_athletics.texas_uil import fetch_and_load as fetch_texas
from sources.state_athletics.florida_fhsaa import fetch_and_load as fetch_florida
from sources.state_athletics.california_cifss import fetch_and_load as fetch_california

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Run all state athletics scrapers."""
    results = {}

    scrapers = [
        ("Ohio (OHSAA)", fetch_ohio),
        ("Texas (UIL)", fetch_texas),
        ("Florida (FHSAA)", fetch_florida),
        ("California (CIFSS)", fetch_california),
    ]

    for name, fetch_func in scrapers:
        print("=" * 60)
        print(f"Loading {name} athletics data...")
        print("=" * 60)

        try:
            count = fetch_func()
            results[name] = count
            print(f"  Matched: {count} schools\n")
        except Exception as e:
            logger.exception(f"Error loading {name}: {e}")
            results[name] = f"ERROR: {e}"
            print(f"  ERROR: {e}\n")

    # Summary
    print("=" * 60)
    print("Athletics Data Load Complete!")
    print("=" * 60)
    total = 0
    for name, count in results.items():
        if isinstance(count, int):
            print(f"  {name}: {count} schools")
            total += count
        else:
            print(f"  {name}: {count}")
    print("-" * 60)
    print(f"  TOTAL: {total} athletic programs loaded")
    print("=" * 60)

    return total


if __name__ == "__main__":
    main()
