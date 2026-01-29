#!/usr/bin/env python3
"""Fetch all data from external sources."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sources.nces import fetch_and_load as fetch_nces
from sources.census import fetch_and_load as fetch_census


def main():
    """Run all data fetchers."""
    print("=" * 60)
    print("Fetching NCES school data...")
    print("=" * 60)
    school_count = fetch_nces()
    print(f"Total schools loaded: {school_count}\n")

    print("=" * 60)
    print("Fetching Census county data...")
    print("=" * 60)
    county_count = fetch_census()
    print(f"Total counties loaded: {county_count}\n")

    print("=" * 60)
    print("Data fetch complete!")
    print(f"  Schools: {school_count}")
    print(f"  Counties: {county_count}")
    print("=" * 60)


if __name__ == "__main__":
    main()
