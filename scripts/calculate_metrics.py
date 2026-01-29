#!/usr/bin/env python3
"""Calculate all derived metrics for schools.

Runs after athletics data is loaded to compute:
- Competitive Index (enrollment percentile within classification)
- Travel Burden (average distance to opponents)
- Regional Density (programs per county)
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from metrics.competitive_index import calculate_all_competitive_indices
from metrics.travel_burden import calculate_all_travel_burdens
from metrics.density import get_state_summary, get_highest_density_counties

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Calculate all metrics."""
    print("=" * 60)
    print("Calculating Competitive Indices...")
    print("=" * 60)
    ci_count = calculate_all_competitive_indices()
    print(f"  Updated: {ci_count} schools\n")

    print("=" * 60)
    print("Calculating Travel Burdens...")
    print("=" * 60)
    tb_count = calculate_all_travel_burdens()
    print(f"  Updated: {tb_count} schools\n")

    print("=" * 60)
    print("Regional Density Summary")
    print("=" * 60)
    for state in ["TX", "CA", "FL", "OH"]:
        summary = get_state_summary(state)
        print(f"\n{state}:")
        print(f"  Programs: {summary.get('total_programs', 0)}")
        if 'avg_density_per_100sqmi' in summary:
            print(f"  Avg Density: {summary['avg_density_per_100sqmi']} per 100 sq mi")

        top_counties = get_highest_density_counties(state, limit=3)
        if top_counties:
            print(f"  Top 3 counties:")
            for c in top_counties:
                print(f"    - {c['county']}: {c['density_per_100sqmi']} per 100 sq mi")

    print("\n" + "=" * 60)
    print("Metrics Calculation Complete!")
    print(f"  Competitive Index: {ci_count} schools")
    print(f"  Travel Burden: {tb_count} schools")
    print("=" * 60)


if __name__ == "__main__":
    main()
