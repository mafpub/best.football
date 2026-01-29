"""Derived metrics calculations.

This module provides:
- Competitive Index: Enrollment percentile within classification
- Travel Burden: Average distance to conference/division opponents
- Regional Density: Football programs per square mile by county
"""

from .competitive_index import (
    calculate_all_competitive_indices,
    calculate_enrollment_percentile,
    get_rankings_by_classification,
    get_school_competitive_index,
)
from .travel_burden import (
    calculate_all_travel_burdens,
    get_school_travel_burden,
    get_schools_by_travel_burden,
    haversine_distance,
)
from .density import (
    calculate_all_densities,
    calculate_county_density,
    get_county_density,
    get_highest_density_counties,
    get_state_summary,
)

__all__ = [
    # Competitive Index
    "calculate_all_competitive_indices",
    "calculate_enrollment_percentile",
    "get_rankings_by_classification",
    "get_school_competitive_index",
    # Travel Burden
    "calculate_all_travel_burdens",
    "get_school_travel_burden",
    "get_schools_by_travel_burden",
    "haversine_distance",
    # Density
    "calculate_all_densities",
    "calculate_county_density",
    "get_county_density",
    "get_highest_density_counties",
    "get_state_summary",
]
