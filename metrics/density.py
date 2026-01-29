"""Regional Density calculation.

Measures the density of high school football programs per county/region.

Higher density = more local competition, shorter travel, more talent in area.
Lower density = rural area, fewer nearby opponents, potentially longer travel.
"""

import logging
from typing import Optional

from pipeline.database import get_db

logger = logging.getLogger(__name__)


def calculate_county_density(
    school_count: int,
    area_sq_mi: float,
) -> float:
    """
    Calculate school density for a county.

    Args:
        school_count: Number of schools with football programs
        area_sq_mi: County area in square miles

    Returns:
        Schools per 100 square miles
    """
    if not area_sq_mi or area_sq_mi <= 0:
        return 0.0

    # Return schools per 100 sq mi for readable numbers
    return (school_count / area_sq_mi) * 100


def calculate_all_densities() -> dict:
    """
    Calculate school density for all counties and store results.

    Returns:
        Dict with county stats: {(state, county): density}
    """
    with get_db() as conn:
        # Get school counts by county
        school_counts = conn.execute("""
            SELECT
                s.state,
                s.county,
                COUNT(*) as school_count
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            WHERE s.county IS NOT NULL
            GROUP BY s.state, s.county
        """).fetchall()

        logger.info("Found %d counties with football programs", len(school_counts))

        densities = {}

        for row in school_counts:
            state = row["state"]
            county_name = row["county"]
            school_count = row["school_count"]

            # Look up county area from counties table
            county_data = conn.execute("""
                SELECT area_sq_mi, population
                FROM counties
                WHERE state = ? AND LOWER(name) = LOWER(?)
            """, (state, county_name)).fetchone()

            if county_data and county_data["area_sq_mi"]:
                density = calculate_county_density(
                    school_count,
                    county_data["area_sq_mi"]
                )

                # Also calculate programs per capita (per 10,000 residents)
                if county_data["population"] and county_data["population"] > 0:
                    per_capita = (school_count / county_data["population"]) * 10000
                else:
                    per_capita = None
            else:
                # No area data - use a rough estimate based on state averages
                density = None
                per_capita = None

            densities[(state, county_name)] = {
                "school_count": school_count,
                "density_per_100sqmi": density,
                "per_capita_10k": per_capita,
            }

        return densities


def get_county_density(state: str, county: str) -> Optional[dict]:
    """
    Get density metrics for a specific county.

    Args:
        state: State code
        county: County name

    Returns:
        Dict with density metrics or None
    """
    with get_db() as conn:
        # Get school count
        school_count = conn.execute("""
            SELECT COUNT(*) as cnt
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            WHERE s.state = ? AND LOWER(s.county) = LOWER(?)
        """, (state, county)).fetchone()["cnt"]

        # Get county data
        county_data = conn.execute("""
            SELECT area_sq_mi, population
            FROM counties
            WHERE state = ? AND LOWER(name) = LOWER(?)
        """, (state, county)).fetchone()

        if not school_count:
            return None

        result = {"school_count": school_count}

        if county_data:
            if county_data["area_sq_mi"]:
                result["area_sq_mi"] = county_data["area_sq_mi"]
                result["density_per_100sqmi"] = calculate_county_density(
                    school_count, county_data["area_sq_mi"]
                )

            if county_data["population"]:
                result["population"] = county_data["population"]
                result["per_capita_10k"] = (
                    school_count / county_data["population"]
                ) * 10000

        return result


def get_highest_density_counties(
    state: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """
    Get counties with highest football program density.

    Args:
        state: Optional state filter
        limit: Maximum results

    Returns:
        List of county dicts sorted by density
    """
    with get_db() as conn:
        if state:
            query = """
                SELECT
                    s.state,
                    s.county,
                    COUNT(*) as school_count,
                    c.area_sq_mi,
                    c.population,
                    CASE WHEN c.area_sq_mi > 0
                         THEN (COUNT(*) * 100.0 / c.area_sq_mi)
                         ELSE NULL END as density
                FROM schools s
                JOIN athletic_programs ap ON s.nces_id = ap.school_id
                LEFT JOIN counties c ON s.state = c.state AND LOWER(s.county) = LOWER(c.name)
                WHERE s.county IS NOT NULL AND s.state = ?
                GROUP BY s.state, s.county
                HAVING density IS NOT NULL
                ORDER BY density DESC
                LIMIT ?
            """
            rows = conn.execute(query, (state, limit)).fetchall()
        else:
            query = """
                SELECT
                    s.state,
                    s.county,
                    COUNT(*) as school_count,
                    c.area_sq_mi,
                    c.population,
                    CASE WHEN c.area_sq_mi > 0
                         THEN (COUNT(*) * 100.0 / c.area_sq_mi)
                         ELSE NULL END as density
                FROM schools s
                JOIN athletic_programs ap ON s.nces_id = ap.school_id
                LEFT JOIN counties c ON s.state = c.state AND LOWER(s.county) = LOWER(c.name)
                WHERE s.county IS NOT NULL
                GROUP BY s.state, s.county
                HAVING density IS NOT NULL
                ORDER BY density DESC
                LIMIT ?
            """
            rows = conn.execute(query, (limit,)).fetchall()

        return [
            {
                "state": row["state"],
                "county": row["county"],
                "school_count": row["school_count"],
                "area_sq_mi": row["area_sq_mi"],
                "population": row["population"],
                "density_per_100sqmi": round(row["density"], 2) if row["density"] else None,
            }
            for row in rows
        ]


def get_state_summary(state: str) -> dict:
    """
    Get summary density statistics for a state.

    Args:
        state: State code

    Returns:
        Dict with state-level density stats
    """
    with get_db() as conn:
        stats = conn.execute("""
            SELECT
                COUNT(DISTINCT s.county) as county_count,
                COUNT(*) as total_programs,
                SUM(c.area_sq_mi) as total_area,
                SUM(c.population) as total_population
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            LEFT JOIN counties c ON s.state = c.state AND LOWER(s.county) = LOWER(c.name)
            WHERE s.state = ?
        """, (state,)).fetchone()

        result = {
            "state": state,
            "county_count": stats["county_count"],
            "total_programs": stats["total_programs"],
        }

        if stats["total_area"]:
            result["total_area_sq_mi"] = stats["total_area"]
            result["avg_density_per_100sqmi"] = round(
                (stats["total_programs"] / stats["total_area"]) * 100, 2
            )

        if stats["total_population"]:
            result["total_population"] = stats["total_population"]
            result["programs_per_capita_10k"] = round(
                (stats["total_programs"] / stats["total_population"]) * 10000, 4
            )

        return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Print density stats for each state
    for state in ["TX", "CA", "FL", "OH"]:
        summary = get_state_summary(state)
        print(f"\n{state}:")
        for key, value in summary.items():
            print(f"  {key}: {value}")

        print(f"\n  Top 5 densest counties:")
        for county in get_highest_density_counties(state, limit=5):
            print(f"    {county['county']}: {county['density_per_100sqmi']} per 100 sq mi")
