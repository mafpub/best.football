"""Travel Burden calculation.

Travel burden measures the average distance a school must travel to compete
against schools in their conference/division.

Higher travel burden = more travel required for regular season games.

Uses Haversine formula for great-circle distance between school coordinates.
"""

import logging
import math
from typing import Optional

from pipeline.database import get_db

logger = logging.getLogger(__name__)

# Earth's radius in miles
EARTH_RADIUS_MI = 3958.8


def haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Calculate great-circle distance between two points using Haversine formula.

    Args:
        lat1, lng1: First point coordinates in degrees
        lat2, lng2: Second point coordinates in degrees

    Returns:
        Distance in miles
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)

    # Haversine formula
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    return EARTH_RADIUS_MI * c


def calculate_travel_burden_for_school(
    conn,
    school_id: str,
    lat: float,
    lng: float,
    state: str,
    conference: Optional[str],
    classification: Optional[str],
) -> Optional[float]:
    """
    Calculate travel burden for a single school.

    Uses conference if available, otherwise falls back to classification
    within the same state.

    Args:
        conn: Database connection
        school_id: NCES ID of the school
        lat, lng: School coordinates
        state: State code
        conference: Conference name (may be None)
        classification: Classification (e.g., "6A")

    Returns:
        Average distance to peer schools in miles, or None if insufficient data
    """
    # Try to find peer schools by conference first
    if conference:
        peers = conn.execute("""
            SELECT s.lat, s.lng
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            WHERE ap.conference = ?
              AND s.state = ?
              AND s.nces_id != ?
              AND s.lat IS NOT NULL
              AND s.lng IS NOT NULL
        """, (conference, state, school_id)).fetchall()
    else:
        peers = []

    # Fall back to classification if no conference peers
    if len(peers) < 3 and classification:
        peers = conn.execute("""
            SELECT s.lat, s.lng
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            WHERE ap.classification = ?
              AND s.state = ?
              AND s.nces_id != ?
              AND s.lat IS NOT NULL
              AND s.lng IS NOT NULL
        """, (classification, state, school_id)).fetchall()

    if not peers:
        return None

    # Calculate average distance to all peers
    total_distance = 0.0
    for peer in peers:
        distance = haversine_distance(lat, lng, peer["lat"], peer["lng"])
        total_distance += distance

    return total_distance / len(peers)


def calculate_all_travel_burdens() -> int:
    """
    Calculate travel burden for all schools with coordinates.

    Returns:
        Number of schools updated
    """
    with get_db() as conn:
        # Get all schools with athletic programs and coordinates
        schools = conn.execute("""
            SELECT
                s.nces_id,
                s.lat,
                s.lng,
                s.state,
                ap.conference,
                ap.classification
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            WHERE s.lat IS NOT NULL
              AND s.lng IS NOT NULL
        """).fetchall()

        logger.info("Calculating travel burden for %d schools", len(schools))

        updated_count = 0

        for school in schools:
            travel_burden = calculate_travel_burden_for_school(
                conn,
                school["nces_id"],
                school["lat"],
                school["lng"],
                school["state"],
                school["conference"],
                school["classification"],
            )

            if travel_burden is not None:
                # Upsert into school_metrics
                conn.execute("""
                    INSERT INTO school_metrics (school_id, travel_burden_score, updated_at)
                    VALUES (?, ?, datetime('now'))
                    ON CONFLICT(school_id) DO UPDATE SET
                        travel_burden_score = excluded.travel_burden_score,
                        updated_at = excluded.updated_at
                """, (school["nces_id"], round(travel_burden, 2)))
                updated_count += 1

        logger.info("Updated travel burden for %d schools", updated_count)
        return updated_count


def get_school_travel_burden(nces_id: str) -> Optional[float]:
    """
    Get the travel burden for a specific school.

    Args:
        nces_id: School's NCES ID

    Returns:
        Travel burden in miles or None if not calculated
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT travel_burden_score FROM school_metrics WHERE school_id = ?",
            (nces_id,)
        ).fetchone()

        return row["travel_burden_score"] if row else None


def get_schools_by_travel_burden(
    state: str,
    order: str = "ASC",
    limit: int = 50,
) -> list[dict]:
    """
    Get schools ranked by travel burden.

    Args:
        state: State code
        order: "ASC" for lowest burden first, "DESC" for highest
        limit: Maximum results

    Returns:
        List of school dicts with travel info
    """
    order = "ASC" if order.upper() == "ASC" else "DESC"

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT
                s.nces_id,
                s.name,
                s.city,
                s.county,
                ap.conference,
                ap.classification,
                sm.travel_burden_score
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            JOIN school_metrics sm ON s.nces_id = sm.school_id
            WHERE s.state = ?
              AND sm.travel_burden_score IS NOT NULL
            ORDER BY sm.travel_burden_score {order}
            LIMIT ?
        """, (state, limit)).fetchall()

        return [dict(row) for row in rows]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    calculate_all_travel_burdens()
