"""Competitive Index calculation.

The Competitive Index measures a school's relative size within its classification,
indicating whether they compete against larger or smaller schools in their division.

Formula: (enrollment - min_enrollment) / (max_enrollment - min_enrollment)

Where min/max are calculated within the same classification (e.g., all 6A-D1 schools).

A competitive index of:
- 1.0 = Largest school in the classification
- 0.5 = Middle-sized for the classification
- 0.0 = Smallest school in the classification

Schools with higher competitive indices have an enrollment advantage within their class.
"""

import logging
from typing import Optional

from pipeline.database import get_db

logger = logging.getLogger(__name__)


def calculate_enrollment_percentile(
    enrollment: int,
    min_enrollment: int,
    max_enrollment: int,
) -> float:
    """
    Calculate enrollment percentile within a classification.

    Args:
        enrollment: School's enrollment
        min_enrollment: Minimum enrollment in the classification
        max_enrollment: Maximum enrollment in the classification

    Returns:
        Percentile from 0.0 to 1.0
    """
    if max_enrollment == min_enrollment:
        return 0.5  # All schools same size

    return (enrollment - min_enrollment) / (max_enrollment - min_enrollment)


def calculate_all_competitive_indices() -> int:
    """
    Calculate competitive index for all schools with athletic programs.

    Groups schools by state + classification and calculates percentile
    within each group.

    Returns:
        Number of schools updated
    """
    with get_db() as conn:
        # Get all classifications with their enrollment ranges
        classifications = conn.execute("""
            SELECT
                s.state,
                ap.classification,
                MIN(s.enrollment) as min_enrollment,
                MAX(s.enrollment) as max_enrollment,
                COUNT(*) as school_count
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            WHERE s.enrollment IS NOT NULL
              AND s.enrollment > 0
              AND ap.classification IS NOT NULL
            GROUP BY s.state, ap.classification
            HAVING COUNT(*) >= 2
        """).fetchall()

        logger.info("Found %d state/classification groups", len(classifications))

        updated_count = 0

        for row in classifications:
            state = row["state"]
            classification = row["classification"]
            min_enroll = row["min_enrollment"]
            max_enroll = row["max_enrollment"]

            # Get all schools in this classification
            schools = conn.execute("""
                SELECT s.nces_id, s.enrollment
                FROM schools s
                JOIN athletic_programs ap ON s.nces_id = ap.school_id
                WHERE s.state = ?
                  AND ap.classification = ?
                  AND s.enrollment IS NOT NULL
                  AND s.enrollment > 0
            """, (state, classification)).fetchall()

            for school in schools:
                competitive_index = calculate_enrollment_percentile(
                    school["enrollment"],
                    min_enroll,
                    max_enroll,
                )

                # Also calculate overall enrollment percentile within state
                enrollment_percentile = conn.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM schools
                         WHERE state = ? AND enrollment < ? AND enrollment > 0) * 1.0 /
                        (SELECT COUNT(*) FROM schools
                         WHERE state = ? AND enrollment > 0) as percentile
                """, (state, school["enrollment"], state)).fetchone()

                # Upsert into school_metrics
                conn.execute("""
                    INSERT INTO school_metrics (school_id, competitive_index, enrollment_percentile, updated_at)
                    VALUES (?, ?, ?, datetime('now'))
                    ON CONFLICT(school_id) DO UPDATE SET
                        competitive_index = excluded.competitive_index,
                        enrollment_percentile = excluded.enrollment_percentile,
                        updated_at = excluded.updated_at
                """, (
                    school["nces_id"],
                    round(competitive_index, 4),
                    round(enrollment_percentile["percentile"], 4) if enrollment_percentile["percentile"] else None,
                ))
                updated_count += 1

        logger.info("Updated competitive index for %d schools", updated_count)
        return updated_count


def get_school_competitive_index(nces_id: str) -> Optional[float]:
    """
    Get the competitive index for a specific school.

    Args:
        nces_id: School's NCES ID

    Returns:
        Competitive index (0-1) or None if not calculated
    """
    with get_db() as conn:
        row = conn.execute(
            "SELECT competitive_index FROM school_metrics WHERE school_id = ?",
            (nces_id,)
        ).fetchone()

        return row["competitive_index"] if row else None


def get_rankings_by_classification(
    state: str,
    classification: str,
    limit: int = 100,
) -> list[dict]:
    """
    Get schools ranked by competitive index within a classification.

    Args:
        state: State code (e.g., "TX", "OH")
        classification: Classification (e.g., "6A", "Division I")
        limit: Maximum number of results

    Returns:
        List of school dicts with name, enrollment, competitive_index
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                s.nces_id,
                s.name,
                s.city,
                s.enrollment,
                sm.competitive_index,
                sm.enrollment_percentile
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            JOIN school_metrics sm ON s.nces_id = sm.school_id
            WHERE s.state = ?
              AND ap.classification = ?
            ORDER BY sm.competitive_index DESC
            LIMIT ?
        """, (state, classification, limit)).fetchall()

        return [dict(row) for row in rows]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    calculate_all_competitive_indices()
