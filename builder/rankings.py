"""Rankings page generator.

Generates ranking pages for each state and classification combination.
Rankings are based on competitive index (enrollment percentile within classification).
"""

import re
from pathlib import Path
from typing import Optional

from jinja2 import Environment

from pipeline.database import get_db

PROJECT_ROOT = Path(__file__).parent.parent
HTDOCS_DIR = PROJECT_ROOT / "htdocs"

STATE_NAMES = {
    "TX": "Texas",
    "CA": "California",
    "FL": "Florida",
    "OH": "Ohio",
}

# State association names and typical classifications
STATE_ASSOCIATIONS = {
    "TX": {
        "name": "UIL",
        "full_name": "University Interscholastic League",
        "classifications": ["6A", "5A", "4A", "3A", "2A", "1A"],
    },
    "CA": {
        "name": "CIF",
        "full_name": "California Interscholastic Federation",
        "classifications": ["CIFSS"],  # We only have Southern Section data
    },
    "FL": {
        "name": "FHSAA",
        "full_name": "Florida High School Athletic Association",
        "classifications": ["7A", "6A", "5A", "4A", "3A", "2A", "1A"],
    },
    "OH": {
        "name": "OHSAA",
        "full_name": "Ohio High School Athletic Association",
        "classifications": ["I", "II", "III", "IV", "V", "VI", "VII"],
    },
}


def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text


def get_classifications_for_state(state: str) -> list[dict]:
    """
    Get all unique classifications for a state from the database.

    Returns:
        List of dicts with classification and school_count
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                ap.classification,
                COUNT(*) as school_count
            FROM athletic_programs ap
            JOIN schools s ON ap.school_id = s.nces_id
            WHERE s.state = ?
              AND ap.classification IS NOT NULL
            GROUP BY ap.classification
            ORDER BY ap.classification
        """, (state,)).fetchall()

        return [
            {
                "classification": row["classification"],
                "school_count": row["school_count"],
                "slug": slugify(row["classification"]),
            }
            for row in rows
        ]


def get_rankings_for_classification(
    state: str,
    classification: str,
    limit: int = 100,
) -> list[dict]:
    """
    Get schools ranked by competitive index within a classification.

    Args:
        state: State code
        classification: Classification (e.g., "6A", "Division I")
        limit: Maximum number of results

    Returns:
        List of school dicts with ranking info
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                s.nces_id,
                s.name,
                s.city,
                s.county,
                s.enrollment,
                ap.conference,
                ap.division,
                sm.competitive_index,
                sm.enrollment_percentile,
                sm.travel_burden_score
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            LEFT JOIN school_metrics sm ON s.nces_id = sm.school_id
            WHERE s.state = ?
              AND ap.classification = ?
            ORDER BY
                COALESCE(sm.competitive_index, 0) DESC,
                s.enrollment DESC
            LIMIT ?
        """, (state, classification, limit)).fetchall()

        return [
            {
                "rank": i + 1,
                "nces_id": row["nces_id"],
                "name": row["name"],
                "slug": slugify(row["name"]),
                "city": row["city"],
                "county": row["county"],
                "enrollment": row["enrollment"],
                "conference": row["conference"],
                "division": row["division"],
                "competitive_index": row["competitive_index"],
                "enrollment_percentile": row["enrollment_percentile"],
                "travel_burden": row["travel_burden_score"],
            }
            for i, row in enumerate(rows)
        ]


def generate_state_rankings_index(env: Environment, state: str) -> None:
    """Generate the state rankings index page (e.g., /rankings/tx/index.html)."""
    state_name = STATE_NAMES.get(state, state)
    association = STATE_ASSOCIATIONS.get(state, {})

    # Get all classifications for this state
    classifications = get_classifications_for_state(state)

    # Get total program count
    with get_db() as conn:
        total = conn.execute("""
            SELECT COUNT(*) FROM athletic_programs ap
            JOIN schools s ON ap.school_id = s.nces_id
            WHERE s.state = ?
        """, (state,)).fetchone()[0]

    template = env.get_template("rankings_state.html")
    html = template.render(
        state=state,
        state_name=state_name,
        association_name=association.get("name", ""),
        association_full_name=association.get("full_name", ""),
        classifications=classifications,
        total_programs=total,
    )

    output_dir = HTDOCS_DIR / "rankings" / state.lower()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(html)


def generate_classification_rankings(
    env: Environment,
    state: str,
    classification: str,
) -> None:
    """Generate a specific classification rankings page."""
    state_name = STATE_NAMES.get(state, state)
    association = STATE_ASSOCIATIONS.get(state, {})

    # Get ranked schools
    schools = get_rankings_for_classification(state, classification)

    # Get enrollment stats for this classification
    with get_db() as conn:
        stats = conn.execute("""
            SELECT
                MIN(s.enrollment) as min_enrollment,
                MAX(s.enrollment) as max_enrollment,
                AVG(s.enrollment) as avg_enrollment
            FROM schools s
            JOIN athletic_programs ap ON s.nces_id = ap.school_id
            WHERE s.state = ? AND ap.classification = ? AND s.enrollment > 0
        """, (state, classification)).fetchone()

    template = env.get_template("rankings_list.html")
    html = template.render(
        state=state,
        state_name=state_name,
        association_name=association.get("name", ""),
        classification=classification,
        schools=schools,
        school_count=len(schools),
        min_enrollment=stats["min_enrollment"],
        max_enrollment=stats["max_enrollment"],
        avg_enrollment=stats["avg_enrollment"],
    )

    output_dir = HTDOCS_DIR / "rankings" / state.lower()
    output_dir.mkdir(parents=True, exist_ok=True)

    slug = slugify(classification)
    (output_dir / f"{slug}.html").write_text(html)


def generate_all_rankings(env: Environment) -> int:
    """
    Generate all rankings pages.

    Returns:
        Total number of pages generated
    """
    count = 0

    for state in STATE_NAMES.keys():
        # Generate state index
        generate_state_rankings_index(env, state)
        count += 1

        # Get classifications and generate pages for each
        classifications = get_classifications_for_state(state)
        for cls in classifications:
            generate_classification_rankings(env, state, cls["classification"])
            count += 1

    return count


if __name__ == "__main__":
    from jinja2 import FileSystemLoader

    templates_dir = PROJECT_ROOT / "templates"
    env = Environment(
        loader=FileSystemLoader(templates_dir),
        autoescape=True,
    )
    env.filters["slugify"] = slugify

    count = generate_all_rankings(env)
    print(f"Generated {count} ranking pages")
