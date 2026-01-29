#!/usr/bin/env python3
"""Main static site generator orchestrator."""

import re
import shutil
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from pipeline.database import get_db

PROJECT_ROOT = Path(__file__).parent.parent
TEMPLATES_DIR = PROJECT_ROOT / "templates"
HTDOCS_DIR = PROJECT_ROOT / "htdocs"

STATE_NAMES = {
    "TX": "Texas",
    "CA": "California",
    "FL": "Florida",
    "OH": "Ohio",
}


def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text


def get_jinja_env() -> Environment:
    """Get configured Jinja2 environment."""
    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=True,
    )
    env.filters["slugify"] = slugify
    return env


def generate_school_pages(env: Environment) -> int:
    """Generate individual school profile pages."""
    count = 0

    with get_db() as conn:
        schools = conn.execute("""
            SELECT s.*, ap.classification, ap.conference, ap.division,
                   c.name as county_name, c.population as county_pop,
                   c.median_income as county_income
            FROM schools s
            LEFT JOIN athletic_programs ap ON s.nces_id = ap.school_id
            LEFT JOIN counties c ON s.county = c.name AND s.state = c.state
            ORDER BY s.state, s.name
        """).fetchall()

        template = env.get_template("school.html")

        for row in schools:
            school = dict(row)
            school["slug"] = slugify(school["name"])

            state_dir = HTDOCS_DIR / "schools" / school["state"].lower()
            state_dir.mkdir(parents=True, exist_ok=True)

            program = None
            if school["classification"]:
                program = {
                    "classification": school["classification"],
                    "conference": school["conference"],
                    "division": school["division"],
                }

            county = None
            if school["county_name"]:
                county = {
                    "name": school["county_name"],
                    "population": school["county_pop"],
                    "median_income": school["county_income"],
                }

            html = template.render(
                school=school,
                program=program,
                metrics=None,  # TODO: Add metrics
                county=county,
                nearby_camps=[],  # TODO: Query nearby camps
            )

            output_path = state_dir / f"{school['slug']}.html"
            output_path.write_text(html)
            count += 1

            if count % 1000 == 0:
                print(f"  Generated {count} school pages...")

    return count


def generate_state_pages(env: Environment) -> int:
    """Generate state overview pages."""
    count = 0

    with get_db() as conn:
        template = env.get_template("state.html")

        for state, state_name in STATE_NAMES.items():
            # Get school stats
            stats = conn.execute("""
                SELECT
                    COUNT(*) as school_count,
                    SUM(CASE WHEN school_type = 'public' THEN 1 ELSE 0 END) as public_count,
                    SUM(CASE WHEN school_type = 'private' THEN 1 ELSE 0 END) as private_count,
                    AVG(enrollment) as avg_enrollment
                FROM schools WHERE state = ?
            """, (state,)).fetchone()

            # Get counties
            counties = conn.execute("""
                SELECT c.name, c.fips, COUNT(s.nces_id) as school_count
                FROM counties c
                LEFT JOIN schools s ON c.name = s.county AND c.state = s.state
                WHERE c.state = ?
                GROUP BY c.fips
                ORDER BY c.name
            """, (state,)).fetchall()

            county_list = [
                {"name": c["name"], "slug": slugify(c["name"]), "school_count": c["school_count"]}
                for c in counties
            ]

            # Get top schools by enrollment
            top_schools = conn.execute("""
                SELECT s.name, s.city, s.enrollment, ap.classification
                FROM schools s
                LEFT JOIN athletic_programs ap ON s.nces_id = ap.school_id
                WHERE s.state = ? AND s.enrollment IS NOT NULL
                ORDER BY s.enrollment DESC
                LIMIT 25
            """, (state,)).fetchall()

            top_list = [
                {
                    "name": s["name"],
                    "slug": slugify(s["name"]),
                    "city": s["city"],
                    "enrollment": s["enrollment"],
                    "classification": s["classification"],
                }
                for s in top_schools
            ]

            html = template.render(
                state=state,
                state_name=state_name,
                school_count=stats["school_count"],
                public_count=stats["public_count"],
                private_count=stats["private_count"],
                avg_enrollment=stats["avg_enrollment"],
                county_count=len(counties),
                counties=county_list,
                top_schools=top_list,
                classifications=[],  # TODO: Get from athletic_programs
                upcoming_camps=[],
            )

            state_dir = HTDOCS_DIR / "schools" / state.lower()
            state_dir.mkdir(parents=True, exist_ok=True)
            (state_dir / "index.html").write_text(html)
            count += 1

    return count


def generate_county_pages(env: Environment) -> int:
    """Generate county/region pages."""
    count = 0

    with get_db() as conn:
        template = env.get_template("county.html")

        counties = conn.execute("""
            SELECT * FROM counties ORDER BY state, name
        """).fetchall()

        for county_row in counties:
            county = dict(county_row)
            county["slug"] = slugify(county["name"])

            # Get schools in this county
            schools = conn.execute("""
                SELECT s.*, ap.classification
                FROM schools s
                LEFT JOIN athletic_programs ap ON s.nces_id = ap.school_id
                WHERE s.county = ? AND s.state = ?
                ORDER BY s.name
            """, (county["name"], county["state"])).fetchall()

            school_list = []
            total_enrollment = 0
            for s in schools:
                school = dict(s)
                school["slug"] = slugify(school["name"])
                school_list.append(school)
                if school["enrollment"]:
                    total_enrollment += school["enrollment"]

            avg_enrollment = total_enrollment / len(schools) if schools else None

            html = template.render(
                state=county["state"],
                state_name=STATE_NAMES.get(county["state"], county["state"]),
                county=county,
                schools=school_list,
                school_count=len(schools),
                avg_enrollment=avg_enrollment,
                camps=[],  # TODO: Query camps
            )

            region_dir = HTDOCS_DIR / "regions" / county["state"].lower()
            region_dir.mkdir(parents=True, exist_ok=True)
            (region_dir / f"{county['slug']}.html").write_text(html)
            count += 1

    return count


def generate_homepage(env: Environment) -> None:
    """Generate the homepage."""
    with get_db() as conn:
        # Get state stats
        states = []
        for abbr, name in STATE_NAMES.items():
            count = conn.execute(
                "SELECT COUNT(*) FROM schools WHERE state = ?", (abbr,)
            ).fetchone()[0]
            states.append({"abbr": abbr, "name": name, "school_count": count})

        template = env.get_template("index.html")
        html = template.render(
            states=states,
            featured_guides=[],  # TODO: Query guides
        )

        (HTDOCS_DIR / "index.html").write_text(html)


def generate_schools_index(env: Environment) -> None:
    """Generate the schools index page listing all states."""
    with get_db() as conn:
        states = []
        total_schools = 0
        for abbr, name in STATE_NAMES.items():
            school_count = conn.execute(
                "SELECT COUNT(*) FROM schools WHERE state = ?", (abbr,)
            ).fetchone()[0]
            county_count = conn.execute(
                "SELECT COUNT(*) FROM counties WHERE state = ?", (abbr,)
            ).fetchone()[0]
            states.append({
                "abbr": abbr,
                "name": name,
                "school_count": school_count,
                "county_count": county_count,
            })
            total_schools += school_count

        template = env.get_template("schools_index.html")
        html = template.render(
            states=states,
            total_schools=total_schools,
        )

        schools_dir = HTDOCS_DIR / "schools"
        schools_dir.mkdir(parents=True, exist_ok=True)
        (schools_dir / "index.html").write_text(html)


def generate_regions_index(env: Environment) -> None:
    """Generate the regions index page listing all states."""
    with get_db() as conn:
        states = []
        for abbr, name in STATE_NAMES.items():
            school_count = conn.execute(
                "SELECT COUNT(*) FROM schools WHERE state = ?", (abbr,)
            ).fetchone()[0]
            county_count = conn.execute(
                "SELECT COUNT(*) FROM counties WHERE state = ?", (abbr,)
            ).fetchone()[0]
            states.append({
                "abbr": abbr,
                "name": name,
                "school_count": school_count,
                "county_count": county_count,
            })

        template = env.get_template("regions_index.html")
        html = template.render(states=states)

        regions_dir = HTDOCS_DIR / "regions"
        regions_dir.mkdir(parents=True, exist_ok=True)
        (regions_dir / "index.html").write_text(html)


def generate_state_regions_indexes(env: Environment) -> int:
    """Generate state-level region index pages (e.g., /regions/tx/index.html)."""
    count = 0

    with get_db() as conn:
        template = env.get_template("state_regions.html")

        for state, state_name in STATE_NAMES.items():
            # Get school count
            school_count = conn.execute(
                "SELECT COUNT(*) FROM schools WHERE state = ?", (state,)
            ).fetchone()[0]

            # Get total population
            pop_result = conn.execute(
                "SELECT SUM(population) FROM counties WHERE state = ?", (state,)
            ).fetchone()[0]
            total_population = pop_result or 0

            # Get counties with school counts
            counties = conn.execute("""
                SELECT c.name, c.fips, COUNT(s.nces_id) as school_count
                FROM counties c
                LEFT JOIN schools s ON c.name = s.county AND c.state = s.state
                WHERE c.state = ?
                GROUP BY c.fips
                ORDER BY c.name
            """, (state,)).fetchall()

            county_list = [
                {"name": c["name"], "slug": slugify(c["name"]), "school_count": c["school_count"]}
                for c in counties
            ]

            html = template.render(
                state=state,
                state_name=state_name,
                county_count=len(counties),
                school_count=school_count,
                total_population=total_population,
                counties=county_list,
            )

            region_dir = HTDOCS_DIR / "regions" / state.lower()
            region_dir.mkdir(parents=True, exist_ok=True)
            (region_dir / "index.html").write_text(html)
            count += 1

    return count


def build_site() -> dict:
    """Build the entire static site."""
    print("Building best.football static site...")

    env = get_jinja_env()
    stats = {}

    # Ensure output directories exist
    for subdir in ["schools", "regions", "camps", "guides", "rankings"]:
        (HTDOCS_DIR / subdir).mkdir(parents=True, exist_ok=True)

    print("Generating homepage...")
    generate_homepage(env)

    print("Generating schools index...")
    generate_schools_index(env)

    print("Generating regions index...")
    generate_regions_index(env)

    print("Generating state region indexes...")
    stats["state_regions"] = generate_state_regions_indexes(env)
    print(f"  Generated {stats['state_regions']} state region index pages")

    print("Generating state pages...")
    stats["states"] = generate_state_pages(env)
    print(f"  Generated {stats['states']} state pages")

    print("Generating county pages...")
    stats["counties"] = generate_county_pages(env)
    print(f"  Generated {stats['counties']} county pages")

    print("Generating school pages...")
    stats["schools"] = generate_school_pages(env)
    print(f"  Generated {stats['schools']} school pages")

    print("\nBuild complete!")
    return stats


if __name__ == "__main__":
    build_site()
