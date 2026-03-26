#!/usr/bin/env python3
"""Main static site generator orchestrator."""

import json
import re
import shutil
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from builder.guides import generate_guide_pages, generate_guides_index, get_featured_guides
from builder.rankings import generate_all_rankings
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


def _clean_text(value):
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()


def _coerce_dict(value):
    return value if isinstance(value, dict) else {}


def _coerce_list(value):
    return value if isinstance(value, list) else []


def _extract_scraped_program(payload):
    if not isinstance(payload, dict):
        return None

    extracted = _coerce_dict(payload.get("extracted_items"))
    football_program = _coerce_dict(extracted.get("football_program"))
    football_staff = _coerce_dict(extracted.get("football_staff"))
    varsity_roster = _coerce_dict(extracted.get("varsity_roster"))
    directory_contacts = _coerce_list(extracted.get("football_directory_contact"))
    school_page = _coerce_dict(extracted.get("school_page"))
    maxpreps = _coerce_dict(extracted.get("maxpreps"))
    maxpreps_home = _coerce_dict(maxpreps.get("home"))
    maxpreps_schedule = _coerce_dict(maxpreps.get("schedule"))
    maxpreps_roster = _coerce_dict(maxpreps.get("roster"))
    maxpreps_staff = _coerce_dict(maxpreps.get("staff"))
    booster_site = _coerce_dict(extracted.get("booster_site"))

    coach_roles = []
    for item in _coerce_list(football_staff.get("coach_roles")):
        if not isinstance(item, dict):
            continue
        name = _clean_text(item.get("name"))
        role = _clean_text(item.get("role"))
        if not name:
            continue
        coach_roles.append({"name": name, "role": role})

    if not coach_roles:
        for item in _coerce_list(maxpreps_staff.get("staff")):
            if not isinstance(item, dict):
                continue
            name = _clean_text(item.get("name"))
            role = _clean_text(item.get("position"))
            if not name:
                continue
            coach_roles.append({"name": name, "role": role})

    contact_phone = _clean_text(football_program.get("contact_phone"))
    contact_address = _clean_text(football_program.get("contact_address"))
    if not contact_phone or not contact_address:
        for item in directory_contacts:
            if not isinstance(item, dict):
                continue
            contact_phone = contact_phone or _clean_text(item.get("phone"))
            contact_address = contact_address or _clean_text(item.get("address"))
            if contact_phone and contact_address:
                break

    if not contact_phone:
        for line in _coerce_list(school_page.get("relevant_lines")):
            text = _clean_text(line)
            if "athletic director" in text.lower() and "@" in text:
                contact_phone = text
                break

    roster_count = varsity_roster.get("player_count")
    if not roster_count:
        roster_count = len(_coerce_list(varsity_roster.get("players")))
        roster_count = roster_count or len(_coerce_list(varsity_roster.get("players_sample")))
    if not roster_count:
        roster_count = maxpreps_roster.get("player_count") or 0

    schedule_count = len(_coerce_list(extracted.get("varsity_schedule")))
    if not schedule_count:
        schedule_count = maxpreps_schedule.get("game_count") or 0

    summary = {
        "last_scraped_at": _clean_text(_coerce_dict(payload.get("scrape_meta")).get("scraped_at")),
        "source_pages": [page for page in _coerce_list(payload.get("source_pages")) if isinstance(page, str) and page.strip()],
        "football_home_url": (
            _clean_text(football_program.get("football_home_url"))
            or _clean_text(maxpreps_home.get("url"))
            or _clean_text(school_page.get("url"))
            or _clean_text(booster_site.get("home_url"))
        ),
        "schedule_url": _clean_text(football_program.get("schedule_url")) or _clean_text(maxpreps_schedule.get("url")),
        "roster_url": _clean_text(football_program.get("roster_url")) or _clean_text(maxpreps_roster.get("url")),
        "staff_url": (
            _clean_text(football_program.get("staff_url"))
            or _clean_text(maxpreps_staff.get("url"))
            or _clean_text(booster_site.get("contact_url"))
        ),
        "contact_phone": contact_phone,
        "contact_address": contact_address or _clean_text(booster_site.get("contact_email")),
        "coach_roles": coach_roles[:6],
        "schedule_count": schedule_count,
        "roster_count": roster_count or 0,
    }

    if not any(
        [
            summary["football_home_url"],
            summary["schedule_url"],
            summary["roster_url"],
            summary["staff_url"],
            summary["contact_phone"],
            summary["contact_address"],
            summary["coach_roles"],
            summary["schedule_count"],
            summary["roster_count"],
            summary["source_pages"],
        ]
    ):
        return None

    return summary


def _load_latest_successful_scrape_payloads(conn) -> dict[str, dict]:
    rows = conn.execute(
        """
        SELECT r.nces_id, r.output_json
        FROM school_scrape_runs r
        JOIN (
            SELECT nces_id, MAX(ended_at) AS latest_ended_at
            FROM school_scrape_runs
            WHERE status = 'success' AND output_json IS NOT NULL AND TRIM(output_json) != ''
            GROUP BY nces_id
        ) latest
          ON latest.nces_id = r.nces_id
         AND latest.latest_ended_at = r.ended_at
        WHERE r.status = 'success'
        """
    ).fetchall()

    payloads: dict[str, dict] = {}
    for row in rows:
        raw = row["output_json"]
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads[row["nces_id"]] = payload
    return payloads


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
        latest_scrape_payloads = _load_latest_successful_scrape_payloads(conn)
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

            # Query school metrics
            metrics_row = conn.execute("""
                SELECT competitive_index, playoff_appearances_5yr,
                       enrollment_percentile, travel_burden_score
                FROM school_metrics
                WHERE school_id = ?
            """, (school["nces_id"],)).fetchone()

            metrics = None
            if metrics_row:
                metrics = {
                    "competitive_index": metrics_row["competitive_index"],
                    "playoff_appearances_5yr": metrics_row["playoff_appearances_5yr"],
                    "enrollment_percentile": metrics_row["enrollment_percentile"] * 100 if metrics_row["enrollment_percentile"] else None,
                    "travel_burden_score": metrics_row["travel_burden_score"],
                }

            # Query related schools for internal linking
            same_city = conn.execute("""
                SELECT nces_id, name, city, state
                FROM schools
                WHERE state = ? AND city = ? AND nces_id != ?
                ORDER BY name
                LIMIT 5
            """, (school["state"], school["city"], school["nces_id"])).fetchall()

            same_classification = []
            if school["classification"]:
                same_classification = conn.execute("""
                    SELECT s.nces_id, s.name, s.city, s.state
                    FROM schools s
                    JOIN athletic_programs ap ON s.nces_id = ap.school_id
                    WHERE s.state = ? AND ap.classification = ? AND s.nces_id != ?
                    ORDER BY RANDOM()
                    LIMIT 5
                """, (school["state"], school["classification"], school["nces_id"])).fetchall()

            related_schools = {
                "same_city": [
                    {"nces_id": r["nces_id"], "name": r["name"], "city": r["city"], "state": r["state"], "slug": slugify(r["name"])}
                    for r in same_city
                ],
                "same_classification": [
                    {"nces_id": r["nces_id"], "name": r["name"], "city": r["city"], "state": r["state"], "slug": slugify(r["name"])}
                    for r in same_classification
                ],
            }

            # Query nearby camps - same city first, then same state
            camps_same_city = conn.execute('''
                SELECT id, name, city, state, start_date
                FROM camps
                WHERE verified = 1 AND state = ? AND LOWER(city) = LOWER(?)
                LIMIT 3
            ''', (school['state'], school['city'])).fetchall()

            nearby_camps = []
            for c in camps_same_city:
                nearby_camps.append({
                    "id": c["id"],
                    "name": c["name"],
                    "city": c["city"],
                    "state": c["state"],
                    "slug": slugify(c["name"]),
                    "start_date": c["start_date"],
                })

            # Get more camps from same state if needed
            if len(nearby_camps) < 5:
                camps_same_state = conn.execute('''
                    SELECT id, name, city, state, start_date
                    FROM camps
                    WHERE verified = 1 AND state = ? AND LOWER(city) != LOWER(?)
                    LIMIT ?
                ''', (school['state'], school['city'], 5 - len(nearby_camps))).fetchall()

                for c in camps_same_state:
                    nearby_camps.append({
                        "id": c["id"],
                        "name": c["name"],
                        "city": c["city"],
                        "state": c["state"],
                        "slug": slugify(c["name"]),
                        "start_date": c["start_date"],
                    })

            html = template.render(
                school=school,
                program=program,
                metrics=metrics,
                county=county,
                related_schools=related_schools,
                nearby_camps=nearby_camps,
                scraped_program=_extract_scraped_program(
                    latest_scrape_payloads.get(school["nces_id"])
                ),
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

            # Get classifications with school counts
            classifications = conn.execute("""
                SELECT ap.classification, COUNT(*) as school_count
                FROM schools s
                JOIN athletic_programs ap ON s.nces_id = ap.school_id
                WHERE s.state = ? AND ap.classification IS NOT NULL
                GROUP BY ap.classification
                ORDER BY ap.classification
            """, (state,)).fetchall()

            classification_list = [
                {
                    "name": c["classification"],
                    "slug": slugify(c["classification"]),
                    "count": c["school_count"],
                }
                for c in classifications
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
                classifications=classification_list,
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
        total_schools = 0
        for abbr, name in STATE_NAMES.items():
            count = conn.execute(
                "SELECT COUNT(*) FROM schools WHERE state = ?", (abbr,)
            ).fetchone()[0]
            states.append({"abbr": abbr, "name": name, "school_count": count})
            total_schools += count

        # Get featured guides for homepage
        featured_guides = get_featured_guides(limit=4)
        guide_count = len(get_featured_guides(limit=100))  # Get total count

        template = env.get_template("index.html")
        html = template.render(
            states=states,
            featured_guides=featured_guides,
            total_schools=total_schools,
            guide_count=guide_count,
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

    print("Generating guide pages...")
    stats["guides"] = generate_guide_pages(env)
    print(f"  Generated {stats['guides']} guide pages")

    print("Generating guides index...")
    generate_guides_index(env)

    print("Generating rankings pages...")
    stats["rankings"] = generate_all_rankings(env)
    print(f"  Generated {stats['rankings']} rankings pages")

    print("\nBuild complete!")
    return stats


if __name__ == "__main__":
    build_site()
