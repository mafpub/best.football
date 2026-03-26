import json
from pathlib import Path

import builder.generate as generate
import pipeline.database as db


def test_generate_school_page_includes_latest_successful_scrape_data(tmp_path):
    db.DB_PATH = tmp_path / "test.db"
    generate.HTDOCS_DIR = tmp_path / "htdocs"
    generate.HTDOCS_DIR.mkdir(parents=True, exist_ok=True)

    with db.get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE schools (
                nces_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT,
                city TEXT,
                county TEXT,
                state TEXT NOT NULL,
                zip TEXT,
                lat REAL,
                lng REAL,
                enrollment INTEGER,
                grades TEXT,
                school_type TEXT,
                title_i BOOLEAN,
                urban_locale TEXT,
                website TEXT
            );

            CREATE TABLE athletic_programs (
                id INTEGER PRIMARY KEY,
                school_id TEXT,
                sport TEXT DEFAULT 'football',
                classification TEXT,
                conference TEXT,
                division TEXT,
                state_association_id TEXT
            );

            CREATE TABLE counties (
                fips TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                population INTEGER,
                median_income INTEGER,
                area_sq_mi REAL
            );

            CREATE TABLE school_metrics (
                school_id TEXT PRIMARY KEY,
                competitive_index REAL,
                playoff_appearances_5yr INTEGER,
                enrollment_percentile REAL,
                travel_burden_score REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE camps (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT,
                state TEXT NOT NULL,
                start_date TEXT,
                verified BOOLEAN DEFAULT FALSE
            );

            CREATE TABLE school_scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nces_id TEXT NOT NULL,
                status TEXT NOT NULL,
                script_path TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                error_message TEXT,
                output_json TEXT
            );
            """
        )
        conn.execute(
            """
            INSERT INTO schools (
                nces_id, name, address, city, county, state, zip, enrollment,
                grades, school_type, urban_locale, website
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "1",
                "Alpha High",
                "1 Main St",
                "Alpha",
                "Example",
                "CA",
                "99999",
                1200,
                "9-12",
                "public",
                "Suburb",
                "https://alpha.example",
            ),
        )
        conn.execute(
            """
            INSERT INTO athletic_programs (
                school_id, sport, classification, conference, division
            ) VALUES (?, 'football', ?, ?, ?)
            """,
            ("1", "Division I", "Alpha League", "West"),
        )
        payload = {
            "nces_id": "1",
            "school_name": "Alpha High",
            "state": "CA",
            "source_pages": [
                "https://alpha.example",
                "https://alpha.example/football",
            ],
            "extracted_items": {
                "football_program": {
                    "football_home_url": "https://alpha.example/football",
                    "schedule_url": "https://alpha.example/football/schedule",
                    "roster_url": "https://alpha.example/football/roster",
                    "staff_url": "https://alpha.example/football/staff",
                    "contact_phone": "(555) 111-2222",
                    "contact_address": "1 Main St Alpha, CA",
                },
                "football_staff": {
                    "coach_roles": [
                        {"name": "Jane Coach", "role": "Head Coach"},
                        {"name": "Alex Coach", "role": "Assistant Coach"},
                    ]
                },
                "varsity_schedule": [
                    {"date": "2026-09-01", "opponent": "Beta High"},
                    {"date": "2026-09-08", "opponent": "Gamma High"},
                ],
                "varsity_roster": {
                    "player_count": 42,
                },
            },
            "scrape_meta": {
                "scraped_at": "2026-03-25T18:00:00+00:00",
            },
            "errors": [],
        }
        conn.execute(
            """
            INSERT INTO school_scrape_runs (
                nces_id, status, script_path, started_at, ended_at, output_json
            ) VALUES (?, 'success', ?, ?, ?, ?)
            """,
            (
                "1",
                "scrapers/schools/ca/1.py",
                "2026-03-25T17:59:00+00:00",
                "2026-03-25T18:00:00+00:00",
                json.dumps(payload),
            ),
        )

    env = generate.get_jinja_env()
    count = generate.generate_school_pages(env)

    assert count == 1

    output_path = tmp_path / "htdocs" / "schools" / "ca" / "alpha-high.html"
    html = output_path.read_text(encoding="utf-8")

    assert "Latest Football Data" in html
    assert "Jane Coach" in html
    assert "https://alpha.example/football/schedule" in html
    assert "42" in html


def test_generate_school_page_includes_maxpreps_style_scrape_data(tmp_path):
    db.DB_PATH = tmp_path / "test.db"
    generate.HTDOCS_DIR = tmp_path / "htdocs"
    generate.HTDOCS_DIR.mkdir(parents=True, exist_ok=True)

    with db.get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE schools (
                nces_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT,
                city TEXT,
                county TEXT,
                state TEXT NOT NULL,
                zip TEXT,
                lat REAL,
                lng REAL,
                enrollment INTEGER,
                grades TEXT,
                school_type TEXT,
                title_i BOOLEAN,
                urban_locale TEXT,
                website TEXT
            );

            CREATE TABLE athletic_programs (
                id INTEGER PRIMARY KEY,
                school_id TEXT,
                sport TEXT DEFAULT 'football',
                classification TEXT,
                conference TEXT,
                division TEXT,
                state_association_id TEXT
            );

            CREATE TABLE counties (
                fips TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                state TEXT NOT NULL,
                population INTEGER,
                median_income INTEGER,
                area_sq_mi REAL
            );

            CREATE TABLE school_metrics (
                school_id TEXT PRIMARY KEY,
                competitive_index REAL,
                playoff_appearances_5yr INTEGER,
                enrollment_percentile REAL,
                travel_burden_score REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE camps (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                city TEXT,
                state TEXT NOT NULL,
                start_date TEXT,
                verified BOOLEAN DEFAULT FALSE
            );

            CREATE TABLE school_scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nces_id TEXT NOT NULL,
                status TEXT NOT NULL,
                script_path TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                error_message TEXT,
                output_json TEXT
            );
            """
        )
        conn.execute(
            """
            INSERT INTO schools (
                nces_id, name, address, city, county, state, zip, enrollment,
                grades, school_type, urban_locale, website
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "2",
                "Lincoln High",
                "123 Zebra Way",
                "Lincoln",
                "Placer",
                "CA",
                "95648",
                1800,
                "9-12",
                "public",
                "Suburb",
                "https://lhs.wpusd.org/",
            ),
        )
        payload = {
            "nces_id": "2",
            "school_name": "Lincoln High",
            "state": "CA",
            "source_pages": [
                "https://lhs.wpusd.org/athletics/zebra-athletics/fall-sports/football",
                "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/",
                "https://fightingzebrasfootball.com/home",
            ],
            "extracted_items": {
                "school_page": {
                    "url": "https://lhs.wpusd.org/athletics/zebra-athletics/fall-sports/football",
                    "title": "Football - Lincoln High School",
                    "relevant_lines": [
                        "Mike Hankins / mhankins@wpusd.org / Athletic Director",
                    ],
                },
                "maxpreps": {
                    "home": {
                        "url": "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/",
                    },
                    "schedule": {
                        "url": "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/schedule/",
                        "game_count": 12,
                    },
                    "roster": {
                        "url": "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/roster/",
                        "player_count": 48,
                    },
                    "staff": {
                        "url": "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/staff/",
                        "staff": [
                            {"name": "Chris Bean", "position": "Head Coach"},
                        ],
                    },
                },
                "booster_site": {
                    "home_url": "https://fightingzebrasfootball.com/home",
                    "contact_url": "https://fightingzebrasfootball.com/contact",
                    "contact_email": "president@fightingzebrasfootball.com",
                },
            },
            "scrape_meta": {
                "scraped_at": "2026-03-25T19:45:00+00:00",
            },
            "errors": [],
        }
        conn.execute(
            """
            INSERT INTO school_scrape_runs (
                nces_id, status, script_path, started_at, ended_at, output_json
            ) VALUES (?, 'success', ?, ?, ?, ?)
            """,
            (
                "2",
                "scrapers/schools/ca/2.py",
                "2026-03-25T19:44:00+00:00",
                "2026-03-25T19:45:00+00:00",
                json.dumps(payload),
            ),
        )

    env = generate.get_jinja_env()
    count = generate.generate_school_pages(env)

    assert count == 1

    output_path = tmp_path / "htdocs" / "schools" / "ca" / "lincoln-high.html"
    html = output_path.read_text(encoding="utf-8")

    assert "Latest Football Data" in html
    assert "Football Program" not in html
    assert "legacy program table" not in html
    assert "No athletic program data available." not in html
    assert "https://www.maxpreps.com/ca/lincoln/lincoln-fighting-zebras/football/schedule/" in html
    assert "Chris Bean" in html
    assert "48" in html
