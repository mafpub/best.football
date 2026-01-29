"""SQLite database helpers."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "best_football.db"


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize the database schema."""
    schema = """
    -- Core Entities
    CREATE TABLE IF NOT EXISTS schools (
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
        school_type TEXT,  -- 'public', 'private', 'charter'
        title_i BOOLEAN,
        urban_locale TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS athletic_programs (
        id INTEGER PRIMARY KEY,
        school_id TEXT REFERENCES schools(nces_id),
        sport TEXT DEFAULT 'football',
        classification TEXT,
        conference TEXT,
        division TEXT,
        state_association_id TEXT,
        UNIQUE(school_id, sport)
    );

    CREATE TABLE IF NOT EXISTS conferences (
        id TEXT PRIMARY KEY,
        state TEXT NOT NULL,
        name TEXT NOT NULL,
        classification TEXT,
        region TEXT
    );

    CREATE TABLE IF NOT EXISTS counties (
        fips TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        state TEXT NOT NULL,
        population INTEGER,
        median_income INTEGER,
        area_sq_mi REAL
    );

    -- Derived Metrics (recalculated from source data)
    CREATE TABLE IF NOT EXISTS school_metrics (
        school_id TEXT PRIMARY KEY REFERENCES schools(nces_id),
        competitive_index REAL,
        playoff_appearances_5yr INTEGER,
        enrollment_percentile REAL,
        travel_burden_score REAL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    -- User-Submitted Content
    CREATE TABLE IF NOT EXISTS camps (
        id TEXT PRIMARY KEY,  -- UUID
        name TEXT NOT NULL,
        organizer_type TEXT,  -- 'university', 'private', 'school', 'organization'
        school_id TEXT REFERENCES schools(nces_id),
        venue_name TEXT,
        address TEXT,
        city TEXT,
        state TEXT NOT NULL,
        zip TEXT,
        lat REAL,
        lng REAL,
        start_date TEXT,
        end_date TEXT,
        ages_min INTEGER,
        ages_max INTEGER,
        skill_levels TEXT,  -- JSON array
        focus_areas TEXT,   -- JSON array
        overnight BOOLEAN DEFAULT FALSE,
        cost_min REAL,
        cost_max REAL,
        registration_url TEXT,
        submitted_by TEXT,
        submitted_email TEXT,
        verified BOOLEAN DEFAULT FALSE,
        featured BOOLEAN DEFAULT FALSE,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    -- Guides (long-form content)
    CREATE TABLE IF NOT EXISTS guides (
        slug TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        content_md TEXT,
        category TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_schools_state ON schools(state);
    CREATE INDEX IF NOT EXISTS idx_schools_county ON schools(county, state);
    CREATE INDEX IF NOT EXISTS idx_athletic_programs_school ON athletic_programs(school_id);
    CREATE INDEX IF NOT EXISTS idx_camps_state_city ON camps(state, city);
    CREATE INDEX IF NOT EXISTS idx_camps_verified ON camps(verified);
    """

    with get_db() as conn:
        conn.executescript(schema)
        print(f"Database initialized at {DB_PATH}")
