#!/usr/bin/env python3
"""Migration script to add website column to schools table.

Usage:
    python scripts/migrate_add_website.py
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.database import get_db


def migrate():
    """Add website column to schools table if it doesn't exist."""
    with get_db() as conn:
        # Check if column exists
        cursor = conn.execute("PRAGMA table_info(schools)")
        columns = [row["name"] for row in cursor.fetchall()]

        if "website" not in columns:
            print("Adding website column to schools table...")
            conn.execute("ALTER TABLE schools ADD COLUMN website TEXT")
            conn.commit()
            print("✓ Added website column")
        else:
            print("✓ website column already exists")

    print("\nMigration complete!")
    print("Next: Run 'uv run python sources/nces.py' to refresh school data with websites")


if __name__ == "__main__":
    migrate()
