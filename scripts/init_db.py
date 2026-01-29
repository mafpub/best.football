#!/usr/bin/env python3
"""Initialize the best.football database."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.database import init_db

if __name__ == "__main__":
    init_db()
