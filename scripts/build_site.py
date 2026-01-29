#!/usr/bin/env python3
"""Build the static site."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from builder.generate import build_site

if __name__ == "__main__":
    stats = build_site()
    print("\nGenerated pages:")
    for key, count in stats.items():
        print(f"  {key}: {count}")
