#!/usr/bin/env python3
"""Build the static site."""

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from builder.generate import build_site
from builder.sitemap import generate_sitemap

PROJECT_ROOT = Path(__file__).parent.parent
HTDOCS_DIR = PROJECT_ROOT / "htdocs"


def run_pagefind():
    """Run Pagefind to index the site for static search."""
    print("\nRunning Pagefind indexer...")
    result = subprocess.run(
        ["npx", "pagefind", "--site", str(HTDOCS_DIR), "--output-path", str(HTDOCS_DIR / "search")],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Pagefind error: {result.stderr}")
        sys.exit(1)
    print(result.stdout)


if __name__ == "__main__":
    stats = build_site()
    print("\nGenerated pages:")
    for key, count in stats.items():
        print(f"  {key}: {count}")

    # Generate sitemap
    sitemap_stats = generate_sitemap()
    print(f"\nSitemap: {sitemap_stats['total_urls']} URLs indexed")

    run_pagefind()
