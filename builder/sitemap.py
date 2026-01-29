#!/usr/bin/env python3
"""Sitemap generator for best.football.

Generates sitemap.xml or sitemap index with per-state sitemaps if URL count exceeds 50,000.
"""

import os
from datetime import datetime
from pathlib import Path
from typing import NamedTuple
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent

PROJECT_ROOT = Path(__file__).parent.parent
HTDOCS_DIR = PROJECT_ROOT / "htdocs"

BASE_URL = "https://best.football"
MAX_URLS_PER_SITEMAP = 50000


class SitemapEntry(NamedTuple):
    """A single sitemap URL entry."""
    loc: str
    lastmod: str
    priority: float
    changefreq: str = "weekly"


def get_lastmod(file_path: Path) -> str:
    """Get lastmod date from file modification time or current date."""
    if file_path.exists():
        mtime = os.path.getmtime(file_path)
        return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")
    return datetime.now().strftime("%Y-%m-%d")


def collect_urls() -> dict[str, list[SitemapEntry]]:
    """Collect all URLs grouped by category for sitemap generation.

    Returns dict with keys: 'main' (homepage, camps, guides) and state codes (tx, ca, etc.)
    """
    urls: dict[str, list[SitemapEntry]] = {"main": []}

    # Homepage
    homepage_path = HTDOCS_DIR / "index.html"
    urls["main"].append(SitemapEntry(
        loc=f"{BASE_URL}/",
        lastmod=get_lastmod(homepage_path),
        priority=1.0,
        changefreq="daily"
    ))

    # Camps index
    camps_dir = HTDOCS_DIR / "camps"
    camps_index = camps_dir / "index.html"
    if camps_index.exists() or camps_dir.exists():
        urls["main"].append(SitemapEntry(
            loc=f"{BASE_URL}/camps/",
            lastmod=get_lastmod(camps_index) if camps_index.exists() else get_lastmod(HTDOCS_DIR),
            priority=0.8,
            changefreq="weekly"
        ))

    # Guides index
    guides_dir = HTDOCS_DIR / "guides"
    guides_index = guides_dir / "index.html"
    if guides_index.exists() or guides_dir.exists():
        urls["main"].append(SitemapEntry(
            loc=f"{BASE_URL}/guides/",
            lastmod=get_lastmod(guides_index) if guides_index.exists() else get_lastmod(HTDOCS_DIR),
            priority=0.8,
            changefreq="weekly"
        ))

    # State index pages and school pages
    schools_dir = HTDOCS_DIR / "schools"
    if schools_dir.exists():
        for state_dir in sorted(schools_dir.iterdir()):
            if state_dir.is_dir():
                state = state_dir.name.lower()
                if state not in urls:
                    urls[state] = []

                # State index page (priority 0.8)
                state_index = state_dir / "index.html"
                if state_index.exists():
                    urls[state].append(SitemapEntry(
                        loc=f"{BASE_URL}/schools/{state}/",
                        lastmod=get_lastmod(state_index),
                        priority=0.8,
                        changefreq="weekly"
                    ))

                # Individual school pages (priority 0.6)
                for school_file in sorted(state_dir.glob("*.html")):
                    if school_file.name != "index.html":
                        slug = school_file.stem
                        urls[state].append(SitemapEntry(
                            loc=f"{BASE_URL}/schools/{state}/{slug}.html",
                            lastmod=get_lastmod(school_file),
                            priority=0.6,
                            changefreq="monthly"
                        ))

    # County/region pages
    regions_dir = HTDOCS_DIR / "regions"
    if regions_dir.exists():
        for state_dir in sorted(regions_dir.iterdir()):
            if state_dir.is_dir():
                state = state_dir.name.lower()
                if state not in urls:
                    urls[state] = []

                # Individual county pages (priority 0.6)
                for county_file in sorted(state_dir.glob("*.html")):
                    slug = county_file.stem
                    urls[state].append(SitemapEntry(
                        loc=f"{BASE_URL}/regions/{state}/{slug}.html",
                        lastmod=get_lastmod(county_file),
                        priority=0.6,
                        changefreq="monthly"
                    ))

    return urls


def create_sitemap_xml(entries: list[SitemapEntry]) -> Element:
    """Create a sitemap XML element from URL entries."""
    urlset = Element("urlset")
    urlset.set("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9")

    for entry in entries:
        url = SubElement(urlset, "url")
        SubElement(url, "loc").text = entry.loc
        SubElement(url, "lastmod").text = entry.lastmod
        SubElement(url, "changefreq").text = entry.changefreq
        SubElement(url, "priority").text = str(entry.priority)

    return urlset


def create_sitemap_index_xml(sitemaps: list[tuple[str, str]]) -> Element:
    """Create a sitemap index XML element.

    Args:
        sitemaps: List of (sitemap_loc, lastmod) tuples
    """
    sitemapindex = Element("sitemapindex")
    sitemapindex.set("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9")

    for loc, lastmod in sitemaps:
        sitemap = SubElement(sitemapindex, "sitemap")
        SubElement(sitemap, "loc").text = loc
        SubElement(sitemap, "lastmod").text = lastmod

    return sitemapindex


def write_xml(element: Element, output_path: Path) -> None:
    """Write XML element to file with proper formatting."""
    indent(element, space="  ")
    tree = ElementTree(element)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        tree.write(f, encoding="unicode", xml_declaration=False)


def generate_sitemap() -> dict:
    """Generate sitemap(s) for the site.

    Returns stats about generated sitemaps.
    """
    print("Generating sitemap...")

    urls = collect_urls()

    # Count total URLs
    total_urls = sum(len(entries) for entries in urls.values())
    print(f"  Found {total_urls} URLs")

    stats = {"total_urls": total_urls, "sitemaps": []}

    if total_urls <= MAX_URLS_PER_SITEMAP:
        # Single sitemap
        all_entries = []
        for entries in urls.values():
            all_entries.extend(entries)

        # Sort by priority (descending) then by loc
        all_entries.sort(key=lambda e: (-e.priority, e.loc))

        sitemap_xml = create_sitemap_xml(all_entries)
        output_path = HTDOCS_DIR / "sitemap.xml"
        write_xml(sitemap_xml, output_path)

        stats["sitemaps"].append(str(output_path))
        print(f"  Generated {output_path} with {len(all_entries)} URLs")
    else:
        # Multiple sitemaps with index
        sitemap_refs = []
        today = datetime.now().strftime("%Y-%m-%d")

        # Main sitemap (homepage, camps, guides)
        if urls.get("main"):
            main_entries = urls["main"]
            sitemap_xml = create_sitemap_xml(main_entries)
            output_path = HTDOCS_DIR / "sitemap-main.xml"
            write_xml(sitemap_xml, output_path)
            sitemap_refs.append((f"{BASE_URL}/sitemap-main.xml", today))
            stats["sitemaps"].append(str(output_path))
            print(f"  Generated {output_path} with {len(main_entries)} URLs")

        # Per-state sitemaps
        for state in sorted(urls.keys()):
            if state == "main":
                continue

            entries = urls[state]
            if not entries:
                continue

            # Sort entries by priority then loc
            entries.sort(key=lambda e: (-e.priority, e.loc))

            sitemap_xml = create_sitemap_xml(entries)
            output_path = HTDOCS_DIR / f"sitemap-{state}.xml"
            write_xml(sitemap_xml, output_path)
            sitemap_refs.append((f"{BASE_URL}/sitemap-{state}.xml", today))
            stats["sitemaps"].append(str(output_path))
            print(f"  Generated {output_path} with {len(entries)} URLs")

        # Sitemap index
        index_xml = create_sitemap_index_xml(sitemap_refs)
        index_path = HTDOCS_DIR / "sitemap.xml"
        write_xml(index_xml, index_path)
        stats["sitemap_index"] = str(index_path)
        print(f"  Generated sitemap index at {index_path}")

    return stats


if __name__ == "__main__":
    stats = generate_sitemap()
    print(f"\nSitemap generation complete: {stats['total_urls']} URLs")
