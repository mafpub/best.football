#!/usr/bin/env python3
"""Guide page generator - loads markdown files and renders to HTML."""

import re
from datetime import datetime
from pathlib import Path

import markdown
import yaml
from jinja2 import Environment

PROJECT_ROOT = Path(__file__).parent.parent
GUIDES_DIR = PROJECT_ROOT / "data" / "guides"
HTDOCS_DIR = PROJECT_ROOT / "htdocs"


def slugify(text: str) -> str:
    """Convert text to URL-safe slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "-", text)
    return text


def parse_frontmatter(content: str) -> tuple[dict, str]:
    """Parse YAML frontmatter from markdown content.

    Returns a tuple of (frontmatter_dict, body_content).
    """
    if not content.startswith("---"):
        return {}, content

    # Find the closing ---
    end_match = re.search(r"\n---\s*\n", content[3:])
    if not end_match:
        return {}, content

    frontmatter_end = end_match.start() + 3
    frontmatter_str = content[3:frontmatter_end]
    body = content[frontmatter_end + end_match.end() - end_match.start():]

    try:
        frontmatter = yaml.safe_load(frontmatter_str)
        if frontmatter is None:
            frontmatter = {}
    except yaml.YAMLError:
        frontmatter = {}

    return frontmatter, body.strip()


def load_guide(filepath: Path) -> dict | None:
    """Load a single guide markdown file.

    Returns a dict with guide metadata and content, or None if loading fails.
    """
    try:
        content = filepath.read_text(encoding="utf-8")
    except IOError:
        return None

    frontmatter, body = parse_frontmatter(content)

    # Generate slug from filename
    slug = filepath.stem

    # Convert markdown to HTML
    md = markdown.Markdown(extensions=["extra", "smarty", "toc"])
    content_html = md.convert(body)

    # Get file modification time for dates
    mtime = datetime.fromtimestamp(filepath.stat().st_mtime)

    guide = {
        "slug": slug,
        "title": frontmatter.get("title", slug.replace("-", " ").title()),
        "category": frontmatter.get("category", ""),
        "description": frontmatter.get("description", ""),
        "excerpt": frontmatter.get("description", ""),  # Alias for template compatibility
        "content_html": content_html,
        "created_at": frontmatter.get("created_at", mtime.strftime("%Y-%m-%d")),
        "updated_at": frontmatter.get("updated_at", mtime.strftime("%Y-%m-%d")),
        "toc": md.toc if hasattr(md, "toc") else "",
    }

    return guide


def load_all_guides() -> list[dict]:
    """Load all guide markdown files from the guides directory.

    Returns a list of guide dicts sorted by title.
    """
    guides = []

    if not GUIDES_DIR.exists():
        return guides

    for filepath in GUIDES_DIR.glob("*.md"):
        guide = load_guide(filepath)
        if guide:
            guides.append(guide)

    # Sort by title
    guides.sort(key=lambda g: g["title"])

    return guides


def get_related_guides(current_guide: dict, all_guides: list[dict], max_related: int = 3) -> list[dict]:
    """Get related guides based on category.

    Returns guides in the same category, excluding the current guide.
    """
    related = []
    current_category = current_guide.get("category", "")

    for guide in all_guides:
        if guide["slug"] == current_guide["slug"]:
            continue
        if current_category and guide.get("category") == current_category:
            related.append(guide)
        if len(related) >= max_related:
            break

    # If not enough related guides in same category, add others
    if len(related) < max_related:
        for guide in all_guides:
            if guide["slug"] == current_guide["slug"]:
                continue
            if guide not in related:
                related.append(guide)
            if len(related) >= max_related:
                break

    return related


def generate_guide_pages(env: Environment) -> int:
    """Generate individual guide pages.

    Args:
        env: Configured Jinja2 environment

    Returns:
        Number of guide pages generated
    """
    guides = load_all_guides()
    if not guides:
        return 0

    template = env.get_template("guide.html")
    guides_dir = HTDOCS_DIR / "guides"
    guides_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for guide in guides:
        related = get_related_guides(guide, guides)

        html = template.render(
            guide=guide,
            related_guides=related,
        )

        output_path = guides_dir / f"{guide['slug']}.html"
        output_path.write_text(html, encoding="utf-8")
        count += 1

    return count


def generate_guides_index(env: Environment) -> None:
    """Generate the guides index page listing all guides.

    Args:
        env: Configured Jinja2 environment
    """
    guides = load_all_guides()

    # Group guides by category
    categories = {}
    for guide in guides:
        category = guide.get("category", "General")
        if category not in categories:
            categories[category] = []
        categories[category].append(guide)

    # Sort categories alphabetically
    sorted_categories = dict(sorted(categories.items()))

    template = env.get_template("guides_index.html")
    html = template.render(
        guides=guides,
        categories=sorted_categories,
        guide_count=len(guides),
    )

    guides_dir = HTDOCS_DIR / "guides"
    guides_dir.mkdir(parents=True, exist_ok=True)
    (guides_dir / "index.html").write_text(html, encoding="utf-8")


def get_featured_guides(limit: int = 4) -> list[dict]:
    """Get featured guides for homepage display.

    Args:
        limit: Maximum number of guides to return

    Returns:
        List of guide dicts suitable for homepage cards
    """
    guides = load_all_guides()
    return guides[:limit]
