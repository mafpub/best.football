# best.football

Youth and high school football information platform. Combines public datasets (NCES, Census, State Athletics) to create programmatic pages at geographic and school-entity scale.

## Quick Start

```bash
# Install dependencies
uv sync

# Initialize database
uv run python scripts/init_db.py

# Fetch data (NCES schools + Census demographics)
uv run python scripts/fetch_all.py

# Build static site
uv run python scripts/build_site.py

# Run local dev server (requires nginx configured)
# Static: http://localhost:8625

# Run API server
uv run uvicorn api.main:app --port 8626
```

## Architecture

- **Data Pipeline**: Python scripts fetch from NCES, Census, and state athletic associations
- **Database**: SQLite stores all entities and derived metrics
- **Static Generation**: Jinja2 templates generate 35K+ HTML pages
- **Search**: Pagefind provides client-side search
- **API**: FastAPI handles camp submissions and dynamic search

## MVP Scope

Top 4 states by football participation:
- Texas (~9,000 schools)
- California (~8,000 schools)
- Florida (~5,000 schools)
- Ohio (~3,000 schools)

## Data Sources

| Source | Refresh | Notes |
|--------|---------|-------|
| NCES CCD | Yearly (Sept) | School enrollment, location, type |
| Census ACS | Yearly (Dec) | County demographics |
| State Athletics | Weekly (season) | Classifications, conferences |

## Deployment

Static files deploy to Cloudflare Pages. Camp API runs on ha1.

```bash
./scripts/deploy.sh
```
