# best.football

Youth/high school football information platform.

## Stack
- Python 3.11+ (ETL, static generation, API)
- SQLite (data storage)
- Jinja2 (templates)
- Tailwind CSS (styling via CDN)
- HTMX (dynamic interactions)
- Pagefind (static search)
- Cloudflare Pages (static hosting)
- FastAPI on ha1 (camp API)

## Commands
- `uv run python scripts/init_db.py` - Create database
- `uv run python scripts/fetch_all.py` - Run data pipelines
- `uv run python scripts/build_site.py` - Generate static site
- `uv run uvicorn api.main:app --port 8626` - Run camp API
- `./scripts/deploy.sh` - Deploy to Cloudflare Pages

## Local Development
- Static site: http://localhost:8625 (nginx with SSI)
- Camp API: http://localhost:8626 (uvicorn)

## Production
- Static: https://best.football (Cloudflare Pages)
- API: https://best.football/api/* → ha1:8626

## Data Sources
- NCES (schools): Yearly refresh (September)
- Census (demographics): Yearly refresh (December)
- State Athletics: Weekly during season

## Database
- SQLite at `data/best_football.db`
- Schema in `scripts/init_db.py`

## MVP Scope
Top 4 states: Texas, California, Florida, Ohio (~35K schools)
