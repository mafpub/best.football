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
- `uv run python scripts/discover_schools.py --status` - View school scraper queue
- `uv run python scripts/school_creator_loop.py --creator-command "..."` - Run creator loop
- `uv run python scripts/run_repair_queue.py --repair-command "..."` - Run repair loop
- `uv run python scripts/run_school_scrapes.py --workers 8` - Execute generated school scrapers

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

## School Scrapers
- Per-school scripts live at `scrapers/schools/{state_lower}/{nces_id}.py`.
- Recon uses the Oxylabs-backed `browse` CLI.
- Runtime remains proxied Playwright.
- Creator/repair launcher subprocesses inherit proxy env from `scripts/agent_session_adapter.py`.
- Default Oxylabs mobile pool: `https://us-pr.oxylabs.io:10001`, `10002`, `10003`.
- IP-whitelist mode is supported; credentials are optional when the whitelist is active.
- Use `AGENT_CONDUCTOR.md` when resuming bulk scraper creation.

## MVP Scope
Top 4 states: Texas, California, Florida, Ohio (~35K schools)
