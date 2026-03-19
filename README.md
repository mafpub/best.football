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

## Deterministic School Scrapers

Per-school scraping uses deterministic scripts at `scrapers/schools/{state_lower}/{nces_id}.py`.

Current scraper workflow:
- Reconnaissance uses the Oxylabs-backed `browse` CLI.
- Final scraper scripts remain deterministic Playwright.
- Creator and repair launcher subprocesses inherit Oxylabs proxy env from `scripts/agent_session_adapter.py`.
- The default proxy pool is `https://us-pr.oxylabs.io:10001`, `10002`, and `10003`.
- Blocklisted domains in `~/.web_scraper_blocklist.json` are skipped.

Environment:
- IP-whitelist mode works without `OXYLABS_USERNAME` or `OXYLABS_PASSWORD`.
- Credential auth is opt-in via `OXYLABS_PROXY_AUTH_MODE=credentials`.
- If credentials are required for your Oxylabs setup, export them before running creator, repair, or scraper execution.
- Override the proxy pool only if needed via `OXYLABS_PROXY_SERVER` or `OXYLABS_PROXY_SERVERS`.

Queue lifecycle:

```bash
# Seed queue rows for schools that have websites
uv run python scripts/discover_schools.py --seed

# View queue status
uv run python scripts/discover_schools.py --status

# Optional when not using IP whitelist
export OXYLABS_USERNAME=...
export OXYLABS_PASSWORD=...

# Run one creator loop (single-browser lock)
uv run python scripts/school_creator_loop.py \
  --creator-command "uv run python scripts/agent_session_adapter.py \
    --mode create \
    --launcher-command '<your-launcher-command using {prompt_path}>' \
    --nces-id {nces_id} --school-name {name} --state {state} \
    --website {website} --city {city} --script-path {script_path}"

# Run weekly school scrapes in parallel
uv run python scripts/run_school_scrapes.py --workers 8

# Run repair queue for scripts in needs_repair (single-browser lock)
uv run python scripts/run_repair_queue.py \
  --repair-command "uv run python scripts/agent_session_adapter.py \
    --mode repair \
    --launcher-command '<your-launcher-command using {prompt_path}>' \
    --nces-id {nces_id} --school-name {name} --state {state} \
    --website {website} --city {city} --script-path {script_path} \
    --failure-reason {failure_reason}"

# Requeue blocked schools whose recheck date has arrived
uv run python scripts/recheck_blocked.py

# Force-clear blocked schools back to pending
uv run python scripts/discover_schools.py --clear-blocked
```

The creator and repair prompts are in `templates/agent_prompts/school_creator.md` and `templates/agent_prompts/school_repair.md`. Use them as the contract for worker behavior.
