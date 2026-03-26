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
- Mobile uses the profile-specific gateway `https://pr.oxylabs.io:7777` by default.
- Blocklisted domains are loaded from profile-specific files:
  - `~/.web_scraper_blocklist_mobile.json`
  - `~/.web_scraper_blocklist_datacenter.json`
- Creator work should use the `datacenter` proxy profile.
- Creator queue seeding/claiming is restricted to the latest completed datacenter website survey `success` set.
- This workflow is football-only. If a school has sports but no football program, mark it `no_football` and do not create a scraper.
- Status meanings:
  - `restricted`: Oxylabs/provider restriction
  - `blocked`: target-side blocks such as Cloudflare or other site denials
  - `no_football`: school inspected, no public football program found, no scraper created

Environment:
- Mobile profile:
  - `OXYLABS_MOBILE_PROXY_SERVER` (optional, defaults to `https://pr.oxylabs.io:7777`)
  - `OXYLABS_MOBILE_USERNAME`
  - `OXYLABS_MOBILE_PASSWORD`
- Datacenter profile:
  - `OXYLABS_DATACENTER_PROXY_SERVER` (required if datacenter profile is selected)
  - `OXYLABS_DATACENTER_USERNAME`
  - `OXYLABS_DATACENTER_PASSWORD`
- Optional profile selector: `OXYLABS_PROXY_PROFILE=mobile|datacenter`.

Optional flags:
- `--proxy-profile mobile|datacenter` on queue scripts.

Queue lifecycle:

```bash
# Seed queue rows for schools that have websites
uv run python scripts/discover_schools.py --seed

# Run/update the datacenter website survey before creator work
uv run python scripts/probe_school_websites.py --workers 10 --wave-delay 1 --proxy-profile datacenter

# View queue status
uv run python scripts/discover_schools.py --status

# Mark a school as resolved with no football program
uv run python scripts/discover_schools.py --no-football <nces_id> \
  --reason "no_public_football_program_found" \
  --notes "<evidence summary>"

# Optional credentialed profile configuration
export OXYLABS_MOBILE_USERNAME=...
export OXYLABS_MOBILE_PASSWORD=...
export OXYLABS_MOBILE_PROXY_SERVER=https://pr.oxylabs.io:7777

# Run one creator loop (single-browser lock)
uv run python scripts/school_creator_loop.py \
  --creator-command "uv run python scripts/agent_session_adapter.py \
    --mode create \
    --launcher-command '<your-launcher-command using {prompt_path}>' \
    --nces-id {nces_id} --school-name {name} --state {state} \
    --website {website} --city {city} --script-path {script_path}" \
  --proxy-profile datacenter

# Run weekly school scrapes in parallel
uv run python scripts/run_school_scrapes.py --workers 8

# Run repair queue for scripts in needs_repair (single-browser lock)
uv run python scripts/run_repair_queue.py \
  --repair-command "uv run python scripts/agent_session_adapter.py \
    --mode repair \
    --launcher-command '<your-launcher-command using {prompt_path}>' \
    --nces-id {nces_id} --school-name {name} --state {state} \
    --website {website} --city {city} --script-path {script_path} \
    --failure-reason {failure_reason}" \
  --proxy-profile datacenter

# Run the local maintenance loop from this machine:
# scrape completed schools, drain repairs through Codex, rebuild, and upload to ha1
uv run python scripts/local_school_maintenance.py --proxy-profile datacenter --deploy-ha1

# Requeue blocked schools whose recheck date has arrived
uv run python scripts/recheck_blocked.py

# Force-clear blocked schools back to pending
uv run python scripts/discover_schools.py --clear-blocked
```

The creator and repair prompts are in `templates/agent_prompts/school_creator.md` and `templates/agent_prompts/school_repair.md`. Use them as the contract for worker behavior.
