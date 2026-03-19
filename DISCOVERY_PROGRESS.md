# School Scraper Progress

**Last Updated:** 2026-03-05  
**System:** Deterministic per-school Playwright scrapers (current implementation)

---

## Current Queue Snapshot

- **Total schools with websites:** 5,353
- **Pending:** 5,334
- **In Progress:** 0
- **Complete:** 14
- **Blocked:** 4
- **Failed:** 1
- **Needs Repair:** 0
- **Progress (complete only):** 0.3%

Notes:
- Queue is now the single source of truth in `school_scraper_status`.
- Repo-local env loading is active via `pipeline/env.py` and `.env`/`.env.local`.
- Proxy credentials are now sourced per-repo (not via global shell startup).

---

## Canonical Architecture (Current)

### 1) One Script Per School

- Path convention: `scrapers/schools/{state_lower}/{nces_id}.py`
- Each script is deterministic and individualized to one school site.
- No generic multi-school selector templates for runtime scraping.

### 2) Agent Role

Agents are ephemeral and scoped to one school only:

- **Creator session:** explores one school, writes one script, validates once.
- **Repair session:** triggered after consecutive runtime failures, compares live site vs script, patches one script, validates once.

### 3) Runtime Flow

- Production scraping runs completed school scripts in parallel worker pool.
- Failed scripts increment `consecutive_failures`.
- At 2 consecutive failures, status escalates to `needs_repair`.

### 4) Proxy and Safety Policy

- Proxy is mandatory in runtime paths.
- Profile-aware configuration is required:
  - `OXYLABS_MOBILE_*` for the mobile profile (default).
  - `OXYLABS_DATACENTER_*` for the datacenter profile.
- Repo scripts auto-load credentials from project `.env`/`.env.local`.
- Profile blocklists are enforced:
  - `~/.web_scraper_blocklist_mobile.json`
  - `~/.web_scraper_blocklist_datacenter.json`.
- No non-proxy execution path should be used.

---

## Required Payload Contract

Every school script must produce a top-level envelope:

- `nces_id`
- `school_name`
- `state`
- `source_pages`
- `extracted_items`
- `scrape_meta`
- `errors`

Validation gate before `complete`:

1. Envelope contract is valid.
2. Extraction is non-empty.

---

## Queue Lifecycle

Valid statuses:

- `pending`
- `in_progress`
- `complete`
- `blocked`
- `failed`
- `needs_repair`

Key fields tracked:

- `attempts`
- `consecutive_failures`
- `last_success_at`
- `last_failure_at`
- `failure_reason`
- `next_recheck_at`
- `scraper_file`

---

## Daily/Operational Commands

```bash
# Queue status
uv run python scripts/discover_schools.py --status

# Seed missing queue rows from schools with websites
uv run python scripts/discover_schools.py --seed

# Get next schools
uv run python scripts/discover_schools.py --next-batch --count 10

# Claim one school (moves to in_progress)
uv run python scripts/discover_schools.py --claim-next

# Mark complete (with script path)
uv run python scripts/discover_schools.py --complete <nces_id> \
  --scraper-file scrapers/schools/<state>/<nces_id>.py

# Mark blocked / failed / needs_repair manually
uv run python scripts/discover_schools.py --blocked <nces_id> --reason "<reason>"
uv run python scripts/discover_schools.py --failed <nces_id> --reason "<reason>"
uv run python scripts/discover_schools.py --needs-repair <nces_id> --reason "<reason>"

# Requeue blocked rows whose recheck date is due
uv run python scripts/recheck_blocked.py

# Force-clear blocked rows back to pending
uv run python scripts/discover_schools.py --clear-blocked
```

---

## Creator and Repair Loops

### Creator Loop (single browser lock)

Constructor agents should inspect each site with the hardwired Oxylabs-backed `browse` CLI, then turn that reconnaissance into a deterministic Playwright scraper script.

```bash
uv run python scripts/school_creator_loop.py \
  --creator-command "uv run python scripts/agent_session_adapter.py \
    --mode create \
    --launcher-command '<your-launcher-command using {prompt_path}>' \
    --nces-id {nces_id} --school-name {name} --state {state} \
    --website {website} --city {city} --script-path {script_path}"
```

### Repair Loop (single browser lock)

Repair agents should use the same proxied `browse` reconnaissance pass before patching the failing Playwright script.

```bash
uv run python scripts/run_repair_queue.py \
  --repair-command "uv run python scripts/agent_session_adapter.py \
    --mode repair \
    --launcher-command '<your-launcher-command using {prompt_path}>' \
    --nces-id {nces_id} --school-name {name} --state {state} \
    --website {website} --city {city} --script-path {script_path} \
    --failure-reason {failure_reason}"
```

### Production Runtime (parallel workers)

```bash
uv run python scripts/run_school_scrapes.py --workers 8
```

---

## Queue Reset / Cleanup Utilities

```bash
# Preview cleanup actions
uv run python scripts/normalize_queue_state.py --dry-run

# Reset stuck rows if needed
uv run python scripts/normalize_queue_state.py --reset-in-progress

# Clear all blocked rows to pending
uv run python scripts/normalize_queue_state.py --clear-blocked
```

---

## Immediate Objective

Continue deterministic script creation school-by-school from pending queue, validate each script live, and grow complete coverage while keeping repair backlog near zero.
