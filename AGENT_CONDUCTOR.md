# Agent Conductor (Resume Protocol)

Use this file when asked to continue school scraper creation from the last completed point.

## Non-Negotiables
- One school at a time.
- Creator/repair work is done by fire-once worker agents, not by the main orchestrator.
- Proxy-only browsing (Oxylabs). Never run direct/local-IP browsing.
- Use the proxied `browse` CLI for reconnaissance and page inspection.
- In this repo, creator/repair launcher subprocesses already inject proxy env for browser work.
- The deliverable is either a deterministic Playwright football scraper or a terminal `no_football` outcome.
- Optimize for football-specific public data only, not generic athletics existence.
- Respect profile-specific blocklists:
  - `~/.web_scraper_blocklist_mobile.json`
  - `~/.web_scraper_blocklist_datacenter.json`.
- No heuristic-only decisions; worker must navigate pages like a human.

## Source of Truth
- Queue table `school_scraper_status` in `data/best_football.db`.
- Creator eligibility comes from the latest completed datacenter website survey run in `school_website_probe_runs` / `school_website_probe_results`.
- Resume point is always the next `pending` row in queue order that is also `success` in that latest datacenter survey run.

## Quick Resume
1. Check status:
   - `uv run python scripts/discover_schools.py --status`
2. Claim next eligible school:
   - `uv run python scripts/discover_schools.py --claim-next`
3. Resolve deterministic output path:
   - `scrapers/schools/{state_lower}/{nces_id}.py`
4. Spawn one worker agent for that school only using prompt template:
   - `templates/agent_prompts/school_creator.md`
5. Worker must return one-line JSON:
   - `{"status":"complete|no_football|blocked|restricted|failed","script_path":"...","reason":"...","notes":"..."}`
6. If `complete`, validate script output contract + non-empty extraction, then mark complete:
   - `uv run python scripts/discover_schools.py --complete <nces_id> --scraper-file <path>`
7. If `no_football`, do not create a scraper; mark terminal outcome with notes:
   - `uv run python scripts/discover_schools.py --no-football <nces_id> --reason "<reason>" --notes "<evidence>"`
8. If `restricted`, mark restricted with a proxy/provider reason.
9. If `blocked`, mark blocked with a target-side reason.
10. If `failed`, mark failed with reason.
11. Sleep 180 seconds.
12. Repeat.

## Codex Default Worker Path (No External Launcher Required)
- In Codex sessions, prefer `spawn_agent` with `agent_type=worker` for Step 4.
- Fill template fields directly from the claimed row and resolved script path.
- Keep worker ownership to one file only: `scrapers/schools/{state_lower}/{nces_id}.py`.
- Worker reconnaissance must use the hardwired Oxylabs-backed `browse` CLI, not direct browsing.
- Worker must use the `datacenter` proxy profile, not `mobile`.
- Worker output must be either a Playwright scraper script that can run in the normal proxied runtime or a `no_football` terminal decision with notes.
- Do not claim or work schools outside the latest completed datacenter survey `success` set.
- Proxy variance is normal. A site that loaded on one Oxylabs exit may block or behave differently on another, so treat access failures as evidence to evaluate, not as proof that your earlier reconnaissance was wrong.
- When the school site exposes football pages, prioritize fields that would improve a football page on best.football: football page URLs, team names, coach names, schedule links, contact info, practice/location details, and other concrete football signals.
- If the school site exposes sports but no football program, return `no_football`. Do not build a general sports scraper.
- Use `restricted` for Oxylabs/provider restrictions and `blocked` for target-side blocks such as Cloudflare or other site-side denials.
- Wait for worker completion; if wait times out, continue polling same agent id instead of spawning duplicates.
- Parse the worker's final one-line JSON and update queue status immediately.

## Helper Commands
- Run URL-targeted one-shot creation wrapper:
  - `uv run python scripts/create_scraper_from_url.py --url "<school_url>" --launcher-command "<launcher with {prompt_path}>" [--nces-id <id>]`
- Proxy profiles:
  - `mobile`: defaults to `https://pr.oxylabs.io:7777`
  - `datacenter`: uses `OXYLABS_DATACENTER_PROXY_SERVER`
- Creator work should use `datacenter`.
- Requeue due blocked rows:
  - `uv run python scripts/recheck_blocked.py`
- Force clear blocked rows (only when explicitly requested):
  - `uv run python scripts/discover_schools.py --clear-blocked`

## Completion Standard Per School
- If football content exists:
  - Deterministic script exists at `scrapers/schools/{state_lower}/{nces_id}.py`.
  - Script was derived from live proxied browsing, not offline heuristics.
  - Script focuses on football-relevant extraction, not a generic athletics summary.
  - Script returns required envelope keys:
    - `nces_id`, `school_name`, `state`, `source_pages`, `extracted_items`, `scrape_meta`, `errors`
  - Validation passes and `extracted_items` is non-empty.
- If football content does not exist:
  - Queue status is `no_football`.
  - No scraper is created.
  - Queue notes summarize the evidence.
