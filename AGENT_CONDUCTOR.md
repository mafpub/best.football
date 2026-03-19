# Agent Conductor (Resume Protocol)

Use this file when asked to continue school scraper creation from the last completed point.

## Non-Negotiables
- One school at a time.
- Creator/repair work is done by fire-once worker agents, not by the main orchestrator.
- Proxy-only browsing (Oxylabs). Never run direct/local-IP browsing.
- Use the proxied `browse` CLI for reconnaissance and page inspection.
- In this repo, creator/repair launcher subprocesses already inject proxy env for browser work.
- The deliverable remains a deterministic Playwright scraper script.
- Optimize for football-specific public data, not generic athletics existence.
- Respect `~/.web_scraper_blocklist.json`.
- No heuristic-only decisions; worker must navigate pages like a human.

## Source of Truth
- Queue table `school_scraper_status` in `data/best_football.db`.
- Resume point is always the next `pending` row in queue order.

## Quick Resume
1. Check status:
   - `uv run python scripts/discover_schools.py --status`
2. Claim next school:
   - `uv run python scripts/discover_schools.py --claim-next`
3. Resolve deterministic output path:
   - `scrapers/schools/{state_lower}/{nces_id}.py`
4. Spawn one worker agent for that school only using prompt template:
   - `templates/agent_prompts/school_creator.md`
5. Worker must return one-line JSON:
   - `{"status":"complete|blocked|failed","script_path":"...","reason":"..."}`
6. If `complete`, validate script output contract + non-empty extraction, then mark complete:
   - `uv run python scripts/discover_schools.py --complete <nces_id> --scraper-file <path>`
7. If `blocked`, mark blocked with concrete reason.
8. If `failed`, mark failed with reason.
9. Sleep 180 seconds.
10. Repeat.

## Codex Default Worker Path (No External Launcher Required)
- In Codex sessions, prefer `spawn_agent` with `agent_type=worker` for Step 4.
- Fill template fields directly from the claimed row and resolved script path.
- Keep worker ownership to one file only: `scrapers/schools/{state_lower}/{nces_id}.py`.
- Worker reconnaissance must use the hardwired Oxylabs-backed `browse` CLI, not direct browsing.
- Worker output must be a Playwright scraper script that can run in the normal proxied runtime.
- Proxy variance is normal. A site that loaded on one Oxylabs exit may block or behave differently on another, so treat access failures as evidence to evaluate, not as proof that your earlier reconnaissance was wrong.
- When the school site exposes football pages, prioritize fields that would improve a football page on best.football: football page URLs, team names, coach names, schedule links, contact info, practice/location details, and other concrete football signals.
- Wait for worker completion; if wait times out, continue polling same agent id instead of spawning duplicates.
- Parse the worker's final one-line JSON and update queue status immediately.

## Helper Commands
- Run URL-targeted one-shot creation wrapper:
  - `uv run python scripts/create_scraper_from_url.py --url "<school_url>" --launcher-command "<launcher with {prompt_path}>" [--nces-id <id>]`
- Default mobile proxy pool:
  - `https://us-pr.oxylabs.io:10001`
  - `https://us-pr.oxylabs.io:10002`
  - `https://us-pr.oxylabs.io:10003`
- Requeue due blocked rows:
  - `uv run python scripts/recheck_blocked.py`
- Force clear blocked rows (only when explicitly requested):
  - `uv run python scripts/discover_schools.py --clear-blocked`

## Completion Standard Per School
- Deterministic script exists at `scrapers/schools/{state_lower}/{nces_id}.py`.
- Script was derived from live proxied browsing, not offline heuristics.
- Script focuses on football-relevant extraction where football content exists, rather than returning a generic athletics summary.
- Script returns required envelope keys:
  - `nces_id`, `school_name`, `state`, `source_pages`, `extracted_items`, `scrape_meta`, `errors`
- Validation passes and `extracted_items` is non-empty (for complete).
