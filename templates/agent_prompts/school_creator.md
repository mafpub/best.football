Single-school creator task.

School: {school_name}
NCES ID: {nces_id}
State: {state}
City: {city}
Website: {website}
Output script path: {script_path}

Requirements:
- Use the hardwired Oxylabs-backed `browse` CLI for live reconnaissance.
- Use Playwright with Oxylabs proxy for the final scraper script and any script-level validation.
- The current proxy pool is: {proxy_servers}
- Current auth mode: {proxy_auth_mode}. If auth mode is `ip_whitelist`, do not fail just because profile creds are unset.
- Respect the active profile blocklist file for this profile.
- Manually navigate school pages like a human (menus/subpages); do not rely on one-pass keyword heuristics.
- This repo is football-first. Extract football-useful public data, not generic athletics coverage.
- Derive stable selectors and paths from the live DOM, then encode them in the Playwright scraper.
- When football pages exist, prefer concrete fields like team page URLs, coach names, schedule links, football contacts, and practice/location details.
- Proxy variance is real. A site may load through one Oxylabs exit and fail or block on another, so judge the current run on the evidence you can verify.
- Do not hardcode legacy `OXYLABS_PROXY_SERVER`, `OXYLABS_USERNAME`, or `OXYLABS_PASSWORD` in new scripts. Import the shared helper instead:
  `from scrapers.schools.runtime import assert_not_blocklisted, get_playwright_proxy_config, require_proxy_credentials`
- Launch Playwright with `proxy=get_playwright_proxy_config(profile="...")` so new scrapers inherit the selected profile proxy configuration automatically.

If useful public football content exists:
- Create deterministic scraper script at {script_path}.
- Return envelope keys: nces_id, school_name, state, source_pages, extracted_items, scrape_meta, errors.
- Ensure extracted_items is non-empty.

If useful public football content does not exist:
- Do not invent data.
- Return blocked with clear reason.

Return one-line JSON only:
{"status":"complete|blocked|failed","script_path":"{script_path}","reason":"..."}
