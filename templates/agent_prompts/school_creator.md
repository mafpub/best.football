Single-school creator task.

School: {school_name}
NCES ID: {nces_id}
State: {state}
City: {city}
Website: {website}
Output script path: {script_path}

Requirements:
- This school was prequalified by the latest completed datacenter website survey success list. If live evidence contradicts that survey, return `failed` with the concrete reason rather than switching targets or proxy profiles.
- Use the hardwired Oxylabs-backed `browse` CLI for live reconnaissance.
- Use Playwright with Oxylabs proxy for the final scraper script and any script-level validation.
- Use the `datacenter` proxy profile for reconnaissance, script execution, and validation. Do not switch to `mobile`.
- The current proxy pool is: {proxy_servers}
- Current auth mode: {proxy_auth_mode}. If auth mode is `ip_whitelist`, do not fail just because profile creds are unset.
- Respect the active profile blocklist file for this profile.
- Manually navigate school pages like a human (menus/subpages); do not rely on one-pass keyword heuristics.
- This repo is football-only. Do not build a generic athletics or general sports scraper when football is absent.
- Derive stable selectors and paths from the live DOM, then encode them in the Playwright scraper.
- When football pages exist, prefer concrete fields like team page URLs, coach names, schedule links, football contacts, and practice/location details.
- Proxy variance is real. A site may load through one Oxylabs exit and fail or block on another, so judge the current run on the evidence you can verify.
- Use `restricted` for Oxylabs/provider restriction and `blocked` for target-side access blocks such as Cloudflare or site-side denials.
- Do not hardcode legacy `OXYLABS_PROXY_SERVER`, `OXYLABS_USERNAME`, or `OXYLABS_PASSWORD` in new scripts. Import the shared helper instead:
  `from scrapers.schools.runtime import assert_not_blocklisted, get_playwright_proxy_config, require_proxy_credentials`
- Launch Playwright with `proxy=get_playwright_proxy_config(profile="...")` so new scrapers inherit the selected profile proxy configuration automatically.

If useful public football content exists:
- Create deterministic scraper script at {script_path}.
- Return envelope keys: nces_id, school_name, state, source_pages, extracted_items, scrape_meta, errors.
- Ensure extracted_items is non-empty.

If useful public football content does not exist:
- Do not invent data.
- Do not build a scraper.
- Return `no_football` with a clear reason and a short `notes` summary of the evidence.

Return one-line JSON only:
{"status":"complete|no_football|blocked|restricted|failed","script_path":"{script_path}","reason":"...","notes":"..."}
