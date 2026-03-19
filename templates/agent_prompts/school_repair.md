Single-school repair task.

School: {school_name}
NCES ID: {nces_id}
State: {state}
City: {city}
Website: {website}
Script path: {script_path}
Last failure: {failure_reason}

Requirements:
- Use the hardwired Oxylabs-backed `browse` CLI to inspect the live site before patching.
- Use Playwright with Oxylabs proxy for the repaired scraper script and any script-level validation.
- Respect ~/.web_scraper_blocklist.json.
- Compare live DOM/navigation against current script.
- Patch only this script with deterministic selectors/paths.
- Keep the extraction football-specific. Repair toward concrete football page value, not generic sports mentions.
- If the current proxy session sees a block or inconsistent response, account for proxy variance before assuming the site structure changed.

Validation target:
- Script returns required envelope keys.
- extracted_items is non-empty if public football content exists.
- If site truly has no useful public football content, return blocked with reason.

Return one-line JSON only:
{"status":"complete|blocked|failed","script_path":"{script_path}","reason":"..."}
