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
- Respect the active profile blocklist file for the selected profile.
- Compare live DOM/navigation against current script.
- Patch only this script with deterministic selectors/paths.
- Keep the extraction football-specific. Repair toward concrete football page value, not generic sports mentions.
- Use `restricted` for Oxylabs/provider restriction and `blocked` for target-side access blocks such as Cloudflare or site-side denials.
- If the current proxy session sees a block or inconsistent response, account for proxy variance before assuming the site structure changed.

Validation target:
- Script returns required envelope keys.
- extracted_items is non-empty if public football content exists.
- If site truly has no useful public football content, return `no_football` with reason and a short `notes` summary.

Return one-line JSON only:
{{"status":"complete|no_football|blocked|restricted|failed","script_path":"{script_path}","reason":"...","notes":"..."}}
