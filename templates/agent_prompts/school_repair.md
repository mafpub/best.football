Single-school repair task.

School: {school_name}
NCES ID: {nces_id}
State: {state}
City: {city}
Website: {website}
Script path: {script_path}
Last failure: {failure_reason}

Requirements:
- Use Playwright with Oxylabs proxy only.
- Respect ~/.web_scraper_blocklist.json.
- Compare live DOM/navigation against current script.
- Patch only this script with deterministic selectors/paths.

Validation target:
- Script returns required envelope keys.
- extracted_items is non-empty if athletics content exists.
- If site truly has no public athletics content, return blocked with reason.

Return one-line JSON only:
{"status":"complete|blocked|failed","script_path":"{script_path}","reason":"..."}
