Single-school creator task.

School: {school_name}
NCES ID: {nces_id}
State: {state}
City: {city}
Website: {website}
Output script path: {script_path}

Requirements:
- Use Playwright with Oxylabs proxy only.
- Respect ~/.web_scraper_blocklist.json.
- Manually navigate school pages like a human (menus/subpages); do not rely on one-pass keyword heuristics.
- Determine if public athletics content/program exists.

If athletics exists:
- Create deterministic scraper script at {script_path}.
- Return envelope keys: nces_id, school_name, state, source_pages, extracted_items, scrape_meta, errors.
- Ensure extracted_items is non-empty.

If athletics does not exist:
- Do not invent data.
- Return blocked with clear reason.

Return one-line JSON only:
{"status":"complete|blocked|failed","script_path":"{script_path}","reason":"..."}
