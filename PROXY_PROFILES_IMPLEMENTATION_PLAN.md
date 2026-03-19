# Proxy Profiles Implementation Plan

## Required End State

Implement exactly two proxy profiles:

- `mobile`
- `datacenter`

Each profile must:

- use its own proxy configuration
- use its own blocklist
- enforce only its own blocklist
- append newly discovered provider-side restricted targets to its own blocklist immediately

There is no shared provider blocklist anymore.

## Concrete Proxy Definitions

### Mobile

Mobile is the authenticated rotating Oxylabs gateway.

Use:

- host: `pr.oxylabs.io:7777`
- auth: username/password
- geography: encoded in the username

Example shape:

- `https://<mobile-username>:<mobile-password>@pr.oxylabs.io:7777`

This is the mobile proxy path. Do not model mobile as the earlier `us-pr:10001-10003` sticky pool.

### Datacenter

Datacenter remains a separate proxy profile with its own host(s), credentials, and behavior.

It is not interchangeable with mobile.

## Required Configuration Model

Proxy selection must be explicit and profile-based.

Use one selector:

- `OXYLABS_PROXY_PROFILE`

Allowed values:

- `mobile`
- `datacenter`

Use distinct config variables for each profile.

### Mobile config

- `OXYLABS_MOBILE_PROXY_SERVER`
- `OXYLABS_MOBILE_USERNAME`
- `OXYLABS_MOBILE_PASSWORD`

Expected server value:

- `https://pr.oxylabs.io:7777`

### Datacenter config

- `OXYLABS_DATACENTER_PROXY_SERVER`
- `OXYLABS_DATACENTER_USERNAME`
- `OXYLABS_DATACENTER_PASSWORD`

Do not collapse mobile and datacenter auth into one shared credential path.

## Required Blocklist Model

Use separate blocklist files.

### Mobile blocklist

- `~/.web_scraper_blocklist_mobile.json`

### Datacenter blocklist

- `~/.web_scraper_blocklist_datacenter.json`

The old shared file:

- `~/.web_scraper_blocklist.json`

must no longer be the runtime source of truth.

## Required Runtime Behavior

### Blocklist enforcement

When running under `mobile`:

- check only the mobile blocklist

When running under `datacenter`:

- check only the datacenter blocklist

### Restricted target detection

When the active proxy path returns a provider-side restricted-target signal, append the domain to that same profile’s blocklist immediately.

Provider-side restricted-target signal includes:

- `x-error-description: Access denied: restricted target`

Do not write that domain into the other profile’s blocklist.

Do not treat Cloudflare pages or site-origin 403s as provider restrictions.

## Required Code Changes

### 1. `pipeline/proxy.py`

Refactor this file to be profile-based.

It must:

- resolve the active proxy profile
- resolve profile-specific server
- resolve profile-specific credentials
- build profile-specific Playwright proxy config
- build profile-specific browser env
- describe the active proxy mode/profile for logging and prompts

Expected helper shape:

- `get_proxy_profile()`
- `get_proxy_server(profile: str | None = None)`
- `get_proxy_auth(profile: str | None = None)`
- `get_playwright_proxy_config(proxy_index: int | None = None, profile: str | None = None)`
- `get_browser_proxy_env(proxy_index: int | None = None, profile: str | None = None)`
- `describe_proxy_mode(profile: str | None = None)`

`mobile` should resolve to the rotating `pr.oxylabs.io:7777` gateway.

### 2. `scrapers/schools/runtime.py`

Refactor runtime blocklist logic to be profile-aware.

It must:

- resolve the blocklist file from the active profile
- load only that profile’s blocklist
- enforce only that profile’s blocklist
- support appending newly restricted domains to that profile’s blocklist

Expected helper shape:

- `get_blocklist_file(profile: str | None = None)`
- `load_blocklist_domains(profile: str | None = None)`
- `append_blocklist_domain(url_or_domain: str, profile: str | None = None, reason: str | None = None)`
- `assert_not_blocklisted(urls: list[str], profile: str | None = None)`

### 3. Queue / orchestration scripts

Update all relevant scripts so proxy profile is explicit and passed through consistently.

Relevant scripts include:

- `scripts/school_creator_loop.py`
- `scripts/run_repair_queue.py`
- `scripts/run_school_scrapes.py`
- `scripts/create_scraper_from_url.py`
- `scripts/agent_session_adapter.py`
- `scripts/reclassify_blocked_restricted.py`

Each script should accept:

- `--proxy-profile mobile|datacenter`

Each script must:

- use that profile for proxy config
- use that profile for blocklist enforcement
- use that profile when appending restricted domains

### 4. `scripts/reclassify_blocked_restricted.py`

Refactor this script to be profile-aware.

It must:

- accept `--proxy-profile`
- probe through the selected profile only
- write restricted domains into the selected profile’s blocklist only

### 5. `browse` integration

When implementation starts, align the local browse runtime with the same profile model.

Browse must:

- use the selected profile’s proxy configuration
- stop using hardcoded legacy proxy hosts
- respect the selected profile’s proxy env

## Required CLI Behavior

Profile choice should be explicit everywhere it matters.

Required command flag:

- `--proxy-profile`

Allowed values:

- `mobile`
- `datacenter`

If not provided, scripts may use `OXYLABS_PROXY_PROFILE`, but the implementation should still be cleanly profile-driven.

## Queue Semantics

Keep the current `restricted` queue status.

Do not merge it back into `blocked`.

Do not make `restricted` decay.

Do not allow `requeue_due_blocked` or `clear_blocked` to affect `restricted`.

## Generated Scraper Compatibility

Existing generated scripts that hardcode legacy datacenter proxy values are legacy datacenter artifacts.

Implementation should:

- preserve their operability until regenerated
- make new shared-runtime-based work profile-aware

Do not try to mass-rewrite every generated school scraper as part of this change.

## Testing Requirements

Add or update tests for:

- profile selection
- mobile proxy resolution
- datacenter proxy resolution
- profile-specific auth resolution
- profile-specific blocklist file selection
- profile-specific blocklist enforcement
- append-on-restricted writing only to the active profile blocklist
- `restricted` queue semantics staying non-decaying

Integration checks should confirm:

- a domain restricted under `mobile` is added only to mobile blocklist
- datacenter blocklist remains unchanged
- a domain restricted under `datacenter` is added only to datacenter blocklist
- mobile and datacenter runs consult different blocklist files

## Implementation Order

1. Refactor `pipeline/proxy.py` to support only the required two-profile model.
2. Refactor `scrapers/schools/runtime.py` to support per-profile blocklists and append-on-restricted.
3. Update orchestration scripts to accept and propagate `--proxy-profile`.
4. Update the reclassifier to operate per profile.
5. Update browse integration to stop using hardcoded legacy proxies.
6. Add and run tests.

## Explicit Non-Goals

Do not add:

- fallback proxy models
- phased rollout logic
- optional compatibility branches beyond what is needed to keep legacy datacenter scrapers running until regeneration
- one shared provider blocklist
- a design that treats mobile and datacenter restrictions as interchangeable
