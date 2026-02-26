# best.football TODO

## Guides to Write

Priority guides based on computed data analysis. These leverage unique data (competitive_index, enrollment_percentile, classifications) that competitors can't easily replicate.

### High Priority (State Classification Guides)

- [x] **Texas UIL Classifications: 6A Through 1A Explained**
  - Data: 988 schools, 11 classifications (6A, 5A-D1/D2, 4A-D1/D2, etc.)
  - Include enrollment cutoffs, Division I vs II differences
  - Competitive index varies: 4A-D1 (0.726 avg) vs 2A-D2 (0.08 avg)

- [x] **Ohio OHSAA Divisions and Regions**
  - Data: 702 schools, 7 divisions (I-VII), 84 conferences
  - Cover 6 geographic regions (Northeast, Northwest, Southwest, Central, Southeast, East)
  - Explain Division I avg enrollment (1,846) vs Division VII (263)

- [x] **Florida FHSAA Classifications: 1A-7A**
  - Data: 324 schools, 7 classifications, 32 geographic divisions
  - Note unusual enrollment distributions (large schools in small classifications)
  - Navarre HS: 2,430 students in 1A; Atlantic Coast: 2,768 in 2A

- [x] **California CIF Sections Overview**
  - Data: 271 CIFSS schools, 12 conferences
  - Explain section-based system vs statewide classifications
  - CIFSS (Southern Section) structure

### Medium Priority (Data-Driven Insights)

- [x] **School Size vs Classification: Competitive Challenges**
  - Use competitive_index data (2,266 schools)
  - Highlight "Davids vs Goliaths" - schools at bottom of classification ranges
  - TX 3A-D2 enrollment range: 4-2,719 students

- [x] **Competitive Index Explained**
  - Define the metric and how it's calculated
  - State comparisons: OH Div II (0.575) vs Div I (0.472)
  - How parents can use this data

- [x] **Charter Schools in Texas Football**
  - Data: 130 TX charter programs (13% of TX athletic programs)
  - Compare to other states (CA: 10, FL: 11, OH: 7)
  - UIL eligibility for charter schools

### Lower Priority (Geography & Structure)

- [x] **Texas Districts and Regions**
  - 16 primary districts (49-67 schools each)
  - How district alignment affects scheduling/playoffs

- [x] **Ohio Conference Landscape**
  - 84 unique conferences
  - Largest: Ohio Capital (23), Chagrin Valley (19), Columbus City League (14)

- [x] **Public vs Charter Football Comparison**
  - 1,462 charter vs 6,331 public schools in database
  - Eligibility, facilities, funding differences

## Features

### Data Pipeline

- [ ] Add more California CIF sections (currently only CIFSS with 271 schools)
- [ ] Improve school name matching for athletics data (fuzzy matching)
- [ ] Add playoff history data for competitive_index calculation
- [ ] Implement travel_burden metric calculations

### Site Generation

- [x] Add school metrics to school pages (currently TODO in generate.py)
- [x] Add nearby camps to school pages
- [x] Add classifications list to state pages
- [ ] Improve search with Pagefind filters

### API

- [ ] Add camp verification workflow
- [ ] Add admin endpoints for camp management
- [ ] Email notifications for camp submissions

### SEO

- [x] Add structured data to school pages (LocalBusiness schema)
- [ ] Generate state-specific landing pages
- [x] Add internal linking between related schools/counties

## Data Quality

- [ ] Review Florida classification assignments (large schools in 1A/2A)
- [ ] Audit NCES to athletics matching (currently 2,285 of 7,793 matched)
- [ ] Add missing county data for some schools

## Infrastructure

- [x] Deploy to ha1 server
- [x] Security audit and fixes
- [x] Rate limiting on API
- [ ] Set up monitoring/alerting
- [ ] Configure Cloudflare caching rules
- [ ] Set up automated data refresh pipeline

---

## School Athletic Website Discovery (DEVELOPMENT PHASE)

### Overview

**This is a one-time setup phase to build 7,000+ individual school scrapers.**

After this phase is complete, running all scrapers will be fast and parallelizable. During development, Claude (the AI operator) manually discovers each school's athletic website using Playwright.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 1: Discovery (Current Phase - SLOW, One-Time)            │
└─────────────────────────────────────────────────────────────────┘

For each of ~7,426 schools:
1. Claude uses Playwright MCP tool
2. Browses school website like a human
3. Finds athletic section (/athletics, /sports, /football, etc.)
4. Identifies exact selectors for:
   - News/announcements
   - Schedule/games
   - Rosters
   - Coaches
5. Generates deterministic Python scraper script
6. Saves to: scrapers/schools/{state}/{nces_id}.py

┌─────────────────────────────────────────────────────────────────┐
│ PHASE 2: Runtime (After Discovery - FAST, Parallel)            │
└─────────────────────────────────────────────────────────────────┘

$ uv run python scrapers/schools/tx/060000103278.py  # Allen HS
$ uv run python scrapers/schools/ca/060000210346.py  # CA School for Blind
... (fire off 7,000+ scripts in parallel batches)

Each script:
- Uses Playwright with Oxylabs proxy
- Has deterministic selectors for THAT school
- Returns structured data (news, schedule, roster, etc.)
- Writes to database

┌─────────────────────────────────────────────────────────────────┐
│ PHASE 3: Repair (When Scrapers Break)                          │
└─────────────────────────────────────────────────────────────────┘

When scraper fails:
1. Claude uses Playwright MCP tool
2. Re-browses the school athletic website
3. Sees what changed (new DOM structure)
4. Updates the scraper script with new selectors
5. Git commits the fix
6. Reset failure counter
```

### Key Design Decisions

**Individual Scripts, NOT Generic Templates**
- Each school gets its own Python script
- No "one size fits all" YAML configs
- Deterministic selectors for each site
- If Allen High School changes their site, only `allen_high.py` breaks

**Claude is the Discovery Agent**
- Uses Playwright MCP (mcp__playwright-stealth__ tools)
- Browses like a human - clicks menus, looks for links
- Intelligently finds athletic content
- Generates working scraper scripts

**Sequential Discovery, Parallel Runtime**
- Discovery: ONE school at a time (slow, careful)
- Runtime: ALL schools in parallel (fast, efficient)

**Self-Healing via Repair Agent**
- 2 consecutive failures triggers repair
- Claude re-inspects site with Playwright
- Updates selectors, tests, git commits
- No manual intervention for most breaks

### Data to Scrape

From each school athletic website:
- News/announcements (articles, updates)
- Football schedule (games, times, locations)
- Roster (player names, numbers, positions)
- Coaching staff (head coach, assistants)
- Stadium/location details

### Files

**Orchestration:**
- `scripts/discover_schools.py` - Get next batch, mark status
- `scrapers/schools/{state}/{nces_id}.py` - Generated scrapers

**Database Tables:**
- `school_scraper_status` - Track discovery progress
- `school_athletic_urls` - Discovered athletic URLs
- `scraped_athletic_data` - Actual scraped content

### Commands

```bash
# Get next batch of schools to discover
python scripts/discover_schools.py --next-batch --count 10

# Mark school as complete (with generated scraper)
python scripts/discover_schools.py --complete 060000103278

# Mark school as blocked (Cloudflare, etc.)
python scripts/discover_schools.py --blocked 060000103278 --reason "cloudflare"

# Status report
python scripts/discover_schools.py --status
```

### Progress Tracking

Goal: 7,426 individual school scrapers
- TX: ~1,900 schools
- CA: ~2,800 schools
- FL: ~800 schools
- OH: ~700 schools
- Other states: ~1,200 schools

Blocked sites (separate bucket):
- Cloudflare-protected
- Require JavaScript beyond Playwright
- No athletic website found
- Need manual review
