# School Discovery Progress Tracking

**Last Updated:** 2026-02-26
**Session:** School Athletic Website Discovery - Batch Processing

---

## Quick Resume Commands

```bash
# Check current status
uv run python scripts/discover_schools.py --status

# Get next batch of schools to discover
uv run python scripts/discover_schools.py --next-batch --count 5

# Mark school as complete (with scraper)
uv run python scripts/discover_schools.py --complete <nces_id> --scraper-file scrapers/schools/<state>/<nces_id>.py

# Mark school as blocked
uv run python scripts/discover_schools.py --blocked <nces_id> --reason "<reason>"
```

---

## Current Status

**Total Schools:** 5,353 (CA schools with websites from NCES)
**Processed:** 56 schools
- **Complete:** 12 scrapers generated
- **Blocked:** 44 (no athletics program)
- **Failed:** 0
- **Pending:** 5,297

**Progress:** 1.0%

---

## Completed Scrapers (12)

### Batch 1
1. ✅ `060162813064` - ABLE Charter School (ca/060162813064.py)
2. ✅ `060222711563` - AIMS College Prep High (ca/060222711563.py)
3. ✅ `062271003139` - Abraham Lincoln Senior High (ca/062271003139.py)

### Batch 2
4. ✅ `064214014396` - ATLAS Learning Academy (ca/064214014396.py)

### Batch 3
5. ✅ `060191107296` - Academy for Academic Excellence (ca/060191107296.py)

### Batch 4
6. ✅ `063697212990` - Adelanto High (ca/063697212990.py)
7. ✅ `062927004516` - Adolfo Camarillo High (ca/062927004516.py)
8. ✅ `063543006055` - Adrian Wilcox High (ca/063543006055.py)

### Batch 5
9. ✅ `062100002518` - Agoura High (ca/062100002518.py)

### Batch 6
10. ✅ `062271003064` - Alexander Hamilton Senior High (ca/062271003064.py)

### Batch 7
11. ✅ `062403003612` - Alhambra Senior High-Martinez (ca/062403003612.py)
12. ✅ `060744002328` - Aliso Niguel High (ca/060744002328.py)

---

## Blocked Reasons (44 schools)

### Common Block Reasons:
- **alternative_school_no_athletics** (8): Alternative/continuation schools
- **charter_school_no_athletics** (7): Small charters without sports
- **continuation_school_no_athletics** (6): Continuation high schools
- **middle_college_high_school_no_athletics** (4): Middle college programs
- **county_community_school_no_athletics** (3): County court/community schools
- **cloudflare_blocked_403_forbidden** (3): Cloudflare protection
- **school_closed** (2): Permanently closed schools
- **website_404_not_found** (2): Broken/dead links
- **district_site_no_school_specific_athletics_page** (2): Only district page available
- **juvenile_hall/court_school** (2): Incarcerated youth programs
- **special_education_center** (2): Special ed only
- **virtual_alternative_school** (1): Online-only programs
- **sla_small_learning_community** (2): Students participate at main high school
- **adult_transition_program** (1): Post-secondary special ed
- **tk8_school** (1): No high school grades
- **stem_charter** (1): STEM-focused without athletics
- **independent_study** (1): Independent study program

---

## Discovery Agent Workflow

### For Each School:

1. **Get school info** from `--next-batch` command
2. **Navigate to website** using stealth playwright:
   ```javascript
   await page.goto('https://<school-website>/', { waitUntil: 'domcontentloaded', timeout: 30000 });
   ```
3. **Check for athletics**:
   ```javascript
   const hasAthletics = await page.evaluate(() => {
     const bodyText = document.body.innerText.toLowerCase();
     return /athletic|sport|football|basketball|baseball|soccer|volleyball|track/i.test(bodyText);
   });
   ```
4. **If athletics found:**
   - Navigate to athletics page
   - Identify sports offered (Fall/Winter/Spring)
   - Look for: athletic director, coaches, schedules, rosters
   - Generate scraper script in `scrapers/schools/<state>/<nces_id>.py`
   - Mark complete: `--complete <nces_id> --scraper-file scrapers/schools/<state>/<nces_id>.py`

5. **If NO athletics:**
   - Mark blocked: `--blocked <nces_id> --reason "<reason>"`

---

## Block Reasons Reference

Use these standardized reasons:

### School Type
- `alternative_school_no_athletics` - Alternative/continuation school
- `continuation_school_no_athletics` - Continuation high school
- `middle_college_high_school_no_athletics` - Middle college program
- `county_community_school_no_athletics` - County community school
- `county_court_school_no_athletics` - County court school
- `juvenile_hall_court_school_no_athletics` - Juvenile hall
- `special_education_center_no_athletics` - Special ed only
- `adult_transition_special_education_program_no_athletics` - Adult transition
- `tk8_school_no_traditional_high_school_athletics` - Only TK-8 grades
- `junior_high_middle_school_not_high_school` - Middle school only

### Charter/Private
- `small_charter_school_no_athletics` - Small charter (<500 students)
- `charter_school_no_athletics` - Charter without sports program
- `stem_charter_school_no_athletics` - STEM-focused charter
- `virtual_alternative_school_no_athletics_program` - Online-only

### Shared Athletics
- `sla_small_learning_community_shares_athletics_with_<host>_hs` - SLC at comprehensive school
- `charter_students_participate_at_<district>_high_no_own_program` - Charter students participate elsewhere

### Website Issues
- `cloudflare_blocked_403_forbidden` - Cloudflare protection
- `website_requires_login_no_public_content` - Login required
- `website_404_page_not_found` - Dead link
- `website_403_forbidden_access_denied` - Access denied
- `website_timeout_slow_loading` - Site times out
- `dns_error_domain_not_resolved` - Domain doesn't exist

### District Issues
- `district_site_no_school_specific_athletics_page` - Only district page

### Closed
- `school_closed_<month>_<year>` - Permanently closed

### Unknown
- `likely_continuation_school_no_info_on_district_site` - Can't find info

---

## Scraper Template

```python
"""<School Name> athletic data scraper.

Generated by discovery agent using Playwright MCP.
School: <School Name>
NCES ID: <nces_id>
Website: <website>
Athletic URL: <athletics_url>
Generated: <date>

Data available:
- <list what athletic data was found>
"""

from playwright.async_api import async_playwright
import os
from datetime import datetime
import re

# Oxylabs proxy credentials (REQUIRED)
PROXY_SERVER = "ddc.oxylabs.io:8001"
PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

async def scrape_<school>_athletics() -> dict:
    """Scrape athletic data from <school> website."""
    if not PROXY_USERNAME or not PROXY_PASSWORD:
        raise ValueError("Oxylabs credentials not set")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USERNAME,
                "password": PROXY_PASSWORD,
            },
            headless=True,
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

        page = await context.new_page()
        await page.goto("<athletic_url>", wait_until="domcontentloaded")
        content = await page.content()
        await browser.close()

        return {
            "nces_id": "<nces_id>",
            "school_name": "<School Name>",
            "athletic_url": "<athletic_url>",
            "scraped_at": datetime.now().isoformat(),
            # Add extracted data
        }

async def main():
    result = await scrape_<school>_athletics()
    print(f"Scraped {result['school_name']}")
    return result

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

---

## Next Steps

Continue discovery in batches of 5-10 schools per session. At current pace (~1% per session), will need ~100 sessions to complete all CA schools.

After CA complete, expand to: TX, FL, OH

---

## Notes

- Use stealth playwright (`mcp__playwright-stealth__browser_execute`) for lower context usage
- Regular playwright (`mcp__playwright__*`) tools available if needed
- Always mark schools (complete or blocked) to track progress
- Scraper files go in: `scrapers/schools/<state_lower>/<nces_id>.py`
- Database tracks everything in `school_scraper_status` table
