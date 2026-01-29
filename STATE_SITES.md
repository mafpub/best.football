# State Athletic Association Data Sources

Research completed: 2026-01-29

This document catalogs data access paths for high school football programs across major state athletic associations.

---

## Texas UIL (University Interscholastic League)

**Website**: https://www.uiltexas.org

### Data Access

| Resource | URL | Format |
|----------|-----|--------|
| Football Home | https://www.uiltexas.org/football | HTML |
| Alignments Page | https://www.uiltexas.org/football/alignments | HTML + PDF links |
| Alignment PDFs | See pattern below | PDF |

### PDF URL Pattern

```
http://www.uiltexas.org/files/alignments/{CLASS}{DIV}FB{YEAR}.pdf
```

**Examples:**
- `1AD1FB2024.pdf` - Class 1A Division I Football 2024
- `1AD2FB2024.pdf` - Class 1A Division II Football 2024
- `6ABBFB2024.pdf` - Class 6A (no division split) Football 2024

### Classification Structure

| Class | Divisions | Notes |
|-------|-----------|-------|
| 1A | Division I, Division II | Smallest schools |
| 2A | Division I, Division II | |
| 3A | Division I, Division II | |
| 4A | Division I, Division II | |
| 5A | Division I, Division II | |
| 6A | Division I, Division II | Largest schools |

### Data Format

- **Format**: PDF documents
- **Content**: School names, districts, regions organized by classification
- **Update Frequency**: Biennial (every 2 years)
- **Authentication**: None required

### Notes

- Alignment PDFs contain district assignments within each classification
- Each classification/division combination has its own PDF
- Data structured by UIL regions and districts

---

## California CIF (California Interscholastic Federation)

**Website**: https://cifstate.org

### Federated Structure

California CIF is organized into 10 autonomous sections, each with their own website and data management:

| Section | Website | Coverage |
|---------|---------|----------|
| CIF Central | https://cifcentral.org | Central Valley |
| CIF Central Coast | https://cifccs.org | Central Coast |
| CIF Los Angeles City | https://ciflacs.org | LA City Schools |
| CIF North Coast | https://cifncs.org | North Coast |
| CIF Northern | https://cifns.org | Far Northern |
| CIF Oakland | https://cifoak.org | Oakland Area |
| CIF Sac-Joaquin | https://cifsjs.org | Sacramento/San Joaquin |
| CIF San Diego | https://cifsds.org | San Diego County |
| CIF San Francisco | https://cifsf.org | San Francisco |
| CIF Southern | https://cifss.org | Southern California |

### CIF Southern Section (CIFSS) - Example

**Website**: https://cifss.org

| Resource | URL | Format |
|----------|-----|--------|
| School Directory | https://cifss.org/directory/ | HTML (iframe) |
| Football Page | https://cifss.org/sports/football-11/ | HTML |
| Playoff Divisions | Available on football page | PDF |

### Data Access Challenges

- **No centralized statewide database**
- Each section maintains its own school directory
- Directory data often in iframe-based interactive widgets
- Playoff divisions published as PDFs on individual section sites

### Classification Structure (varies by section)

Southern Section example divisions:
- Division 1 through Division 14 (football-specific playoff divisions)
- Based on competitive equity formula, not just enrollment

### Notes

- CIF State handles only state championship playoffs
- Regular season and section playoffs managed by individual sections
- MaxPreps partnership mentioned across multiple sections
- Data aggregation requires scraping 10 separate section websites

---

## Florida FHSAA (Florida High School Athletic Association)

**Website**: https://fhsaa.com

### Data Access

| Resource | URL | Format |
|----------|-----|--------|
| Member Directory | https://fhsaa.com/sports/2020/1/28/member_directory.aspx | HTML (iframe) |
| School Directory Widget | https://www.fhsaahome.org/widget/school-directory-locations | Interactive Web App |
| Football Classifications | Via widget with `sport_id=1` | Web App + PDF export |

### Interactive Widget URL Parameters

```
https://www.fhsaahome.org/widget/school-directory-locations?sport_id=1&school_id=&county_id=&section_id=&division_id=&school_category_id=&class_id=&region_id=&district_id=
```

**Parameter Values:**

| Parameter | Values |
|-----------|--------|
| `sport_id` | 1 = Football (11 man) |
| `class_id` | 1A, 2A, 3A, 4A, 5A, 6A, 7A, Rural |
| `section_id` | Section 1, Section 2, Section 3, Section 4, MaxPreps |
| `division_id` | Div1 through Div32 |
| `school_category_id` | Public, Private, Charter, University Laboratory, Virtual |
| `region_id` | NA, R1, R2, R3, R4 |
| `district_id` | D1 through D16, NA |
| `county_id` | Florida counties (67 total) |

### Classification Structure

| Class | Notes |
|-------|-------|
| 1A | Smallest enrollment |
| 2A | |
| 3A | |
| 4A | |
| 5A | |
| 6A | |
| 7A | Largest enrollment |
| Rural | Rural classification |

### Data Format

- **Format**: Interactive Google Maps widget with filters
- **Export**: PDF export available via "Export PDF" button
- **API**: Widget appears to load data dynamically, but no public API documented
- **Authentication**: None required

### Notes

- Widget powered by fhsaahome.org (member portal)
- Google Maps integration shows school locations
- Comprehensive filter system for sport/class/region/district
- Data includes school contact info, addresses, classifications

---

## Ohio OHSAA (Ohio High School Athletic Association)

**Website**: https://www.ohsaa.org

### Data Access

| Resource | URL | Format |
|----------|-----|--------|
| School Directory | https://ohsaa.finalforms.com/state_schools | HTML Table |
| Map Mode | https://ohsaa.finalforms.com/state_schools/map | Interactive Map |
| District View | https://ohsaa.finalforms.com/state_districts | HTML Table |
| Divisional Breakdowns | https://www.ohsaa.org/School-Resources/Divisional-Breakdowns-2025-26-School-Year | HTML |
| Football Page | https://www.ohsaa.org/sports/football | HTML |

### FinalForms School Directory

**Base URL**: `https://ohsaa.finalforms.com/state_schools`

**URL Parameters:**
```
?state_schools.athletic_association_abbreviation_eq=OHSAA
&state_schools.is_archived_eq=false
&page=1
&direction=asc
&sort=state_schools.full_name
```

**Available Data Fields:**
- School name and grade levels
- Student count (enrollment)
- School district
- NCES ID and State ID
- Conference affiliation
- Athletic District (Northeast, Northwest, Southwest, Southeast, Central, East)
- Classes (A, AA, AAA)
- Divisions (I through VII) - sport-specific
- Address and county
- Contact information (website, phone, email)

### Football Division Structure (2025 Season)

| Division | Adjusted Enrollment | Schools |
|----------|---------------------|---------|
| I | 592 and more | 72 |
| II | 378 - 591 | 104 |
| III | 268 - 377 | 107 |
| IV | 202 - 267 | 105 |
| V | 157 - 201 | 106 |
| VI | 112 - 156 | 106 |
| VII | 111 and less | 106 |
| **Total** | | **706** |

### Athletic Districts

| District | Region |
|----------|--------|
| Northeast | NE Ohio |
| Northwest | NW Ohio |
| Southwest | SW Ohio |
| Southeast | SE Ohio |
| Central | Central Ohio |
| East | East Ohio |

### Data Format

- **Format**: Paginated HTML tables (15 records per page, 95 pages total = 1,421 schools)
- **Platform**: FinalForms (BC Technologies Company)
- **Sortable**: By name, address, student count
- **Searchable**: By school name, NCES ID, district, conference
- **Authentication**: None required
- **NCES Integration**: Links to NCES school database

### Notes

- FinalForms provides comprehensive school database
- Division assignments are sport-specific (differ between football, basketball, etc.)
- Divisions based on adjusted enrollment with competitive balance factors
- Annual divisional breakdowns approved by OHSAA Board of Directors
- Schools have until October deadline to modify tournament participation

---

## Summary: Data Access Comparison

| State | Primary Data Source | Format | Ease of Access |
|-------|---------------------|--------|----------------|
| **Texas UIL** | PDF alignment documents | PDF | Easy - direct download |
| **California CIF** | 10 separate section websites | Mixed | Difficult - federated |
| **Florida FHSAA** | Interactive widget | Web App/PDF | Medium - widget-based |
| **Ohio OHSAA** | FinalForms database | HTML Table | Easy - structured HTML |

### Recommended Scraping Approach

1. **Texas UIL**: Download PDFs, parse with PDF extraction tools
2. **California CIF**: Focus on largest sections (CIFSS), scrape individual section sites
3. **Florida FHSAA**: Use widget URL parameters, capture PDF exports
4. **Ohio OHSAA**: Paginate through FinalForms HTML tables, most structured data

### Common Data Partners

- **MaxPreps**: Mentioned across multiple states as data/scheduling partner
- **FinalForms**: Used by Ohio (and other states) for school management
- **NCES**: Federal school database IDs used for cross-referencing
