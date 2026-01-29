"""FastAPI application for camp submissions and search."""

import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, EmailStr

from pipeline.database import get_db

# Setup Jinja2 for HTML responses
TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

app = FastAPI(
    title="best.football API",
    description="Camp submissions and search for best.football",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://best.football", "http://localhost:8625"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


class CampSubmission(BaseModel):
    """Camp submission payload."""

    name: str
    organizer_type: str  # 'university', 'private', 'school', 'organization'
    venue_name: str | None = None
    address: str | None = None
    city: str
    state: str
    zip: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    ages_min: int | None = None
    ages_max: int | None = None
    skill_levels: list[str] | None = None
    focus_areas: list[str] | None = None
    overnight: bool = False
    cost_min: float | None = None
    cost_max: float | None = None
    registration_url: str | None = None
    submitted_by: str
    submitted_email: EmailStr


class CampResponse(BaseModel):
    """Camp response with ID."""

    id: str
    name: str
    city: str
    state: str
    verified: bool


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}


@app.post("/api/camps", response_model=CampResponse)
async def submit_camp(camp: CampSubmission):
    """Submit a new camp for review."""
    camp_id = str(uuid.uuid4())

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO camps
            (id, name, organizer_type, venue_name, address, city, state, zip,
             start_date, end_date, ages_min, ages_max, skill_levels, focus_areas,
             overnight, cost_min, cost_max, registration_url, submitted_by,
             submitted_email, verified, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                camp_id,
                camp.name,
                camp.organizer_type,
                camp.venue_name,
                camp.address,
                camp.city,
                camp.state.upper(),
                camp.zip,
                camp.start_date,
                camp.end_date,
                camp.ages_min,
                camp.ages_max,
                str(camp.skill_levels) if camp.skill_levels else None,
                str(camp.focus_areas) if camp.focus_areas else None,
                camp.overnight,
                camp.cost_min,
                camp.cost_max,
                camp.registration_url,
                camp.submitted_by,
                camp.submitted_email,
                False,  # Not verified by default
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
            ),
        )

    return CampResponse(
        id=camp_id,
        name=camp.name,
        city=camp.city,
        state=camp.state.upper(),
        verified=False,
    )


@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(10, le=50, description="Max results"),
):
    """Search schools and camps."""
    results = []
    query = f"%{q}%"

    with get_db() as conn:
        # Search schools
        schools = conn.execute(
            """
            SELECT nces_id, name, city, state, 'school' as type
            FROM schools
            WHERE name LIKE ? OR city LIKE ?
            LIMIT ?
            """,
            (query, query, limit),
        ).fetchall()

        for s in schools:
            results.append({
                "id": s["nces_id"],
                "name": s["name"],
                "location": f"{s['city']}, {s['state']}",
                "type": "school",
                "url": f"/schools/{s['state'].lower()}/{s['name'].lower().replace(' ', '-')}.html",
            })

        # Search camps
        camps = conn.execute(
            """
            SELECT id, name, city, state, 'camp' as type
            FROM camps
            WHERE verified = 1 AND (name LIKE ? OR city LIKE ?)
            LIMIT ?
            """,
            (query, query, limit),
        ).fetchall()

        for c in camps:
            results.append({
                "id": c["id"],
                "name": c["name"],
                "location": f"{c['city']}, {c['state']}",
                "type": "camp",
                "url": f"/camps/{c['state'].lower()}/{c['id']}.html",
            })

    return {"results": results[:limit]}


@app.get("/api/camps")
async def list_camps(
    request: Request,
    state: str | None = Query(None, description="Filter by state"),
    city: str | None = Query(None, description="Filter by city"),
    type: list[str] | None = Query(None, description="Filter by organizer type"),
    overnight: str | None = Query(None, description="Filter for overnight camps"),
    verified_only: bool = Query(True, description="Only show verified camps"),
    limit: int = Query(50, le=200, description="Max results"),
):
    """List camps with optional filters. Returns HTML for HTMX requests, JSON otherwise."""
    with get_db() as conn:
        query = "SELECT * FROM camps WHERE 1=1"
        params = []

        if verified_only:
            query += " AND verified = 1"

        if state:
            query += " AND state = ?"
            params.append(state.upper())

        if city:
            query += " AND LOWER(city) LIKE LOWER(?)"
            params.append(f"%{city}%")

        if type:
            placeholders = ",".join("?" * len(type))
            query += f" AND organizer_type IN ({placeholders})"
            params.extend(type)

        if overnight == "on":
            query += " AND overnight = 1"

        query += " ORDER BY start_date ASC LIMIT ?"
        params.append(limit)

        camps = conn.execute(query, params).fetchall()
        camps_list = [dict(c) for c in camps]

        # Return HTML for HTMX requests
        if request.headers.get("HX-Request"):
            template = jinja_env.get_template("partials/camp_results.html")
            html = template.render(camps=camps_list)
            return HTMLResponse(content=html)

        return {
            "camps": camps_list,
            "total": len(camps_list),
        }
