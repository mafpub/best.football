"""FastAPI application for camp submissions and search."""

import logging
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, EmailStr, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from pipeline.database import get_db

logger = logging.getLogger(__name__)

# Setup Jinja2 for HTML responses
TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

# Rate limiting
limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="best.football API",
    description="Camp submissions and search for best.football",
    version="0.1.0",
)

# Add rate limiter to app state
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://best.football", "http://localhost:8625"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Valid organizer types for validation
VALID_ORGANIZER_TYPES = {"university", "private", "school", "organization"}


class OrganizerType(str, Enum):
    """Valid organizer types."""
    university = "university"
    private = "private"
    school = "school"
    organization = "organization"


# Generic exception handler for production (hide internal details)
@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions without leaking internal details."""
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal error occurred. Please try again later."},
    )


class CampSubmission(BaseModel):
    """Camp submission payload."""

    name: str
    organizer_type: OrganizerType  # Validated enum
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

    @field_validator("name", "city", "submitted_by")
    @classmethod
    def strip_and_validate_length(cls, v: str) -> str:
        """Strip whitespace and validate length."""
        v = v.strip()
        if len(v) < 2:
            raise ValueError("Must be at least 2 characters")
        if len(v) > 200:
            raise ValueError("Must be 200 characters or less")
        return v

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        """Validate state is one of the supported states."""
        v = v.strip().upper()
        if v not in {"TX", "CA", "FL", "OH"}:
            raise ValueError("State must be TX, CA, FL, or OH")
        return v


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
@limiter.limit("5/minute")
async def submit_camp(request: Request, camp: CampSubmission):
    """Submit a new camp for review. Rate limited to 5 per minute per IP."""
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
                camp.organizer_type.value,  # Convert enum to string
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
@limiter.limit("30/minute")
async def search(
    request: Request,
    q: str = Query(..., min_length=2, max_length=100, description="Search query"),
    limit: int = Query(10, le=50, description="Max results"),
):
    """Search schools and camps. Rate limited to 30 per minute per IP."""
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
@limiter.limit("60/minute")
async def list_camps(
    request: Request,
    state: str | None = Query(None, description="Filter by state"),
    city: str | None = Query(None, max_length=100, description="Filter by city"),
    type: list[str] | None = Query(None, description="Filter by organizer type"),
    overnight: str | None = Query(None, description="Filter for overnight camps"),
    verified_only: bool = Query(True, description="Only show verified camps"),
    limit: int = Query(50, le=200, description="Max results"),
):
    """List camps with optional filters. Returns HTML for HTMX requests, JSON otherwise."""
    # Validate organizer types if provided
    if type:
        invalid_types = set(type) - VALID_ORGANIZER_TYPES
        if invalid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid organizer types: {', '.join(invalid_types)}",
            )

    with get_db() as conn:
        query = "SELECT * FROM camps WHERE 1=1"
        params = []

        if verified_only:
            query += " AND verified = 1"

        if state:
            query += " AND state = ?"
            params.append(state.upper()[:2])  # Limit to 2 chars

        if city:
            query += " AND LOWER(city) LIKE LOWER(?)"
            params.append(f"%{city[:100]}%")  # Limit city length

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
