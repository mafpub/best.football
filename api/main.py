"""FastAPI application for camp submissions and search."""

import logging
import secrets
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
from api.zoho_email import (
    send_camp_submitted_email,
    send_camp_approved_email,
    send_camp_rejected_email,
)

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
    tos_consent: bool  # Required, must be True
    marketing_consent: bool = False  # Optional

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

    @field_validator("tos_consent")
    @classmethod
    def validate_tos_consent(cls, v: bool) -> bool:
        """Validate that TOS consent is True."""
        if not v:
            raise ValueError("You must agree to the Terms of Service and Privacy Policy")
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
    action_token = secrets.token_urlsafe(32)

    # Get client info for consent logging
    client_ip = get_remote_address(request)
    user_agent = request.headers.get("user-agent", "")

    with get_db() as conn:
        # Insert camp with action token
        conn.execute(
            """
            INSERT INTO camps
            (id, name, organizer_type, venue_name, address, city, state, zip,
             start_date, end_date, ages_min, ages_max, skill_levels, focus_areas,
             overnight, cost_min, cost_max, registration_url, submitted_by,
             submitted_email, verified, action_token, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                action_token,
                datetime.utcnow().isoformat(),
                datetime.utcnow().isoformat(),
            ),
        )

        # Log consent for compliance
        conn.execute(
            """
            INSERT INTO consent_log
            (camp_id, ip_address, user_agent, tos_consent, marketing_consent,
             consent_timestamp, tos_version, privacy_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                camp_id,
                client_ip,
                user_agent[:500] if user_agent else None,  # Truncate long user agents
                camp.tos_consent,
                camp.marketing_consent,
                datetime.utcnow().isoformat(),
                "1.0",
                "1.0",
            ),
        )

    # Send admin notification email (non-blocking, don't fail if email fails)
    try:
        send_camp_submitted_email(
            camp_id=camp_id,
            camp_name=camp.name,
            city=camp.city,
            state=camp.state.upper(),
            organizer_type=camp.organizer_type.value,
            submitted_by=camp.submitted_by,
            submitted_email=camp.submitted_email,
            start_date=camp.start_date,
            end_date=camp.end_date,
            action_token=action_token,
        )
    except Exception as e:
        logger.error(f"Failed to send camp submission email: {e}")

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


@app.get("/api/camps/{camp_id}/approve")
async def approve_camp(
    camp_id: str,
    token: str = Query(..., description="Security token for approval"),
):
    """Approve a camp submission. Called from admin email link."""
    with get_db() as conn:
        # Fetch camp and validate token
        camp = conn.execute(
            "SELECT * FROM camps WHERE id = ?",
            (camp_id,),
        ).fetchone()

        if not camp:
            return HTMLResponse(
                content=_render_action_result("Camp Not Found", "This camp does not exist.", False),
                status_code=404,
            )

        if camp["action_token"] != token:
            return HTMLResponse(
                content=_render_action_result("Invalid Token", "The approval link is invalid or expired.", False),
                status_code=403,
            )

        if camp["verified"]:
            return HTMLResponse(
                content=_render_action_result("Already Approved", f"The camp '{camp['name']}' has already been approved.", True),
            )

        # Approve the camp
        conn.execute(
            "UPDATE camps SET verified = 1, updated_at = ? WHERE id = ?",
            (datetime.utcnow().isoformat(), camp_id),
        )

    # Send approval email to submitter
    try:
        send_camp_approved_email(
            to_email=camp["submitted_email"],
            camp_name=camp["name"],
            camp_id=camp_id,
            state=camp["state"],
        )
    except Exception as e:
        logger.error(f"Failed to send approval email: {e}")

    return HTMLResponse(
        content=_render_action_result(
            "Camp Approved!",
            f"The camp '{camp['name']}' is now live on best.football. An email notification has been sent to {camp['submitted_email']}.",
            True,
        ),
    )


@app.get("/api/camps/{camp_id}/reject")
async def reject_camp(
    camp_id: str,
    token: str = Query(..., description="Security token for rejection"),
):
    """Reject a camp submission. Called from admin email link."""
    with get_db() as conn:
        # Fetch camp and validate token
        camp = conn.execute(
            "SELECT * FROM camps WHERE id = ?",
            (camp_id,),
        ).fetchone()

        if not camp:
            return HTMLResponse(
                content=_render_action_result("Camp Not Found", "This camp does not exist or has already been deleted.", False),
                status_code=404,
            )

        if camp["action_token"] != token:
            return HTMLResponse(
                content=_render_action_result("Invalid Token", "The rejection link is invalid or expired.", False),
                status_code=403,
            )

        submitter_email = camp["submitted_email"]
        camp_name = camp["name"]

        # Delete the camp
        conn.execute("DELETE FROM camps WHERE id = ?", (camp_id,))

    # Send rejection email to submitter
    try:
        send_camp_rejected_email(
            to_email=submitter_email,
            camp_name=camp_name,
        )
    except Exception as e:
        logger.error(f"Failed to send rejection email: {e}")

    return HTMLResponse(
        content=_render_action_result(
            "Camp Rejected",
            f"The camp '{camp_name}' has been removed. An email notification has been sent to {submitter_email}.",
            True,
        ),
    )


def _render_action_result(title: str, message: str, success: bool) -> str:
    """Render HTML result page for approve/reject actions."""
    color = "#27ae60" if success else "#e74c3c"
    icon = "&#10003;" if success else "&#10007;"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} | best.football</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            margin: 0;
            background: #f5f5f5;
        }}
        .card {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 40px;
            text-align: center;
            max-width: 500px;
        }}
        .icon {{
            font-size: 60px;
            color: {color};
            margin-bottom: 20px;
        }}
        h1 {{
            color: #333;
            margin-bottom: 15px;
        }}
        p {{
            color: #666;
            line-height: 1.6;
        }}
        a {{
            color: #2d5a27;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <div class="card">
        <div class="icon">{icon}</div>
        <h1>{title}</h1>
        <p>{message}</p>
        <p style="margin-top: 30px;"><a href="https://best.football">&larr; Back to best.football</a></p>
    </div>
</body>
</html>"""
