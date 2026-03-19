"""Queue and lifecycle helpers for per-school deterministic scrapers."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

from pipeline.database import get_connection, get_db

STATUS_PENDING = "pending"
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETE = "complete"
STATUS_BLOCKED = "blocked"
STATUS_RESTRICTED = "restricted"
STATUS_FAILED = "failed"
STATUS_NEEDS_REPAIR = "needs_repair"

ALL_STATUSES = {
    STATUS_PENDING,
    STATUS_IN_PROGRESS,
    STATUS_COMPLETE,
    STATUS_BLOCKED,
    STATUS_RESTRICTED,
    STATUS_FAILED,
    STATUS_NEEDS_REPAIR,
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _recheck_at(days: int = 182) -> str:
    return (datetime.now() + timedelta(days=days)).isoformat(timespec="seconds")


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def init_tables() -> None:
    """Create/upgrade scraper queue tables."""
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS school_scraper_status (
                nces_id TEXT PRIMARY KEY REFERENCES schools(nces_id),
                status TEXT NOT NULL DEFAULT 'pending',
                scraper_file TEXT,
                started_at TEXT,
                completed_at TEXT,
                last_success_at TEXT,
                last_failure_at TEXT,
                failure_reason TEXT,
                notes TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                next_recheck_at TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        columns = _table_columns(conn, "school_scraper_status")
        desired = {
            "started_at": "TEXT",
            "completed_at": "TEXT",
            "last_success_at": "TEXT",
            "last_failure_at": "TEXT",
            "failure_reason": "TEXT",
            "notes": "TEXT",
            "attempts": "INTEGER NOT NULL DEFAULT 0",
            "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
            "next_recheck_at": "TEXT",
            "updated_at": "TEXT",
            "scraper_file": "TEXT",
            "status": "TEXT NOT NULL DEFAULT 'pending'",
        }
        for name, type_expr in desired.items():
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE school_scraper_status ADD COLUMN {name} {type_expr}"
                )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS school_scrape_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nces_id TEXT NOT NULL REFERENCES schools(nces_id),
                status TEXT NOT NULL,
                script_path TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                error_message TEXT,
                output_json TEXT
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_school_scraper_status_status ON school_scraper_status(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_school_scraper_status_recheck ON school_scraper_status(next_recheck_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_school_scrape_runs_nces ON school_scrape_runs(nces_id, ended_at)"
        )


def seed_queue(state: str | None = None, limit: int | None = None) -> int:
    """Insert pending queue rows for schools with websites missing status rows."""
    init_tables()
    with get_db() as conn:
        params: list[object] = []
        where_parts = ["s.website IS NOT NULL", "TRIM(s.website) != ''"]
        if state:
            where_parts.append("s.state = ?")
            params.append(state.upper())

        limit_clause = ""
        if limit is not None and limit > 0:
            limit_clause = " LIMIT ?"
            params.append(limit)

        rows = conn.execute(
            f"""
            SELECT s.nces_id
            FROM schools s
            LEFT JOIN school_scraper_status q ON q.nces_id = s.nces_id
            WHERE {' AND '.join(where_parts)}
              AND q.nces_id IS NULL
            ORDER BY s.state, s.name
            {limit_clause}
            """,
            tuple(params),
        ).fetchall()

        if not rows:
            return 0

        now = _now()
        conn.executemany(
            """
            INSERT INTO school_scraper_status (nces_id, status, updated_at)
            VALUES (?, ?, ?)
            """,
            [(row["nces_id"], STATUS_PENDING, now) for row in rows],
        )
        return len(rows)


def _status_list(statuses: Iterable[str]) -> tuple[str, ...]:
    values = tuple(statuses)
    if not values:
        raise ValueError("At least one status is required")
    for value in values:
        if value not in ALL_STATUSES:
            raise ValueError(f"Unknown status: {value}")
    return values


def get_next_batch(
    count: int = 10,
    state: str | None = None,
    statuses: Iterable[str] = (STATUS_PENDING,),
) -> list[dict]:
    """Get the next schools matching queue states."""
    init_tables()
    seed_queue(state=state)
    status_values = _status_list(statuses)

    with get_db() as conn:
        params: list[object] = list(status_values)
        where_parts = [f"q.status IN ({','.join('?' * len(status_values))})"]
        where_parts.extend(["s.website IS NOT NULL", "TRIM(s.website) != ''"])

        if STATUS_BLOCKED in status_values:
            where_parts.append("(q.next_recheck_at IS NULL OR q.next_recheck_at <= ?)")
            params.append(_now())

        if state:
            where_parts.append("s.state = ?")
            params.append(state.upper())

        params.append(max(1, count))

        rows = conn.execute(
            f"""
            SELECT s.nces_id, s.name, s.website, s.city, s.state,
                   q.status, q.scraper_file, q.failure_reason, q.attempts,
                   q.consecutive_failures, q.next_recheck_at
            FROM schools s
            JOIN school_scraper_status q ON q.nces_id = s.nces_id
            WHERE {' AND '.join(where_parts)}
            ORDER BY s.state, s.name
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

        return [dict(row) for row in rows]


def claim_next_school(
    state: str | None = None,
    statuses: Iterable[str] = (STATUS_PENDING,),
) -> Optional[dict]:
    """Atomically claim one school and set it to in_progress."""
    init_tables()
    seed_queue(state=state)
    status_values = _status_list(statuses)

    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")

        params: list[object] = list(status_values)
        where_parts = [f"q.status IN ({','.join('?' * len(status_values))})"]
        where_parts.extend(["s.website IS NOT NULL", "TRIM(s.website) != ''"])

        if STATUS_BLOCKED in status_values:
            where_parts.append("(q.next_recheck_at IS NULL OR q.next_recheck_at <= ?)")
            params.append(_now())

        if state:
            where_parts.append("s.state = ?")
            params.append(state.upper())

        row = conn.execute(
            f"""
            SELECT s.nces_id, s.name, s.website, s.city, s.state,
                   q.status, q.scraper_file, q.failure_reason,
                   q.attempts, q.consecutive_failures
            FROM schools s
            JOIN school_scraper_status q ON q.nces_id = s.nces_id
            WHERE {' AND '.join(where_parts)}
            ORDER BY s.state, s.name
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()

        if not row:
            conn.commit()
            return None

        now = _now()
        conn.execute(
            """
            UPDATE school_scraper_status
            SET status = ?,
                started_at = ?,
                attempts = COALESCE(attempts, 0) + 1,
                updated_at = ?
            WHERE nces_id = ?
            """,
            (STATUS_IN_PROGRESS, now, now, row["nces_id"]),
        )
        conn.commit()

        claimed = dict(row)
        claimed["status"] = STATUS_IN_PROGRESS
        return claimed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def claim_school(nces_id: str) -> Optional[dict]:
    """Atomically claim a specific school by NCES ID."""
    init_tables()
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT s.nces_id, s.name, s.website, s.city, s.state,
                   q.status, q.scraper_file, q.failure_reason,
                   q.attempts, q.consecutive_failures
            FROM schools s
            JOIN school_scraper_status q ON q.nces_id = s.nces_id
            WHERE s.nces_id = ?
            LIMIT 1
            """,
            (nces_id,),
        ).fetchone()

        if not row:
            conn.commit()
            return None

        now = _now()
        conn.execute(
            """
            UPDATE school_scraper_status
            SET status = ?,
                started_at = ?,
                attempts = COALESCE(attempts, 0) + 1,
                updated_at = ?
            WHERE nces_id = ?
            """,
            (STATUS_IN_PROGRESS, now, now, nces_id),
        )
        conn.commit()

        claimed = dict(row)
        claimed["status"] = STATUS_IN_PROGRESS
        return claimed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_status(
    nces_id: str,
    status: str,
    *,
    scraper_file: str | None = None,
    reason: str | None = None,
    notes: str | None = None,
    reset_failures: bool = False,
    blocked_recheck_days: int = 182,
) -> None:
    """Upsert a queue status row with lifecycle-aware defaults."""
    if status not in ALL_STATUSES:
        raise ValueError(f"Unknown status: {status}")

    init_tables()

    now = _now()
    with get_db() as conn:
        existing = conn.execute(
            "SELECT attempts, consecutive_failures FROM school_scraper_status WHERE nces_id = ?",
            (nces_id,),
        ).fetchone()

        attempts = existing["attempts"] if existing else 0
        failures = existing["consecutive_failures"] if existing else 0

        completed_at = now if status in {STATUS_COMPLETE, STATUS_BLOCKED, STATUS_RESTRICTED, STATUS_FAILED} else None
        last_success_at = now if status == STATUS_COMPLETE else None
        last_failure_at = now if status in {STATUS_FAILED, STATUS_NEEDS_REPAIR} else None
        next_recheck_at = _recheck_at(blocked_recheck_days) if status == STATUS_BLOCKED else None

        if reset_failures or status == STATUS_COMPLETE:
            failures = 0

        conn.execute(
            """
            INSERT INTO school_scraper_status (
                nces_id, status, scraper_file, started_at, completed_at,
                last_success_at, last_failure_at, failure_reason, notes,
                attempts, consecutive_failures, next_recheck_at, updated_at
            ) VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(nces_id) DO UPDATE SET
                status = excluded.status,
                scraper_file = COALESCE(excluded.scraper_file, school_scraper_status.scraper_file),
                completed_at = excluded.completed_at,
                last_success_at = COALESCE(excluded.last_success_at, school_scraper_status.last_success_at),
                last_failure_at = COALESCE(excluded.last_failure_at, school_scraper_status.last_failure_at),
                failure_reason = excluded.failure_reason,
                notes = COALESCE(excluded.notes, school_scraper_status.notes),
                attempts = school_scraper_status.attempts,
                consecutive_failures = excluded.consecutive_failures,
                next_recheck_at = excluded.next_recheck_at,
                updated_at = excluded.updated_at
            """,
            (
                nces_id,
                status,
                scraper_file,
                completed_at,
                last_success_at,
                last_failure_at,
                reason,
                notes,
                attempts,
                failures,
                next_recheck_at,
                now,
            ),
        )


def mark_complete(nces_id: str, scraper_file: str) -> None:
    upsert_status(
        nces_id,
        STATUS_COMPLETE,
        scraper_file=scraper_file,
        reason=None,
        reset_failures=True,
    )


def mark_blocked(nces_id: str, reason: str, blocked_recheck_days: int = 182) -> None:
    upsert_status(
        nces_id,
        STATUS_BLOCKED,
        reason=reason,
        blocked_recheck_days=blocked_recheck_days,
    )


def mark_restricted(
    nces_id: str,
    reason: str,
    blocked_recheck_days: int = 182,
) -> None:
    upsert_status(
        nces_id,
        STATUS_RESTRICTED,
        reason=reason,
        blocked_recheck_days=blocked_recheck_days,
    )


def mark_failed(nces_id: str, reason: str, notes: str | None = None) -> None:
    init_tables()
    now = _now()

    with get_db() as conn:
        row = conn.execute(
            "SELECT attempts, consecutive_failures FROM school_scraper_status WHERE nces_id = ?",
            (nces_id,),
        ).fetchone()
        attempts = row["attempts"] if row else 0
        failures = (row["consecutive_failures"] if row else 0) + 1
        status = STATUS_NEEDS_REPAIR if failures >= 2 else STATUS_FAILED

        conn.execute(
            """
            INSERT INTO school_scraper_status (
                nces_id, status, completed_at, last_failure_at,
                failure_reason, notes, attempts, consecutive_failures,
                next_recheck_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(nces_id) DO UPDATE SET
                status = excluded.status,
                completed_at = excluded.completed_at,
                last_failure_at = excluded.last_failure_at,
                failure_reason = excluded.failure_reason,
                notes = COALESCE(excluded.notes, school_scraper_status.notes),
                attempts = school_scraper_status.attempts,
                consecutive_failures = excluded.consecutive_failures,
                updated_at = excluded.updated_at
            """,
            (
                nces_id,
                status,
                now,
                now,
                reason,
                notes,
                attempts,
                failures,
                now,
            ),
        )


def mark_needs_repair(nces_id: str, reason: str | None = None) -> None:
    init_tables()
    now = _now()
    with get_db() as conn:
        row = conn.execute(
            "SELECT attempts, consecutive_failures FROM school_scraper_status WHERE nces_id = ?",
            (nces_id,),
        ).fetchone()
        attempts = row["attempts"] if row else 0
        failures = max((row["consecutive_failures"] if row else 0), 2)
        conn.execute(
            """
            INSERT INTO school_scraper_status (
                nces_id, status, completed_at, last_failure_at,
                failure_reason, attempts, consecutive_failures,
                next_recheck_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT(nces_id) DO UPDATE SET
                status = excluded.status,
                completed_at = excluded.completed_at,
                last_failure_at = excluded.last_failure_at,
                failure_reason = COALESCE(excluded.failure_reason, school_scraper_status.failure_reason),
                attempts = school_scraper_status.attempts,
                consecutive_failures = excluded.consecutive_failures,
                updated_at = excluded.updated_at
            """,
            (nces_id, STATUS_NEEDS_REPAIR, now, now, reason, attempts, failures, now),
        )


def requeue_due_blocked(limit: int | None = None) -> int:
    """Move blocked schools due for recheck back to pending."""
    init_tables()
    with get_db() as conn:
        params: list[object] = [_now()]
        limit_clause = ""
        if limit and limit > 0:
            limit_clause = " LIMIT ?"
            params.append(limit)

        rows = conn.execute(
            f"""
            SELECT nces_id
            FROM school_scraper_status
            WHERE status = ?
              AND next_recheck_at IS NOT NULL
              AND next_recheck_at <= ?
            ORDER BY next_recheck_at
            {limit_clause}
            """,
            (STATUS_BLOCKED, *params),
        ).fetchall()

        if not rows:
            return 0

        now = _now()
        conn.executemany(
            """
            UPDATE school_scraper_status
            SET status = ?,
                failure_reason = NULL,
                next_recheck_at = NULL,
                updated_at = ?
            WHERE nces_id = ?
            """,
            [(STATUS_PENDING, now, row["nces_id"]) for row in rows],
        )

        return len(rows)


def clear_blocked(
    *,
    state: str | None = None,
    limit: int | None = None,
) -> int:
    """Move blocked schools back to pending regardless of recheck date."""
    init_tables()
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        params: list[object] = [STATUS_BLOCKED]
        where = ["q.status = ?"]
        if state:
            where.append("s.state = ?")
            params.append(state.upper())

        limit_clause = ""
        if limit and limit > 0:
            limit_clause = " LIMIT ?"
            params.append(limit)

        rows = conn.execute(
            f"""
            SELECT q.nces_id
            FROM school_scraper_status q
            JOIN schools s ON s.nces_id = q.nces_id
            WHERE {' AND '.join(where)}
            ORDER BY s.state, s.name
            {limit_clause}
            """,
            tuple(params),
        ).fetchall()

        if not rows:
            conn.commit()
            return 0

        now = _now()
        conn.executemany(
            """
            UPDATE school_scraper_status
            SET status = ?,
                failure_reason = NULL,
                notes = NULL,
                completed_at = NULL,
                next_recheck_at = NULL,
                updated_at = ?
            WHERE nces_id = ?
            """,
            [(STATUS_PENDING, now, row["nces_id"]) for row in rows],
        )
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def add_scrape_run(
    nces_id: str,
    status: str,
    *,
    script_path: str,
    started_at: str,
    ended_at: str,
    error_message: str | None = None,
    payload: dict | None = None,
) -> None:
    """Append one runtime scrape result row."""
    init_tables()
    output_json = json.dumps(payload, ensure_ascii=True) if payload is not None else None

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO school_scrape_runs (
                nces_id, status, script_path, started_at, ended_at,
                error_message, output_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                nces_id,
                status,
                script_path,
                started_at,
                ended_at,
                error_message,
                output_json,
            ),
        )


def get_complete_rows(
    state: str | None = None,
    limit: int | None = None,
    statuses: Iterable[str] = (STATUS_COMPLETE, STATUS_FAILED),
) -> list[dict]:
    """Get queue rows eligible for production scraping."""
    init_tables()
    status_values = _status_list(statuses)
    with get_db() as conn:
        params: list[object] = list(status_values)
        where = [
            f"q.status IN ({','.join('?' * len(status_values))})",
            "q.scraper_file IS NOT NULL",
            "TRIM(q.scraper_file) != ''",
        ]
        if state:
            where.append("s.state = ?")
            params.append(state.upper())

        limit_clause = ""
        if limit and limit > 0:
            limit_clause = " LIMIT ?"
            params.append(limit)

        rows = conn.execute(
            f"""
            SELECT s.nces_id, s.name, s.state, s.city, s.website,
                   q.scraper_file, q.consecutive_failures, q.failure_reason
            FROM school_scraper_status q
            JOIN schools s ON s.nces_id = q.nces_id
            WHERE {' AND '.join(where)}
            ORDER BY s.state, s.name
            {limit_clause}
            """,
            tuple(params),
        ).fetchall()
        return [dict(row) for row in rows]


def get_status_report(state: str | None = None) -> dict:
    """Get queue status counts and progress."""
    init_tables()
    with get_db() as conn:
        base_where = ["website IS NOT NULL", "TRIM(website) != ''"]
        joined_where = ["s.website IS NOT NULL", "TRIM(s.website) != ''"]
        params: list[object] = []
        if state:
            base_where.append("state = ?")
            joined_where.append("s.state = ?")
            params.append(state.upper())

        total = conn.execute(
            f"SELECT COUNT(*) FROM schools WHERE {' AND '.join(base_where)}",
            tuple(params),
        ).fetchone()[0]

        status_rows = conn.execute(
            f"""
            SELECT q.status, COUNT(*) as count
            FROM school_scraper_status q
            JOIN schools s ON s.nces_id = q.nces_id
            WHERE {' AND '.join(joined_where)}
            GROUP BY q.status
            """,
            tuple(params),
        ).fetchall()

        counts = {status: 0 for status in sorted(ALL_STATUSES)}
        for row in status_rows:
            counts[row["status"]] = row["count"]

        known = sum(counts.values())
        pending = total - known + counts[STATUS_PENDING]
        counts[STATUS_PENDING] = max(0, pending)

        return {
            "total_schools": total,
            **counts,
            "progress_complete_pct": (counts[STATUS_COMPLETE] / total * 100.0) if total else 0.0,
        }


def get_school(nces_id: str) -> Optional[dict]:
    """Get one school row by NCES ID."""
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT nces_id, name, website, city, state
            FROM schools
            WHERE nces_id = ?
            """,
            (nces_id,),
        ).fetchone()
        return dict(row) if row else None


def resolve_script_path(project_root: Path, nces_id: str, state: str) -> Path:
    """Build deterministic script path for a school."""
    return project_root / "scrapers" / "schools" / state.lower() / f"{nces_id}.py"
