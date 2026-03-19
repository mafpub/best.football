from pathlib import Path

import pipeline.database as db
from pipeline import school_scraper_queue as queue


def _init_minimal_schema(tmp_path: Path):
    db.DB_PATH = tmp_path / "test.db"
    with db.get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS schools (
                nces_id TEXT PRIMARY KEY,
                name TEXT,
                city TEXT,
                state TEXT,
                website TEXT
            );
            """
        )
        conn.executemany(
            "INSERT INTO schools (nces_id, name, city, state, website) VALUES (?, ?, ?, ?, ?)",
            [
                ("1", "Alpha", "A", "CA", "alpha.edu"),
                ("2", "Beta", "B", "CA", "beta.edu"),
                ("3", "Gamma", "C", "TX", None),
            ],
        )


def _seed_probe_results():
    with db.get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS school_website_probe_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                proxy_profile TEXT NOT NULL,
                probe_strategy TEXT NOT NULL,
                state TEXT,
                target_limit INTEGER,
                total_targets INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS school_website_probe_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL,
                nces_id TEXT NOT NULL,
                school_name TEXT NOT NULL,
                state TEXT NOT NULL,
                website TEXT NOT NULL,
                normalized_url TEXT NOT NULL,
                probe_method TEXT NOT NULL,
                result TEXT NOT NULL,
                status_code INTEGER,
                redirect_target TEXT,
                error_type TEXT,
                error_message TEXT,
                response_headers_json TEXT,
                checked_at TEXT NOT NULL
            );
            """
        )
        run_id = conn.execute(
            """
            INSERT INTO school_website_probe_runs (
                started_at, completed_at, proxy_profile, probe_strategy, total_targets
            ) VALUES ('2026-03-19T00:00:00+00:00', '2026-03-19T00:10:00+00:00', 'datacenter', 'HEAD_THEN_RANGED_GET', 2)
            """
        ).lastrowid
        conn.executemany(
            """
            INSERT INTO school_website_probe_results (
                run_id, nces_id, school_name, state, website, normalized_url,
                probe_method, result, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'HEAD', ?, '2026-03-19T00:05:00+00:00')
            """,
            [
                (run_id, "1", "Alpha", "CA", "alpha.edu", "https://alpha.edu", "success"),
                (run_id, "2", "Beta", "CA", "beta.edu", "https://beta.edu", "403"),
            ],
        )
    return int(run_id)


def test_seed_and_claim(tmp_path):
    _init_minimal_schema(tmp_path)
    run_id = _seed_probe_results()

    inserted = queue.seed_queue(survey_run_id=run_id)
    assert inserted == 1

    batch = queue.get_next_batch(count=10, survey_run_id=run_id)
    assert len(batch) == 1
    assert batch[0]["nces_id"] == "1"

    claimed = queue.claim_next_school(survey_run_id=run_id)
    assert claimed is not None
    assert claimed["nces_id"] == "1"
    assert claimed["status"] == queue.STATUS_IN_PROGRESS

    report = queue.get_status_report()
    assert report[queue.STATUS_IN_PROGRESS] == 1


def test_claim_school_rejects_ineligible_creator_target(tmp_path):
    _init_minimal_schema(tmp_path)
    run_id = _seed_probe_results()
    queue.seed_queue(survey_run_id=run_id)

    assert queue.claim_school("2", survey_run_id=run_id) is None
    assert queue.is_creator_eligible("1", survey_run_id=run_id) is True
    assert queue.is_creator_eligible("2", survey_run_id=run_id) is False


def test_failure_escalates_to_needs_repair(tmp_path):
    _init_minimal_schema(tmp_path)
    queue.seed_queue()

    queue.upsert_status("1", queue.STATUS_COMPLETE, scraper_file="scrapers/schools/ca/1.py")

    queue.mark_failed("1", "first")
    report = queue.get_status_report()
    assert report[queue.STATUS_FAILED] == 1

    queue.mark_failed("1", "second")
    report = queue.get_status_report()
    assert report[queue.STATUS_NEEDS_REPAIR] == 1


def test_blocked_requeue_due(tmp_path):
    _init_minimal_schema(tmp_path)
    queue.seed_queue()

    queue.mark_blocked("1", "no_athletics", blocked_recheck_days=0)
    moved = queue.requeue_due_blocked()
    assert moved == 1

    row = queue.get_next_batch(count=10, statuses=(queue.STATUS_PENDING,))
    ids = {item["nces_id"] for item in row}
    assert "1" in ids


def test_restricted_does_not_requeue(tmp_path):
    _init_minimal_schema(tmp_path)
    queue.seed_queue()

    queue.mark_restricted("1", "restricted_target", blocked_recheck_days=0)
    moved = queue.requeue_due_blocked()
    assert moved == 0

    report = queue.get_status_report()
    assert report[queue.STATUS_RESTRICTED] == 1
