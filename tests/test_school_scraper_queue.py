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


def test_seed_and_claim(tmp_path):
    _init_minimal_schema(tmp_path)

    inserted = queue.seed_queue()
    assert inserted == 2

    batch = queue.get_next_batch(count=10)
    assert len(batch) == 2

    claimed = queue.claim_next_school()
    assert claimed is not None
    assert claimed["status"] == queue.STATUS_IN_PROGRESS

    report = queue.get_status_report()
    assert report[queue.STATUS_IN_PROGRESS] == 1


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
