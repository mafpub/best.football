import json
from pathlib import Path

from scrapers.schools import runtime


def test_normalize_legacy_payload():
    raw = {
        "nces_id": "1",
        "school_name": "Alpha High",
        "state": "ca",
        "athletic_url": "alpha.edu/athletics",
        "sports": ["Football"],
        "scraped_at": "2026-01-01T00:00:00",
    }

    payload = runtime.normalize_payload(raw)
    assert set(runtime.REQUIRED_KEYS).issubset(payload.keys())
    assert payload["state"] == "CA"
    assert payload["source_pages"]
    assert "sports" in payload["extracted_items"]


def test_validate_payload_missing_keys():
    errors = runtime.validate_payload({"nces_id": "1"})
    assert errors
    assert "Missing required keys" in errors[0]


def test_blocklist_enforced(tmp_path):
    blocklist = tmp_path / "blocklist.json"
    blocklist.write_text(json.dumps({"domains": ["blocked.example"]}), encoding="utf-8")

    original = runtime.BLOCKLIST_FILE
    runtime.BLOCKLIST_FILE = blocklist
    try:
        try:
            runtime.assert_not_blocklisted(["https://blocked.example/path"])
        except runtime.BlocklistedDomainError:
            pass
        else:
            raise AssertionError("Expected BlocklistedDomainError")
    finally:
        runtime.BLOCKLIST_FILE = original
