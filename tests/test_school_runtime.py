import json

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


def test_blocklist_enforced_by_profile_isolation(tmp_path):
    original = runtime.BLOCKLIST_FILE_BY_PROFILE.copy()
    runtime.BLOCKLIST_FILE_BY_PROFILE.update(
        {
            "mobile": tmp_path / "mobile.json",
            "datacenter": tmp_path / "datacenter.json",
        }
    )

    mobile_file = runtime.BLOCKLIST_FILE_BY_PROFILE["mobile"]
    datacenter_file = runtime.BLOCKLIST_FILE_BY_PROFILE["datacenter"]
    mobile_file.write_text(json.dumps({"domains": ["blocked.example"]}), encoding="utf-8")
    datacenter_file.write_text(json.dumps({"domains": []}), encoding="utf-8")

    try:
        try:
            runtime.assert_not_blocklisted(["https://blocked.example/path"], profile="mobile")
        except runtime.BlocklistedDomainError:
            pass
        else:
            raise AssertionError("Expected BlocklistedDomainError")

        runtime.assert_not_blocklisted(["https://blocked.example/path"], profile="datacenter")
    finally:
        runtime.BLOCKLIST_FILE_BY_PROFILE.update(original)


def test_append_blocklist_domain_respects_active_profile(tmp_path):
    original = runtime.BLOCKLIST_FILE_BY_PROFILE.copy()
    runtime.BLOCKLIST_FILE_BY_PROFILE.update(
        {
            "mobile": tmp_path / "mobile.json",
            "datacenter": tmp_path / "datacenter.json",
        }
    )

    try:
        runtime.append_blocklist_domain("https://mobile.only/path", profile="mobile")
        assert runtime.load_blocklist_domains(profile="mobile") == {"mobile.only"}
        assert runtime.load_blocklist_domains(profile="datacenter") == set()
    finally:
        runtime.BLOCKLIST_FILE_BY_PROFILE.update(original)


def test_playwright_proxy_config_defaults_to_mobile_gateway():
    proxy = runtime.get_playwright_proxy_config(profile="mobile", proxy_index=1)
    assert proxy["server"] == "https://pr.oxylabs.io:7777"
    assert "username" not in proxy
    assert "password" not in proxy


def test_playwright_proxy_config_uses_profile_credentials(monkeypatch):
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "https://dc-proxy.example:7777")
    monkeypatch.setenv("OXYLABS_DATACENTER_USERNAME", "user")
    monkeypatch.setenv("OXYLABS_DATACENTER_PASSWORD", "pass")

    proxy = runtime.get_playwright_proxy_config(profile="datacenter", proxy_index=0)

    assert proxy["server"] == "https://dc-proxy.example:7777"
    assert proxy["username"] == "user"
    assert proxy["password"] == "pass"

