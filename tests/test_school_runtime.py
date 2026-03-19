import json
import textwrap
from pathlib import Path

from scrapers.schools import runtime


def _write_scraper(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


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


def test_run_scraper_file_scopes_selected_profile_into_default_helpers(tmp_path, monkeypatch):
    scraper_path = _write_scraper(
        tmp_path / "profiled_scraper.py",
        """
        import os

        from scrapers.schools.runtime import assert_not_blocklisted, require_proxy_credentials


        async def scrape_school():
            require_proxy_credentials()
            assert_not_blocklisted(["https://blocked.example/path"])
            return {
                "nces_id": "1",
                "school_name": "Alpha High",
                "state": "CA",
                "source_pages": ["https://blocked.example/path"],
                "extracted_items": {"proxy_profile": os.environ.get("OXYLABS_PROXY_PROFILE")},
                "scrape_meta": {},
                "errors": [],
            }
        """,
    )

    original = runtime.BLOCKLIST_FILE_BY_PROFILE.copy()
    runtime.BLOCKLIST_FILE_BY_PROFILE.update(
        {
            "mobile": tmp_path / "mobile.json",
            "datacenter": tmp_path / "datacenter.json",
        }
    )
    runtime.BLOCKLIST_FILE_BY_PROFILE["mobile"].write_text(json.dumps({"domains": []}), encoding="utf-8")
    runtime.BLOCKLIST_FILE_BY_PROFILE["datacenter"].write_text(
        json.dumps({"domains": ["blocked.example"]}),
        encoding="utf-8",
    )
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "https://dc-proxy.example:7777")
    monkeypatch.setenv("OXYLABS_DATACENTER_USERNAME", "dc-user")
    monkeypatch.setenv("OXYLABS_DATACENTER_PASSWORD", "dc-pass")

    try:
        try:
            runtime.run_scraper_file_sync(scraper_path, profile="datacenter")
        except runtime.BlocklistedDomainError:
            pass
        else:
            raise AssertionError("Expected BlocklistedDomainError for datacenter blocklist")

        run = runtime.run_scraper_file_sync(scraper_path, profile="mobile")
        assert run.payload["extracted_items"]["proxy_profile"] == "mobile"
    finally:
        runtime.BLOCKLIST_FILE_BY_PROFILE.update(original)


def test_run_scraper_file_patches_legacy_proxy_launch_for_mobile_profile(tmp_path, monkeypatch):
    scraper_path = _write_scraper(
        tmp_path / "legacy_scraper.py",
        """
        import os


        CAPTURED_PROXY = {}
        PROXY_SERVER = "ddc.oxylabs.io:8001"
        PROXY_USERNAME = os.environ.get("OXYLABS_USERNAME")
        PROXY_PASSWORD = os.environ.get("OXYLABS_PASSWORD")


        class _FakeBrowser:
            async def close(self):
                return None


        class _FakeBrowserType:
            async def launch(self, **kwargs):
                global CAPTURED_PROXY
                CAPTURED_PROXY = dict(kwargs["proxy"])
                return _FakeBrowser()


        class _FakePlaywright:
            def __init__(self):
                self.chromium = _FakeBrowserType()


        class _FakeAsyncPlaywright:
            async def __aenter__(self):
                return _FakePlaywright()

            async def __aexit__(self, exc_type, exc, tb):
                return False


        def async_playwright():
            return _FakeAsyncPlaywright()


        async def scrape_school():
            if not PROXY_USERNAME or not PROXY_PASSWORD:
                raise ValueError("Oxylabs credentials not set")

            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(
                    proxy={
                        "server": PROXY_SERVER,
                        "username": PROXY_USERNAME,
                        "password": PROXY_PASSWORD,
                    },
                    headless=True,
                )
                await browser.close()

            return {
                "nces_id": "2",
                "school_name": "Legacy High",
                "state": "CA",
                "source_pages": ["https://legacy.example"],
                "extracted_items": {
                    "legacy_server": PROXY_SERVER,
                    "legacy_username": PROXY_USERNAME,
                    "legacy_password": PROXY_PASSWORD,
                    "captured_proxy": CAPTURED_PROXY,
                },
                "scrape_meta": {},
                "errors": [],
            }
        """,
    )

    monkeypatch.delenv("OXYLABS_USERNAME", raising=False)
    monkeypatch.delenv("OXYLABS_PASSWORD", raising=False)
    monkeypatch.setenv("OXYLABS_MOBILE_PROXY_SERVER", "https://pr.oxylabs.io:7777")
    monkeypatch.delenv("OXYLABS_MOBILE_USERNAME", raising=False)
    monkeypatch.delenv("OXYLABS_MOBILE_PASSWORD", raising=False)

    run = runtime.run_scraper_file_sync(scraper_path, profile="mobile")
    extracted = run.payload["extracted_items"]

    assert extracted["legacy_server"] == "https://pr.oxylabs.io:7777"
    assert extracted["legacy_username"] == runtime.LEGACY_IP_WHITELIST_SENTINEL
    assert extracted["legacy_password"] == runtime.LEGACY_IP_WHITELIST_SENTINEL
    assert extracted["captured_proxy"] == {"server": "https://pr.oxylabs.io:7777"}
