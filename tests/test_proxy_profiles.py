import os

from pipeline import proxy


def test_get_proxy_profile_defaults_and_override(monkeypatch):
    monkeypatch.delenv("OXYLABS_PROXY_PROFILE", raising=False)
    monkeypatch.delenv("OXYLABS_DATACENTER_PROXY_SERVER", raising=False)
    assert proxy.get_proxy_profile() == "datacenter"

    monkeypatch.setenv("OXYLABS_PROXY_PROFILE", "datacenter")
    assert proxy.get_proxy_profile() == "datacenter"


def test_get_proxy_profile_invalid_value(monkeypatch):
    monkeypatch.setenv("OXYLABS_PROXY_PROFILE", "legacy")

    try:
        proxy.get_proxy_profile()
    except ValueError as exc:
        assert "Unknown proxy profile" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_mobile_proxy_defaults_to_mobile_gateway(monkeypatch):
    monkeypatch.delenv("OXYLABS_PROXY_PROFILE", raising=False)
    monkeypatch.delenv("OXYLABS_MOBILE_PROXY_SERVER", raising=False)

    assert proxy.get_proxy_servers("mobile") == ("https://pr.oxylabs.io:7777",)


def test_datacenter_proxy_resolution_from_env(monkeypatch):
    monkeypatch.setenv("OXYLABS_PROXY_PROFILE", "datacenter")
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "dc-1.example:10001,dc-2.example:10002\n")

    assert proxy.get_proxy_servers("datacenter") == (
        "http://dc-1.example:10001",
        "http://dc-2.example:10002",
    )
    assert proxy.get_proxy_server(profile="datacenter", proxy_index=1) == "http://dc-2.example:10002"


def test_datacenter_proxy_missing_without_profile_env_raises(monkeypatch):
    monkeypatch.delenv("OXYLABS_PROXY_PROFILE", raising=False)
    monkeypatch.delenv("OXYLABS_DATACENTER_PROXY_SERVER", raising=False)

    try:
        proxy.get_proxy_servers("datacenter")
    except ValueError as exc:
        assert "No proxy servers configured for profile 'datacenter'" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_profile_auth_mode_uses_profile_credentials(monkeypatch):
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "https://dc-gateway.example")
    monkeypatch.setenv("OXYLABS_DATACENTER_USERNAME", "dc-user")
    monkeypatch.setenv("OXYLABS_DATACENTER_PASSWORD", "dc-pass")

    assert proxy.get_proxy_auth(profile="datacenter") == ("dc-user", "dc-pass")
    assert proxy.get_proxy_auth_mode(profile="datacenter") == "credentials"
    assert proxy.get_httpx_proxy_url(profile="datacenter") == "https://dc-user:dc-pass@dc-gateway.example"


def test_profile_auth_mode_defaults_to_ip_whitelist_when_missing(monkeypatch):
    monkeypatch.setenv("OXYLABS_PROXY_PROFILE", "mobile")
    monkeypatch.setenv("OXYLABS_MOBILE_PROXY_SERVER", "https://dc.example:9999")
    monkeypatch.delenv("OXYLABS_MOBILE_USERNAME", raising=False)
    monkeypatch.delenv("OXYLABS_MOBILE_PASSWORD", raising=False)

    assert proxy.get_proxy_auth_mode(profile="mobile") == "ip_whitelist"
    assert proxy.get_httpx_proxy_url(profile="mobile") == "https://dc.example:9999"


def test_browser_proxy_env_includes_proxy_profile(monkeypatch):
    monkeypatch.setenv("OXYLABS_PROXY_PROFILE", "datacenter")
    monkeypatch.setenv("OXYLABS_DATACENTER_PROXY_SERVER", "https://dc-gateway.example")
    monkeypatch.setenv("OXYLABS_DATACENTER_USERNAME", "dc-user")
    monkeypatch.setenv("OXYLABS_DATACENTER_PASSWORD", "dc-pass")

    env = proxy.get_browser_proxy_env(profile="datacenter")
    assert env["OXYLABS_PROXY_PROFILE"] == "datacenter"
    assert env["HTTP_PROXY"] == "https://dc-user:dc-pass@dc-gateway.example"
    assert env["HTTPS_PROXY"] == "https://dc-user:dc-pass@dc-gateway.example"
