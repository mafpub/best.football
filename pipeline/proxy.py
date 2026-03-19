"""Shared Oxylabs proxy configuration helpers."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote, urlsplit

from pipeline.env import load_repo_env

load_repo_env()

DEFAULT_MOBILE_PROXY_SERVER = "https://pr.oxylabs.io:7777"
DEFAULT_PROFILE_CONFIGS = {
    "mobile": {
        "server": "OXYLABS_MOBILE_PROXY_SERVER",
        "username": "OXYLABS_MOBILE_USERNAME",
        "password": "OXYLABS_MOBILE_PASSWORD",
    },
    "datacenter": {
        "server": "OXYLABS_DATACENTER_PROXY_SERVER",
        "username": "OXYLABS_DATACENTER_USERNAME",
        "password": "OXYLABS_DATACENTER_PASSWORD",
    },
}
PROXY_PROFILE_OPTIONS = {"mobile", "datacenter"}


def get_proxy_profile(profile: str | None = None) -> str:
    """Return active proxy profile from argument or environment."""
    raw = (profile or os.environ.get("OXYLABS_PROXY_PROFILE") or "datacenter").strip().lower()
    if not raw:
        return "datacenter"

    if raw not in PROXY_PROFILE_OPTIONS:
        raise ValueError(
            "Unknown proxy profile. Expected 'mobile' or 'datacenter', "
            f"got: {raw}"
        )

    return raw


def _normalize_proxy_server(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if "://" not in raw:
        return f"http://{raw}"
    return raw


def _split_proxy_servers(raw: str) -> list[str]:
    text = raw.replace("\n", ",")
    servers: list[str] = []
    for value in text.split(","):
        normalized = _normalize_proxy_server(value)
        if normalized:
            servers.append(normalized)
    return servers


def get_proxy_servers(profile: str | None = None) -> tuple[str, ...]:
    """Return configured Oxylabs proxy servers for the active profile."""
    active_profile = get_proxy_profile(profile)
    env_name = DEFAULT_PROFILE_CONFIGS[active_profile]["server"]
    raw = os.environ.get(env_name, "")

    if raw:
        servers = _split_proxy_servers(raw)
        if servers:
            return tuple(servers)

    if active_profile == "mobile":
        return (DEFAULT_MOBILE_PROXY_SERVER,)

    raise ValueError(f"No proxy servers configured for profile '{active_profile}'")


def get_oxylabs_proxy_servers(profile: str | None = None) -> tuple[str, ...]:
    """Compatibility wrapper for existing callers."""
    return get_proxy_servers(profile=profile)


def get_proxy_auth(profile: str | None = None) -> tuple[str | None, str | None]:
    """Return optional Oxylabs username/password credentials for the active profile."""
    active_profile = get_proxy_profile(profile)
    username_env = DEFAULT_PROFILE_CONFIGS[active_profile]["username"]
    password_env = DEFAULT_PROFILE_CONFIGS[active_profile]["password"]

    username = os.environ.get(username_env) or None
    password = os.environ.get(password_env) or None

    # Keep a compatibility path for pre-migration artifacts that still use shared creds.
    if active_profile == "datacenter" and (not username or not password):
        username = username or os.environ.get("OXYLABS_USERNAME")
        password = password or os.environ.get("OXYLABS_PASSWORD")

    return username, password


def get_oxylabs_proxy_auth(profile: str | None = None) -> tuple[str | None, str | None]:
    """Compatibility wrapper for existing callers."""
    return get_proxy_auth(profile=profile)


def get_proxy_auth_mode(profile: str | None = None) -> str:
    """Return proxy auth mode for the active profile."""
    username, password = get_proxy_auth(profile)
    return "credentials" if username and password else "ip_whitelist"


def get_oxylabs_proxy_auth_mode(profile: str | None = None) -> str:
    """Compatibility wrapper for existing callers."""
    return get_proxy_auth_mode(profile=profile)


def get_proxy_server(
    profile: str | None = None,
    proxy_index: int | None = None,
) -> str:
    """Return one proxy server, rotating when index is omitted."""
    servers = get_proxy_servers(profile)
    if not servers:
        raise ValueError("No Oxylabs proxy servers configured")

    if proxy_index is None:
        proxy_index = 0

    return servers[proxy_index % len(servers)]


def get_oxylabs_proxy_server(
    proxy_index: int | None = None,
    profile: str | None = None,
) -> str:
    """Compatibility wrapper for existing callers."""
    return get_proxy_server(profile=profile, proxy_index=proxy_index)


def get_playwright_proxy_config(
    proxy_index: int | None = None,
    profile: str | None = None,
) -> dict[str, str]:
    """Return Playwright proxy settings for Oxylabs."""
    username, password = get_proxy_auth(profile)
    proxy: dict[str, str] = {"server": get_proxy_server(profile=profile, proxy_index=proxy_index)}
    if username and password:
        proxy["username"] = username
        proxy["password"] = password
    return proxy


def get_httpx_proxy_url(proxy_index: int | None = None, profile: str | None = None) -> str:
    """Return an httpx-compatible proxy URL."""
    server = get_proxy_server(profile=profile, proxy_index=proxy_index)
    username, password = get_proxy_auth(profile)
    if not username or not password:
        return server

    parsed = urlsplit(server)
    netloc = parsed.netloc or parsed.path
    scheme = parsed.scheme or "http"
    auth = f"{quote(username, safe='')}:{quote(password, safe='')}"
    return f"{scheme}://{auth}@{netloc}"


def get_browser_proxy_env(proxy_index: int | None = None, profile: str | None = None) -> dict[str, str]:
    """Return proxy env vars for browser/CLI launcher subprocesses."""
    active_profile = get_proxy_profile(profile)
    proxy_url = get_httpx_proxy_url(proxy_index=proxy_index, profile=profile)
    return {
        "OXYLABS_PROXY_PROFILE": active_profile,
        "HTTP_PROXY": proxy_url,
        "HTTPS_PROXY": proxy_url,
        "ALL_PROXY": proxy_url,
        "http_proxy": proxy_url,
        "https_proxy": proxy_url,
        "all_proxy": proxy_url,
        # Keep local control-plane traffic direct.
        "NO_PROXY": "127.0.0.1,localhost,::1",
        "no_proxy": "127.0.0.1,localhost,::1",
    }


def require_oxylabs_proxy_configuration(profile: str | None = None) -> None:
    """Fail fast when active profile proxy configuration is incomplete."""
    profile_name = get_proxy_profile(profile)
    get_proxy_servers(profile_name)
    get_proxy_auth(profile_name)


def describe_proxy_mode(profile: str | None = None) -> dict[str, Any]:
    """Return lightweight metadata for prompts and diagnostics."""
    profile_name = get_proxy_profile(profile)
    servers = list(get_proxy_servers(profile_name))
    auth_mode = get_proxy_auth_mode(profile_name)

    return {
        "profile": profile_name,
        "servers": servers,
        "auth_mode": auth_mode,
    }


def describe_oxylabs_proxy_mode(profile: str | None = None) -> dict[str, Any]:
    """Backward-compatible metadata function name used by existing agents."""
    return describe_proxy_mode(profile=profile)
