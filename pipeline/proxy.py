"""Shared Oxylabs proxy configuration helpers."""

from __future__ import annotations

import itertools
import os
from typing import Any
from urllib.parse import quote, urlsplit

from pipeline.env import load_repo_env

load_repo_env()

DEFAULT_OXYLABS_PROXY_SERVERS = (
    "https://us-pr.oxylabs.io:10001",
    "https://us-pr.oxylabs.io:10002",
    "https://us-pr.oxylabs.io:10003",
)

_PROXY_CURSOR = itertools.count()


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


def get_oxylabs_proxy_servers() -> tuple[str, ...]:
    """Return configured Oxylabs proxy servers, defaulting to the mobile pool."""
    multi = os.environ.get("OXYLABS_PROXY_SERVERS")
    if multi:
        servers = _split_proxy_servers(multi)
        if servers:
            return tuple(servers)

    single = os.environ.get("OXYLABS_PROXY_SERVER")
    if single:
        normalized = _normalize_proxy_server(single)
        if normalized:
            return (normalized,)

    return DEFAULT_OXYLABS_PROXY_SERVERS


def get_oxylabs_proxy_auth() -> tuple[str | None, str | None]:
    """Return optional Oxylabs username/password credentials."""
    username = os.environ.get("OXYLABS_USERNAME") or None
    password = os.environ.get("OXYLABS_PASSWORD") or None
    return username, password


def get_oxylabs_proxy_server(proxy_index: int | None = None) -> str:
    """Return one proxy server, rotating when index is omitted."""
    servers = get_oxylabs_proxy_servers()
    if not servers:
        raise ValueError("No Oxylabs proxy servers configured")

    index = next(_PROXY_CURSOR) if proxy_index is None else proxy_index
    return servers[index % len(servers)]


def get_playwright_proxy_config(proxy_index: int | None = None) -> dict[str, str]:
    """Return Playwright proxy settings for Oxylabs."""
    username, password = get_oxylabs_proxy_auth()
    proxy: dict[str, str] = {"server": get_oxylabs_proxy_server(proxy_index)}
    if username and password:
        proxy["username"] = username
        proxy["password"] = password
    return proxy


def get_httpx_proxy_url(proxy_index: int | None = None) -> str:
    """Return an httpx-compatible proxy URL."""
    server = get_oxylabs_proxy_server(proxy_index)
    username, password = get_oxylabs_proxy_auth()
    if not username or not password:
        return server

    parsed = urlsplit(server)
    netloc = parsed.netloc or parsed.path
    scheme = parsed.scheme or "http"
    auth = f"{quote(username, safe='')}:{quote(password, safe='')}"
    return f"{scheme}://{auth}@{netloc}"


def require_oxylabs_proxy_configuration() -> None:
    """Fail fast only when no proxy endpoints are configured at all."""
    if not get_oxylabs_proxy_servers():
        raise ValueError(
            "Oxylabs proxy servers not configured. "
            "Set OXYLABS_PROXY_SERVERS or OXYLABS_PROXY_SERVER."
        )


def describe_oxylabs_proxy_mode() -> dict[str, Any]:
    """Return lightweight metadata for prompts and diagnostics."""
    username, password = get_oxylabs_proxy_auth()
    return {
        "servers": list(get_oxylabs_proxy_servers()),
        "auth_mode": "credentials" if username and password else "ip_whitelist",
    }
