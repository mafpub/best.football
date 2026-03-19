#!/usr/bin/env python3
"""Probe school websites through a selected proxy profile without fetching full pages.

The probe strategy is:
1. Send HEAD without following redirects.
2. If the server rejects HEAD, fall back to a streamed GET with Range: bytes=0-0.
3. Record only headers/status plus transport errors, not page bodies.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.database import get_db
from pipeline.proxy import (
    get_httpx_proxy_url,
    get_proxy_profile,
    get_proxy_servers,
    require_oxylabs_proxy_configuration,
)

PROBE_STRATEGY = "HEAD_THEN_RANGED_GET"
HEAD_HEADERS = {
    "User-Agent": "best.football connectivity probe/1.0",
    "Accept": "*/*",
    "Cache-Control": "no-cache",
}
GET_FALLBACK_HEADERS = {
    **HEAD_HEADERS,
    "Range": "bytes=0-0",
    "Accept-Encoding": "identity",
}
HEAD_FALLBACK_STATUS_CODES = {405, 501}
RESTRICTED_HEADER_MARKER = "access denied: restricted target"
HEADER_KEYS_TO_RECORD = (
    "location",
    "server",
    "content-type",
    "content-length",
    "x-error-description",
    "cf-ray",
    "via",
)


@dataclass
class ProbeResult:
    nces_id: str
    school_name: str
    state: str
    website: str
    normalized_url: str
    probe_method: str
    result: str
    status_code: int | None
    redirect_target: str | None
    error_type: str | None
    error_message: str | None
    response_headers_json: str | None
    checked_at: str


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        return f"https://{raw}"
    return raw


def _resolve_redirect_target(response: httpx.Response) -> str | None:
    location = response.headers.get("location")
    if not location:
        return None
    return urljoin(str(response.request.url), location)


def _response_headers_json(response: httpx.Response) -> str | None:
    headers = {
        key: value
        for key in HEADER_KEYS_TO_RECORD
        if (value := response.headers.get(key)) is not None
    }
    return json.dumps(headers, ensure_ascii=True, sort_keys=True) if headers else None


def _classify_response(response: httpx.Response) -> tuple[str, str | None]:
    status_code = response.status_code
    redirect_target = _resolve_redirect_target(response)
    restricted_header = (response.headers.get("x-error-description") or "").lower()

    if RESTRICTED_HEADER_MARKER in restricted_header:
        return "restricted", redirect_target
    if 200 <= status_code < 300:
        return "success", redirect_target
    if status_code == 403:
        return "403", redirect_target
    if 300 <= status_code < 400:
        return "redirect", redirect_target
    return "http_error", redirect_target


def _classify_exception(exc: Exception) -> tuple[str, str, str]:
    if isinstance(exc, httpx.TimeoutException):
        return "timeout", type(exc).__name__, str(exc)
    if isinstance(exc, httpx.ProxyError):
        return "proxy_error", type(exc).__name__, str(exc)
    if isinstance(exc, httpx.ConnectError):
        return "connect_error", type(exc).__name__, str(exc)
    if isinstance(exc, httpx.HTTPError):
        return "transport_error", type(exc).__name__, str(exc)
    return "error", type(exc).__name__, str(exc)


def _should_retry_with_get(exc: Exception) -> bool:
    return isinstance(exc, (httpx.ReadError, httpx.RemoteProtocolError, httpx.WriteError))


async def _probe_with_head(client: httpx.AsyncClient, url: str) -> ProbeResult | None:
    response = await client.head(url, headers=HEAD_HEADERS)
    if response.status_code in HEAD_FALLBACK_STATUS_CODES:
        return None

    result, redirect_target = _classify_response(response)
    return ProbeResult(
        nces_id="",
        school_name="",
        state="",
        website="",
        normalized_url=url,
        probe_method="HEAD",
        result=result,
        status_code=response.status_code,
        redirect_target=redirect_target,
        error_type=None,
        error_message=None,
        response_headers_json=_response_headers_json(response),
        checked_at=_utcnow(),
    )


async def _probe_with_streamed_get(client: httpx.AsyncClient, url: str) -> ProbeResult:
    async with client.stream("GET", url, headers=GET_FALLBACK_HEADERS) as response:
        result, redirect_target = _classify_response(response)
        return ProbeResult(
            nces_id="",
            school_name="",
            state="",
            website="",
            normalized_url=url,
            probe_method="GET_RANGE",
            result=result,
            status_code=response.status_code,
            redirect_target=redirect_target,
            error_type=None,
            error_message=None,
            response_headers_json=_response_headers_json(response),
            checked_at=_utcnow(),
        )


async def _probe_url(client: httpx.AsyncClient, row: dict[str, Any]) -> ProbeResult:
    url = _normalize_url(str(row.get("website") or ""))
    base = {
        "nces_id": str(row["nces_id"]),
        "school_name": str(row["name"]),
        "state": str(row["state"]),
        "website": str(row["website"]),
        "normalized_url": url,
    }

    if not url:
        return ProbeResult(
            **base,
            probe_method="NONE",
            result="missing_url",
            status_code=None,
            redirect_target=None,
            error_type="MissingURL",
            error_message="website is empty",
            response_headers_json=None,
            checked_at=_utcnow(),
        )

    try:
        head_result = await _probe_with_head(client, url)
        if head_result is not None:
            return ProbeResult(
                **base,
                probe_method=head_result.probe_method,
                result=head_result.result,
                status_code=head_result.status_code,
                redirect_target=head_result.redirect_target,
                error_type=head_result.error_type,
                error_message=head_result.error_message,
                response_headers_json=head_result.response_headers_json,
                checked_at=head_result.checked_at,
            )
    except Exception as exc:
        if not _should_retry_with_get(exc):
            result, error_type, error_message = _classify_exception(exc)
            return ProbeResult(
                **base,
                probe_method="HEAD",
                result=result,
                status_code=None,
                redirect_target=None,
                error_type=error_type,
                error_message=error_message,
                response_headers_json=None,
                checked_at=_utcnow(),
            )

    try:
        get_result = await _probe_with_streamed_get(client, url)
        return ProbeResult(
            **base,
            probe_method=get_result.probe_method,
            result=get_result.result,
            status_code=get_result.status_code,
            redirect_target=get_result.redirect_target,
            error_type=get_result.error_type,
            error_message=get_result.error_message,
            response_headers_json=get_result.response_headers_json,
            checked_at=get_result.checked_at,
        )
    except Exception as exc:
        result, error_type, error_message = _classify_exception(exc)
        return ProbeResult(
            **base,
            probe_method="GET_RANGE",
            result=result,
            status_code=None,
            redirect_target=None,
            error_type=error_type,
            error_message=error_message,
            response_headers_json=None,
            checked_at=_utcnow(),
        )


def _ensure_probe_tables() -> None:
    schema = """
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
        run_id INTEGER NOT NULL REFERENCES school_website_probe_runs(id) ON DELETE CASCADE,
        nces_id TEXT NOT NULL REFERENCES schools(nces_id),
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

    CREATE INDEX IF NOT EXISTS idx_school_website_probe_results_run
        ON school_website_probe_results(run_id, result);
    CREATE INDEX IF NOT EXISTS idx_school_website_probe_results_school
        ON school_website_probe_results(nces_id, checked_at);
    """
    with get_db() as conn:
        conn.executescript(schema)


def _select_rows(state: str | None, limit: int | None) -> list[dict[str, Any]]:
    where_parts = ["website IS NOT NULL", "TRIM(website) != ''"]
    params: list[Any] = []
    if state:
        where_parts.append("state = ?")
        params.append(state.upper())

    sql = f"""
        SELECT nces_id, name, state, website
        FROM schools
        WHERE {' AND '.join(where_parts)}
        ORDER BY state, nces_id
    """
    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def _create_run(proxy_profile: str, state: str | None, limit: int | None, total_targets: int) -> int:
    with get_db() as conn:
        cursor = conn.execute(
            """
            INSERT INTO school_website_probe_runs (
                started_at,
                proxy_profile,
                probe_strategy,
                state,
                target_limit,
                total_targets
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (_utcnow(), proxy_profile, PROBE_STRATEGY, state, limit, total_targets),
        )
        return int(cursor.lastrowid)


def _finish_run(run_id: int) -> None:
    with get_db() as conn:
        conn.execute(
            "UPDATE school_website_probe_runs SET completed_at = ? WHERE id = ?",
            (_utcnow(), run_id),
        )


def _store_results(run_id: int, results: list[ProbeResult]) -> None:
    rows = [
        (
            run_id,
            item.nces_id,
            item.school_name,
            item.state,
            item.website,
            item.normalized_url,
            item.probe_method,
            item.result,
            item.status_code,
            item.redirect_target,
            item.error_type,
            item.error_message,
            item.response_headers_json,
            item.checked_at,
        )
        for item in results
    ]
    with get_db() as conn:
        conn.executemany(
            """
            INSERT INTO school_website_probe_results (
                run_id,
                nces_id,
                school_name,
                state,
                website,
                normalized_url,
                probe_method,
                result,
                status_code,
                redirect_target,
                error_type,
                error_message,
                response_headers_json,
                checked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


async def _probe_all(
    run_id: int,
    rows: list[dict[str, Any]],
    proxy_profile: str,
    wave_size: int,
    wave_delay_seconds: float,
    timeout_seconds: float,
) -> Counter[str]:
    proxy_servers = get_proxy_servers(proxy_profile)
    clients = [
        httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(proxy=get_httpx_proxy_url(proxy_index=index, profile=proxy_profile)),
            follow_redirects=False,
            timeout=httpx.Timeout(timeout_seconds),
            verify=True,
        )
        for index, _server in enumerate(proxy_servers)
    ]
    summary: Counter[str] = Counter()
    effective_wave_size = max(1, wave_size)
    total_waves = math.ceil(len(rows) / effective_wave_size)

    try:
        for wave_index, start in enumerate(range(0, len(rows), effective_wave_size), start=1):
            wave_rows = rows[start : start + effective_wave_size]
            tasks = [
                asyncio.create_task(_probe_url(clients[(start + offset) % len(clients)], row))
                for offset, row in enumerate(wave_rows)
            ]
            wave_results = await asyncio.gather(*tasks)
            _store_results(run_id, wave_results)
            summary.update(item.result for item in wave_results)
            print(
                f"Stored wave {wave_index}/{total_waves} "
                f"({len(wave_results)} results, processed {start + len(wave_results)}/{len(rows)})"
            )
            if start + effective_wave_size < len(rows):
                await asyncio.sleep(max(0.0, wave_delay_seconds))
        return summary
    finally:
        await asyncio.gather(*(client.aclose() for client in clients))


def _print_summary(run_id: int, total_targets: int, counts: Counter[str]) -> None:
    print(f"Probe run {run_id} complete")
    print(f"Total targets: {total_targets}")
    for key in sorted(counts):
        print(f"{key}: {counts[key]}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe school websites without downloading full pages"
    )
    parser.add_argument("--state", help="Optional state filter")
    parser.add_argument("--limit", type=int, help="Optional maximum number of schools to probe")
    parser.add_argument("--workers", type=int, default=10, help="Requests per wave")
    parser.add_argument(
        "--wave-delay",
        type=float,
        default=1.0,
        help="Seconds to sleep between waves",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Per-request timeout in seconds")
    parser.add_argument(
        "--proxy-profile",
        choices=["mobile", "datacenter"],
        default="datacenter",
        help="Proxy profile to use. Defaults to datacenter.",
    )
    args = parser.parse_args()

    proxy_profile = get_proxy_profile(args.proxy_profile)
    require_oxylabs_proxy_configuration(proxy_profile)
    _ensure_probe_tables()

    rows = _select_rows(args.state, args.limit)
    if not rows:
        print("No schools with websites matched the requested scope")
        return 0

    run_id = _create_run(proxy_profile, args.state.upper() if args.state else None, args.limit, len(rows))
    counts: Counter[str] = Counter()
    return_code = 0
    try:
        counts = asyncio.run(
            _probe_all(
                run_id,
                rows,
                proxy_profile=proxy_profile,
                wave_size=args.workers,
                wave_delay_seconds=args.wave_delay,
                timeout_seconds=args.timeout,
            )
        )
    except KeyboardInterrupt:
        print(f"Interrupted probe run {run_id}; partial results are already stored")
        return_code = 130
    finally:
        _finish_run(run_id)

    _print_summary(run_id, len(rows), counts)
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
