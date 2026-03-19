import httpx

from scripts import probe_school_websites as probe


def test_normalize_url_adds_https_for_bare_host():
    assert probe._normalize_url("example.org/path") == "https://example.org/path"


def test_normalize_url_preserves_existing_scheme():
    assert probe._normalize_url("http://example.org") == "http://example.org"


def test_classify_response_redirect_and_resolve_target():
    request = httpx.Request("HEAD", "https://example.org/start")
    response = httpx.Response(301, headers={"location": "/next"}, request=request)

    result, redirect_target = probe._classify_response(response)

    assert result == "redirect"
    assert redirect_target == "https://example.org/next"


def test_classify_response_restricted_header_wins():
    request = httpx.Request("HEAD", "https://example.org")
    response = httpx.Response(
        403,
        headers={"x-error-description": "Access denied: restricted target"},
        request=request,
    )

    result, redirect_target = probe._classify_response(response)

    assert result == "restricted"
    assert redirect_target is None
