"""Base scraper with Oxylabs proxy support and caching.

Uses the same proxy configuration as ~/tools/web-scraper.py
"""

import hashlib
import json
import logging
import os
import random
import time
from pathlib import Path
from typing import Optional

import httpx

# Module logger
logger = logging.getLogger(__name__)

# Oxylabs proxy configuration from environment variables with fallback defaults
_OXYLABS_USERNAME = os.environ.get("OXYLABS_USERNAME", "palpha_Thtm9")
_OXYLABS_PASSWORD = os.environ.get("OXYLABS_PASSWORD", "ULcLdrJ+d_4mXBM")

OXYLABS_PROXIES = [
    {"proxy": "ddc.oxylabs.io:8001"},
    {"proxy": "ddc.oxylabs.io:8002"},
    {"proxy": "ddc.oxylabs.io:8003"},
]

# Shared blocklist path
BLOCKLIST_FILE = Path.home() / ".web_scraper_blocklist.json"

# Default user agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"


class ProxiedScraper:
    """Base scraper with Oxylabs proxy rotation and blocklist checking."""

    def __init__(self, cache_dir: Path, respect_delay: float = 1.0):
        """
        Initialize scraper.

        Args:
            cache_dir: Directory for caching responses
            respect_delay: Minimum seconds between requests (be respectful)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.respect_delay = respect_delay
        self.last_request_time = 0
        self.proxy_index = random.randint(0, len(OXYLABS_PROXIES) - 1)
        self.blocklist = self._load_blocklist()

    def _load_blocklist(self) -> set:
        """Load shared blocklist."""
        if BLOCKLIST_FILE.exists():
            try:
                data = json.loads(BLOCKLIST_FILE.read_text())
                return set(data.get("domains", []))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to load blocklist from %s: %s", BLOCKLIST_FILE, e)
        return set()

    def _get_proxy_url(self) -> str:
        """Get next proxy URL with auth."""
        proxy = OXYLABS_PROXIES[self.proxy_index]
        self.proxy_index = (self.proxy_index + 1) % len(OXYLABS_PROXIES)
        return f"http://{_OXYLABS_USERNAME}:{_OXYLABS_PASSWORD}@{proxy['proxy']}"

    def _respect_rate_limit(self):
        """Wait if needed to respect rate limits."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.respect_delay:
            time.sleep(self.respect_delay - elapsed)
        self.last_request_time = time.time()

    def _get_cache_path(self, url: str) -> Path:
        """Get cache file path for a URL."""
        # Create a safe filename from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        return self.cache_dir / f"{url_hash}.html"

    def _get_cached(self, url: str, max_age_hours: int = 24) -> Optional[str]:
        """Get cached response if fresh enough."""
        cache_path = self._get_cache_path(url)
        if not cache_path.exists():
            return None

        # Check age
        age_seconds = time.time() - cache_path.stat().st_mtime
        if age_seconds > max_age_hours * 3600:
            return None

        return cache_path.read_text(encoding="utf-8")

    def _save_cache(self, url: str, content: str):
        """Save response to cache."""
        cache_path = self._get_cache_path(url)
        cache_path.write_text(content, encoding="utf-8")

    def fetch(
        self,
        url: str,
        use_proxy: bool = True,
        max_retries: int = 3,
        cache_hours: int = 24,
    ) -> Optional[str]:
        """
        Fetch a URL with optional proxy and caching.

        Args:
            url: URL to fetch
            use_proxy: Whether to use Oxylabs proxy
            max_retries: Number of retry attempts
            cache_hours: Cache validity in hours (0 to disable)

        Returns:
            HTML content or None if failed
        """
        # Check cache first
        if cache_hours > 0:
            cached = self._get_cached(url, cache_hours)
            if cached:
                logger.debug("Cache hit: %s", url)
                return cached

        # Respect rate limit
        self._respect_rate_limit()

        headers = {"User-Agent": USER_AGENT}

        for attempt in range(max_retries):
            try:
                if use_proxy:
                    proxy_url = self._get_proxy_url()
                    transport = httpx.HTTPTransport(proxy=proxy_url)
                else:
                    transport = None

                with httpx.Client(transport=transport, timeout=30.0) as client:
                    logger.debug("Fetching %s (attempt %d)", url, attempt + 1)
                    response = client.get(url, headers=headers, follow_redirects=True)
                    response.raise_for_status()

                    content = response.text

                    # Cache successful response
                    if cache_hours > 0:
                        self._save_cache(url, content)

                    return content

            except (httpx.HTTPStatusError, httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning("Fetch error for %s: %s", url, e)
                if attempt < max_retries - 1:
                    wait = random.uniform(2, 5)
                    logger.debug("Retrying in %.1fs...", wait)
                    time.sleep(wait)

        return None
