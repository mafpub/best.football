"""Base Playwright scraper with mandatory Oxylabs proxy support.

All scrapers MUST use Oxylabs proxy - there is no bypass option.
This ensures reliable, rate-limited access to state athletics websites.
"""

import hashlib
import logging
import os
import random
from pathlib import Path
from typing import Any, Optional

from playwright.async_api import async_playwright, Browser, Page, BrowserContext
import yaml

# Module logger
logger = logging.getLogger(__name__)

# Oxylabs proxy configuration (REQUIRED - no bypass)
_OXYLABS_USERNAME = os.environ.get("OXYLABS_USERNAME")
_OXYLABS_PASSWORD = os.environ.get("OXYLABS_PASSWORD")

OXYLABS_PROXIES = [
    "ddc.oxylabs.io:8001",
    "ddc.oxylabs.io:8002",
    "ddc.oxylabs.io:8003",
]


def _check_proxy_credentials():
    """Verify proxy credentials are available."""
    if not _OXYLABS_USERNAME or not _OXYLABS_PASSWORD:
        raise ValueError(
            "Oxylabs proxy credentials not configured. "
            "Set OXYLABS_USERNAME and OXYLABS_PASSWORD environment variables."
        )


class SelectorConfig:
    """Load and manage scraper selectors from YAML."""

    def __init__(self, yaml_path: Path):
        """Load selector configuration from YAML file.

        Args:
            yaml_path: Path to selector YAML file
        """
        self.yaml_path = yaml_path
        self.config = self._load_yaml()
        self._hash = self._compute_hash()

    def _load_yaml(self) -> dict:
        """Load YAML configuration."""
        if not self.yaml_path.exists():
            raise FileNotFoundError(f"Selector config not found: {self.yaml_path}")

        with open(self.yaml_path, "r") as f:
            return yaml.safe_load(f)

    def _compute_hash(self) -> str:
        """Compute hash of selector configuration for change detection."""
        yaml_content = self.yaml_path.read_text()
        return hashlib.sha256(yaml_content.encode()).hexdigest()

    @property
    def hash(self) -> str:
        """Get hash of current selector configuration."""
        return self._hash

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(key, default)

    def get_selector(self, selector_path: str) -> Optional[str]:
        """Get a selector value from nested config.

        Args:
            selector_path: Dot-separated path (e.g., "selectors.school_name")

        Returns:
            Selector string or None
        """
        keys = selector_path.split(".")
        value = self.config

        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None

        return value if isinstance(value, str) else None

    @property
    def base_url(self) -> str:
        """Get base URL for scraping."""
        return self.config.get("page", {}).get("base_url", "")

    @property
    def wait_for_selector(self) -> Optional[str]:
        """Get selector to wait for before scraping."""
        page_config = self.config.get("page", {})
        return page_config.get("wait_for") or page_config.get("wait_for_selector")


class PlaywrightScraper:
    """Base scraper using Playwright with mandatory Oxylabs proxy.

    Features:
    - Oxylabs proxy rotation (8001-8003)
    - Selector configuration from YAML
    - Content hashing for change detection
    - Automatic retry with proxy rotation
    """

    def __init__(
        self,
        selector_yaml: Path,
        state: str,
        association_name: str,
        respect_delay: float = 1.5,
    ):
        """Initialize the Playwright scraper.

        Args:
            selector_yaml: Path to YAML selector configuration
            state: Two-letter state code (e.g., "TX")
            association_name: Name of athletics association (e.g., "UIL")
            respect_delay: Minimum seconds between requests
        """
        _check_proxy_credentials()

        self.selector_yaml = Path(selector_yaml)
        self.selectors = SelectorConfig(self.selector_yaml)
        self.state = state
        self.association_name = association_name
        self.respect_delay = respect_delay

        self.last_request_time = 0
        self.proxy_index = random.randint(0, len(OXYLABS_PROXIES) - 1)

        logger.info(
            "Initialized %s scraper for %s with selectors from %s",
            association_name,
            state,
            selector_yaml,
        )

    def _get_proxy_server(self) -> str:
        """Get next proxy server URL."""
        proxy = OXYLABS_PROXIES[self.proxy_index]
        self.proxy_index = (self.proxy_index + 1) % len(OXYLABS_PROXIES)
        return f"http://{proxy}"

    async def _get_browser(self) -> Browser:
        """Launch browser with Oxylabs proxy.

        Proxy is REQUIRED - no bypass option available.

        Returns:
            Playwright Browser instance configured with proxy
        """
        proxy_server = self._get_proxy_server()

        logger.debug("Launching browser with proxy: %s", proxy_server)

        playwright_instance = await async_playwright().start()
        browser = await playwright_instance.chromium.launch(
            proxy={
                "server": proxy_server,
                "username": _OXYLABS_USERNAME,
                "password": _OXYLABS_PASSWORD,
            },
            headless=True,
        )

        return browser

    async def _new_context(self, browser: Browser) -> BrowserContext:
        """Create new browser context with realistic user agent."""
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        return context

    def _respect_rate_limit(self):
        """Wait if needed to respect rate limits."""
        import time

        elapsed = time.time() - self.last_request_time
        if elapsed < self.respect_delay:
            time.sleep(self.respect_delay - elapsed)
        self.last_request_time = time.time()

    async def fetch_page(
        self,
        url: str,
        wait_for_selector: Optional[str] = None,
        timeout: int = 30000,
    ) -> Optional[Page]:
        """Fetch a page with Playwright.

        Args:
            url: URL to fetch
            wait_for_selector: CSS selector to wait for (uses config if None)
            timeout: Timeout in milliseconds

        Returns:
            Playwright Page object or None if failed
        """
        self._respect_rate_limit()

        # Use configured selector if none provided
        if wait_for_selector is None:
            wait_for_selector = self.selectors.wait_for_selector

        browser = None
        try:
            browser = await self._get_browser()
            context = await self._new_context(browser)
            page = await context.new_page()

            logger.debug("Navigating to: %s", url)

            await page.goto(url, timeout=timeout, wait_until="domcontentloaded")

            # Wait for specific selector if configured
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=timeout)
                    logger.debug("Found selector: %s", wait_for_selector)
                except Exception:
                    logger.warning("Selector not found: %s", wait_for_selector)

            return page

        except Exception as e:
            logger.error("Failed to fetch page %s: %s", url, e)
            return None

        finally:
            # Browser will be closed by caller
            pass

    async def scrape_page_content(
        self,
        url: str,
        wait_for_selector: Optional[str] = None,
    ) -> Optional[tuple[str, str]]:
        """Scrape page content and compute hash.

        Args:
            url: URL to scrape
            wait_for_selector: CSS selector to wait for

        Returns:
            (html_content, sha256_hash) tuple or None if failed
        """
        page = None
        browser = None

        try:
            browser = await self._get_browser()
            context = await self._new_context(browser)
            page = await context.new_page()

            await page.goto(url, wait_until="domcontentloaded")

            if wait_for_selector:
                await page.wait_for_selector(wait_for_selector, timeout=15000)

            # Get page content
            content = await page.content()

            # Compute hash for change detection
            content_hash = hashlib.sha256(content.encode()).hexdigest()

            return content, content_hash

        except Exception as e:
            logger.error("Failed to scrape content from %s: %s", url, e)
            return None

        finally:
            if page:
                await page.close()
            if browser:
                await browser.close()

    def hash_content(self, content: str) -> str:
        """Compute SHA256 hash of content.

        Args:
            content: Content to hash

        Returns:
            Hexadecimal SHA256 hash
        """
        return hashlib.sha256(content.encode()).hexdigest()

    async def close(self):
        """Cleanup resources."""
        # Called by subclasses for any cleanup
        pass

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
