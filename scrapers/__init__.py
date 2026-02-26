"""Playwright-based scrapers for state athletics associations.

This module provides robust, self-healing scrapers that:
- Use Playwright for dynamic content rendering
- Require Oxylabs proxy for all requests
- Load selectors from YAML configs for easy repair
- Track failures and trigger repair agents
- Detect content changes for incremental updates
"""

from scrapers.base import PlaywrightScraper

__all__ = ["PlaywrightScraper"]
