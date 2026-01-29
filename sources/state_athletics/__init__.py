"""State athletics association scrapers."""

from .base import BaseAthleticsScraper
from .scraper_base import ProxiedScraper
from .ohio_ohsaa import OHSAAScraper

__all__ = ["BaseAthleticsScraper", "ProxiedScraper", "OHSAAScraper"]
