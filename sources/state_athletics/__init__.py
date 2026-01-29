"""State athletics association scrapers."""

from .base import BaseAthleticsScraper
from .scraper_base import ProxiedScraper
from .ohio_ohsaa import OHSAAScraper
from .florida_fhsaa import FHSAAScraper
from .texas_uil import TexasUILScraper
from .california_cifss import CIFSSScraper

__all__ = [
    "BaseAthleticsScraper",
    "ProxiedScraper",
    "OHSAAScraper",
    "FHSAAScraper",
    "TexasUILScraper",
    "CIFSSScraper",
]
