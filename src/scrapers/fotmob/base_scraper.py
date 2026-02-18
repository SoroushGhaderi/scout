"""Base scraper – uses a headless browser so x-mas tokens are auto-generated."""

import time
import random
from typing import Optional, Dict, Any

from ...core import ScraperProtocol
from ...core.constants import Defaults
from config import FotMobConfig
from ...utils.logging_utils import get_logger
from .playwright_fetcher import PlaywrightFetcher


class BaseScraper(ScraperProtocol):
    """
    Base class for FotMob API scrapers.

    All HTTP requests are routed through a single headless Chromium instance
    (via PlaywrightFetcher) so that FotMob's client-side JavaScript can attach
    a valid, URL-specific x-mas token to every outgoing request automatically.
    """

    def __init__(self, config: FotMobConfig):
        self.config = config
        self.logger = get_logger()
        self._fetcher = PlaywrightFetcher(config)

    def _delay_request(self):
        """Random inter-request delay to avoid rate-limiting."""
        delay = random.uniform(
            self.config.request.delay_min,
            self.config.request.delay_max,
        )
        self.logger.debug(f"Waiting {delay:.2f}s before next request")
        time.sleep(delay)

    def make_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        method: str = "GET",
    ) -> Optional[Dict[str, Any]]:
        """
        Make a browser-based GET request with automatic x-mas token injection.

        The *headers* and *method* parameters are accepted for API compatibility
        but are not used – the browser handles all headers internally.
        """
        self._delay_request()
        self.logger.debug(f"Browser request → {url}")

        result = self._fetcher.fetch_json(url, params)

        if result is None:
            self.logger.error(f"Request failed: {url}")

        return result

    def close(self):
        """Shut down the headless browser."""
        self._fetcher.close()
        self.logger.debug("Scraper closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
