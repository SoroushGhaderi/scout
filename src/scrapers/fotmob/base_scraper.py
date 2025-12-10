"""Base scraper class with retry logic and error handling."""

import time
import random
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ...config import FotMobConfig
from ...utils.logging_utils import get_logger


class BaseScraper:
    """Base class for FotMob API scrapers with built-in retry logic."""

    def __init__(self, config: FotMobConfig):
        """
        Initialize the base scraper.

        Args:
            config: FotMob configuration object
        """
        self.config = config
        self.logger = get_logger()
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create a requests session with automatic retries.

        Returns:
            Configured requests session
        """
        session = requests.Session()

        retry_strategy = Retry(
            total=self.config.retry.max_attempts,
            backoff_factor=self.config.retry.backoff_factor,
            status_forcelist=list(self.config.retry.status_codes),
            allowed_methods=["GET", "POST"],
            raise_on_status=False
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _delay_request(self):
        """Add random delay between requests to avoid rate limiting."""
        delay = random.uniform(
            self.config.request.delay_min,
            self.config.request.delay_max
        )
        self.logger.debug(f"Waiting {delay:.2f} seconds before next request")
        time.sleep(delay)

    def make_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        method: str = "GET"
    ) -> Optional[Dict[str, Any]]:
        """
        Make an HTTP request with retry logic.

        Args:
            url: URL to request
            params: Query parameters
            headers: HTTP headers (uses default if None)
            method: HTTP method (GET or POST)

        Returns:
            JSON response as dictionary, or None if request failed
        """
        if headers is None:
            headers = self.config.api.get_headers()

        self._delay_request()

        try:
            self.logger.debug(f"Making {method} request to {url}")

            if method.upper() == "GET":
                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.config.request.timeout
                )
            elif method.upper() == "POST":
                response = self.session.post(
                    url,
                    json=params,
                    headers=headers,
                    timeout=self.config.request.timeout
                )
            else:
                self.logger.error(f"Unsupported HTTP method: {method}")
                return None

            if response.status_code == 200:
                self.logger.debug(f"Request successful: {url}")
                return response.json()
            elif response.status_code == 404:
                self.logger.warning(f"Resource not found (404): {url}")
                return None
            elif response.status_code == 429:
                self.logger.warning(f"Rate limited (429): {url}")
                time.sleep(5)
                return None
            else:
                self.logger.error(
                    f"Request failed with status {response.status_code}: {url}"
                )
                return None

        except requests.exceptions.Timeout:
            self.logger.error(f"Request timeout: {url}")
            return None
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error: {url} - {str(e)}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {url} - {str(e)}")
            return None
        except ValueError as e:
            self.logger.error(
                f"Failed to decode JSON response: {url} - {str(e)}"
            )
            return None
        except Exception as e:
            self.logger.exception(
                f"Unexpected error during request: {url} - {str(e)}"
            )
            return None

    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()
        self.logger.debug("Session closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
