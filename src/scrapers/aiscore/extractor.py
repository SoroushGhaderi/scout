"""Link extraction from page elements."""

import re
from typing import Optional, List, Callable
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By

from .config import Config
from .exceptions import ParsingError
from ...utils.logging_utils import get_logger

logger = get_logger()


class LinkExtractor:
    """Extracts links from web elements using multiple strategies."""

    def __init__(self, config: Config):
        self.config = config
        self.excluded_paths = config.validation.excluded_paths
        self.required_pattern = config.validation.required_pattern

    def extract_link(self, container: WebElement) -> Optional[str]:
        """
        Extract link from container using multiple methods.

        Args:
            container: WebElement to extract link from

        Returns:
            Extracted URL or None if not found
        """
        extraction_methods = [
            self._extract_from_tag,
            self._extract_from_data_attrs,
            self._extract_from_onclick,
            self._extract_from_child_link,
            self._extract_from_any_href,
        ]

        for method in extraction_methods:
            try:
                href = method(container)
                if href and self.is_valid_match_url(href):
                    return self._normalize_url(href)
            except Exception as e:
                logger.debug(f"Extraction method {method.__name__} failed: {e}")
                continue

        return None

    def _extract_from_tag(self, container: WebElement) -> Optional[str]:
        """Extract if container itself is an <a> tag."""
        if container.tag_name == 'a':
            return container.get_attribute('href')
        return None

    def _extract_from_data_attrs(self, container: WebElement) -> Optional[str]:
        """Extract from data attributes."""
        attrs = ['data-href', 'data-url', 'data-link']
        for attr in attrs:
            href = container.get_attribute(attr)
            if href:
                return href
        return None

    def _extract_from_onclick(self, container: WebElement) -> Optional[str]:
        """Extract URL from onclick attribute."""
        onclick = container.get_attribute('onclick')
        if onclick and 'match' in onclick.lower():
            match = re.search(r'["\']([^"\']*match[^"\']*)["\']', onclick)
            if match:
                return match.group(1)
        return None

    def _extract_from_child_link(self, container: WebElement) -> Optional[str]:
        """Find <a> tag inside container."""
        try:
            link_element = container.find_element(By.TAG_NAME, "a")
            return link_element.get_attribute('href')
        except Exception:
            return None

    def _extract_from_any_href(self, container: WebElement) -> Optional[str]:
        """Find any element with href attribute."""
        try:
            link_element = container.find_element(By.CSS_SELECTOR, "[href]")
            return link_element.get_attribute('href')
        except Exception:
            return None

    def _normalize_url(self, url: str) -> str:
        """Normalize URL to prevent duplicates."""
        if not url:
            return ""

        url = url.rstrip('/')

        if '?' in url:
            url = url.split('?')[0]

        if url.endswith('/h2h'):
            url = url[:-4]

        return url

    def is_valid_match_url(self, url: str) -> bool:
        """
        Check if URL is a valid match URL.

        Args:
            url: URL to validate

        Returns:
            True if valid match URL
        """
        if not url:
            return False

        url_lower = url.lower()

        if self.required_pattern not in url_lower:
            return False

        if any(exc in url_lower for exc in self.excluded_paths):
            return False

        return True

    def extract_match_id(self, url: str) -> str:
        """
        Extract match ID from URL.

        Args:
            url: URL to extract ID from

        Returns:
            Extracted match ID
        """
        if not url:
            return ""

        url = self._normalize_url(url)
        parts = url.split('/')

        match_id = parts[-1] if parts else ''

        if match_id.lower() in self.excluded_paths and len(parts) >= 2:
            match_id = parts[-2]

        return match_id

    def extract_links_from_containers(
        self,
        containers: List[WebElement]
    ) -> List[str]:
        """
        Extract all valid links from multiple containers.

        Args:
            containers: List of WebElement containers

        Returns:
            List of extracted URLs
        """
        links = []

        for container in containers:
            try:
                link = self.extract_link(container)
                if link:
                    links.append(link)
            except Exception as e:
                logger.debug(f"Failed to extract link from container: {e}")
                continue

        return links
