"""Main scraper orchestration."""

import time
from typing import List, Optional
from datetime import datetime
from selenium.webdriver.common.by import By
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import Config
from .browser import BrowserManager
from .extractor import LinkExtractor
from .models import MatchLk, ScrapingResult
from .metrics import ScrapingMetrics
from .exceptions import (
    ScraperError, NetworkError, CloudflareError,
    ElementNotFoundError, BrowserError
)
from ...storage.aiscore_storage import AIScoreBronzeStorage
from ...utils.logging_utils import get_logger

logger = get_logger()


class FootballScraper:
    """Main scraper class with comprehensive error handling."""

    def __init__(
        self,
        config: Optional[Config] = None,
        storage: Optional[AIScoreBronzeStorage] = None
    ):
        """Initialize the scraper.

        Args:
            config: Scraper configuration. If None, uses default config.
            storage: Bronze storage instance. If None, creates default storage.
        """
        self.config = config or Config()
        self.config.ensure_directories()

        self.storage = storage or AIScoreBronzeStorage()
        self.browser = BrowserManager(self.config)
        self.extractor = LinkExtractor(self.config)

        logger.info("FootballScraper initialized")

    def scrape_date(
        self,
        date_str: str,
        metrics: Optional[ScrapingMetrics] = None
    ) -> ScrapingResult:
        """
        Scrape match links for a specific date.

        Args:
            date_str: Date in format 'YYYYMMDD' or 'YYYY-MM-DD'
            metrics: Optional metrics object to update

        Returns:
            ScrapingResult with outcome details
        """









        start_time = time.time()
        date_formatted = date_str.replace('-', '')

        logger.info(f"Starting link collection for {date_formatted}")

        try:
            self._navigate_to_date_page(date_formatted)

            self._click_all_tab()

            if not self._wait_for_match_containers():
                logger.warning("No match containers found, continuing anyway")

            result_data = self._collect_all_links(date_formatted, metrics)

            duration = time.time() - start_time

            if metrics:
                metrics.pages_scraped += 1

            result = ScrapingResult(
                success=True,
                links_found=result_data['total_found'],
                links_inserted=result_data['total_inserted'],
                duplicates=result_data['duplicates'],
                errors=0,
                duration=duration
            )

            logger.info(str(result))
            return result

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Scraping failed for date {date_formatted}: {e}", exc_info=True)

            if metrics:
                metrics.errors_encountered += 1

            return ScrapingResult(
                success=False,
                links_found=0,
                links_inserted=0,
                duplicates=0,
                errors=1,
                duration=duration,
                error_message=str(e)
            )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((NetworkError, BrowserError)),
        reraise=True
    )
    def _navigate_to_date_page(self, date_formatted: str):
        """Navigate to date page with retry logic."""
        url = f"{self.config.scraping.base_url}/{date_formatted}"

        try:
            logger.debug("Loading base page")
            self.browser.driver.get(self.config.scraping.base_url)
            time.sleep(self.config.scraping.delays.initial_load)

            if not self._wait_for_cloudflare_pass(max_wait=10):
                raise CloudflareError("Cloudflare check timeout on main page")

            logger.info(f"Navigating to {url}")
            self.browser.driver.get(url)
            time.sleep(self.config.scraping.delays.initial_load)

            if not self._wait_for_cloudflare_pass(
                max_wait=self.config.scraping.timeouts.cloudflare_max
            ):
                raise CloudflareError("Cloudflare check timeout on date page")

            logger.debug("Page loaded successfully")

        except Exception as e:
            logger.error(f"Navigation failed: {e}")
            raise NetworkError(f"Failed to navigate to page: {e}") from e

    def _wait_for_cloudflare_pass(self, max_wait: int = 15) -> bool:
        """Wait for Cloudflare protection to pass."""
        wait_time = 0
        while wait_time < max_wait:
            time.sleep(1)
            wait_time += 1

            try:
                title = self.browser.driver.title
                if "Just a moment" not in title and "Checking your browser" not in title:
                    logger.debug(f"Cloudflare check passed ({wait_time}s)")
                    return True
            except Exception:
                pass

        return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        reraise=True
    )
    def _click_all_tab(self):
        """Click the 'All' tab with retry logic."""
        try:
            logger.debug("Locating 'All' tab")

            elements = self.browser.driver.find_elements(
                By.CSS_SELECTOR,
                self.config.selectors.all_tab
            )

            if not elements:
                raise ElementNotFoundError("All tab elements not found")

            all_tab = None
            for elem in elements:
                text = elem.text.strip()
                if text.lower() == 'all':
                    all_tab = elem
                    break

            if not all_tab:
                logger.debug("'All' tab not found, using default view")
                return

            try:
                all_tab.click()
                logger.debug("Activated 'All' tab")
            except Exception:
                self.browser.driver.execute_script("arguments[0].click();", all_tab)
                logger.debug("Activated 'All' tab via JavaScript")

            time.sleep(self.config.scraping.delays.after_click)

        except ElementNotFoundError:
            logger.warning("Could not find 'All' tab")
        except Exception as e:
            logger.warning(f"Error clicking 'All' tab: {e}")

    def _wait_for_match_containers(self) -> bool:
        """Wait for match containers to appear."""
        return self.browser.wait_for_element(
            self.config.selectors.match_container,
            timeout=self.config.scraping.timeouts.element_wait
        )

    def _save_links_to_storage(
        self,
        links: List[MatchLk],
        source_date: str
    ) -> int:
        """Save match links to Bronze storage layer.

        Args:
            links: List of MatchLk objects to save
            source_date: Date string YYYYMMDD format

        Returns:
            Number of links successfully saved (new, not duplicates)
        """
        inserted = 0
        for link in links:
            try:
                # Check if match already exists
                if self.storage.match_exists(link.match_id, source_date):
                    continue

                # Save link data as a simple match record
                link_data = {
                    "match_id": link.match_id,
                    "url": link.url,
                    "source_date": source_date,
                    "link_scraped": True,
                    "odds_scraped": False,
                }
                self.storage.save_raw_match_data(link.match_id, link_data, source_date)
                inserted += 1
            except Exception as e:
                logger.debug(f"Failed to save link {link.match_id}: {e}")

        return inserted

    def _collect_all_links(
        self,
        source_date: str,
        metrics: Optional[ScrapingMetrics] = None
    ) -> dict:
        """
        Collect and save links by scrolling through the page.

        Saves links to Bronze storage after each scroll batch.

        Returns:
            dict with 'total_found', 'total_inserted', 'duplicates'
        """


        logger.info("Starting link collection via scroll extraction")

        seen_urls = set()
        total_inserted = 0
        total_duplicates = 0
        no_change_count = 0
        scroll_iteration = 0

        while no_change_count < self.config.scraping.scroll.max_no_change:
            scroll_iteration += 1
            scroll_start = time.time()

            position = self.browser.get_scroll_position()

            containers = self.browser.driver.find_elements(
                By.CSS_SELECTOR,
                self.config.selectors.match_container
            )

            logger.debug(
                f"Scroll #{scroll_iteration}: {len(containers)} containers at {position}px"
            )

            new_urls = []
            extraction_start = time.time()

            for container in containers:
                try:
                    url = self.extractor.extract_link(container)
                    if url and url not in seen_urls:
                        new_urls.append(url)
                        seen_urls.add(url)
                except Exception as e:
                    logger.debug(f"Failed to extract link: {e}")

            extraction_time = time.time() - extraction_start

            if new_urls:
                new_links = []
                for url in new_urls:
                    try:
                        match_id = self.extractor.extract_match_id(url)
                        link = MatchLk(
                            url=url,
                            match_id=match_id,
                            source_date=source_date
                        )
                        new_links.append(link)
                    except Exception as e:
                        logger.debug(f"Failed to create MatchLk for {url}: {e}")

                if new_links:
                    try:
                        inserted = self._save_links_to_storage(new_links, source_date)
                        duplicates = len(new_links) - inserted
                        total_inserted += inserted
                        total_duplicates += duplicates

                        logger.info(
                            f"Collected {len(new_urls)} new links (+{inserted} to storage, {duplicates} duplicates). "
                            f"Total: {total_inserted}"
                        )

                        if metrics:
                            metrics.links_found += len(new_urls)
                            metrics.links_inserted += inserted
                            metrics.duplicates_prevented += duplicates

                        no_change_count = 0
                    except Exception as e:
                        logger.error(f"Failed to save links: {e}")
            else:
                no_change_count += 1
                logger.debug(
                    f"No new links (attempt {no_change_count}/{self.config.scraping.scroll.max_no_change})"
                )

            if metrics:
                scroll_time = time.time() - scroll_start
                metrics.total_scroll_time += scroll_time
                metrics.total_extraction_time += extraction_time
                metrics.scroll_count += 1

            self.browser.scroll_page(self.config.scraping.scroll.increment)
            time.sleep(self.config.scraping.scroll.pause)

            if self.browser.is_at_bottom() and no_change_count >= 3:
                logger.debug("Reached bottom of page")
                break

        logger.debug("Performing final collection pass")
        containers = self.browser.driver.find_elements(
            By.CSS_SELECTOR,
            self.config.selectors.match_container
        )

        final_urls = []
        for container in containers:
            try:
                url = self.extractor.extract_link(container)
                if url and url not in seen_urls:
                    final_urls.append(url)
                    seen_urls.add(url)
            except Exception:
                pass

        if final_urls:
            final_links = []
            for url in final_urls:
                try:
                    match_id = self.extractor.extract_match_id(url)
                    link = MatchLk(
                        url=url,
                        match_id=match_id,
                        source_date=source_date
                    )
                    final_links.append(link)
                except Exception as e:
                    logger.debug(f"Failed to create MatchLk for {url}: {e}")

            if final_links:
                try:
                    inserted = self._save_links_to_storage(final_links, source_date)
                    duplicates = len(final_links) - inserted
                    total_inserted += inserted
                    total_duplicates += duplicates

                    if inserted > 0:
                        logger.info(f"Final pass collected {inserted} additional links")

                    if metrics:
                        metrics.links_found += len(final_urls)
                        metrics.links_inserted += inserted
                        metrics.duplicates_prevented += duplicates
                except Exception as e:
                    logger.error(f"Failed to save final batch: {e}")

        logger.info(
            f"Link collection complete: {len(seen_urls)} unique URLs found, "
            f"{total_inserted} saved to storage, {total_duplicates} duplicates filtered"
        )

        return {
            'total_found': len(seen_urls),
            'total_inserted': total_inserted,
            'duplicates': total_duplicates
        }

    def initialize(self):
        """Initialize scraper (browser and storage)."""
        try:
            self.browser.create_driver()
            logger.info("Scraper initialized successfully")
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise ScraperError(f"Failed to initialize scraper: {e}") from e

    def cleanup(self):
        """Cleanup resources."""
        try:
            self.browser.close()
            logger.info("Scraper cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")

    def __enter__(self):
        """Context manager enter."""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()
        return False
