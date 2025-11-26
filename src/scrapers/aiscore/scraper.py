"""Main scraper orchestration"""

import time
import logging
from typing import List, Optional
from datetime import datetime
from selenium.webdriver.common.by import By
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import Config
from .browser import BrowserManager
from .extractor import LinkExtractor
from .models import MatchLink, ScrapingResult
from .metrics import ScrapingMetrics
from .exceptions import (
    ScraperError, NetworkError, CloudflareError,
    ElementNotFoundError, BrowserError
)

logger = logging.getLogger(__name__)


class FootballScraper:
    """Main scraper class with comprehensive error handling"""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.config.ensure_directories()
        
        # NO DATABASE - using JSON only
        self.db = None
        self.browser = BrowserManager(self.config)
        self.extractor = LinkExtractor(self.config)
        
        logger.info("FootballScraper initialized")
    
    def scrape_date(
        self,
        date_str: str,
        metrics: Optional[ScrapingMetrics] = None
    ) -> ScrapingResult:
        """
        Scrape match links for a specific date
        
        Args:
            date_str: Date in format 'YYYYMMDD' or 'YYYY-MM-DD'
            metrics: Optional metrics object to update
            
        Returns:
            ScrapingResult with outcome details
        """
        start_time = time.time()
        date_formatted = date_str.replace('-', '')
        
        logger.info(f"Starting scrape for date: {date_formatted}")
        
        try:
            # Navigate to page
            self._navigate_to_date_page(date_formatted)
            
            # Wait and click All tab
            self._click_all_tab()
            
            # Wait for content
            if not self._wait_for_match_containers():
                logger.warning("No match containers found, continuing anyway")
            
            # Collect and save links (saves after each scroll)
            result_data = self._collect_all_links(date_formatted, metrics)
            
            # Calculate metrics
            duration = time.time() - start_time
            
            # Update metrics
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
        """Navigate to date page with retry logic"""
        url = f"{self.config.scraping.base_url}/{date_formatted}"
        
        try:
            # First, navigate to main page to bypass Cloudflare
            logger.info("Opening main page...")
            self.browser.driver.get(self.config.scraping.base_url)
            time.sleep(self.config.scraping.delays.initial_load)
            
            # Check for Cloudflare
            if not self._wait_for_cloudflare_pass(max_wait=10):
                raise CloudflareError("Cloudflare check timeout on main page")
            
            # Now navigate to date page
            logger.info(f"Navigating to date page: {url}")
            self.browser.driver.get(url)
            time.sleep(self.config.scraping.delays.initial_load)
            
            # Check for Cloudflare again
            if not self._wait_for_cloudflare_pass(
                max_wait=self.config.scraping.timeouts.cloudflare_max
            ):
                raise CloudflareError("Cloudflare check timeout on date page")
            
            logger.info("Successfully navigated to date page")
            
        except Exception as e:
            logger.error(f"Navigation failed: {e}")
            raise NetworkError(f"Failed to navigate to page: {e}")
    
    def _wait_for_cloudflare_pass(self, max_wait: int = 15) -> bool:
        """Wait for Cloudflare protection to pass"""
        wait_time = 0
        while wait_time < max_wait:
            time.sleep(1)
            wait_time += 1
            
            try:
                title = self.browser.driver.title
                if "Just a moment" not in title and "Checking your browser" not in title:
                    logger.info(f"Cloudflare passed (waited {wait_time}s)")
                    return True
            except:
                pass
        
        return False
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        reraise=True
    )
    def _click_all_tab(self):
        """Click the 'All' tab with retry logic"""
        try:
            logger.info("Looking for 'All' tab...")
            
            elements = self.browser.driver.find_elements(
                By.CSS_SELECTOR,
                self.config.selectors.all_tab
            )
            
            if not elements:
                raise ElementNotFoundError("All tab elements not found")
            
            logger.debug(f"Found {len(elements)} tab elements")
            
            # Find the "All" tab
            all_tab = None
            for elem in elements:
                text = elem.text.strip()
                if text.lower() == 'all':
                    all_tab = elem
                    break
            
            if not all_tab:
                logger.warning("'All' tab not found, continuing anyway")
                return
            
            # Click it
            try:
                all_tab.click()
                logger.info("Clicked 'All' tab")
            except:
                # Try JavaScript click as fallback
                self.browser.driver.execute_script("arguments[0].click();", all_tab)
                logger.info("Clicked 'All' tab (JavaScript)")
            
            time.sleep(self.config.scraping.delays.after_click)
            
        except ElementNotFoundError:
            logger.warning("Could not find 'All' tab")
        except Exception as e:
            logger.warning(f"Error clicking 'All' tab: {e}")
    
    def _wait_for_match_containers(self) -> bool:
        """Wait for match containers to appear"""
        return self.browser.wait_for_element(
            self.config.selectors.match_container,
            timeout=self.config.scraping.timeouts.element_wait
        )
    
    def _collect_all_links(
        self,
        source_date: str,
        metrics: Optional[ScrapingMetrics] = None
    ) -> dict:
        """
        Collect and save links by scrolling through the page.
        Saves links to database after each scroll.
        
        Returns:
            dict with 'total_found', 'total_inserted', 'duplicates'
        """
        logger.info("=" * 60)
        logger.info("Starting link collection...")
        logger.info("=" * 60)
        
        seen_urls = set()  # Track URLs we've already processed
        total_inserted = 0
        total_duplicates = 0
        no_change_count = 0
        scroll_iteration = 0
        
        while no_change_count < self.config.scraping.scroll.max_no_change:
            scroll_iteration += 1
            scroll_start = time.time()
            
            # Get current position
            position = self.browser.get_scroll_position()
            
            # Find containers
            containers = self.browser.driver.find_elements(
                By.CSS_SELECTOR,
                self.config.selectors.match_container
            )
            
            logger.info(
                f"Scroll #{scroll_iteration} | Position: {position}px | "
                f"Found {len(containers)} containers"
            )
            
            # Extract NEW links from this scroll
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
            
            # Save new links to database immediately
            if new_urls:
                # Convert to MatchLink objects
                new_links = []
                for url in new_urls:
                    try:
                        match_id = self.extractor.extract_match_id(url)
                        link = MatchLink(
                            url=url,
                            match_id=match_id,
                            source_date=source_date
                        )
                        new_links.append(link)
                    except Exception as e:
                        logger.debug(f"Failed to create MatchLink for {url}: {e}")
                
                # Save to database
                if new_links:
                    try:
                        inserted = self.db.batch_insert_links(new_links)
                        duplicates = len(new_links) - inserted
                        total_inserted += inserted
                        total_duplicates += duplicates
                        
                        logger.info(
                            f"  [+] Found {len(new_urls)} NEW links | "
                            f"Saved {inserted} to DB | "
                            f"Duplicates {duplicates} | "
                            f"Total in DB: {total_inserted}"
                        )
                        
                        # Update metrics
                        if metrics:
                            metrics.links_found += len(new_urls)
                            metrics.links_inserted += inserted
                            metrics.duplicates_prevented += duplicates
                        
                        no_change_count = 0  # Reset counter
                    except Exception as e:
                        logger.error(f"Failed to save links: {e}")
            else:
                no_change_count += 1
                logger.info(
                    f"  [-] No new links (no change: {no_change_count}/"
                    f"{self.config.scraping.scroll.max_no_change})"
                )
            
            # Update metrics
            if metrics:
                scroll_time = time.time() - scroll_start
                metrics.total_scroll_time += scroll_time
                metrics.total_extraction_time += extraction_time
                metrics.scroll_count += 1
            
            # Scroll down
            self.browser.scroll_page(self.config.scraping.scroll.increment)
            time.sleep(self.config.scraping.scroll.pause)
            
            # Check if at bottom
            if self.browser.is_at_bottom() and no_change_count >= 3:
                logger.info("Reached bottom of page")
                break
        
        # Final collection pass
        logger.info("Final collection pass...")
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
            except:
                pass
        
        # Save final batch
        if final_urls:
            final_links = []
            for url in final_urls:
                try:
                    match_id = self.extractor.extract_match_id(url)
                    link = MatchLink(
                        url=url,
                        match_id=match_id,
                        source_date=source_date
                    )
                    final_links.append(link)
                except Exception as e:
                    logger.debug(f"Failed to create MatchLink for {url}: {e}")
            
            if final_links:
                try:
                    inserted = self.db.batch_insert_links(final_links)
                    duplicates = len(final_links) - inserted
                    total_inserted += inserted
                    total_duplicates += duplicates
                    
                    logger.info(f"Final pass: Saved {inserted} additional links")
                    
                    if metrics:
                        metrics.links_found += len(final_urls)
                        metrics.links_inserted += inserted
                        metrics.duplicates_prevented += duplicates
                except Exception as e:
                    logger.error(f"Failed to save final batch: {e}")
        
        logger.info("=" * 60)
        logger.info(f"Collection complete!")
        logger.info(f"  Total unique URLs found: {len(seen_urls)}")
        logger.info(f"  Total saved to DB: {total_inserted}")
        logger.info(f"  Duplicates prevented: {total_duplicates}")
        logger.info("=" * 60)
        
        return {
            'total_found': len(seen_urls),
            'total_inserted': total_inserted,
            'duplicates': total_duplicates
        }
    
    def _save_links_to_database(self, links: List[MatchLink]) -> int:
        """Save links to database in batch"""
        if not links:
            return 0
        
        try:
            inserted = self.db.batch_insert_links(links)
            logger.info(f"Saved {inserted}/{len(links)} links to database")
            return inserted
        except Exception as e:
            logger.error(f"Failed to save links: {e}")
            raise
    
    def initialize(self):
        """Initialize scraper (database, browser)"""
        try:
            # Initialize database (connect and keep open)
            self.db.connect()
            self.db.init_schema()
            self.db.remove_invalid_links()
            
            # Create browser
            self.browser.create_driver()
            
            logger.info("Scraper initialized successfully")
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise ScraperError(f"Failed to initialize scraper: {e}")
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            # Close browser
            self.browser.close()
            
            # Close database
            self.db.close()
            
            logger.info("Scraper cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
    
    def __enter__(self):
        """Context manager enter"""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()
        return False

