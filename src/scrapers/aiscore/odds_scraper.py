"""Odds scraper for football matches"""

import logging
import time
import json
from datetime import datetime
from typing import List, Optional, Set, Dict, Any, Tuple
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from .models import Odds1X2, OddsAsianHandicap, OddsOverUnder
from .browser import BrowserManager
from .bronze_storage import BronzeStorage
from .exceptions import ScraperError

logger = logging.getLogger(__name__)


class OddsScraper:
    """Scrapes odds data from match pages with bronze layer JSON storage.

    This scraper extracts comprehensive match data and odds information,
    storing them as JSON manifests in the data lake bronze layer.

    Attributes:
        config: Configuration instance
        browser: Browser manager
        bronze_storage: Bronze layer storage manager
        processed_matches: Set of processed match IDs
    """

    def __init__(self, config, db, browser: BrowserManager):
        """Initialize odds scraper.

        Note: db parameter is kept for backward compatibility but ignored.
        """
        self.config = config
        self.browser = browser
        # Use storage.bronze_path from config (e.g., "data/aiscore")
        # Support both new config structure (storage.bronze_path) and old (bronze_layer.path)
        if hasattr(config, "storage") and hasattr(config.storage, "bronze_path"):
            bronze_path = config.storage.bronze_path
        elif hasattr(config, "bronze_layer") and hasattr(config.bronze_layer, "path"):
            bronze_path = config.bronze_layer.path
        else:
            bronze_path = "data/aiscore"  # Default fallback
        self.bronze_storage = BronzeStorage(base_path=bronze_path)
        self.processed_matches = set()

    def scrape_match_odds(
        self, match_url: str, match_id: str, game_date: str = None
    ) -> Tuple[List, Dict[str, Any]]:
        """Scrape odds for a single match with complete metadata extraction.

        Args:
            match_url: Base match URL
            match_id: Match ID
            game_date: Game date in YYYYMMDD format (extracted if not provided)

        Returns:
            Tuple of (odds_list, match_info) containing:
                - odds_list: List of specialized odds objects
                - match_info: Dictionary with match metadata (teams, result, league, etc.)
        """
        scrape_start = datetime.now()
        odds_list = []
        errors = []
        warnings = []
        match_info = {
            "match_id": match_id,
            "match_url": match_url,
            "game_date": game_date,
            "home_team": None,
            "away_team": None,
            "match_result": None,
            "league": None,
            "match_time": None,
        }

        try:
            odds_url = self._build_odds_url(match_url)

            self.browser.driver.get(odds_url)
            time.sleep(self.config.scraping.delays.initial_load)

            # Check if content element exists - if not, no odds are available, skip this match
            try:
                content_elements = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".content"
                )
                if not content_elements:
                    logger.info(
                        f"No content element found for match {match_id}, skipping (no odds available)"
                    )
                    match_info = self._extract_match_metadata(
                        match_url, match_id, game_date
                    )

                    # Save status indicating no odds available
                    scrape_end = datetime.now()
                    scrape_timestamp = scrape_end.strftime("%Y%m%d")
                    final_game_date = game_date or scrape_timestamp

                    try:
                        self.bronze_storage.save_complete_match(
                            match_id=match_id,
                            match_url=match_url,
                            game_date=final_game_date,
                            scrape_date=final_game_date,
                            scrape_start=scrape_start,
                            scrape_end=scrape_end,
                            scrape_status="no_odds_available",
                            teams=match_info.get("teams", {"home": None, "away": None}),
                            match_result=match_info.get("match_result"),
                            league=match_info.get("league"),
                            odds_1x2=[],
                            odds_asian_handicap=[],
                            odds_over_under=[],
                        )
                    except Exception as save_error:
                        logger.warning(
                            f"Could not save match {match_id} status: {save_error}"
                        )

                    match_info["error_status"] = "no_odds_available"
                    return [], match_info
            except Exception as e:
                logger.debug(
                    f"Error checking for content element for match {match_id}: {e}"
                )
                # Continue anyway - might be a temporary issue

            match_info = self._extract_match_metadata(match_url, match_id, game_date)

            # SPEED FIX: Reduced timeout from 10s to 2s (8 seconds saved!)
            try:
                WebDriverWait(self.browser.driver, 1).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".lookBox.brb"))
                )
            except TimeoutException:
                pass

            look_boxes = self.browser.driver.find_elements(
                By.CSS_SELECTOR, ".lookBox.brb"
            )

            if not look_boxes:
                tabs = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, "span.changeItem"
                )
            else:
                first_box = look_boxes[0]
                try:
                    first_box.click()
                except:
                    self.browser.driver.execute_script(
                        "arguments[0].click();", first_box
                    )

                time.sleep(self.config.scraping.delays.after_click)
                tabs = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, "span.changeItem"
                )

            if tabs:
                tab_names = [tab.text.strip() for tab in tabs if tab.text.strip()]
                total_tabs = len(tabs)  # Store count (tabs become stale after clicking)

                for tab_idx in range(total_tabs):
                    try:
                        current_tabs = (
                            self._get_tabs_from_changTabBox()
                        )  # Re-fetch to avoid stale reference

                        if tab_idx >= len(current_tabs):
                            logger.debug(f"Tab {tab_idx} not found after re-fetch")
                            continue

                        current_tab = current_tabs[tab_idx]
                        tab_name = current_tab.text.strip() or f"Tab_{tab_idx}"

                        try:
                            classes_before = current_tab.get_attribute("class")
                            is_active_before = "activeElsTab" in classes_before
                        except:
                            is_active_before = False

                        click_success = False

                        try:
                            self.browser.driver.execute_script(
                                "arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});",
                                current_tab,
                            )
                            time.sleep(self.config.scraping.delays.tab_scroll)
                            self.browser.driver.execute_script(
                                "arguments[0].click();", current_tab
                            )
                            click_success = True
                        except:
                            pass

                        if not click_success:
                            try:
                                current_tab.click()
                                click_success = True
                            except:
                                pass

                        if not click_success:
                            try:
                                from selenium.webdriver.common.action_chains import (
                                    ActionChains,
                                )

                                actions = ActionChains(self.browser.driver)
                                actions.move_to_element(current_tab).click().perform()
                                click_success = True
                            except:
                                pass

                        if not click_success:
                            logger.debug(f"Tab {tab_name} click failed, skipping")
                            continue

                        # Wait for table content to load after tab click
                        wait_start = time.time()
                        content_loaded = False
                        max_wait_iterations = 15  # Increased from 10 to 15
                        for iteration in range(max_wait_iterations):
                            try:
                                if self.browser.driver.find_elements(
                                    By.CSS_SELECTOR, ".el-table"
                                ):
                                    content_loaded = True
                                    logger.debug(
                                        f"Table found in tab {tab_name} after {iteration + 1} checks"
                                    )
                                    break
                            except:
                                pass
                            time.sleep(
                                self.config.scraping.delays.content_check_interval
                            )

                        if not content_loaded:
                            logger.debug(
                                f"Table not found in tab {tab_name} after {max_wait_iterations} checks, waiting fallback..."
                            )
                            time.sleep(self.config.scraping.delays.content_fallback)

                        # Wait for tab content to load after click
                        time.sleep(0.3)

                        # Don't skip based on active status - try to scrape anyway
                        # Some tabs might have content even if not marked as active
                        # The table check in _scrape_current_tab_odds will handle empty tabs

                        tab_odds = self._scrape_current_tab_odds(
                            match_url, match_id, tab_name
                        )
                        if tab_odds:
                            odds_list.extend(tab_odds)
                            logger.debug(
                                f"Scraped {len(tab_odds)} odds from tab {tab_name}"
                            )
                        else:
                            logger.debug(f"No odds found in tab {tab_name}")

                    except Exception as e:
                        logger.debug(f"Failed to scrape tab {tab_idx}: {e}")
                        continue
            else:
                tab_odds = self._scrape_current_tab_odds(match_url, match_id, "Default")
                if tab_odds:
                    odds_list.extend(tab_odds)

            scrape_end = datetime.now()
            scrape_timestamp = scrape_end.strftime("%Y%m%d")

            # Use game_date for organizing files (not today's date)
            final_game_date = game_date or scrape_timestamp

            try:
                odds_1x2 = [o for o in odds_list if isinstance(o, Odds1X2)]
                odds_ah = [o for o in odds_list if isinstance(o, OddsAsianHandicap)]
                odds_ou = [o for o in odds_list if isinstance(o, OddsOverUnder)]

                odds_1x2_dicts = [self._odds_to_dict(o) for o in odds_1x2]
                odds_ah_dicts = [self._odds_to_dict(o) for o in odds_ah]
                odds_ou_dicts = [self._odds_to_dict(o) for o in odds_ou]

                if errors:
                    status = "failed" if not odds_list else "partial"
                else:
                    status = "success"

                file_path = self.bronze_storage.save_complete_match(
                    match_id=match_id,
                    match_url=match_url,
                    game_date=final_game_date,
                    scrape_date=final_game_date,
                    scrape_start=scrape_start,
                    scrape_end=scrape_end,
                    scrape_status=status,
                    teams=match_info.get("teams", {"home": None, "away": None}),
                    match_result=match_info.get("match_result"),
                    league=match_info.get("league"),
                    odds_1x2=odds_1x2_dicts,
                    odds_asian_handicap=odds_ah_dicts,
                    odds_over_under=odds_ou_dicts,
                )
                logger.info(f"Match data saved to: {file_path}")

            except Exception as e:
                logger.error(f"Failed to save to bronze storage: {e}", exc_info=True)
                errors.append(f"Bronze storage failed: {str(e)}")
                # Re-raise to ensure the error is visible
                raise

            return odds_list, match_info

        except Exception as e:
            error_msg = f"Failed to scrape odds for {match_id}: {e}"
            logger.error(error_msg)
            errors.append(error_msg)

            # Detect timeout errors (connection issues that should be retried)
            error_str = str(e).lower()
            is_timeout = any(
                keyword in error_str
                for keyword in [
                    "timeout",
                    "timed out",
                    "time out",
                    "receiving message from renderer",
                    "connection refused",
                    "connection reset",
                    "no such window",
                    "target window already closed",
                ]
            )

            scrape_status = "failed_by_timeout" if is_timeout else "failed"

            scrape_end = datetime.now()
            scrape_timestamp = scrape_end.strftime("%Y%m%d")
            final_game_date = game_date or scrape_timestamp
            try:
                self.bronze_storage.save_complete_match(
                    match_id=match_id,
                    match_url=match_url,
                    game_date=final_game_date,
                    scrape_date=final_game_date,  # Use game_date for organizing files
                    scrape_start=scrape_start,
                    scrape_end=scrape_end,
                    scrape_status=scrape_status,
                    teams=match_info.get("teams", {"home": None, "away": None}),
                    match_result=match_info.get("match_result"),
                    league=match_info.get("league"),
                    odds_1x2=[],
                    odds_asian_handicap=[],
                    odds_over_under=[],
                )
            except:
                pass  # Don't fail on bronze write during error handling

            # Return empty list with error status in match_info (status already saved in bronze)
            match_info["error_status"] = scrape_status
            return [], match_info

    def _build_odds_url(self, match_url: str) -> str:
        """Build odds URL from match URL"""
        match_url = match_url.rstrip("/")
        if not match_url.endswith("/odds"):
            return f"{match_url}/odds"
        return match_url

    def _get_tabs_from_changTabBox(self) -> List:
        """
        Get fresh tabs from changTabBox container

        Finds span elements with class "changeItem bh ml-12 elseMaxWidth"

        This is called each time before clicking a tab to avoid stale element references.

        Returns:
            List of WebElement tabs (span elements)
        """
        tabs = []

        try:
            tabs = self.browser.driver.find_elements(By.CSS_SELECTOR, "span.changeItem")

            if not tabs:
                tabs = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, "span.elseMaxWidth"
                )

            if not tabs:
                tabs = self.browser.driver.find_elements(
                    By.XPATH, "//span[contains(@class, 'changeItem')]"
                )

            visible_tabs = []
            for tab in tabs:
                try:
                    if tab.is_displayed() and tab.text.strip():
                        visible_tabs.append(tab)
                except:
                    continue

            return visible_tabs

        except Exception as e:
            return []

    def _scrape_current_tab_odds(
        self, match_url: str, match_id: str, tab_name: str
    ) -> List:
        """
        Scrape odds from table in the currently active tab

        Args:
            match_url: Match URL
            match_id: Match ID
            tab_name: Name of the current tab (e.g., "1 X 2", "Asian Handicap")

        Returns:
            List of specialized odds objects (Odds1X2, OddsAsianHandicap, etc.)
        """
        odds_list = []

        try:
            # Wait for table to appear (increased timeout to ensure table loads)
            try:
                WebDriverWait(self.browser.driver, 2.0).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".el-table"))
                )
            except TimeoutException:
                logger.debug(f"No table in {tab_name} after waiting")
                return odds_list

            tables = self.browser.driver.find_elements(By.CSS_SELECTOR, ".el-table")
            if not tables:
                logger.debug(f"No .el-table elements found in tab {tab_name}")
                return odds_list
            table = tables[0]
            logger.debug(f"Found table in tab {tab_name}, processing rows...")

            try:
                table_body = table.find_element(
                    By.CSS_SELECTOR, ".el-table__body-wrapper"
                )
                max_height = table_body.get_attribute("style")
                if "max-height" in (max_height or ""):
                    self._scroll_table_body(table_body)
            except:
                pass

            rows = table.find_elements(By.CSS_SELECTOR, ".el-table__body tbody tr")
            if not rows:
                alt_rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
                if alt_rows:
                    rows = alt_rows
                else:
                    logger.debug(f"No rows found in table for tab {tab_name}")
                    return odds_list

            logger.debug(f"Found {len(rows)} rows in tab {tab_name}")

            from .odds_parsers import OddsParserFactory

            parser = OddsParserFactory.get_parser(tab_name)

            odds_types_count = {}
            parsed_count = 0
            failed_count = 0

            for idx, row in enumerate(rows):
                try:
                    odds_data = self._parse_table_row(
                        row, match_url, match_id, parser, idx
                    )
                    if odds_data:
                        odds_list.append(odds_data)
                        odds_type = type(odds_data).__name__
                        odds_types_count[odds_type] = (
                            odds_types_count.get(odds_type, 0) + 1
                        )
                        parsed_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    continue

            # (Parsing result logs removed for cleaner output)

        except Exception as e:
            logger.error(f"Error scraping tab {tab_name}: {e}", exc_info=True)

        return odds_list

    def _scroll_table_body(self, table_body_element):
        """
        Scroll through table body to load all rows

        Args:
            table_body_element: The .el-table__body-wrapper element
        """
        try:

            # Get initial scroll height
            last_height = self.browser.driver.execute_script(
                "return arguments[0].scrollHeight", table_body_element
            )

            scroll_attempts = 0
            max_scrolls = 10
            previous_row_count = 0  # Track row count for smart waiting

            while scroll_attempts < max_scrolls:
                # Scroll to bottom
                self.browser.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight",
                    table_body_element,
                )

                # OPTIMIZATION: Smart wait for scroll content - reduced wait time
                wait_start = time.time()
                for _ in range(3):  # Max 0.15s (3 x 0.05s) - faster checks
                    try:
                        new_rows = self.browser.driver.find_elements(
                            By.CSS_SELECTOR, ".el-table__row"
                        )
                        if len(new_rows) > previous_row_count:
                            break
                    except:
                        pass
                    time.sleep(self.config.scraping.delays.content_check_interval)

                previous_row_count = len(new_rows) if "new_rows" in locals() else 0

                # Get new scroll height
                new_height = self.browser.driver.execute_script(
                    "return arguments[0].scrollHeight", table_body_element
                )

                # Check if we've reached the bottom
                if new_height == last_height:
                    break

                last_height = new_height
                scroll_attempts += 1

        except Exception as e:
            pass  # Scrolling errors are non-critical

    def _parse_table_row(
        self, row_element, match_url: str, match_id: str, parser, row_idx: int
    ):
        """
        Parse a single table row using the provided parser

        Args:
            row_element: The tr element
            match_url: Match URL
            match_id: Match ID
            parser: The specialized parser (from OddsParserFactory)
            row_idx: Row index

        Returns:
            Specialized odds object (Odds1X2, OddsAsianHandicap, etc.) or None
        """
        try:
            # Find all cells in the row
            cells = row_element.find_elements(By.TAG_NAME, "td")

            if not cells:
                return None

            # Extract text from each cell
            cell_values = []
            for cell in cells:
                try:
                    text = cell.text.strip()
                    cell_values.append(text)
                except:
                    cell_values.append("")

            # Use the specialized parser
            parsed = parser.parse_row(cell_values, match_id, match_url)
            return parsed

        except Exception as e:
            return None

    def _extract_match_metadata(
        self, match_url: str, match_id: str, game_date: str = None
    ) -> Dict[str, Any]:
        """Extract comprehensive match metadata including teams, result, league, and match time.

        Args:
            match_url: Full match URL
            match_id: Match identifier
            game_date: Game date in YYYYMMDD format (optional)

        Returns:
            Dictionary containing all extracted match metadata
        """
        metadata = {
            "match_id": match_id,
            "match_url": match_url,
            "game_date": game_date,
            "teams": {"home": None, "away": None},
            "home_team": None,  # Keep for backward compatibility
            "away_team": None,  # Keep for backward compatibility
            "match_result": None,
            "home_score": None,
            "away_score": None,
            "league": None,
            "match_time": None,  # FT, HT, Live, etc.
            "match_date_display": None,
            "venue": None,
            "referee": None,
        }

        try:
            # Temporarily disable implicit wait for faster element lookup
            # Store original value to restore later
            original_implicit_wait = self.browser.driver.implicitly_wait(0)

            try:
                # Extract home team - use find_elements() for immediate return if not found
                home_boxes = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".home-box .teamName a"
                )
                if home_boxes:
                    metadata["teams"]["home"] = home_boxes[0].text.strip()
                    metadata["home_team"] = metadata["teams"]["home"]

                # Extract away team
                away_boxes = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".away-box .teamName a"
                )
                if away_boxes:
                    metadata["teams"]["away"] = away_boxes[0].text.strip()
                    metadata["away_team"] = metadata["teams"]["away"]

                # Extract home score
                home_score_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".home-score"
                )
                if home_score_elems:
                    home_score_text = home_score_elems[0].text.strip()
                    metadata["home_score"] = (
                        int(home_score_text) if home_score_text.isdigit() else None
                    )

                # Extract away score
                away_score_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".away-score"
                )
                if away_score_elems:
                    away_score_text = away_score_elems[0].text.strip()
                    metadata["away_score"] = (
                        int(away_score_text) if away_score_text.isdigit() else None
                    )

                # Build match result string
                if (
                    metadata["home_score"] is not None
                    and metadata["away_score"] is not None
                ):
                    metadata["match_result"] = (
                        f"{metadata['home_score']}-{metadata['away_score']}"
                    )

                # Extract league/competition name
                league_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".league-name, .competition-name, .breadcrumb a"
                )
                if league_elems:
                    metadata["league"] = league_elems[0].text.strip()

                # Extract match time/status (FT, HT, Live, etc.)
                status_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".match-status, .matchStatus, .time-status"
                )
                if status_elems:
                    metadata["match_time"] = status_elems[0].text.strip()

                # Extract match date display
                date_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".h-top-center .matchStatus, .match-date"
                )
                if date_elems:
                    metadata["match_date_display"] = date_elems[0].text.strip()

                # Extract venue if available
                venue_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".venue-name, .stadium-name"
                )
                if venue_elems:
                    metadata["venue"] = venue_elems[0].text.strip()

                # Extract referee if available
                referee_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".referee-name"
                )
                if referee_elems:
                    metadata["referee"] = referee_elems[0].text.strip()

            finally:
                # Restore original implicit wait timeout
                self.browser.driver.implicitly_wait(original_implicit_wait)

            # If game_date not provided, try to extract from URL or page
            if not metadata["game_date"]:
                # Try to extract from URL pattern
                try:
                    import re

                    # Look for date pattern in URL: YYYYMMDD or YYYY-MM-DD
                    date_match = re.search(r"(\d{8})|(\d{4}-\d{2}-\d{2})", match_url)
                    if date_match:
                        date_str = date_match.group(1) or date_match.group(2).replace(
                            "-", ""
                        )
                        metadata["game_date"] = date_str
                except:
                    pass

        except Exception as e:
            logger.error(f"Error extracting match metadata: {e}")

        return metadata

    def _odds_to_dict(self, odds: Any) -> Dict[str, Any]:
        """Convert odds object to dictionary.

        Args:
            odds: Odds object (Odds1X2, OddsAsianHandicap, etc.)

        Returns:
            Dictionary representation
        """
        if hasattr(odds, "__dict__"):
            data = odds.__dict__.copy()
        elif hasattr(odds, "to_dict"):
            data = odds.to_dict()
        else:
            from dataclasses import asdict

            data = asdict(odds)

        # Convert datetime objects to ISO strings
        for key, value in list(data.items()):
            if isinstance(value, datetime):
                data[key] = value.isoformat()

        return data

    def _extract_and_save_match_info(self, match_url: str):
        """Legacy method - now using _extract_match_metadata instead."""
        pass  # Keep for backward compatibility

    def scrape_from_daily_matches(self, scrape_date: str, bronze_storage) -> dict:
        """Scrape matches from bronze/{date}/daily_listings.json.

        After successful scraping, updates scrape_status to 'success' in daily_listings file.

        Args:
            scrape_date: Date of daily_listings file (YYYYMMDD)
            bronze_storage: BronzeStorage instance

        Returns:
            Dictionary with scraping statistics
        """
        # Read daily listings
        daily_data = bronze_storage.load_daily_listing(scrape_date)

        if not daily_data:
            listing_path = (
                bronze_storage.daily_listings_dir / scrape_date / "matches.json"
            )
            logger.error(f"Daily listings file not found: {listing_path.absolute()}")
            return {"total_matches": 0, "successful": 0, "failed": 0, "total_odds": 0}

        matches = daily_data.get("matches", [])

        # Check status distribution
        status_counts = {}
        for match in matches:
            status = match.get("scrape_status", "n/a")
            status_counts[status] = status_counts.get(status, 0) + 1

        to_scrape = (
            status_counts.get("n/a", 0)
            + status_counts.get("failed", 0)
            + status_counts.get("pending", 0)
            + status_counts.get("failed_by_timeout", 0)
        )
        to_skip = (
            status_counts.get("success", 0)
            + status_counts.get("partial", 0)
            + status_counts.get("forbidden", 0)
        )

        logger.info(
            f"Scraping odds for {scrape_date}: {to_scrape} matches to process, {to_skip} skipped"
        )

        # Initialize browser
        self.initialize()

        # Track results
        successful = 0
        failed = 0
        total_odds_scraped = 0
        last_logged = 0
        log_interval = max(
            10, len(matches) // 20
        )  # Log every 10 matches or ~5% progress

        # Process each match
        for i, match in enumerate(matches, 1):
            match_id = match["match_id"]
            match_url = match["match_url"]
            game_date = match.get("game_date", scrape_date)
            current_status = match.get("scrape_status", "n/a")

            # Skip forbidden matches silently
            if current_status == "forbidden":
                continue

            # Skip matches with no odds available
            if current_status == "no_odds_available":
                successful += 1  # Count as successful (we determined no odds available)
                continue

            # Skip already scraped matches (resumable)
            # Check both status and actual file existence
            if current_status in ["success", "partial"]:
                successful += 1
                continue

            # Also check if match file actually exists in bronze storage
            # This handles cases where file exists but status wasn't updated
            if bronze_storage.match_exists(match_id, scrape_date):
                successful += 1
                # Update status to success if file exists but status wasn't updated
                try:
                    bronze_storage.update_match_status_in_daily_list(
                        scrape_date, match_id, "success"
                    )
                except Exception as e:
                    logger.warning(f"Could not update status for match {match_id}: {e}")
                continue

            # Change status to pending (retry failed_by_timeout as they may be temporary connection issues)
            if current_status in ["n/a", "failed", "failed_by_timeout"]:
                bronze_storage.update_match_status_in_daily_list(
                    scrape_date, match_id, "pending"
                )

            try:
                # Scrape odds
                odds_list, match_info = self.scrape_match_odds(
                    match_url, match_id, game_date
                )

                # Check if error status was already saved (from exception handler in scrape_match_odds)
                error_status = match_info.get("error_status")

                if error_status:
                    # Status already saved with timeout/failed/no_odds_available in scrape_match_odds
                    if error_status == "no_odds_available":
                        # Update daily listing to mark as no odds available
                        try:
                            bronze_storage.update_match_status_in_daily_list(
                                scrape_date, match_id, "no_odds_available"
                            )
                        except Exception as e:
                            logger.warning(
                                f"Could not update status for match {match_id}: {e}"
                            )
                    failed += 1
                elif odds_list:
                    # Success: update status
                    bronze_storage.update_match_status_in_daily_list(
                        scrape_date, match_id, "success"
                    )
                    total_odds_scraped += len(odds_list)
                    successful += 1
                else:
                    # No odds but no error (shouldn't happen, but handle it)
                    bronze_storage.update_match_status_in_daily_list(
                        scrape_date, match_id, "failed"
                    )
                    failed += 1

                # Log progress periodically
                if i - last_logged >= log_interval or i == len(matches):
                    progress_pct = (i / len(matches)) * 100
                    logger.info(
                        f"Progress: {i}/{len(matches)} ({progress_pct:.0f}%) | Success: {successful} | Failed: {failed} | Odds: {total_odds_scraped}"
                    )
                    last_logged = i

                time.sleep(self.config.scraping.delays.between_matches)

            except Exception as e:
                # Detect timeout errors (connection issues that should be retried)
                error_str = str(e).lower()
                is_timeout = any(
                    keyword in error_str
                    for keyword in [
                        "timeout",
                        "timed out",
                        "time out",
                        "receiving message from renderer",
                        "connection refused",
                        "connection reset",
                        "no such window",
                        "target window already closed",
                    ]
                )

                status = "failed_by_timeout" if is_timeout else "failed"
                bronze_storage.update_match_status_in_daily_list(
                    scrape_date, match_id, status
                )
                failed += 1

                # Log errors only periodically or if critical
                if i - last_logged >= log_interval or not is_timeout:
                    if is_timeout:
                        logger.debug(f"Match {i}/{len(matches)} timeout: {match_id}")
                    else:
                        logger.error(
                            f"Match {i}/{len(matches)} error: {match_id} - {str(e)[:100]}"
                        )
                continue

        logger.info(
            f"Complete: {successful} success, {failed} failed, {total_odds_scraped} total odds"
        )

        return {
            "total_matches": len(matches),
            "successful": successful,
            "failed": failed,
            "total_odds": total_odds_scraped,
        }

    # Legacy database-based method removed - use scrape_from_daily_matches() instead

    def initialize(self):
        """Initialize scraper"""
        try:
            # Create browser if not exists
            if not self.browser.driver:
                self.browser.create_driver()

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise ScraperError(f"Failed to initialize odds scraper: {e}")

    def cleanup(self):
        """Cleanup resources"""
        try:
            # Close all extra windows
            if self.browser.driver:
                original_window = self.browser.driver.current_window_handle
                for handle in self.browser.driver.window_handles:
                    if handle != original_window:
                        self.browser.driver.switch_to.window(handle)
                        self.browser.driver.close()

                self.browser.driver.switch_to.window(original_window)

            logger.info("Odds scraper cleanup completed")

        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
