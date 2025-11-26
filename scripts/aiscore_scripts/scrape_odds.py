"""Odds scraper for football matches"""

import logging
import logging.handlers
import time
import json
from datetime import datetime
from typing import List, Optional, Set, Dict, Any, Tuple
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Add project root to path
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.scrapers.aiscore.models import Odds1X2, OddsAsianHandicap, OddsOverUnder
from src.scrapers.aiscore.browser import BrowserManager
from src.scrapers.aiscore.bronze_storage import BronzeStorage
from src.scrapers.aiscore.exceptions import ScraperError

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
            # STEP 1: Navigate directly to odds page
            odds_url = self._build_odds_url(match_url)
            self.browser.driver.get(odds_url)
            time.sleep(self.config.scraping.delays.initial_load)

            try:
                WebDriverWait(self.browser.driver, 10).until(
                    lambda driver: driver.execute_script("return document.readyState") == "complete"
                )
            except TimeoutException:
                pass  # Continue anyway
            
            current_url = self.browser.driver.current_url
            if "/odds" not in current_url:
                logger.warning(f"[{match_id}] Navigation failed: expected /odds, got {current_url[:80]}")
                self.browser.driver.get(odds_url)
                time.sleep(self.config.scraping.delays.initial_load * 2)
                if "/odds" not in self.browser.driver.current_url:
                    logger.error(f"[{match_id}] Navigation retry failed, skipping match")
                    match_info["error_status"] = "failed_navigation_odds_page"
                    return [], match_info

            # STEP 2: Check if content element exists - try multiple selectors
            # This is now a soft check - we continue even if content element is not found
            content_found = False
            content_selectors = [
                ".content",
                ".main-content",
                ".odds-content",
                "[class*='content']",
                "main",
                ".container",
            ]
            
            try:
                for selector in content_selectors:
                    content_elements = self.browser.driver.find_elements(
                        By.CSS_SELECTOR, selector
                    )
                    if content_elements:
                        content_found = True
                        break
            except Exception as e:
                logger.warning(
                    f"[STEP 2] Error checking for content element for match {match_id}: {e}"
                )
                # Continue anyway - might be a temporary issue
            
            # If content not found, try refresh once
            if not content_found:
                try:
                    self.browser.driver.refresh()
                    time.sleep(self.config.scraping.delays.initial_load)
                    for selector in content_selectors:
                        try:
                            if self.browser.driver.find_elements(By.CSS_SELECTOR, selector):
                                content_found = True
                                break
                        except:
                            continue
                except Exception as refresh_e:
                    logger.debug(f"[{match_id}] Content refresh failed: {refresh_e}")

            # STEP 3: Click on first class="lookBox brb" - a window opens
            look_boxes = []
            look_box_found = False
            in_iframe_context = False
            
            # Temporarily disable implicit wait for faster element lookup
            # Without this, each selector waits up to 10 seconds = 40 seconds total for 4 selectors
            original_implicit_wait = getattr(self.config.scraping.timeouts, 'element_wait', 10)
            self.browser.driver.implicitly_wait(0)
            
            # Try multiple selectors for lookBox
            look_box_selectors = [
                ".lookBox.brb",
                ".lookBox.brb:first-child",
                "[class*='lookBox'][class*='brb']",
                ".lookBox.brb:not([style*='display: none'])",
            ]
            
            # First, try quick check
            try:
                for selector in look_box_selectors:
                    try:
                        look_boxes = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                        if look_boxes:
                            # Filter to only visible ones
                            visible_boxes = [box for box in look_boxes if box.is_displayed()]
                        if visible_boxes:
                            look_boxes = visible_boxes
                            look_box_found = True
                            break
                    except Exception as e:
                        logger.debug(f"[STEP 3] Selector {selector} failed: {e}")
                        continue
                
                # If not found, try 2 refreshes with exactly 3 seconds wait
                if not look_box_found:
                    for refresh_attempt in range(2):
                        try:
                            self.browser.driver.refresh()
                            time.sleep(3.0)
                            for selector in look_box_selectors:
                                try:
                                    look_boxes = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                                    visible_boxes = [box for box in look_boxes if box.is_displayed()]
                                    if visible_boxes:
                                        look_boxes = visible_boxes
                                        look_box_found = True
                                        break
                                except:
                                    continue
                            if look_box_found:
                                break
                        except Exception:
                            pass
                
                if not look_box_found:
                    logger.debug(f"[{match_id}] LookBox.brb not found after 2 refreshes")
            finally:
                # Restore original implicit wait timeout
                try:
                    self.browser.driver.implicitly_wait(original_implicit_wait)
                except Exception as restore_e:
                    logger.debug(f"[STEP 3] Error restoring implicit wait: {restore_e}")
                    # Fallback: set to default 10 seconds
                    try:
                        self.browser.driver.implicitly_wait(5)
                    except:
                        pass
            
            # Now click the lookBox if found
            if look_box_found and look_boxes:
                # Only switch back to default content if we're NOT in an iframe context
                if not in_iframe_context:
                    try:
                        self.browser.driver.switch_to.default_content()
                    except:
                        pass
                
                first_box = look_boxes[0]
                click_success = False
                
                try:
                    self.browser.driver.execute_script(
                        "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                        first_box
                    )
                    time.sleep(0.5)
                    
                    try:
                        first_box.click()
                        click_success = True
                    except:
                        try:
                            self.browser.driver.execute_script("arguments[0].click();", first_box)
                            click_success = True
                        except:
                            try:
                                from selenium.webdriver.common.action_chains import ActionChains
                                ActionChains(self.browser.driver).move_to_element(first_box).click().perform()
                                click_success = True
                            except:
                                pass
                except Exception:
                    pass
                
                if click_success:
                    if in_iframe_context:
                        try:
                            self.browser.driver.switch_to.default_content()
                        except:
                            pass
                    time.sleep(self.config.scraping.delays.after_click)
                else:
                    logger.warning(f"[{match_id}] Failed to click LookBox.brb, continuing anyway")
                    if in_iframe_context:
                        try:
                            self.browser.driver.switch_to.default_content()
                        except:
                            pass
            # If element not found after refreshes, step 3 is skipped - continue to next step
            
            # Get tabs after clicking lookBox (or if lookBox wasn't found)
            tabs = self._get_tabs_from_changTabBox()

            # Extract match metadata AFTER clicking lookBox (elements are now available)
            match_info = self._extract_match_metadata(match_url, match_id, game_date)

            # STEP 4: Process tabs (already found above)
            # Track tabs completion for status reporting
            tabs_completed = 0
            total_tabs = 0
            
            if tabs:
                total_tabs = len(tabs)
                logger.info(f"[{match_id}] Found {total_tabs} tab(s), scraping odds")
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
                            try:
                                current_tab.click()
                                click_success = True
                            except:
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
                            logger.debug(f"[STEP 6] Tab {tab_name} click failed, skipping")
                            continue

                        wait_start = time.time()
                        content_loaded = False
                        for _ in range(10):
                            try:
                                if self.browser.driver.find_elements(
                                    By.CSS_SELECTOR, ".el-table"
                                ):
                                    content_loaded = True
                                    break
                            except:
                                pass
                            time.sleep(
                                self.config.scraping.delays.content_check_interval
                            )

                        if not content_loaded:
                            time.sleep(self.config.scraping.delays.content_fallback)

                        try:
                            current_tabs_check = self._get_tabs_from_changTabBox()
                            if tab_idx < len(current_tabs_check):
                                classes_after = current_tabs_check[
                                    tab_idx
                                ].get_attribute("class")
                                is_active_after = "activeElsTab" in classes_after

                                if not is_active_after and not is_active_before:
                                    logger.debug(f"Tab {tab_name} not active, skipping")
                                    continue
                        except:
                            pass

                        # STEP 7: Scrape table
                        tab_odds = self._scrape_current_tab_odds(
                            match_url, match_id, tab_name
                        )
                        if tab_odds:
                            tabs_completed += 1
                            logger.info(f"[STEP 7] Tab '{tab_name}': {len(tab_odds)} odds")
                            odds_list.extend(tab_odds)
                        else:
                            logger.debug(f"[STEP 7] Tab '{tab_name}': no odds")

                    except Exception as e:
                        logger.debug(f"Failed to scrape tab {tab_idx}: {e}")
                        continue
            else:
                # No tabs found - try to scrape default table
                logger.info(f"[{match_id}] No tabs found, attempting default table scrape")
                total_tabs = 0
                tabs_completed = 0
                
                step4_start_time = time.time()
                timeout_seconds = 5
                
                try:
                    content_loaded = False
                    max_wait_attempts = 20
                    
                    # Try scrolling to trigger lazy loading
                    try:
                        self.browser.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                        time.sleep(0.2)
                        self.browser.driver.execute_script("window.scrollTo(0, 0);")
                        time.sleep(0.2)
                    except:
                        pass
                    
                    # Wait for table to appear
                    for attempt in range(max_wait_attempts):
                        if time.time() - step4_start_time > timeout_seconds:
                            logger.error(f"[{match_id}] Timeout ({timeout_seconds}s) during table search")
                            match_info["error_status"] = "timeout_default_table_scraping"
                            return [], match_info
                        
                        try:
                            if (self.browser.driver.find_elements(By.CSS_SELECTOR, ".el-table") or
                                self.browser.driver.find_elements(By.TAG_NAME, "table")):
                                content_loaded = True
                                break
                        except:
                            pass
                        time.sleep(self.config.scraping.delays.content_check_interval)
                    
                    if time.time() - step4_start_time > timeout_seconds:
                        logger.error(f"[{match_id}] Timeout ({timeout_seconds}s) before fallback wait")
                        match_info["error_status"] = "timeout_default_table_scraping"
                        return [], match_info
                    
                    if not content_loaded:
                        time.sleep(self.config.scraping.delays.content_fallback)
                    
                    if time.time() - step4_start_time > timeout_seconds:
                        logger.error(f"[{match_id}] Timeout ({timeout_seconds}s) before scraping")
                        match_info["error_status"] = "timeout_default_table_scraping"
                        return [], match_info
                    
                    tab_odds = self._scrape_current_tab_odds(match_url, match_id, "Default")
                    
                    elapsed_time = time.time() - step4_start_time
                    if elapsed_time > timeout_seconds:
                        logger.error(f"[{match_id}] Timeout ({elapsed_time:.1f}s > {timeout_seconds}s) during table scrape, skipping")
                        match_info["error_status"] = "timeout_default_table_scraping"
                        return [], match_info
                    
                    if tab_odds:
                        tabs_completed = 1
                        total_tabs = 1
                        logger.info(f"[{match_id}] Default table: {len(tab_odds)} odds")
                        odds_list.extend(tab_odds)
                except Exception as e:
                    elapsed_time = time.time() - step4_start_time
                    if elapsed_time > timeout_seconds:
                        logger.error(f"[{match_id}] Timeout ({elapsed_time:.1f}s) with exception: {e}")
                        match_info["error_status"] = "timeout_default_table_scraping"
                        return [], match_info
                    else:
                        logger.error(f"[{match_id}] Error in default table scraping: {e}", exc_info=True)
                        raise

            scrape_end = datetime.now()
            scrape_timestamp = scrape_end.strftime("%Y%m%d")

            # Use game_date for organizing files (not today's date)
            final_game_date = game_date or scrape_timestamp

            try:
                odds_1x2 = [o for o in odds_list if isinstance(o, Odds1X2)]
                odds_ah = [o for o in odds_list if isinstance(o, OddsAsianHandicap)]
                odds_ou = [o for o in odds_list if isinstance(o, OddsOverUnder)]

                logger.info(f"[{match_id}] Completed: {tabs_completed}/{total_tabs} tabs, {len(odds_list)} odds (1X2:{len(odds_1x2)} AH:{len(odds_ah)} OU:{len(odds_ou)})")

                odds_1x2_dicts = [self._odds_to_dict(o) for o in odds_1x2]
                odds_ah_dicts = [self._odds_to_dict(o) for o in odds_ah]
                odds_ou_dicts = [self._odds_to_dict(o) for o in odds_ou]

                # Set descriptive status based on tabs completed
                if errors:
                    if not odds_list:
                        status = "failed_some_tabs_error"
                    else:
                        status = f"partial_{tabs_completed}_{total_tabs}_tabs"
                else:
                    if total_tabs > 0:
                        status = f"success_{tabs_completed}_{total_tabs}_tabs"
                    else:
                        # Fallback if no tabs found but odds scraped
                        status = "success_1_1_tabs" if odds_list else "failed_no_tabs_found"
                
                # Store status in match_info for use in daily listing update
                match_info["scrape_status"] = status

                paths = self.bronze_storage.save_complete_match(
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

            except Exception as e:
                logger.error(f"Failed to save to bronze storage: {e}")
                errors.append(f"Bronze storage failed: {str(e)}")

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

            # Create descriptive status based on error type
            if is_timeout:
                scrape_status = "failed_timeout_connection"
            else:
                error_msg_lower = error_str[:50] if error_str else ""
                if "navigation" in error_msg_lower or "url" in error_msg_lower:
                    scrape_status = "failed_navigation_error"
                elif "table" in error_msg_lower or "element" in error_msg_lower:
                    scrape_status = "failed_table_not_found"
                elif "parsing" in error_msg_lower or "parse" in error_msg_lower:
                    scrape_status = "failed_parsing_error"
                else:
                    scrape_status = "failed_unknown_error"

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

        Finds tabs inside the changTabBox container (class="changTabBox")

        This is called each time before clicking a tab to avoid stale element references.

        Returns:
            List of WebElement tabs (span elements)
        """
        tabs = []

        # Temporarily disable implicit wait for faster element lookup
        # Without this, each find_elements() waits up to 10 seconds = 30+ seconds total
        # Set to 0: if elements exist, they're found immediately; if not, return immediately
        original_implicit_wait = getattr(self.config.scraping.timeouts, 'element_wait', 10)
        self.browser.driver.implicitly_wait(0)

        try:
            # First, find the changTabBox container
            changTabBox = self.browser.driver.find_elements(By.CSS_SELECTOR, ".changTabBox")
            
            if changTabBox:
                # Find tabs inside the changTabBox container
                tabs = changTabBox[0].find_elements(By.CSS_SELECTOR, "span.changeItem")
            
            # Fallback: try direct selectors if changTabBox not found
            if not tabs:
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
        finally:
            # Restore original implicit wait timeout
            try:
                self.browser.driver.implicitly_wait(original_implicit_wait)
            except Exception as restore_e:
                logger.debug(f"Error restoring implicit wait in _get_tabs_from_changTabBox: {restore_e}")
                # Fallback: set to default
                try:
                    self.browser.driver.implicitly_wait(5)
                except:
                    pass

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
            # Wait for loading indicators to disappear
            self._wait_for_loading_indicators()

            # Additional wait for dynamic content
            time.sleep(0.5)

            # Find table using chain of responsibility pattern
            table = self._find_table_element(match_id, tab_name)

            if table is None:
                logger.warning(f"[{match_id}] No table found after all attempts ({tab_name})")
                return odds_list

            # Try to scroll table body if it exists
            try:
                table_body = table.find_element(
                    By.CSS_SELECTOR, ".el-table__body-wrapper"
                )
                max_height = table_body.get_attribute("style")
                if "max-height" in (max_height or ""):
                    logger.debug(f"[TABLE] Scrolling table body in tab '{tab_name}'...")
                    self._scroll_table_body(table_body)
            except Exception as e:
                logger.debug(f"[TABLE] Could not scroll table body: {e}")

            # Try multiple selectors to find rows, with waiting for dynamic content
            # Disable implicit wait for faster row lookup
            original_implicit_wait_rows = getattr(self.config.scraping.timeouts, 'element_wait', 10)
            self.browser.driver.implicitly_wait(0)
            
            rows = []
            row_selectors = [
                ".el-table__body tbody tr",
                "tbody tr",
                "tr",
                ".el-table__row",
                ".table-row",
                "[class*='row']",
            ]
            
            # Wait for rows to appear (they might load dynamically)
            rows_found = False
            max_row_wait_attempts = 3  # Reduced to 3 attempts
            
            try:
                for attempt in range(max_row_wait_attempts):
                    for selector in row_selectors:
                        try:
                            rows = table.find_elements(By.CSS_SELECTOR, selector)
                            if rows:
                                # Filter out header rows if possible
                                filtered_rows = []
                                for row in rows:
                                    try:
                                        # Skip if it's clearly a header row
                                        row_class = row.get_attribute("class") or ""
                                        if "header" in row_class.lower():
                                            continue
                                        # Skip if it's in thead (check parent)
                                        try:
                                            parent = row.find_element(By.XPATH, "./..")
                                            if parent.tag_name.lower() == "thead":
                                                continue
                                        except:
                                            pass  # Parent check failed, continue anyway
                                        filtered_rows.append(row)
                                    except:
                                        filtered_rows.append(row)
                                
                                if filtered_rows:
                                    rows = filtered_rows
                                    rows_found = True
                                    logger.debug(f"[{match_id}] Found {len(rows)} rows ({selector}, attempt {attempt + 1})")
                                    break
                        except Exception as e:
                            logger.debug(f"[TABLE] Row selector {selector} failed: {e}")
                            continue
                    
                    if rows_found:
                        break
                        
                    # Wait a bit before retrying
                    if attempt < max_row_wait_attempts - 1:
                        time.sleep(self.config.scraping.delays.content_check_interval)
            finally:
                # Restore original implicit wait timeout
                try:
                    self.browser.driver.implicitly_wait(original_implicit_wait_rows)
                except:
                    pass
            
            if not rows:
                logger.warning(f"[{match_id}] No rows found ({tab_name}, {max_row_wait_attempts} attempts)")
                return odds_list

            from src.scrapers.aiscore.odds_parsers import OddsParserFactory

            parser = OddsParserFactory.get_parser(tab_name)
            logger.debug(
                f"[TABLE] Using parser: {type(parser).__name__} for tab '{tab_name}'"
            )

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
                    logger.debug(f"[TABLE] Failed to parse row {idx + 1}: {e}")

            logger.debug(f"[TABLE] Tab '{tab_name}': {parsed_count} parsed, {failed_count} failed")

            # (Parsing result logs removed for cleaner output)

        except Exception as e:
            logger.error(f"Error scraping tab {tab_name}: {e}", exc_info=True)

        return odds_list

    def _wait_for_loading_indicators(self):
        """Wait for loading indicators to disappear before scraping."""
        loading_selectors = [".loading", ".spinner", "[class*='loading']", ".el-loading-mask"]

        for selector in loading_selectors:
            try:
                # Disable implicit wait for quick check
                original_implicit = getattr(self.config.scraping.timeouts, 'element_wait', 10)
                self.browser.driver.implicitly_wait(0)
                loading_elements = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                self.browser.driver.implicitly_wait(original_implicit)

                if loading_elements:
                    # Wait max 2 seconds for loading to disappear
                    try:
                        WebDriverWait(self.browser.driver, 2).until_not(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                    except TimeoutException:
                        logger.debug(f"[TABLE] Loading indicator {selector} still present after 2s, continuing anyway")
            except Exception as e:
                logger.debug(f"[TABLE] Error waiting for loading indicator {selector}: {e}")

    def _find_table_element(self, match_id: str, tab_name: str):
        """
        Find table element using Chain of Responsibility pattern.
        Tries multiple strategies in order of likelihood.

        Args:
            match_id: Match ID for logging
            tab_name: Tab name for logging

        Returns:
            WebElement or None
        """
        # Strategy 1: Quick check with common selectors
        table = self._try_quick_table_selectors(match_id, tab_name)
        if table:
            return table

        # Strategy 2: Wait and retry with timeout
        table = self._try_wait_for_table(match_id, tab_name)
        if table:
            return table

        # Strategy 3: Check dialog containers
        table = self._try_dialog_container_search(match_id, tab_name)
        if table:
            return table

        # Strategy 4: Check iframes
        table = self._try_iframe_search(match_id, tab_name)
        if table:
            return table

        # Strategy 5: Comprehensive search with scrolling
        table = self._try_comprehensive_search(match_id, tab_name)
        if table:
            return table

        # Strategy 6: JavaScript-based detection
        table = self._try_javascript_detection(match_id, tab_name)
        if table:
            return table

        # Strategy 7: Refresh and retry
        table = self._try_refresh_and_retry(match_id, tab_name)
        if table:
            return table

        return None

    def _try_quick_table_selectors(self, match_id: str, tab_name: str):
        """Strategy 1: Try common selectors without waiting."""
        table_selectors = [
            ".el-dialog__body .el-table",
            ".el-dialog__body table",
            ".el-dialog__body",
            ".el-table",
            "table.el-table",
            ".el-table__inner",
            "table",
            ".odds-table",
            ".table",
            "[class*='table']",
        ]

        for selector in table_selectors:
            try:
                tables = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                if tables:
                    logger.debug(f"[{match_id}] Table found: {selector} ({tab_name})")
                    return tables[0]
            except Exception as e:
                logger.debug(f"[TABLE] Selector {selector} failed: {e}")

        return None

    def _try_wait_for_table(self, match_id: str, tab_name: str):
        """Strategy 2: Wait for table to appear with WebDriverWait."""
        wait_time = 3.0
        table_selectors = [
            ".el-dialog__body .el-table",
            ".el-dialog__body table",
            ".el-table",
            "table.el-table",
            "table",
        ]

        for attempt in range(2):
            wait_this_attempt = wait_time / (2 - attempt) if attempt < 1 else wait_time

            for selector in table_selectors:
                try:
                    WebDriverWait(self.browser.driver, wait_this_attempt).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    tables = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                    if tables:
                        logger.debug(f"[{match_id}] Table found after {attempt + 1} attempts ({tab_name})")
                        return tables[0]
                except TimeoutException:
                    continue
                except Exception as e:
                    logger.debug(f"[TABLE] Wait failed for {selector}: {e}")

            # Between attempts, scroll to trigger lazy loading
            if attempt < 1:
                try:
                    self.browser.driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                    time.sleep(0.3)
                except Exception as e:
                    logger.debug(f"[TABLE] Scroll between attempts failed: {e}")

        return None

    def _try_dialog_container_search(self, match_id: str, tab_name: str):
        """Strategy 3: Search within dialog body containers."""
        try:
            dialog_bodies = self.browser.driver.find_elements(By.CSS_SELECTOR, ".el-dialog__body")
            if dialog_bodies:
                for dialog_body in dialog_bodies:
                    dialog_tables = dialog_body.find_elements(By.CSS_SELECTOR, ".el-table, table")
                    if dialog_tables:
                        logger.debug(f"[{match_id}] Table found in dialog body ({tab_name})")
                        return dialog_tables[0]
        except Exception as e:
            logger.debug(f"[TABLE] Dialog container search failed: {e}")

        return None

    def _try_iframe_search(self, match_id: str, tab_name: str):
        """Strategy 4: Check if table is inside an iframe."""
        try:
            iframes = self.browser.driver.find_elements(By.TAG_NAME, "iframe")
            for iframe_idx, iframe in enumerate(iframes):
                try:
                    self.browser.driver.switch_to.frame(iframe)
                    iframe_tables = self.browser.driver.find_elements(By.TAG_NAME, "table")
                    if iframe_tables:
                        logger.debug(f"[{match_id}] Table found in iframe {iframe_idx + 1} ({tab_name})")
                        return iframe_tables[0]
                    self.browser.driver.switch_to.default_content()
                except Exception as e:
                    logger.debug(f"[TABLE] Iframe {iframe_idx} check failed: {e}")
                    try:
                        self.browser.driver.switch_to.default_content()
                    except:
                        pass
        except Exception as e:
            logger.debug(f"[TABLE] Iframe search failed: {e}")
        finally:
            # Ensure we're back to default content
            try:
                self.browser.driver.switch_to.default_content()
            except:
                pass

        return None

    def _try_comprehensive_search(self, match_id: str, tab_name: str):
        """Strategy 5: Comprehensive search with scrolling and nested elements."""
        try:
            # Try scrolling to trigger lazy loading
            self.browser.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.0)
            self.browser.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1.0)
        except Exception as e:
            logger.debug(f"[TABLE] Scroll for lazy loading failed: {e}")

        comprehensive_selectors = [
            ".el-dialog__body table",
            ".el-dialog__body .el-table",
            "table",
            "div[class*='table']",
            ".el-table"
        ]

        for selector in comprehensive_selectors:
            try:
                elements = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for elem in elements:
                        try:
                            nested_tables = elem.find_elements(By.TAG_NAME, "table")
                            if nested_tables:
                                logger.debug(f"[{match_id}] Nested table found ({tab_name})")
                                return nested_tables[0]
                        except Exception as e:
                            logger.debug(f"[TABLE] Nested table check failed: {e}")
            except Exception as e:
                logger.debug(f"[TABLE] Comprehensive selector {selector} failed: {e}")

        # Final fallback: any table tag
        try:
            all_tables = self.browser.driver.find_elements(By.TAG_NAME, "table")
            if all_tables:
                logger.debug(f"[{match_id}] Table found by tag name ({tab_name})")
                return all_tables[0]
        except Exception as e:
            logger.debug(f"[TABLE] Tag name search failed: {e}")

        # Check inside .content container
        try:
            content_elements = self.browser.driver.find_elements(By.CSS_SELECTOR, ".content")
            if content_elements:
                nested_tables = content_elements[0].find_elements(By.TAG_NAME, "table")
                if nested_tables:
                    logger.debug(f"[{match_id}] Found {len(nested_tables)} nested tables")
                    return nested_tables[0]
        except Exception as e:
            logger.debug(f"[TABLE] Error checking nested tables: {e}")

        return None

    def _try_javascript_detection(self, match_id: str, tab_name: str):
        """Strategy 6: Use JavaScript to find tables (handles Shadow DOM)."""
        logger.debug(f"[TABLE] Trying JavaScript-based table detection...")

        try:
            # Find visible tables using JavaScript
            table_index = self.browser.driver.execute_script("""
                var tables = document.querySelectorAll('table');
                for (var i = 0; i < tables.length; i++) {
                    var table = tables[i];
                    var rect = table.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0 && table.rows && table.rows.length > 0) {
                        return i;
                    }
                }
                return -1;
            """)

            if table_index >= 0:
                all_tables_js = self.browser.driver.find_elements(By.TAG_NAME, "table")
                if table_index < len(all_tables_js):
                    logger.debug(f"[{match_id}] Accessed table via JS (index {table_index})")
                    return all_tables_js[table_index]

            # Fallback: manually check visible tables
            all_tables_js = self.browser.driver.find_elements(By.TAG_NAME, "table")
            for t in all_tables_js:
                try:
                    if t.is_displayed():
                        size = t.size
                        if size['width'] > 0 and size['height'] > 0:
                            rows = t.find_elements(By.TAG_NAME, "tr")
                            if len(rows) > 0:
                                logger.debug(f"[{match_id}] Found visible table ({len(rows)} rows)")
                                return t
                except Exception as e:
                    logger.debug(f"[TABLE] Visible table check failed: {e}")

        except Exception as js_error:
            logger.debug(f"[TABLE] Error in JavaScript table detection: {js_error}")

        # Diagnostic: check page source
        self._log_table_diagnostics(match_id)

        return None

    def _try_refresh_and_retry(self, match_id: str, tab_name: str):
        """Strategy 7: Refresh page and retry with primary selectors."""
        logger.debug(f"[{match_id}] No table found, refreshing ({tab_name})")

        try:
            self.browser.driver.refresh()
            time.sleep(self.config.scraping.delays.initial_load)

            # Check again after refresh with primary selectors
            primary_selectors = [
                ".el-dialog__body .el-table",
                ".el-dialog__body table",
                ".el-table",
                "table"
            ]

            for selector in primary_selectors:
                try:
                    tables = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                    if tables:
                        logger.debug(f"[{match_id}] Table found after refresh ({tab_name})")
                        return tables[0]
                except Exception as e:
                    logger.debug(f"[TABLE] Post-refresh selector {selector} failed: {e}")

        except Exception as refresh_e:
            logger.warning(f"[TABLE] Error during refresh: {refresh_e}")

        return None

    def _log_table_diagnostics(self, match_id: str):
        """Log diagnostic information about page structure."""
        try:
            page_source = self.browser.driver.page_source
            if "table" in page_source.lower():
                logger.debug(f"[{match_id}] Table in source but not accessible (shadow DOM?)")

                # Check for table patterns in HTML
                import re
                table_patterns = [
                    r'<table[^>]*>',
                    r'class=["\'][^"\']*table[^"\']*["\']',
                    r'id=["\'][^"\']*table[^"\']*["\']',
                ]

                for pattern in table_patterns:
                    matches = re.findall(pattern, page_source, re.IGNORECASE)
                    if matches:
                        logger.debug(f"[TABLE] Found {len(matches)} matches for pattern {pattern[:30]}...")
                        for i, match in enumerate(matches[:3]):
                            logger.debug(f"[TABLE]   Match {i+1}: {match[:100]}")
            else:
                logger.debug(f"[{match_id}] No table in page source")

            # Log container elements
            container_selectors = [
                ".content",
                ".main-content",
                ".odds-container",
                "[class*='odds']",
                "[class*='betting']",
            ]

            for selector in container_selectors:
                try:
                    containers = self.browser.driver.find_elements(By.CSS_SELECTOR, selector)
                    if containers:
                        logger.debug(f"[TABLE] Found {len(containers)} container(s) with selector: {selector}")
                        for idx, container in enumerate(containers[:2]):
                            try:
                                container_html = container.get_attribute('outerHTML')[:200]
                                logger.debug(f"[TABLE]   Container {idx+1} HTML preview: {container_html}...")
                            except Exception as e:
                                logger.debug(f"[TABLE]   Container {idx+1} HTML preview failed: {e}")
                except Exception as e:
                    logger.debug(f"[TABLE] Container selector {selector} failed: {e}")

        except Exception as e:
            logger.debug(f"[TABLE] Error checking page source: {e}")

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

                # OPTIMIZATION: Smart wait for scroll content
                wait_start = time.time()
                for _ in range(5):  # Max 0.5s (5 x 0.1s)
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
            # Store original value from config (implicitly_wait doesn't return previous value)
            original_implicit_wait = getattr(self.config.scraping.timeouts, 'element_wait', 10)
            # Set to 0 for immediate return
            self.browser.driver.implicitly_wait(0)

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
                    try:
                        home_score_text = home_score_elems[0].text
                        if home_score_text:
                            home_score_text = home_score_text.strip()
                            if home_score_text and home_score_text.isdigit():
                                metadata["home_score"] = int(home_score_text)
                    except Exception as e:
                        logger.debug(f"Error extracting home score: {e}")

                # Extract away score
                away_score_elems = self.browser.driver.find_elements(
                    By.CSS_SELECTOR, ".away-score"
                )
                if away_score_elems:
                    try:
                        away_score_text = away_score_elems[0].text
                        if away_score_text:
                            away_score_text = away_score_text.strip()
                            if away_score_text and away_score_text.isdigit():
                                metadata["away_score"] = int(away_score_text)
                    except Exception as e:
                        logger.debug(f"Error extracting away score: {e}")

                # Build match result string
                if (
                    metadata["home_score"] is not None
                    and metadata["away_score"] is not None
                ):
                    try:
                        # Ensure scores are integers before formatting
                        home_score_val = int(metadata["home_score"]) if metadata["home_score"] is not None else None
                        away_score_val = int(metadata["away_score"]) if metadata["away_score"] is not None else None
                        if home_score_val is not None and away_score_val is not None:
                            metadata["match_result"] = f"{home_score_val}-{away_score_val}"
                    except (ValueError, TypeError) as score_err:
                        logger.debug(f"Error formatting match result: {score_err}")
                        metadata["match_result"] = None

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
                try:
                    self.browser.driver.implicitly_wait(original_implicit_wait)
                except Exception as restore_e:
                    logger.debug(f"Error restoring implicit wait: {restore_e}")
                    # Set a safe default
                    try:
                        self.browser.driver.implicitly_wait(10)
                    except:
                        pass

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
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"Error extracting match metadata: {e}")
            logger.error(f"Full traceback:\n{error_details}")  # Changed from debug to error to ensure it's logged
            # Ensure metadata dict is still returned even on error
            if 'metadata' not in locals():
                metadata = {
                    "match_id": match_id,
                    "match_url": match_url,
                    "game_date": game_date,
                    "teams": {"home": None, "away": None},
                    "home_team": None,
                    "away_team": None,
                    "match_result": None,
                    "home_score": None,
                    "away_score": None,
                    "league": None,
                    "match_time": None,
                    "match_date_display": None,
                    "venue": None,
                    "referee": None,
                }
            # Ensure all numeric fields are properly set to None (not missing)
            if metadata.get("home_score") is None:
                metadata["home_score"] = None
            if metadata.get("away_score") is None:
                metadata["away_score"] = None

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
        
        # Count matches to scrape (failed statuses, n/a, pending)
        to_scrape = status_counts.get("n/a", 0) + status_counts.get("pending", 0)
        for status, count in status_counts.items():
            if status and status.startswith("failed"):
                to_scrape += count
        
        # Count matches to skip (success, partial, forbidden)
        to_skip = status_counts.get("forbidden", 0)
        for status, count in status_counts.items():
            if status and (status.startswith("success") or status.startswith("partial")):
                to_skip += count
        
        logger.info(
            f"Scraping odds for {scrape_date}: {to_scrape} matches to process, {to_skip} already completed, {len(matches)} total"
        )
        
        # Log status distribution for debugging
        if status_counts:
            logger.info(f"Status distribution: {dict(status_counts)}")

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
        
        # Track detailed status counts
        status_tracking = {
            "success": 0,
            "partial": 0,
            "failed": 0,
            "failed_timeout_connection": 0,
            "failed_navigation_error": 0,
            "failed_table_not_found": 0,
            "failed_parsing_error": 0,
            "failed_unknown_error": 0,
            "failed_no_tabs_found": 0,
            "failed_some_tabs_error": 0,
            "timeout_default_table_scraping": 0,
            "failed_navigation_odds_page": 0,
            "pending": 0,
            "forbidden": 0,
            "skipped": 0
        }

        # Process each match
        for i, match in enumerate(matches, 1):
            match_id = str(match["match_id"])  # Ensure string type
            match_url = match["match_url"]
            game_date = match.get("game_date", scrape_date)
            current_status = match.get("scrape_status", "n/a")

            # Skip forbidden matches silently
            if current_status == "forbidden":
                status_tracking["forbidden"] += 1
                continue

            # Skip already scraped matches (resumable) - check for success/partial prefixes
            if current_status and (current_status.startswith("success") or current_status.startswith("partial")):
                successful += 1
                if current_status.startswith("success"):
                    status_tracking["success"] += 1
                elif current_status.startswith("partial"):
                    status_tracking["partial"] += 1
                status_tracking["skipped"] += 1
                if i <= 5 or (i % 50 == 0):  # Log first 5 and every 50th skipped match
                    logger.debug(f"Skipping match {match_id} - already completed with status: {current_status}")
                continue

            # Change status to pending (retry failed statuses as they may be temporary issues)
            if current_status in ["n/a", "pending"] or (current_status and current_status.startswith("failed")):
                if current_status == "pending":
                    status_tracking["pending"] += 1
                elif current_status and current_status.startswith("failed"):
                    # Track specific failure types
                    for status_key in status_tracking.keys():
                        if status_key in current_status:
                            status_tracking[status_key] += 1
                            break
                    else:
                        status_tracking["failed"] += 1
                
                update_success = bronze_storage.update_match_status_in_daily_list(
                    scrape_date, match_id, "pending"
                )
                if not update_success and i <= 10:  # Log first 10 failures
                    logger.warning(f"Failed to update status to 'pending' for match {match_id}")

            try:
                # Scrape odds
                odds_list, match_info = self.scrape_match_odds(
                    match_url, match_id, game_date
                )

                # Check if error status was already saved (from exception handler in scrape_match_odds)
                error_status = match_info.get("error_status")

                if error_status:
                    # Status already saved with descriptive error in scrape_match_odds
                    # Update daily listing with the same descriptive status
                    update_success = bronze_storage.update_match_status_in_daily_list(
                        scrape_date, match_id, error_status
                    )
                    if not update_success:
                        logger.error(f"Failed to update status '{error_status}' for match {match_id} in daily listing")
                    failed += 1
                    
                    # Track specific error status
                    for status_key in status_tracking.keys():
                        if status_key in error_status:
                            status_tracking[status_key] += 1
                            break
                    else:
                        status_tracking["failed"] += 1
                elif odds_list:
                    # Success: status already set in scrape_match_odds with tabs info
                    # Just use the status from match_info if available, otherwise keep existing logic
                    status = match_info.get("scrape_status")
                    if not status or not status.startswith("success"):
                        # Fallback: create status from odds count if tabs info not available
                        status = f"success_{len(odds_list)}_odds"
                    
                    update_success = bronze_storage.update_match_status_in_daily_list(
                        scrape_date, match_id, status
                    )
                    if not update_success:
                        logger.error(f"Failed to update status '{status}' for match {match_id} in daily listing")
                    total_odds_scraped += len(odds_list)
                    successful += 1
                    
                    # Track success status
                    if status.startswith("partial"):
                        status_tracking["partial"] += 1
                    else:
                        status_tracking["success"] += 1
                else:
                    # No odds but no error - likely no tables found
                    status = "failed_no_tables_found"
                    update_success = bronze_storage.update_match_status_in_daily_list(
                        scrape_date, match_id, status
                    )
                    if not update_success:
                        logger.error(f"Failed to update status '{status}' for match {match_id} in daily listing")
                    failed += 1
                    status_tracking["failed_no_tabs_found"] += 1

                # Log progress periodically with detailed status breakdown
                if i - last_logged >= log_interval or i == len(matches):
                    progress_pct = (i / len(matches)) * 100
                    
                    # Build status summary
                    active_statuses = {k: v for k, v in status_tracking.items() if v > 0}
                    status_summary = ", ".join([f"{k}: {v}" for k, v in sorted(active_statuses.items())])
                    
                    logger.info(
                        f"Progress: {i}/{len(matches)} ({progress_pct:.0f}%) | "
                        f"Success: {successful} | Failed: {failed} | Odds: {total_odds_scraped} | "
                        f"Statuses: [{status_summary}]"
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

                # Create descriptive status
                if is_timeout:
                    status = "failed_timeout_connection"
                else:
                    error_msg_lower = error_str[:50] if error_str else ""
                    if "navigation" in error_msg_lower:
                        status = "failed_navigation_error"
                    elif "table" in error_msg_lower or "element" in error_msg_lower:
                        status = "failed_table_not_found"
                    else:
                        status = "failed_exception"
                
                bronze_storage.update_match_status_in_daily_list(
                    scrape_date, match_id, status
                )
                failed += 1
                
                # Track specific error status
                for status_key in status_tracking.keys():
                    if status_key in status:
                        status_tracking[status_key] += 1
                        break
                else:
                    status_tracking["failed"] += 1

                # Log errors only periodically or if critical
                if i - last_logged >= log_interval or not is_timeout:
                    if is_timeout:
                        logger.debug(f"Match {i}/{len(matches)} timeout: {match_id}")
                    else:
                        logger.error(f"Match {i}/{len(matches)} error: {match_id} - {str(e)[:100]}")
                continue

        # Final summary with all status details
        logger.info("=" * 80)
        logger.info(f"Scraping Complete for {scrape_date}")
        logger.info(f"Total matches: {len(matches)}")
        logger.info(f"Successful: {successful} | Failed: {failed} | Total odds: {total_odds_scraped}")
        logger.info("-" * 80)
        logger.info("Status Breakdown:")
        
        # Group statuses for better readability
        success_statuses = {k: v for k, v in status_tracking.items() if k in ["success", "partial"] and v > 0}
        failed_statuses = {k: v for k, v in status_tracking.items() if k.startswith("failed") and v > 0}
        other_statuses = {k: v for k, v in status_tracking.items() if k not in ["success", "partial"] and not k.startswith("failed") and v > 0}
        
        if success_statuses:
            logger.info(f"  Success: {', '.join([f'{k}={v}' for k, v in sorted(success_statuses.items())])}")
        if failed_statuses:
            logger.info(f"  Failed: {', '.join([f'{k}={v}' for k, v in sorted(failed_statuses.items())])}")
        if other_statuses:
            logger.info(f"  Other: {', '.join([f'{k}={v}' for k, v in sorted(other_statuses.items())])}")
        logger.info("=" * 80)
        
        # Update storage statistics in daily listing
        try:
            bronze_storage.update_storage_statistics_in_daily_list(scrape_date)
        except Exception as stats_e:
            logger.warning(f"Failed to update storage statistics: {stats_e}")

        return {
            "total_matches": len(matches),
            "successful": successful,
            "failed": failed,
            "total_odds": total_odds_scraped,
            "status_breakdown": {k: v for k, v in status_tracking.items() if v > 0}
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


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Football Odds Scraper - Scrapes odds from match pages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape_odds.py --date 20251113              # Scrape odds for Nov 13, 2025
  python scrape_odds.py --date 20251113 --visible     # Use visible browser
  
Output:
  Updates bronze/{date}/matches/{match_id}.json with odds data
  Updates scrape_status in bronze/{date}/daily_listings.json
        """,
    )

    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Date to scrape (YYYYMMDD format, e.g., 20251113)",
    )

    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (default: headless)",
    )

    args = parser.parse_args()

    # Validate date format
    date = args.date.replace("-", "")
    if len(date) != 8 or not date.isdigit():
        print(f"Error: Invalid date format. Use YYYYMMDD (got: {args.date})")
        sys.exit(1)

    # Load configuration from .env file
    try:
        from src.scrapers.aiscore.config import Config
        config = Config()
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Override config with CLI args
    if args.visible:
        config.browser.headless = False
        logger.info("Visible mode enabled via --visible flag")

    # Setup logging with date suffix
    from src.scrapers.aiscore.bronze_storage import BronzeStorage

    bronze_storage = BronzeStorage(config.storage.bronze_path)

    # Setup logging
    def setup_logging(config: Config, date_suffix: str = None) -> logging.Logger:
        log_dir = Path(config.logging.file).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = Path(config.logging.file)
        base_name = log_file.stem
        extension = log_file.suffix

        if date_suffix:
            log_file = log_dir / f"{base_name}_{date_suffix}{extension}"
        else:
            from datetime import datetime

            date_suffix = datetime.now().strftime("%Y%m%d")
            log_file = log_dir / f"{base_name}_{date_suffix}{extension}"

        logging.basicConfig(
            level=getattr(logging, config.logging.level),
            format=config.logging.format,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.handlers.RotatingFileHandler(
                    str(log_file),
                    maxBytes=config.logging.max_bytes,
                    backupCount=config.logging.backup_count,
                    encoding="utf-8",
                ),
            ],
        )
        logger = logging.getLogger(__name__)
        logger.info(f"Logging initialized. Log file: {log_file}")
        return logger

    setup_logging(config, date_suffix=date)

    logger.info("=" * 80)
    browser_mode = "Visible" if not config.browser.headless else "Headless"
    logger.info(f"Football Odds Scraper | Date: {date} | Browser: {browser_mode}")
    logger.info("=" * 80)

    # Initialize browser and scraper
    browser = None
    exit_code = 0

    try:
        # Create browser
        browser = BrowserManager(config)
        logger.info(
            f"Creating browser in {'visible' if not config.browser.headless else 'headless'} mode..."
        )

        # Create scraper (db parameter is ignored, kept for backward compatibility)
        scraper = OddsScraper(config, db=None, browser=browser)

        # Scrape odds
        start_time = time.time()
        logger.info("Starting odds scraping...")
        results = scraper.scrape_from_daily_matches(date, bronze_storage)
        elapsed_time = time.time() - start_time

        logger.info("=" * 80)
        logger.info(f"Summary:")
        logger.info(f"  Total matches: {results['total_matches']}")
        logger.info(f"  Successful: {results['successful']}")
        logger.info(f"  Failed: {results['failed']}")
        logger.info(f"  Total odds scraped: {results['total_odds']}")
        logger.info(f"  Time elapsed: {elapsed_time:.1f}s")
        logger.info("=" * 80)

        # Compress files after scraping is complete
        if results["successful"] > 0:
            try:
                logger.info(f"Compressing files for {date}...")
                compression_stats = bronze_storage.compress_date_files(date)

                if compression_stats.get("compressed", 0) > 0:
                    logger.info(
                        f"Compression saved {compression_stats['saved_mb']} MB "
                        f"({compression_stats['saved_pct']}% reduction)"
                    )
            except Exception as e:
                logger.error(f"Error during compression: {e}")
                # Don't fail the whole scraping job if compression fails

        # Only exit with error code if there are actual failures
        # If total_odds == 0 but failed == 0, it means all matches were processed successfully
        # (either already scraped, no odds available, or skipped for valid reasons)
        if results["failed"] > 0:
            logger.error(f"ERROR: {results['failed']} matches failed to scrape!")
            exit_code = 1
        elif results["total_odds"] == 0:
            # This is not an error - matches may have been already scraped or have no odds
            if results["successful"] > 0:
                logger.info(
                    "INFO: No new odds scraped (matches may have been already processed or have no odds available)"
                )
            else:
                logger.warning(
                    "WARNING: No odds were scraped and no matches were processed. Check logs for details."
                )

    except KeyboardInterrupt:
        logger.info("\nScraping interrupted by user")
        exit_code = 130

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        exit_code = 1

    finally:
        if browser:
            try:
                browser.close()
                logger.info("Browser closed")
            except Exception as e:
                logger.error(f"Error during browser cleanup: {e}")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
