"""
Link Scraper - Scrapes match links and saves to bronze/{date}/daily_listings.json

SCRAPER: AIScore
PURPOSE: Scrapes match links and saves to bronze/{date}/daily_listings.json
         NO DATABASE - Direct JSON storage

Usage:
    python scrape_links.py 20251105              # Scrape single date
    python scrape_links.py 20251105 --visible    # Use visible browser
    python scrape_links.py --month 202511        # Scrape entire month (November 2025)
    python scrape_links.py --month 202511 --visible  # Monthly scraping with visible browser
"""

import argparse
import logging
import sys
import json
import time
import re
from pathlib import Path
from datetime import datetime
from calendar import monthrange
from logging.handlers import RotatingFileHandler

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Fix Windows encoding issues
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from src.scrapers.aiscore.config import Config
from src.scrapers.aiscore.browser import BrowserManager
from src.scrapers.aiscore.bronze_storage import BronzeStorage


def setup_logging(config: Config, date_suffix: str = None):
    """Setup logging configuration with optional date suffix in filename"""
    log_file = Path(config.logging.file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Always add date suffix to log filename (like FotMob)
    # Extract base name and extension
    base_name = log_file.stem  # e.g., "aiscore_scraper"
    extension = log_file.suffix  # e.g., ".log"
    
    if date_suffix:
        # Use provided date suffix
        log_file = log_file.parent / f"{base_name}_{date_suffix}{extension}"
    else:
        # Use today's date if no suffix provided (like FotMob)
        from datetime import datetime
        date_suffix = datetime.now().strftime('%Y%m%d')
        log_file = log_file.parent / f"{base_name}_{date_suffix}{extension}"

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, config.logging.level))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, config.logging.level))
    console_formatter = logging.Formatter(config.logging.format)
    console_handler.setFormatter(console_formatter)

    file_handler = RotatingFileHandler(
        str(log_file),
        maxBytes=config.logging.max_bytes,
        backupCount=config.logging.backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(config.logging.format)
    file_handler.setFormatter(file_formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Log the log file path (like FotMob)
    logging.info(f"Logging initialized. Log file: {log_file}")


def scrape_match_links(date: str, browser: BrowserManager, config: Config) -> list:
    """Scrape match links for a specific date directly from website.

    Args:
        date: Date in YYYYMMDD format
        browser: Browser manager instance
        config: Configuration

    Returns:
        List of match URLs
    """
    # Retry logic for timeout errors
    max_retries = getattr(config.retry, "max_attempts", 3)

    for attempt in range(max_retries):
        try:
            return _scrape_match_links_impl(date, browser, config)
        except Exception as e:
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

            if is_timeout and attempt < max_retries - 1:
                logging.warning(
                    f"Timeout (attempt {attempt + 1}/{max_retries}), retrying..."
                )
                try:
                    browser.restart()
                    time.sleep(2)
                except Exception as restart_err:
                    logging.error(f"Browser restart failed: {restart_err}")
                    raise
            else:
                # Not a timeout or last attempt failed
                raise


def _solve_captcha_with_service(browser: BrowserManager, api_key: str = None) -> bool:
    """
    Solve CAPTCHA using 2captcha service (optional).
    
    Args:
        browser: Browser manager instance
        api_key: 2captcha API key (from environment variable CAPTCHA_API_KEY)
    
    Returns:
        True if CAPTCHA was solved, False otherwise
    """
    import os
    if not api_key:
        api_key = os.getenv('CAPTCHA_API_KEY')
        if not api_key:
            logging.warning("CAPTCHA_API_KEY not set. Skipping CAPTCHA solving service.")
            return False
    
    try:
        import requests
        
        # Find CAPTCHA iframe
        try:
            # Look for reCAPTCHA iframe
            recaptcha_iframe = browser.driver.find_elements(By.CSS_SELECTOR, 
                "iframe[src*='recaptcha'], iframe[src*='google.com/recaptcha']")
            
            if not recaptcha_iframe:
                # Look for hCaptcha iframe
                recaptcha_iframe = browser.driver.find_elements(By.CSS_SELECTOR,
                    "iframe[src*='hcaptcha']")
            
            if not recaptcha_iframe:
                logging.debug("No CAPTCHA iframe found")
                return False
            
            # Get site key from iframe src
            iframe_src = recaptcha_iframe[0].get_attribute('src')
            site_key = None
            
            if 'recaptcha' in iframe_src:
                import re
                match = re.search(r'k=([^&]+)', iframe_src)
                if match:
                    site_key = match.group(1)
            elif 'hcaptcha' in iframe_src:
                import re
                match = re.search(r'sitekey=([^&]+)', iframe_src)
                if match:
                    site_key = match.group(1)
            
            if not site_key:
                logging.warning("Could not extract CAPTCHA site key")
                return False
            
            logging.info(f"Found CAPTCHA with site key: {site_key[:20]}...")
            logging.info("Submitting CAPTCHA to 2captcha service...")
            
            # Submit CAPTCHA to 2captcha
            page_url = browser.driver.current_url
            submit_url = "http://2captcha.com/in.php"
            submit_data = {
                'key': api_key,
                'method': 'userrecaptcha' if 'recaptcha' in iframe_src else 'hcaptcha',
                'googlekey': site_key,
                'pageurl': page_url,
                'json': 1
            }
            
            response = requests.post(submit_url, data=submit_data, timeout=30)
            result = response.json()
            
            if result.get('status') != 1:
                logging.error(f"2captcha submission failed: {result.get('request')}")
                return False
            
            captcha_id = result.get('request')
            logging.info(f"CAPTCHA submitted. Task ID: {captcha_id}. Waiting for solution...")
            
            # Poll for solution
            get_url = "http://2captcha.com/res.php"
            max_wait = 120  # 2 minutes max
            wait_start = time.time()
            
            while time.time() - wait_start < max_wait:
                time.sleep(5)  # Check every 5 seconds
                
                get_data = {
                    'key': api_key,
                    'action': 'get',
                    'id': captcha_id,
                    'json': 1
                }
                
                response = requests.get(get_url, params=get_data, timeout=30)
                result = response.json()
                
                if result.get('status') == 1:
                    # Solution ready
                    solution = result.get('request')
                    logging.info("CAPTCHA solved! Injecting solution...")
                    
                    # Inject solution into page
                    browser.driver.execute_script(f"""
                        document.getElementById('g-recaptcha-response').innerHTML = '{solution}';
                        if (typeof ___grecaptcha_cfg !== 'undefined') {{
                            ___grecaptcha_cfg.clients[0].callback('{solution}');
                        }}
                    """)
                    
                    time.sleep(2)
                    return True
                elif result.get('request') == 'CAPCHA_NOT_READY':
                    elapsed = int(time.time() - wait_start)
                    if elapsed % 15 == 0:
                        logging.info(f"Waiting for CAPTCHA solution... ({elapsed}s)")
                    continue
                else:
                    logging.error(f"2captcha error: {result.get('request')}")
                    return False
            
            logging.warning("CAPTCHA solving timed out")
            return False
            
        except Exception as e:
            logging.error(f"Error solving CAPTCHA: {e}")
            return False
            
    except ImportError:
        logging.warning("requests library not available for CAPTCHA solving")
        return False


def _handle_cloudflare(browser: BrowserManager, config: Config, max_wait: int = 30) -> bool:
    """
    Detect and wait for Cloudflare challenge to complete.
    Can also solve CAPTCHAs if 2captcha service is configured.
    
    Args:
        browser: Browser manager instance
        config: Configuration instance
        max_wait: Maximum seconds to wait for Cloudflare (default: 30)
    
    Returns:
        True if Cloudflare was handled successfully, False otherwise
    """
    try:
        # Check for Cloudflare indicators
        page_source = browser.driver.page_source.lower()
        page_title = browser.driver.title.lower()
        page_url = browser.driver.current_url.lower()
        
        cloudflare_indicators = [
            "checking your browser",
            "just a moment",
            "please wait",
            "ddos protection",
            "cloudflare",
            "cf-browser-verification",
            "cf-challenge",
            "challenge-platform"
        ]
        
        captcha_indicators = [
            "captcha",
            "recaptcha",
            "hcaptcha",
            "verify you are human",
            "i'm not a robot"
        ]
        
        is_cloudflare = any(indicator in page_source or indicator in page_title for indicator in cloudflare_indicators)
        has_captcha = any(indicator in page_source or indicator in page_title for indicator in captcha_indicators)
        
        if not is_cloudflare:
            # Also check for Cloudflare-specific elements
            try:
                cf_elements = browser.driver.find_elements(By.CSS_SELECTOR, 
                    "[id*='cf'], [class*='cf'], [id*='challenge'], [class*='challenge']")
                if cf_elements:
                    is_cloudflare = True
            except:
                pass
        
        # Check for CAPTCHA elements
        if not has_captcha:
            try:
                captcha_elements = browser.driver.find_elements(By.CSS_SELECTOR,
                    "iframe[src*='recaptcha'], iframe[src*='hcaptcha'], .g-recaptcha, #captcha")
                if captcha_elements:
                    has_captcha = True
            except:
                pass
        
        if not is_cloudflare and not has_captcha:
            logging.debug("No Cloudflare challenge or CAPTCHA detected")
            return True
        
        if has_captcha:
            logging.info("CAPTCHA detected.")
            
            # Only try automatic solving if API key is explicitly provided
            import os
            api_key = os.getenv('CAPTCHA_API_KEY')
            captcha_solved = False
            
            if api_key:
                logging.info("CAPTCHA_API_KEY found. Attempting automatic solving...")
                captcha_solved = _solve_captcha_with_service(browser, api_key)
                if captcha_solved:
                    logging.info("CAPTCHA solved successfully!")
                    time.sleep(3)  # Wait for page to process solution
                else:
                    logging.warning("Automatic CAPTCHA solving failed.")
            else:
                logging.info("No CAPTCHA_API_KEY set. Skipping automatic solving (free mode).")
            
            # If not solved automatically, wait for manual solving (if visible) or just wait
            if not captcha_solved:
                if not config.browser.headless:
                    logging.info("=" * 60)
                    logging.info("MANUAL CAPTCHA SOLVING REQUIRED")
                    logging.info("=" * 60)
                    logging.info("Browser is visible. Please solve the CAPTCHA in the browser window.")
                    logging.info("The scraper will automatically detect when CAPTCHA is solved.")
                    logging.info("=" * 60)
                    
                    # Wait for CAPTCHA to be solved manually (check every 2 seconds)
                    manual_wait_start = time.time()
                    manual_max_wait = 300  # 5 minutes for manual solving
                    check_interval = 2.0
                    
                    while time.time() - manual_wait_start < manual_max_wait:
                        time.sleep(check_interval)
                        
                        # Check if CAPTCHA is still present
                        try:
                            current_source = browser.driver.page_source.lower()
                            current_title = browser.driver.title.lower()
                            still_captcha = any(
                                indicator in current_source or indicator in current_title
                                for indicator in captcha_indicators
                            )
                            
                            # Check if page content appeared (CAPTCHA solved)
                            try:
                                sport_box = browser.driver.find_elements(By.CLASS_NAME, "sportBox")
                                if sport_box and not still_captcha:
                                    elapsed = int(time.time() - manual_wait_start)
                                    logging.info(f"CAPTCHA solved manually! (took {elapsed} seconds)")
                                    time.sleep(2)
                                    captcha_solved = True
                                    break
                            except:
                                pass
                            
                            # Log progress every 10 seconds
                            elapsed = int(time.time() - manual_wait_start)
                            if elapsed % 10 == 0:
                                logging.info(f"Waiting for manual CAPTCHA solving... ({elapsed}s)")
                        except:
                            pass
                    
                    if not captcha_solved:
                        logging.warning("Manual CAPTCHA solving timed out. Continuing anyway...")
                else:
                    logging.warning("CAPTCHA detected in headless mode. Cannot solve manually.")
                    logging.warning("Options:")
                    logging.warning("  1. Run with --visible flag to solve manually")
                    logging.warning("  2. Set CAPTCHA_API_KEY for automatic solving (paid service)")
                    logging.warning("Continuing and hoping Cloudflare will pass automatically...")
                    # Don't return False - let it try to continue
        
        if is_cloudflare:
            logging.info("Cloudflare challenge detected. Waiting for it to complete...")
        
        # Wait for Cloudflare to complete
        wait_start = time.time()
        check_interval = 1.0  # Check every second
        last_log_time = 0
        
        while time.time() - wait_start < max_wait:
            try:
                current_source = browser.driver.page_source.lower()
                current_title = browser.driver.title.lower()
                
                # Check if Cloudflare challenge is still present
                still_cloudflare = any(
                    indicator in current_source or indicator in current_title 
                    for indicator in cloudflare_indicators
                )
                
                # Check if CAPTCHA is still present
                still_captcha = any(
                    indicator in current_source or indicator in current_title
                    for indicator in captcha_indicators
                )
                
                # Check if we can see the actual page content (sportBox indicates AIScore page loaded)
                try:
                    sport_box = browser.driver.find_elements(By.CLASS_NAME, "sportBox")
                    if sport_box and not still_cloudflare and not still_captcha:
                        elapsed = int(time.time() - wait_start)
                        logging.info(f"Cloudflare challenge completed after {elapsed} seconds")
                        time.sleep(2)  # Brief pause to ensure page is fully loaded
                        return True
                except:
                    pass
                
                # Log progress every 5 seconds
                elapsed = int(time.time() - wait_start)
                if elapsed != last_log_time and elapsed % 5 == 0:
                    status = []
                    if still_cloudflare:
                        status.append("Cloudflare")
                    if still_captcha:
                        status.append("CAPTCHA")
                    status_str = " + ".join(status) if status else "challenge"
                    logging.info(f"Still waiting for {status_str}... ({elapsed}s / {max_wait}s)")
                    last_log_time = elapsed
                
                time.sleep(check_interval)
                
            except Exception as e:
                logging.warning(f"Error checking Cloudflare status: {e}")
                time.sleep(check_interval)
        
        # Final check
        try:
            sport_box = browser.driver.find_elements(By.CLASS_NAME, "sportBox")
            if sport_box:
                logging.info(f"Cloudflare challenge completed (timeout reached, but page loaded)")
                return True
        except:
            pass
        
        logging.warning(f"Cloudflare challenge did not complete within {max_wait} seconds")
        return False
        
    except Exception as e:
        logging.error(f"Error handling Cloudflare: {e}")
        return False


def _scrape_match_links_impl(
    date: str, browser: BrowserManager, config: Config
) -> list:
    """Implementation of match link scraping (wrapped with retry logic).

    Args:
        date: Date in YYYYMMDD format
        browser: Browser manager instance
        config: Configuration

    Returns:
        List of match URLs
    """
    # URL format: https://www.aiscore.com/YYYYMMDD
    base_url = config.scraping.base_url
    url = f"{base_url}/{date}"

    logging.info(f"Loading page: {url}")

    # Check browser health before starting
    if not browser.is_healthy():
        logging.warning("Browser unresponsive, restarting...")
        browser.restart()

    # Navigate directly to date page (skip homepage for speed)
    browser.driver.get(url)
    
    # Check for and handle Cloudflare protection
    cloudflare_handled = _handle_cloudflare(browser, config, max_wait=30)
    if not cloudflare_handled:
        logging.warning("Cloudflare challenge may not have completed. Continuing anyway...")

    # Wait for page to load
    try:
        WebDriverWait(browser.driver, config.scraping.timeouts.page_load).until(
            EC.presence_of_element_located((By.CLASS_NAME, "sportBox"))
        )
    except TimeoutException:
        logging.warning("Timeout waiting for page to load")

    # Wait for dynamic content
    time.sleep(config.scraping.navigation.date_page_load)

    # Click on "All" tab to show all matches
    try:
        all_tab = WebDriverWait(
            browser.driver, config.scraping.timeouts.element_wait
        ).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//span[@class[contains(., 'changeItem')] and contains(text(), 'All')]",
                )
            )
        )
        all_tab.click()
        time.sleep(config.scraping.navigation.tab_click)
    except Exception:
        pass  # Tab may already be selected, continue silently

    # Step 4: Scroll down page to load all matches (lazy loading)
    # Load existing links from file if it exists
    bronze_storage = BronzeStorage(config.storage.bronze_path)
    
    # Log paths for debugging
    logging.debug(f"Bronze storage base: {bronze_storage.base_path.absolute()}")
    logging.debug(f"Daily listings dir: {bronze_storage.daily_listings_dir.absolute()}")

    # Use bronze_storage daily_listings directory structure
    date_folder = bronze_storage.daily_listings_dir / date
    date_folder.mkdir(parents=True, exist_ok=True)

    daily_file = date_folder / "matches.json"
    logging.debug(f"Daily file path: {daily_file.absolute()}")
    
    # Check if links scraping is already complete
    if daily_file.exists():
        try:
            with open(daily_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if existing_data.get("links_scraping_complete", False):
                    completed_at = existing_data.get("links_scraping_completed_at", "unknown")
                    total_matches = existing_data.get("total_matches", 0)
                    logging.info(f"Links scraping already completed for {date} (completed at: {completed_at}, {total_matches} matches found)")
                    logging.info(f"Skipping links scraping. To re-scrape, delete: {daily_file.absolute()}")
                    return []  # Return empty list since scraping is complete
        except Exception as e:
            logging.warning(f"Error checking daily listing file: {e}, continuing with scraping")

    links = set()
    saved_links = set()

    # Load existing links to avoid duplicates
    if daily_file.exists():
        try:
            with open(daily_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                for match in existing_data.get("matches", []):
                    url = match.get("match_url", "")
                    if url:
                        saved_links.add(url)
                        links.add(url)
            logging.info(f"Resuming: {len(saved_links)} existing matches found")
        except Exception:
            pass  # File doesn't exist yet, start fresh

    no_new_content_count = 0
    last_page_height = 0
    scroll_count = 0

    # OPTIMIZATION: Batch buffer for file writes (instead of writing every scroll)
    new_links_buffer = []
    WRITE_BATCH_SIZE = 50  # Write to file every 50 links
    last_write_scroll = 0

    logging.info("Starting scroll to load all matches...")

    # Health check interval (check every N scrolls)
    health_check_interval = 20
    last_logged_scroll = 0

    # OPTIMIZATION: Cache processed match IDs to avoid re-extraction
    processed_match_ids = set()

    # Load existing match IDs from file to avoid re-processing
    if daily_file.exists():
        try:
            with open(daily_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                for match in existing_data.get("matches", []):
                    match_id = match.get("match_id", "")
                    if match_id:
                        processed_match_ids.add(match_id)
        except Exception:
            pass

    # OPTIMIZATION: Cache allowed countries normalization (if filtering by countries)
    allowed_countries_set = None
    if config and config.scraping.filter_by_countries:
        allowed_countries_list = config.scraping.allowed_countries or []
        allowed_countries_set = set(c.strip().lower() for c in allowed_countries_list)

    while True:
        # Periodic browser health check
        if scroll_count > 0 and scroll_count % health_check_interval == 0:
            if not browser.is_healthy():
                raise RuntimeError(
                    f"Browser became unresponsive at scroll {scroll_count}"
                )

        # OPTIMIZATION: Combine multiple JavaScript calls into one
        try:
            page_info_before = browser.driver.execute_script(
                """
                return {
                    scrollHeight: document.body.scrollHeight,
                    linkCount: document.querySelectorAll('a.match-container').length
                };
            """
            )
            page_height_before = page_info_before["scrollHeight"]
            previous_link_count = page_info_before["linkCount"]
        except Exception as e:
            logging.error(f"Error executing JavaScript at scroll {scroll_count}: {e}")
            if scroll_count > 5:
                logging.warning(f"Browser may be unresponsive. Attempting to continue...")
                # Try to get basic info without JavaScript
                try:
                    all_matches = browser.driver.find_elements(By.CSS_SELECTOR, "a.match-container")
                    previous_link_count = len(all_matches)
                    page_height_before = browser.driver.execute_script("return document.body.scrollHeight;")
                except:
                    logging.error("Browser completely unresponsive. Stopping.")
                    break
            else:
                raise

        # Scroll down 500 pixels
        browser.driver.execute_script("window.scrollBy(0, 500);")
        scroll_count += 1

        # Log progress every scroll for first 10 scrolls, then every 5 scrolls
        if scroll_count <= 10 or scroll_count % 5 == 0:
            logging.info(f"Scroll {scroll_count} in progress...")

        # Smart wait - only wait until new content appears (fast exit when content loads)
        wait_start = time.time()
        content_loaded = False
        max_wait = config.scraping.scroll.smart_wait_timeout
        check_interval = config.scraping.scroll.smart_wait_interval

        # OPTIMIZATION: Reuse DOM query result
        all_matches = browser.driver.find_elements(By.CSS_SELECTOR, "a.match-container")
        current_link_count = len(all_matches)
        if current_link_count > previous_link_count:
            content_loaded = True
        else:
            # Only wait if content hasn't loaded yet
            waited_seconds = 0
            while time.time() - wait_start < max_wait:
                time.sleep(check_interval)
                waited_seconds += check_interval
                # Log wait progress every 2 seconds
                if waited_seconds >= 2.0 and int(waited_seconds) % 2 == 0:
                    logging.debug(f"Scroll {scroll_count}: Waiting for content... ({int(waited_seconds)}s)")
                all_matches = browser.driver.find_elements(
                    By.CSS_SELECTOR, "a.match-container"
                )
                current_link_count = len(all_matches)
                if current_link_count > previous_link_count:
                    content_loaded = True
                    if waited_seconds > 1.0:
                        logging.debug(f"Scroll {scroll_count}: Content loaded after {int(waited_seconds)}s wait")
                    break

        # Minimal pause only if content loaded (for stability)
        if content_loaded:
            time.sleep(min(config.scraping.scroll.pause, 0.1))  # Cap at 0.1s max

        # OPTIMIZATION: Combine page info queries
        try:
            page_info_after = browser.driver.execute_script(
                """
                return {
                    scrollHeight: document.body.scrollHeight,
                    scrollPosition: window.pageYOffset + window.innerHeight
                };
            """
            )
            page_height_after = page_info_after["scrollHeight"]
            current_scroll_position = page_info_after["scrollPosition"]
        except Exception as e:
            logging.warning(f"Error getting page info at scroll {scroll_count}: {e}")
            # Fallback: use basic measurements
            try:
                page_height_after = browser.driver.execute_script("return document.body.scrollHeight;")
                current_scroll_position = browser.driver.execute_script("return window.pageYOffset + window.innerHeight;")
            except:
                logging.error("Cannot get page measurements. Browser may be unresponsive.")
                page_height_after = page_height_before
                current_scroll_position = page_height_before

        # LEAGUE-AWARE: Extract only filtered league matches
        new_links_this_scroll = []
        leagues_with_matches = {}  # Store league info for each match

        try:
            # Log extraction start for first few scrolls
            if scroll_count <= 5:
                logging.debug(f"Scroll {scroll_count}: Extracting match links...")
            
            # Get all leagues and their matches (filtered by config)
            # OPTIMIZATION: Pass cached allowed_countries_set for faster filtering
            leagues_data = extract_league_and_matches(
                browser, config, allowed_countries_set
            )

            # Log summary only every 10 scrolls or when matches found
            if leagues_data:
                total_matches_in_leagues = sum(
                    len(ld["matches"]) for ld in leagues_data
                )
                if scroll_count % 10 == 0 or total_matches_in_leagues > 0:
                    countries_in_leagues = set(ld["country"] for ld in leagues_data)
                    logging.info(
                        f"Scroll {scroll_count}: {len(leagues_data)} leagues, {total_matches_in_leagues} matches"
                    )

            previous_count = len(links)
            processed_in_scroll = 0
            new_matches_count = 0
            duplicate_matches_count = 0
            countries_found = set()

            # Process each filtered league
            for league_data in leagues_data:
                league_name = league_data["league_name"]
                country = league_data.get("country", "Unknown")
                matches = league_data["matches"]
                countries_found.add(country)

                for match in matches:
                    clean_url = match.get("url")

                    # Skip if URL is missing or invalid
                    if not clean_url:
                        logging.debug(f"Skipping match with no URL: {match}")
                        processed_in_scroll += 1
                        continue

                    # OPTIMIZATION: Skip if match_id already processed
                    match_id = (
                        match.get("match_id") or clean_url.split("/")[-1].split("?")[0]
                    )
                    if match_id in processed_match_ids:
                        duplicate_matches_count += 1
                        processed_in_scroll += 1
                        continue

                    # Check if it's a new link
                    if clean_url not in links:
                        processed_match_ids.add(match_id)
                        links.add(clean_url)
                        new_links_this_scroll.append(clean_url)
                        new_matches_count += 1

                        # Store league info and team names for this match
                        # Ensure league_name and country are never empty
                        safe_league_name = (
                            league_name
                            if league_name and league_name.strip()
                            else "Unknown"
                        )
                        # Country should be used as-is (don't filter out "Unknown" - it's a valid value if extraction failed)
                        safe_country = (
                            country if country and country.strip() else "Unknown"
                        )
                        league_info = {
                            "league_name": safe_league_name,
                            "country": safe_country,
                            "is_important": True,
                        }

                        # Debug: Log England matches being added
                        if "england" in safe_country.lower():
                            logging.debug(
                                f"Adding England match: {clean_url[:50]}... (country: {safe_country}, league: {safe_league_name})"
                            )

                        # Include team names if extracted from page
                        if "teams" in match:
                            league_info["teams"] = match["teams"]

                        leagues_with_matches[clean_url] = league_info
                    else:
                        duplicate_matches_count += 1

                    processed_in_scroll += 1

            # Debug logging for troubleshooting
            if processed_in_scroll > 0 and new_matches_count == 0:
                logging.debug(
                    f"Scroll {scroll_count}: Processed {processed_in_scroll} matches, {duplicate_matches_count} duplicates, 0 new"
                )

            new_count = len(links)
            if new_count > previous_count:
                new_links = new_count - previous_count
                no_new_content_count = 0

                # Log progress every 10 scrolls or when significant new links found
                if scroll_count - last_logged_scroll >= 10 or new_links >= 20:
                    logging.info(
                        f"Progress: {new_count} matches found | Scroll {scroll_count}"
                    )
                    last_logged_scroll = scroll_count
            else:
                no_new_content_count += 1

            # OPTIMIZATION: Add to buffer with league info (regardless of new count)
            if new_links_this_scroll:
                # Add each link with its league info to buffer
                for url in new_links_this_scroll:
                    league_info = leagues_with_matches.get(url)
                    new_links_buffer.append((url, league_info))

            # Write after every 10 scrolls (guaranteed)
            if scroll_count - last_write_scroll >= 10 and new_links_buffer:
                # Extract URLs and league info for batch save
                urls_to_save = [item[0] for item in new_links_buffer]
                league_info_map = {item[0]: item[1] for item in new_links_buffer}

                save_new_links_incremental_with_leagues(
                    urls_to_save, date, bronze_storage, daily_file, league_info_map
                )
                logging.info(
                    f"Saved {len(new_links_buffer)} matches to file (after {scroll_count} scrolls)"
                )
                new_links_buffer.clear()
                last_write_scroll = scroll_count

        except Exception as e:
            logging.error(
                f"Error collecting links at scroll {scroll_count}: {e}", exc_info=True
            )

        # OPTIMIZATION: Smarter stop conditions (less waiting at end)
        reached_bottom = (
            current_scroll_position >= page_height_after - 50
        )  # 50px tolerance
        page_not_growing = page_height_after == last_page_height

        # Progressive patience: more empty scrolls allowed early, fewer at end
        if scroll_count < 30:
            patience = 8  # Early stage: be more patient
        else:
            patience = 5  # Later stage: can be more confident we got everything

        too_many_empty_scrolls = no_new_content_count >= patience

        # Log stop conditions every 5 scrolls for debugging
        if scroll_count % 5 == 0:
            logging.debug(
                f"Scroll {scroll_count}: reached_bottom={reached_bottom}, page_not_growing={page_not_growing}, "
                f"no_new_content_count={no_new_content_count}/{patience}, links={len(links)}"
            )

        # Stop if at bottom AND (page stopped growing OR too many empty scrolls)
        # BUT: Don't stop too early if we have very few matches (might be still loading)
        if (
            reached_bottom
            and (page_not_growing or too_many_empty_scrolls)
            and scroll_count >= 20
        ):
            # If we have very few matches, be more patient
            if len(links) < 10 and scroll_count < 50:
                logging.warning(
                    f"Only {len(links)} matches found but stopping conditions met. "
                    f"Continuing for a bit longer (scroll {scroll_count}/50)..."
                )
                # Don't break yet - continue scrolling
            else:
                logging.info(
                    f"Scraping complete: {len(links)} matches found after {scroll_count} scrolls"
                )
                logging.info(f"Stop conditions: reached_bottom={reached_bottom}, page_not_growing={page_not_growing}, too_many_empty_scrolls={too_many_empty_scrolls}")

                # Final summary of what was found
                if config and config.scraping.filter_by_countries:
                    logging.info(
                        f"Filtered by countries: {config.scraping.allowed_countries}"
                    )
                
                # Warn if very few matches found
                if len(links) < 10:
                    logging.warning(f"⚠️ WARNING: Only {len(links)} matches found! This seems low.")
                    logging.warning("Possible issues:")
                    logging.warning("  1. Page is not loading all matches")
                    logging.warning("  2. Country filtering is too restrictive")
                    logging.warning("  3. Match extraction is failing")
                    logging.warning("  4. Scrolling stopped too early")
                
                break

        # Update last height
        last_page_height = page_height_after

        # Safety limit: max 300 scrolls
        if scroll_count >= 300:
            logging.warning(
                f"Reached max scroll limit (300), stopping. Found {len(links)} matches"
            )
            break

    # OPTIMIZATION: Write any remaining buffered links before returning
    if new_links_buffer:
        urls_to_save = [item[0] for item in new_links_buffer]
        league_info_map = {item[0]: item[1] for item in new_links_buffer}
        try:
            saved_count = save_new_links_incremental_with_leagues(
                urls_to_save, date, bronze_storage, daily_file, league_info_map
            )
            logging.info(f"Saved final batch: {saved_count} matches to file")
        except Exception as e:
            logging.error(f"Error saving final batch: {e}", exc_info=True)
        new_links_buffer.clear()

    final_count = len(links)
    logging.info(f"Scraping complete. Total unique links found: {final_count}")
    
    # Verify file was created/updated
    if daily_file.exists():
        try:
            with open(daily_file, "r", encoding="utf-8") as f:
                file_data = json.load(f)
                file_count = len(file_data.get("matches", []))
                logging.info(f"File verification: {file_count} matches in {daily_file}")
        except Exception as e:
            logging.warning(f"Could not verify file: {e}")
    else:
        logging.warning(f"WARNING: File {daily_file} was not created!")

    return sorted(list(links))


def save_new_links_incremental_with_leagues(
    new_links: list, game_date: str, bronze_storage, daily_file, league_info_map: dict
):
    """Append new links with league info to JSON file (avoid duplicates).

    Args:
        new_links: List of new match URLs to save
        game_date: Date of games (YYYYMMDD)
        bronze_storage: BronzeStorage instance
        daily_file: Path to the daily listings JSON file
        league_info_map: Dictionary mapping URL to league info dict {'league_name': str, 'is_important': bool}
    """
    import json
    from datetime import datetime
    from pathlib import Path

    scrape_timestamp = datetime.now().isoformat()

    # Ensure date folder exists
    date_folder = daily_file.parent
    date_folder.mkdir(parents=True, exist_ok=True)

    # Load existing data or create new
    if daily_file.exists():
        with open(daily_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"scrape_date": game_date, "total_matches": 0, "matches": []}

    # Get existing URLs to avoid duplicates
    existing_urls = {match["match_url"] for match in data["matches"]}

    # Add only new links
    added_count = 0
    forbidden_count = 0
    for url in new_links:
        if url not in existing_urls:
            # Get league info for this match
            league_info = league_info_map.get(url, {})

            # OPTIMIZATION: Only extract match_id if teams already exist, otherwise full extraction
            if "teams" in league_info:
                teams = league_info["teams"]
                # Extract only match_id from URL (faster than full extraction)
                match_id = url.split("/")[-1].split("?")[
                    0
                ]  # Get last part, remove query params
                match_info = {"match_id": match_id, "teams": teams}
            else:
                match_info = extract_match_info(url)  # Fallback to URL parsing

            # Check if match should be forbidden (e.g., women's matches)
            is_forbidden, forbidden_reason = is_forbidden_match(match_info["teams"])

            if is_forbidden:
                scrape_status = "forbidden"
                forbidden_count += 1
            else:
                scrape_status = "n/a"

            match_record = {
                "match_id": match_info["match_id"],
                "match_url": url,
                "teams": match_info["teams"],
                "game_date": game_date,
                "scrape_timestamp": scrape_timestamp,
                "scrape_status": scrape_status,
            }

            # Add country and league information from map (ALWAYS add these fields)
            if league_info:
                country_value = league_info.get("country", "Unknown")
                league_name = league_info.get("league_name", "Unknown")

                # Clean country - ensure it's not None or empty
                if (
                    not country_value
                    or country_value == "None"
                    or str(country_value).strip() == ""
                ):
                    country_value = "Unknown"
                else:
                    country_value = str(country_value).strip()

                # Clean league name - remove country prefix if present (e.g., "England:" -> "")
                if league_name and ":" in str(league_name):
                    # If league name is "Country:", it's actually just country, no league
                    parts = str(league_name).split(":", 1)
                    if len(parts) == 1 or (len(parts) > 1 and not parts[1].strip()):
                        # League name is just "Country:", so no league name
                        league_name = "Unknown"
                        # But make sure country is set
                        if country_value == "Unknown" and parts[0].strip():
                            country_value = parts[0].strip()
                    else:
                        # League name has content after colon
                        league_name = parts[1].strip()
                        # Country might be in first part
                        if country_value == "Unknown" and parts[0].strip():
                            country_value = parts[0].strip()

                # Final validation
                if (
                    not league_name
                    or league_name.strip() == ""
                    or league_name == country_value
                ):
                    league_name = "Unknown"

                match_record["country"] = country_value
                match_record["league"] = league_name
            else:
                # Default values if league_info is missing
                match_record["country"] = "Unknown"
                match_record["league"] = "Unknown"

            # Add forbidden_reason field if match is forbidden
            if is_forbidden:
                match_record["forbidden_reason"] = forbidden_reason

            data["matches"].append(match_record)
            existing_urls.add(url)
            added_count += 1

    # Log forbidden matches
    if forbidden_count > 0:
        if forbidden_count > 0:
            logging.debug(f"Filtered {forbidden_count} forbidden matches")

    # Update total count
    data["total_matches"] = len(data["matches"])

    # Save to file (always save, even if no new matches, to update metadata)
    try:
        # Ensure directory exists
        date_folder.mkdir(parents=True, exist_ok=True)
        
        # Atomic write
        temp_file = date_folder / ".matches.json.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Verify JSON is valid
        with open(temp_file, "r", encoding="utf-8") as f:
            json.load(f)  # Will raise if invalid
        
        # Atomic rename
        temp_file.replace(daily_file)
        
        # Verify file was written
        if not daily_file.exists():
            raise IOError(f"File was not created: {daily_file.absolute()}")
        
        if added_count > 0:
            logging.info(f"Incremental save: Added {added_count} new matches. Total: {len(data['matches'])} to {daily_file.absolute()}")
        else:
            logging.debug(f"Incremental save: No new matches. Total: {len(data['matches'])} in {daily_file.absolute()}")
    except Exception as e:
        logging.error(f"Error saving incremental data to {daily_file.absolute()}: {e}", exc_info=True)
        if temp_file.exists():
            try:
                temp_file.unlink()
            except:
                pass
        raise

    return added_count


def save_new_links_incremental(
    new_links: list,
    game_date: str,
    bronze_storage,
    daily_file,
    league_info: dict = None,
):
    """Append new links to JSON file (avoid duplicates).

    DEPRECATED: Use save_new_links_incremental_with_leagues instead

    Args:
        new_links: List of new match URLs to save
        game_date: Date of games (YYYYMMDD)
        bronze_storage: BronzeStorage instance
        daily_file: Path to the daily listings JSON file
        league_info: Optional dict with 'league_name' and 'is_important'
    """
    import json
    from datetime import datetime
    from pathlib import Path

    scrape_timestamp = datetime.now().isoformat()

    # Ensure date folder exists
    date_folder = daily_file.parent
    date_folder.mkdir(parents=True, exist_ok=True)

    # Load existing data or create new
    if daily_file.exists():
        with open(daily_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"scrape_date": game_date, "total_matches": 0, "matches": []}

    # Get existing URLs to avoid duplicates
    existing_urls = {match["match_url"] for match in data["matches"]}

    # Add only new links
    added_count = 0
    forbidden_count = 0
    for url in new_links:
        if url not in existing_urls:
            match_info = extract_match_info(url)

            # Check if match should be forbidden (e.g., women's matches)
            is_forbidden, forbidden_reason = is_forbidden_match(match_info["teams"])

            if is_forbidden:
                scrape_status = "forbidden"
                forbidden_count += 1
            else:
                scrape_status = "n/a"

            match_record = {
                "match_id": match_info["match_id"],
                "match_url": url,
                "teams": match_info["teams"],
                "game_date": game_date,
                "scrape_timestamp": scrape_timestamp,
                "scrape_status": scrape_status,
            }

            # Add country and league information if provided (ALWAYS add these fields)
            if league_info:
                country_value = league_info.get("country", "Unknown")
                league_name = league_info.get("league_name", "Unknown")

                # Clean country - ensure it's not None or empty
                if (
                    not country_value
                    or country_value == "None"
                    or str(country_value).strip() == ""
                ):
                    country_value = "Unknown"
                else:
                    country_value = str(country_value).strip()

                # Clean league name - remove country prefix if present (e.g., "England:" -> "")
                if league_name and ":" in str(league_name):
                    # If league name is "Country:", it's actually just country, no league
                    parts = str(league_name).split(":", 1)
                    if len(parts) == 1 or (len(parts) > 1 and not parts[1].strip()):
                        # League name is just "Country:", so no league name
                        league_name = "Unknown"
                        # But make sure country is set
                        if country_value == "Unknown" and parts[0].strip():
                            country_value = parts[0].strip()
                    else:
                        # League name has content after colon
                        league_name = parts[1].strip()
                        # Country might be in first part
                        if country_value == "Unknown" and parts[0].strip():
                            country_value = parts[0].strip()

                # Final validation
                if (
                    not league_name
                    or league_name.strip() == ""
                    or league_name == country_value
                ):
                    league_name = "Unknown"

                match_record["country"] = country_value
                match_record["league"] = league_name
            else:
                # Default values if league_info is missing
                match_record["country"] = "Unknown"
                match_record["league"] = "Unknown"

            # Add forbidden_reason field if match is forbidden
            if is_forbidden:
                match_record["forbidden_reason"] = forbidden_reason

            data["matches"].append(match_record)
            existing_urls.add(url)
            added_count += 1

    # Log forbidden matches
    if forbidden_count > 0:
        if forbidden_count > 0:
            logging.debug(f"Filtered {forbidden_count} forbidden matches")

    # Update total count
    data["total_matches"] = len(data["matches"])

    # Save to file
    if added_count > 0:
        with open(daily_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    return added_count


# OPTIMIZATION: Pre-compile regex patterns at module level
_U_PATTERN = re.compile(r"\bu\d{1,2}\b")
_U_NUM_PATTERN = re.compile(r"\bu(\d{1,2})\b")


def is_forbidden_match(teams: dict) -> tuple:
    """Check if match should be forbidden (women's, youth, reserve teams).

    Args:
        teams: Dictionary with 'home' and 'away' team names

    Returns:
        Tuple of (is_forbidden: bool, reason: str or None)
        - (True, "women's match") if forbidden
        - (False, None) if allowed

    Note:
        Team names are converted to lowercase before checking,
        so 'Women', 'WOMEN', 'women' all match the keyword 'women'.
    """
    if not teams:
        return (False, None)

    # Women's matches (case-insensitive) - expanded patterns
    women_keywords = [
        "women",
        "woman",
        "ladies",
        "female",
        "(w)",
        "fem",
        "wfc",
        "wfc women",
        "women's",
        "womens",
    ]

    # Youth teams (U19 and below) - expanded patterns
    youth_keywords = [
        "u18",
        "u17",
        "u16",
        "u15",
        "u14",
        "u13",
        "u12",
        "under 18",
        "under 17",
        "under 16",
        "under 15",
        "under 14",
        "youth",
        "junior",
        "juniors",
    ]

    # Reserve/B teams - expanded patterns
    reserve_keywords = [
        "reserve",
        "reserves",
        " ii",
        " iii",
        " b",
        " c",
        "second",
        "2nd",
        "third",
        "3rd",
    ]

    # Convert to lowercase for case-insensitive matching
    home_team = str(teams.get("home", "")).lower().strip()
    away_team = str(teams.get("away", "")).lower().strip()

    # Combine both team names for checking
    combined_teams = f"{home_team} {away_team}"

    # Check women's matches (check both individual teams and combined)
    for keyword in women_keywords:
        if keyword in home_team or keyword in away_team or keyword in combined_teams:
            return (True, "women's match")

    # Check youth teams (U19 and below)
    for keyword in youth_keywords:
        if keyword in home_team or keyword in away_team or keyword in combined_teams:
            return (True, "youth team")

    # Check reserve teams
    for keyword in reserve_keywords:
        # For ' b' and ' c', check if it's at the end of team name
        if keyword in [" b", " c", " ii", " iii"]:
            if home_team.endswith(keyword) or away_team.endswith(keyword):
                return (True, "reserve team")
        else:
            if keyword in home_team or keyword in away_team:
                return (True, "reserve team")

    # OPTIMIZATION: Use pre-compiled regex patterns
    if _U_PATTERN.search(home_team) or _U_PATTERN.search(away_team):
        # Extract U number
        u_match = _U_NUM_PATTERN.search(home_team + " " + away_team)
        if u_match:
            u_num = int(u_match.group(1))
            if u_num <= 19:  # U19 and below
                return (True, "youth team")

    return (False, None)


def extract_league_and_matches(
    browser, config=None, allowed_countries_set=None
) -> list:
    """Extract matches grouped by league with importance indicator.

    Checks each comp-container for:
    - icon-yishoucang = important league (collected)
    - icon-weishoucang = not important league (not collected)

    Args:
        browser: BrowserManager instance
        config: Config instance (optional, for team extraction settings)

    Returns:
        List of dicts with league info and matches:
        {
            'league_name': str,
            'is_important': bool,
            'icon_class': str,
            'matches': [{'url': str, 'match_id': str, 'teams': {...}}]
        }
    """
    from selenium.webdriver.common.by import By

    leagues_data = []
    filtered_countries = {}  # Track filtered leagues by country
    included_countries = {}  # Track included leagues by country

    try:
        # Find all league containers
        containers = browser.driver.find_elements(By.CSS_SELECTOR, ".comp-container")

        for idx, container in enumerate(containers):
            try:
                # Extract league name and country
                league_name = "Unknown"
                country = "Unknown"
                full_text = ""
                
                try:
                    # Step 1: Extract league name from .compe-name.minitext (user specified this)
                    try:
                        league_elem = container.find_element(By.CSS_SELECTOR, ".compe-name.minitext")
                        league_name = league_elem.text.strip()
                        if not league_name:
                            league_name = "Unknown"
                    except:
                        league_name = "Unknown"
                    
                    # Step 2: Extract country from .country-name or .comp-name
                    try:
                        # Most common: .country-name or .comp-name
                        country_elem = container.find_element(By.CSS_SELECTOR, ".country-name, .comp-name")
                        full_text = country_elem.text.strip()
                    except:
                        full_text = ""
                    
                    # Step 3: Parse country from full_text (country-name element)
                    # This is the PRIMARY source for country name - always use it
                    if full_text:
                        if ":" in full_text:
                            parts = full_text.split(":", 1)
                            # Country is ALWAYS before colon - set it unconditionally
                            country_before_colon = parts[0].strip()
                            # Always set country from before colon (even if empty, it's better than Unknown)
                            country = (
                                country_before_colon if country_before_colon else "Unknown"
                            )
                            # If league name is still Unknown, try to get it from after colon
                            if (
                                league_name == "Unknown"
                                and len(parts) > 1
                                and parts[1].strip()
                            ):
                                league_name = parts[1].strip()
                        else:
                            # No colon in full_text - full_text IS the country name
                            country = full_text.strip()  # Set country directly
                    
                    # Final validation - ensure country is not empty
                    if not country or country.strip() == "":
                        country = "Unknown"
                    
                    # Final validation - ensure league_name is not empty
                    if not league_name or league_name.strip() == "":
                        league_name = "Unknown"
                        
                except Exception as e:
                    logging.debug(f"Error extracting league/country info: {e}")
                    continue  # Skip container on error

                # Determine if we should process this league
                should_process = False

                if config:
                    # Method 1: Filter by league name (PRIORITY - checked first)
                    if config.scraping.filter_by_leagues:
                        allowed_leagues = config.scraping.allowed_leagues or []
                        if allowed_leagues:
                            # Normalize league name for comparison (case-insensitive)
                            league_normalized = league_name.strip().lower()
                            allowed_leagues_normalized = [l.strip().lower() for l in allowed_leagues]
                            should_process = league_normalized in allowed_leagues_normalized
                            filter_reason = f"league={league_name}"
                        else:
                            # No leagues specified - skip all
                            should_process = False
                            filter_reason = "league=no-leagues-configured"

                    # Method 2: Filter by country (fallback if league filtering not enabled)
                    elif config.scraping.filter_by_countries:
                        # OPTIMIZATION: Use cached allowed_countries_set if available
                        country_normalized = country.strip().lower()
                        if allowed_countries_set is not None:
                            should_process = country_normalized in allowed_countries_set
                        else:
                            # Fallback: compute on the fly (shouldn't happen if optimization is active)
                            allowed_countries = config.scraping.allowed_countries or []
                            allowed_normalized = [
                                c.strip().lower() for c in allowed_countries
                            ]
                            should_process = country_normalized in allowed_normalized
                        filter_reason = f"country={country}"

                    # Method 3: Filter by importance flag (old method)
                    elif config.scraping.filter_by_importance:
                        try:
                            icon_element = container.find_element(
                                By.CSS_SELECTOR, ".collectImg"
                            )
                            icon_class = icon_element.get_attribute("class")
                            should_process = "icon-yishoucang" in icon_class
                            filter_reason = "importance=star"
                        except:
                            should_process = False
                            filter_reason = "importance=no-star"

                    # No filtering: process all leagues
                    else:
                        should_process = True
                        filter_reason = "no-filter"
                else:
                    # No config: default to processing all
                    should_process = True
                    filter_reason = "no-config"

                # Get all match links within this container
                match_links = []
                match_elements = container.find_elements(
                    By.CSS_SELECTOR, "a.match-container"
                )

                for match_elem in match_elements:
                    try:
                        href = match_elem.get_attribute("href")
                        if not href or "/match-" not in href:
                            continue  # Skip invalid matches

                        # Clean URL
                        clean_url = href.rstrip("/")
                        unwanted_paths = [
                            "/h2h",
                            "/odds",
                            "/statistics",
                            "/lineups",
                            "/predictions",
                            "/video",
                            "/live",
                            "/analysis",
                        ]
                        for path in unwanted_paths:
                            if clean_url.endswith(path):
                                clean_url = clean_url[: -len(path)]
                                break

                        clean_url = clean_url.rstrip("/")

                        # Validate URL format
                        if not clean_url or "/match-" not in clean_url:
                            continue

                        # Extract match_id
                        match_id = clean_url.split("/")[-1]

                        # Validate match_id
                        if not match_id or len(match_id) < 5:
                            continue

                        # OPTIMIZATION: Skip team extraction if disabled in config
                        home_team = None
                        away_team = None

                        # Only extract team names if enabled in config
                        extract_teams = (
                            getattr(
                                config.scraping,
                                "extract_team_names_during_link_scraping",
                                False,
                            )
                            if config
                            else False
                        )

                        if extract_teams:
                            try:
                                # PRIMARY METHOD: Use class="team home" and class="team away" selectors
                                try:
                                    home_elem = match_elem.find_element(
                                        By.CSS_SELECTOR,
                                        ".team.home, [class*='team home'], [class*='team-home']",
                                    )
                                    away_elem = match_elem.find_element(
                                        By.CSS_SELECTOR,
                                        ".team.away, [class*='team away'], [class*='team-away']",
                                    )

                                    home_team_raw = home_elem.text.strip()
                                    away_team_raw = away_elem.text.strip()

                                    # Clean and preserve full names (normalize whitespace)
                                    home_team = (
                                        " ".join(home_team_raw.split())
                                        if home_team_raw
                                        else None
                                    )
                                    away_team = (
                                        " ".join(away_team_raw.split())
                                        if away_team_raw
                                        else None
                                    )
                                except:
                                    # FALLBACK: Try alternative selectors
                                    try:
                                        # Try data attributes first (fastest)
                                        home_team_raw = match_elem.get_attribute(
                                            "data-home-team"
                                        )
                                        away_team_raw = match_elem.get_attribute(
                                            "data-away-team"
                                        )
                                        if home_team_raw and away_team_raw:
                                            home_team = " ".join(home_team_raw.split())
                                            away_team = " ".join(away_team_raw.split())
                                    except:
                                        pass

                                    # Try other common selectors
                                    if not home_team or not away_team:
                                        selectors_to_try = [
                                            (".team.home", ".team.away"),
                                            (
                                                "[class*='team home']",
                                                "[class*='team away']",
                                            ),
                                            (
                                                "[class*='team-home']",
                                                "[class*='team-away']",
                                            ),
                                            (".home-team-name", ".away-team-name"),
                                            (
                                                "div[class*='home'] span",
                                                "div[class*='away'] span",
                                            ),
                                            (
                                                ".teamName:first-of-type",
                                                ".teamName:last-of-type",
                                            ),
                                        ]

                                        for home_sel, away_sel in selectors_to_try:
                                            try:
                                                home_elem = match_elem.find_element(
                                                    By.CSS_SELECTOR, home_sel
                                                )
                                                away_elem = match_elem.find_element(
                                                    By.CSS_SELECTOR, away_sel
                                                )

                                                home_team_raw = home_elem.text.strip()
                                                away_team_raw = away_elem.text.strip()

                                                if home_team_raw and away_team_raw:
                                                    home_team = " ".join(
                                                        home_team_raw.split()
                                                    )
                                                    away_team = " ".join(
                                                        away_team_raw.split()
                                                    )
                                                    if (
                                                        len(home_team) >= 2
                                                        and len(away_team) >= 2
                                                    ):
                                                        break
                                            except:
                                                continue

                                # Final validation and cleanup
                                if home_team and away_team:
                                    if len(home_team) < 2 or len(away_team) < 2:
                                        home_team = None
                                        away_team = None
                                    else:
                                        # Remove leading numbers but preserve full team names
                                        home_team = re.sub(
                                            r"^\d+\s+", "", home_team
                                        ).strip()
                                        away_team = re.sub(
                                            r"^\d+\s+", "", away_team
                                        ).strip()
                                        # Ensure we still have valid names after cleanup
                                        if len(home_team) < 2 or len(away_team) < 2:
                                            home_team = None
                                            away_team = None
                            except Exception as e:
                                logging.debug(f"Error extracting team names: {e}")
                                pass

                        match_data = {"url": clean_url, "match_id": match_id}

                        # ALWAYS include team names if extracted from DOM
                        if home_team and away_team:
                            match_data["teams"] = {"home": home_team, "away": away_team}

                        match_links.append(match_data)
                    except Exception as e:
                        continue

                # Store league data if it passes filter
                if match_links and should_process:
                    # Ensure league_name is not empty
                    if not league_name or league_name.strip() == "":
                        league_name = "Unknown"

                    leagues_data.append(
                        {
                            "league_name": league_name,
                            "country": country,
                            "is_important": should_process,
                            "matches": match_links,
                        }
                    )
                    included_countries[country] = included_countries.get(
                        country, 0
                    ) + len(match_links)
                elif match_links and not should_process:
                    filtered_countries[country] = filtered_countries.get(
                        country, 0
                    ) + len(match_links)

            except Exception as e:
                logging.debug(f"Error parsing container {idx}: {e}")
                continue

    except Exception as e:
        logging.error(f"Error extracting leagues: {e}", exc_info=True)

    # Log summary of filtering
    if filtered_countries and config and config.scraping.filter_by_countries:
        total_filtered = sum(filtered_countries.values())
        logging.info(
            f"Filtered out {len(filtered_countries)} countries with {total_filtered} matches: {dict(filtered_countries)}"
        )

    if included_countries:
        total_included = sum(included_countries.values())
        logging.info(
            f"Included {len(included_countries)} countries with {total_included} matches: {dict(included_countries)}"
        )

    # Removed redundant country discovery loop - we already have this info from processing containers

    return leagues_data


def extract_match_info(url: str) -> dict:
    """Extract basic match info from URL.

    Args:
        url: Match URL

    Returns:
        Dictionary with match_id, teams
    """
    # Extract match_id from URL
    # Format: https://www.aiscore.com/match-team1-team2/MATCH_ID
    try:
        # Clean URL first (remove any trailing paths like /h2h)
        clean_url = url.rstrip("/")

        # Remove unwanted paths
        unwanted_paths = [
            "/h2h",
            "/odds",
            "/statistics",
            "/lineups",
            "/predictions",
            "/video",
            "/live",
        ]
        for path in unwanted_paths:
            if path in clean_url:
                clean_url = clean_url.split(path)[0]

        # Extract match_id (last part after /)
        parts = clean_url.split("/")
        match_id = parts[-1]

        # Extract team names from the match-xxx part
        match_part = parts[-2] if len(parts) >= 2 else ""
        teams_part = match_part.replace("match-", "")

        # Try to split teams intelligently
        # Look for common patterns: team1-vs-team2, team1-team2
        words = teams_part.split("-")

        # Look for "vs" separator first (most reliable)
        vs_index = -1
        for i, word in enumerate(words):
            if word.lower() in ["vs", "v", "versus"]:
                vs_index = i
                break

        if vs_index > 0:
            # Found "vs" separator - split there
            home_team = " ".join(words[:vs_index]).title()
            away_team = " ".join(words[vs_index + 1 :]).title()
        elif len(words) >= 3:
            # No "vs" found and 3+ words - use intelligent pattern recognition
            # Common patterns to recognize:
            # - State abbreviations (RS, PR, etc.) belong to previous team
            # - Common suffixes (United, City, Town, FC, CF) belong to previous team
            # - Common prefixes (Club, Atletico, CA, SC, FC) start a new team

            # List of words that typically belong to the previous team (suffixes/abbreviations)
            team_suffixes = {
                "rs",
                "pr",
                "sp",
                "rj",
                "mg",
                "sc",
                "fc",
                "cf",
                "ac",
                "ca",
                "united",
                "city",
                "town",
                "fc",
                "cf",
                "ac",
                "club",
                "sporting",
                "u21",
                "u19",
                "u17",
                "women",
                "w",
                "youth",
                "boldklub",
            }

            # List of words that typically start a new team (prefixes)
            team_prefixes = {
                "club",
                "atletico",
                "sc",
                "fc",
                "cf",
                "ca",
                "ac",
                "sporting",
                "defensores",
                "gimnasia",
                "estudiantes",
                "racing",
                "river",
            }

            # For 3 words: try to detect patterns
            if len(words) == 3:
                # Check if middle word is a suffix (e.g., "chelmsford-city-hornchurch" = "Chelmsford City" vs "Hornchurch")
                if words[1].lower() in team_suffixes:
                    # Middle word is suffix - belongs to first team
                    home_team = " ".join(words[:2]).title()
                    away_team = words[2].title()
                # Check if last word is a suffix/abbreviation (RS, PR, United, etc.)
                elif words[2].lower() in team_suffixes:
                    # Last word is suffix - it belongs to second team
                    # e.g., "fortaleza-gremio-rs" = "Fortaleza" vs "Gremio RS"
                    home_team = words[0].title()
                    away_team = " ".join(words[1:]).title()
                # Check if first word is a short prefix (CR, SC, FC, CA, etc.)
                elif len(words[0]) <= 3 and words[0].lower() not in {
                    "the",
                    "los",
                    "las",
                    "el",
                    "la",
                }:
                    # Short prefix like "cr", "sc", "fc" - likely part of first team
                    # e.g., "cr-flamengo-santos" = "CR Flamengo" vs "Santos"
                    home_team = " ".join(words[:2]).title()
                    away_team = words[2].title()
                # Check if middle word is a common team name connector
                elif words[1].lower() in {"de", "del", "y", "and", "e"}:
                    # Connector word - split after first word
                    # e.g., "sint-maarten-anguilla" = "Sint Maarten" vs "Anguilla"
                    home_team = " ".join(words[:2]).title()
                    away_team = words[2].title()
                else:
                    # Default: split after first word (most common pattern)
                    # e.g., "aveley-welling-united" = "Aveley" vs "Welling United"
                    # e.g., "sporting-braga-moreirense" = "Sporting" vs "Braga Moreirense"
                    home_team = words[0].title()
                    away_team = " ".join(words[1:]).title()
            elif len(words) == 4:
                # 4 words: check for patterns
                # Pattern 1: word 2 is a suffix (e.g., "chelmsford-city-hornchurch" = "Chelmsford City" vs "Hornchurch")
                if words[1].lower() in team_suffixes:
                    home_team = " ".join(words[:2]).title()
                    away_team = " ".join(words[2:]).title()
                # Pattern 2: word 3 is a suffix (e.g., "benfica-casa-pia-ac" = "Benfica" vs "Casa Pia AC")
                elif words[3].lower() in team_suffixes:
                    # Last word is suffix - belongs to second team, split after word 1
                    home_team = words[0].title()
                    away_team = " ".join(words[1:]).title()
                # Pattern 3: word 2 is a prefix (e.g., "club-atletico-team1-team2")
                elif words[2].lower() in team_prefixes:
                    home_team = " ".join(words[:2]).title()
                    away_team = " ".join(words[2:]).title()
                # Pattern 4: both teams have prefixes
                elif (
                    words[0].lower() in team_prefixes
                    and words[2].lower() in team_prefixes
                ):
                    home_team = " ".join(words[:2]).title()
                    away_team = " ".join(words[2:]).title()
                else:
                    # Default: split in middle
                    home_team = " ".join(words[:2]).title()
                    away_team = " ".join(words[2:]).title()
            else:
                # 5+ words: look for common split points using pattern recognition
                # Common patterns:
                # - "de", "del", "y" often appear in team names (don't split on them)
                # - Team prefixes (Club, Atletico, etc.) often start new teams
                # - Try to find natural split point

                split_point = len(words) // 2

                # Look for second team prefix (most reliable indicator)
                for i in range(2, len(words) - 1):
                    if words[i].lower() in team_prefixes:
                        split_point = i
                        break

                # If no prefix found, look for common team name patterns
                # Check for patterns like "Mar Del Plata", "La Plata", "De Cordoba"
                if split_point == len(words) // 2:
                    # Look for "de", "del", "la" patterns that indicate team name continuation
                    for i in range(2, len(words) - 1):
                        if words[i].lower() in {"de", "del", "la", "los", "las"}:
                            # This is likely part of previous team name, don't split here
                            continue
                        elif i > split_point and words[i - 1].lower() not in {
                            "de",
                            "del",
                            "la",
                        }:
                            # Potential split point
                            split_point = i
                            break

                home_team = " ".join(words[:split_point]).title()
                away_team = " ".join(words[split_point:]).title()
        elif len(words) == 2:
            # Only 2 words - likely "team1-team2"
            home_team = words[0].title()
            away_team = words[1].title()
        else:
            home_team = teams_part.replace("-", " ").title()
            away_team = "Unknown"

        # Fix capitalization for common abbreviations (keep them uppercase)
        abbreviation_map = {
            "cr": "CR",
            "rs": "RS",
            "pr": "PR",
            "sp": "SP",
            "rj": "RJ",
            "mg": "MG",
            "sc": "SC",
            "fc": "FC",
            "cf": "CF",
            "ac": "AC",
            "ca": "CA",
            "u21": "U21",
            "u19": "U19",
            "u17": "U17",
        }

        # Fix abbreviations in team names
        def fix_abbreviations(name):
            words_in_name = name.split()
            fixed_words = []
            for word in words_in_name:
                word_lower = word.lower()
                if word_lower in abbreviation_map:
                    fixed_words.append(abbreviation_map[word_lower])
                else:
                    fixed_words.append(word)
            return " ".join(fixed_words)

        home_team = fix_abbreviations(home_team)
        away_team = fix_abbreviations(away_team)

        return {"match_id": match_id, "teams": {"home": home_team, "away": away_team}}

    except Exception as e:
        logging.debug(f"Error extracting info from URL {url}: {e}")
        return {"match_id": "unknown", "teams": {"home": "Unknown", "away": "Unknown"}}


def save_to_json(
    links: list, scrape_date: str, game_date: str, bronze_storage: BronzeStorage
):
    """Save scraped links to bronze/{date}/daily_listings.json

    Args:
        links: List of match URLs
        scrape_date: Date when scraping happened (YYYYMMDD)
        game_date: Date of the games (YYYYMMDD)
        bronze_storage: BronzeStorage instance
    """
    # Get absolute path for logging
    date_folder = bronze_storage.daily_listings_dir / scrape_date
    daily_file = date_folder / "matches.json"
    
    # Log the exact path being used
    logging.info(f"Saving to: {daily_file.absolute()}")
    logging.info(f"Bronze storage base: {bronze_storage.base_path.absolute()}")
    logging.info(f"Daily listings dir: {bronze_storage.daily_listings_dir.absolute()}")

    # Load existing data if file exists (from incremental saves)
    existing_urls = set()
    existing_matches = []
    if daily_file.exists():
        try:
            with open(daily_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                existing_matches = existing_data.get("matches", [])
                existing_urls = {match.get("match_url", "") for match in existing_matches}
                logging.info(f"Found {len(existing_matches)} existing matches in file: {daily_file.absolute()}")
        except Exception as e:
            logging.warning(f"Error reading existing file: {e}, starting fresh", exc_info=True)

    # Prepare matches list from new links
    scrape_timestamp = datetime.now().isoformat()
    new_matches = []
    
    for url in links:
        # Skip if already exists
        if url in existing_urls:
            continue
            
        match_info = extract_match_info(url)

        match_record = {
            "match_id": match_info["match_id"],
            "match_url": url,
            "teams": match_info["teams"],
            "game_date": game_date,
            "scrape_timestamp": scrape_timestamp,
            "scrape_status": "n/a",  # Initial status
        }

        new_matches.append(match_record)
        existing_urls.add(url)  # Track to avoid duplicates

    # Merge existing and new matches
    all_matches = existing_matches + new_matches

    # Create JSON structure
    data = {
        "scrape_date": scrape_date,
        "total_matches": len(all_matches),
        "matches": all_matches,
        "links_scraping_complete": True,  # Mark as complete when saving final file
        "links_scraping_completed_at": datetime.now().isoformat(),
    }

    # Ensure directory exists
    date_folder.mkdir(parents=True, exist_ok=True)
    
    # Write to file (atomic write)
    temp_file = date_folder / ".matches.json.tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Verify JSON is valid
        with open(temp_file, "r", encoding="utf-8") as f:
            json.load(f)  # Will raise if invalid
        
        # Atomic rename
        temp_file.replace(daily_file)
        
        file_size = daily_file.stat().st_size / 1024
        
        # Verify file was written
        if not daily_file.exists():
            raise IOError(f"File was not created: {daily_file.absolute()}")
        
        if new_matches:
            logging.info(f"✓ Added {len(new_matches)} new matches. Total: {len(all_matches)} matches saved to {daily_file.absolute()} ({file_size:.1f} KB)")
        else:
            logging.info(f"✓ No new matches to add. Total: {len(all_matches)} matches in {daily_file.absolute()} ({file_size:.1f} KB)")
            
        # Final verification - read back the file
        with open(daily_file, "r", encoding="utf-8") as f:
            verify_data = json.load(f)
            verify_count = len(verify_data.get("matches", []))
            if verify_count != len(all_matches):
                logging.error(f"VERIFICATION FAILED: Saved {len(all_matches)} but file contains {verify_count} matches!")
            else:
                logging.info(f"✓ Verification passed: File contains {verify_count} matches")
                
    except Exception as e:
        logging.error(f"Error saving to {daily_file.absolute()}: {e}", exc_info=True)
        if temp_file.exists():
            try:
                temp_file.unlink()
            except:
                pass
        raise


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Football Link Scraper - Pure JSON Storage (NO DATABASE)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape_links.py 20251113              # Scrape links for Nov 13, 2025
  python scrape_links.py 20251113 --visible    # Use visible browser
  python scrape_links.py --month 202511       # Scrape entire month (November 2025)
  python scrape_links.py --month 202511 --visible  # Monthly with visible browser
  
Output:
  Creates bronze/{date}/daily_listings.json with all match links
  Initial status: "n/a" (will change when odds scraping starts)
  
NO DATABASE USED - Direct JSON storage
        """,
    )

    parser.add_argument(
        "date",
        type=str,
        nargs="?",
        help="Date to scrape (YYYYMMDD or YYYY-MM-DD). Required unless --month is used.",
    )

    parser.add_argument(
        "--month",
        type=str,
        help="Scrape entire month (YYYYMM format, e.g., 202511 for November 2025)",
    )

    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (default: headless)",
    )

    # Config is read from .env file - no config parameter needed

    args = parser.parse_args()

    # Validate arguments
    if not args.date and not args.month:
        print("Error: Either 'date' argument or '--month' option is required")
        parser.print_help()
        sys.exit(1)

    if args.date and args.month:
        print(
            "Error: Cannot use both 'date' and '--month' options. Use one or the other."
        )
        sys.exit(1)

    # Handle monthly scraping
    if args.month:
        month_str = args.month.replace("-", "")
        if len(month_str) != 6 or not month_str.isdigit():
            print(f"Error: Invalid month format. Use YYYYMM (got: {args.month})")
            sys.exit(1)

        year = int(month_str[:4])
        month = int(month_str[4:6])

        if month < 1 or month > 12:
            print(f"Error: Invalid month. Must be between 01 and 12 (got: {month:02d})")
            sys.exit(1)

        # Generate all dates in the month
        _, last_day = monthrange(year, month)
        dates_to_scrape = []
        for day in range(1, last_day + 1):
            date_str = f"{year}{month:02d}{day:02d}"
            dates_to_scrape.append(date_str)

        logging.info(
            f"Monthly scraping mode: {len(dates_to_scrape)} dates in {year}-{month:02d}"
        )
        return scrape_month(dates_to_scrape, args)

    # Single date scraping (existing logic)
    date = args.date.replace("-", "")

    # Validate date format
    if len(date) != 8 or not date.isdigit():
        print(f"Error: Invalid date format. Use YYYYMMDD (got: {args.date})")
        sys.exit(1)

    # Load configuration from .env file
    try:
        config = Config()
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Override config with CLI args
    if args.visible:
        config.browser.headless = False
        logging.info("Visible mode enabled via --visible flag")

    # Setup logging with date suffix
    setup_logging(config, date_suffix=date)

    logging.info("=" * 80)
    browser_mode = 'Visible' if not config.browser.headless else 'Headless'
    logging.info(
        f"Football Link Scraper | Date: {date} | Browser: {browser_mode}"
    )
    if not config.browser.headless:
        logging.info("NOTE: Browser window should be visible. If running in Docker, use -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix")
    logging.info("=" * 80)

    # Initialize bronze storage
    bronze_storage = BronzeStorage(config.storage.bronze_path)
    
    # Check if links scraping is already complete before starting browser
    date_folder = bronze_storage.daily_listings_dir / date
    daily_file = date_folder / "matches.json"
    
    if daily_file.exists():
        try:
            with open(daily_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if existing_data.get("links_scraping_complete", False):
                    completed_at = existing_data.get("links_scraping_completed_at", "unknown")
                    total_matches = existing_data.get("total_matches", 0)
                    logging.info(f"✓ Links scraping already completed for {date}")
                    logging.info(f"  Completed at: {completed_at}")
                    logging.info(f"  Total matches: {total_matches}")
                    logging.info(f"  Skipping links scraping. To re-scrape, delete: {daily_file.absolute()}")
                    logging.info("=" * 80)
                    sys.exit(0)
        except Exception as e:
            logging.warning(f"Error checking daily listing file: {e}, continuing with scraping")

    # Initialize browser
    browser = None
    exit_code = 0

    try:
        # Create browser
        browser = BrowserManager(config)
        logging.info(f"Creating browser in {'visible' if not config.browser.headless else 'headless'} mode...")
        browser.create_driver()
        if not config.browser.headless:
            logging.info("Browser window should now be visible. Check your screen for the browser window.")

        # Scrape links
        start_time = time.time()
        logging.info("Starting link scraping...")
        links = scrape_match_links(date, browser, config)
        elapsed_time = time.time() - start_time

        logging.info(f"Scraping completed. Found {len(links)} links.")
        
        # Check if file was already created by incremental saves
        date_folder = bronze_storage.daily_listings_dir / date
        daily_file = date_folder / "matches.json"
        
        if not links:
            if daily_file.exists():
                # File exists from incremental saves, just verify
                try:
                    with open(daily_file, "r", encoding="utf-8") as f:
                        existing_data = json.load(f)
                        existing_matches = existing_data.get("matches", [])
                        logging.info(f"Found {len(existing_matches)} matches in existing file")
                        if existing_matches:
                            logging.info("Links were saved incrementally during scraping.")
                            exit_code = 0
                        else:
                            logging.warning("No matches found for this date")
                            exit_code = 1
                except Exception as e:
                    logging.error(f"Error reading existing file: {e}")
                    exit_code = 1
            else:
                logging.warning("No matches found for this date and no file was created")
                exit_code = 1
        else:
            # Save to JSON - use game_date as filename (not today's date)
            logging.info(f"Saving {len(links)} links to file...")
            save_to_json(links, date, date, bronze_storage)
            
            # Verify the save
            if daily_file.exists():
                try:
                    with open(daily_file, "r", encoding="utf-8") as f:
                        saved_data = json.load(f)
                        saved_count = len(saved_data.get("matches", []))
                        logging.info(f"Verified: {saved_count} matches saved to {daily_file}")
                except Exception as e:
                    logging.error(f"Error verifying saved file: {e}")
            
            # Note: Links scraping completion is tracked via data lineage
            # No need to mark as complete - lineage tracks all operations automatically

            logging.info("=" * 80)
            logging.info(
                f"Summary: {len(links)} matches scraped in {elapsed_time:.1f}s"
            )
            logging.info(f"Next: python scrape_odds.py --date {date}")
            logging.info("=" * 80)

    except KeyboardInterrupt:
        logging.info("\nScraping interrupted by user")
        exit_code = 130

    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        exit_code = 1

    finally:
        if browser:
            try:
                browser.close()
                logging.info("Browser closed")
            except:
                pass

    logging.info("Scraping session completed")

    sys.exit(exit_code)


def scrape_month(dates_to_scrape: list, args):
    """Scrape multiple dates (monthly scraping).

    Args:
        dates_to_scrape: List of dates in YYYYMMDD format
        args: Parsed arguments (for visible, etc.)
    """
    # Load configuration from .env file
    try:
        config = Config()
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Override config with CLI args
    if args.visible:
        config.browser.headless = False
        logging.info("Visible mode enabled via --visible flag")

    # Setup logging with month suffix (use first date's month)
    month_suffix = dates_to_scrape[0][:6] if dates_to_scrape else None  # YYYYMM format
    setup_logging(config, date_suffix=month_suffix)

    browser_mode = 'Visible' if not config.browser.headless else 'Headless'
    logging.info("=" * 80)
    logging.info(
        f"Monthly Scraping Mode | {len(dates_to_scrape)} dates | Browser: {browser_mode}"
    )
    if not config.browser.headless:
        logging.info("NOTE: Browser window should be visible. If running in Docker, use -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix")
    logging.info("=" * 80)

    # Initialize bronze storage
    bronze_storage = BronzeStorage(config.storage.bronze_path)

    # Initialize browser (reuse for all dates)
    browser = None
    exit_code = 0
    total_matches = 0
    successful_dates = 0
    failed_dates = 0

    try:
        # Create browser once for the entire month
        browser = BrowserManager(config)
        browser.create_driver()

        month_start_time = time.time()

        for idx, date in enumerate(dates_to_scrape, 1):
            try:
                logging.info("=" * 80)
                logging.info(f"Processing date {idx}/{len(dates_to_scrape)}: {date}")
                logging.info("=" * 80)
                
                # Check if links scraping is already complete for this date
                date_folder = bronze_storage.daily_listings_dir / date
                daily_file = date_folder / "matches.json"
                
                if daily_file.exists():
                    try:
                        with open(daily_file, "r", encoding="utf-8") as f:
                            existing_data = json.load(f)
                            if existing_data.get("links_scraping_complete", False):
                                completed_at = existing_data.get("links_scraping_completed_at", "unknown")
                                total_matches = existing_data.get("total_matches", 0)
                                logging.info(f"✓ Links scraping already completed for {date} ({total_matches} matches, completed at: {completed_at})")
                                logging.info(f"  Skipping. To re-scrape, delete: {daily_file.absolute()}")
                                successful_dates += 1
                                continue
                    except Exception as e:
                        logging.warning(f"Error checking daily listing for {date}: {e}, continuing with scraping")

                # Scrape links for this date
                start_time = time.time()
                links = scrape_match_links(date, browser, config)
                elapsed_time = time.time() - start_time

                if not links:
                    logging.warning(f"No matches found for {date}")
                    failed_dates += 1
                else:
                    # Save to JSON
                    save_to_json(links, date, date, bronze_storage)
                    # Mark links scraping as complete
                    # Note: Links scraping completion is tracked via data lineage
                    total_matches += len(links)
                    successful_dates += 1

                    logging.info(
                        f"✓ {date}: {len(links)} matches scraped in {elapsed_time:.1f}s"
                    )

                # Small delay between dates to avoid overwhelming the server
                if idx < len(dates_to_scrape):
                    delay = getattr(config.scraping.delays, "between_dates", 1.0)
                    time.sleep(delay)

            except KeyboardInterrupt:
                logging.info(f"\nMonthly scraping interrupted by user at date {date}")
                exit_code = 130
                break

            except Exception as e:
                logging.error(f"Error scraping {date}: {e}", exc_info=True)
                failed_dates += 1
                # Continue with next date instead of stopping
                continue

        month_elapsed_time = time.time() - month_start_time

        # Final summary
        logging.info("=" * 80)
        logging.info("MONTHLY SCRAPING SUMMARY")
        logging.info("=" * 80)
        logging.info(f"Total dates processed: {len(dates_to_scrape)}")
        logging.info(f"Successful: {successful_dates}")
        logging.info(f"Failed: {failed_dates}")
        logging.info(f"Total matches found: {total_matches}")
        logging.info(
            f"Total time: {month_elapsed_time:.1f}s ({month_elapsed_time/60:.1f} minutes)"
        )
        logging.info("=" * 80)

    except KeyboardInterrupt:
        logging.info("\nMonthly scraping interrupted by user")
        exit_code = 130

    except Exception as e:
        logging.error(f"Fatal error during monthly scraping: {e}", exc_info=True)
        exit_code = 1

    finally:
        if browser:
            try:
                browser.close()
                logging.info("Browser closed")
            except:
                pass

    logging.info("Monthly scraping session completed")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
