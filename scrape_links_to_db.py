"""Scrape match links and save to database"""

import sqlite3
import time
import logging
import argparse
import calendar
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def init_database(db_path='data/tennis_matches.db'):
    """Create database and tables if not exists"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create match_links table with scraping status tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_url TEXT UNIQUE NOT NULL,
            match_id TEXT,
            source_date TEXT,
            date_discovered TEXT NOT NULL,
            
            -- Scraping status tracking
            is_scraped INTEGER DEFAULT 0,
            scraped_at TEXT,
            last_attempt_at TEXT,
            attempt_count INTEGER DEFAULT 0,
            scraping_error TEXT,
            
            -- Timestamps
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create match_details table for storing scraped data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_url TEXT UNIQUE NOT NULL,
            match_id TEXT,
            
            -- Match info
            player1 TEXT,
            player2 TEXT,
            tournament TEXT,
            score TEXT,
            status TEXT,
            start_time TEXT,
            round TEXT,
            surface TEXT,
            
            -- Additional data (stored as JSON)
            statistics TEXT,
            h2h_data TEXT,
            odds_data TEXT,
            
            -- Metadata
            scraped_at TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (match_url) REFERENCES match_links (match_url)
        )
    ''')
    
    # Create index for faster queries
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_is_scraped 
        ON match_links(is_scraped)
    ''')
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_source_date 
        ON match_links(source_date)
    ''')
    
    conn.commit()
    logger.info(f"Database initialized: {db_path}")
    return conn, cursor


def create_driver(headless=True):
    """Create and configure Selenium WebDriver"""
    chrome_options = Options()
    
    # Basic options
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Anti-detection options
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # User agent
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    if headless:
        chrome_options.add_argument("--headless")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Execute script to remove webdriver property (anti-detection)
    try:
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                })
            '''
        })
    except Exception as e:
        logger.warning(f"Could not execute CDP command: {e}")
    
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    
    logger.info("Selenium driver created")
    return driver


def normalize_url(url):
    """Normalize URL to prevent duplicates"""
    if not url:
        return None
    # Remove trailing slash
    url = url.rstrip('/')
    # Remove query parameters if any
    if '?' in url:
        url = url.split('?')[0]
    # Remove h2h suffix if present
    if url.endswith('/h2h'):
        url = url[:-4]  # Remove '/h2h'
    return url


def is_valid_match_url(url):
    """Check if URL is a valid match URL (not h2h, stats, etc.)"""
    if not url:
        return False
    url_lower = url.lower()
    # Exclude h2h, statistics, and other non-match pages
    excluded = ['/h2h', '/statistics', '/odds', '/predictions', '/lineups']
    return '/tennis/match' in url_lower and not any(exc in url_lower for exc in excluded)


def insert_link(cursor, conn, match_url, source_date):
    """Insert new link into database (ignore if duplicate)"""
    try:
        # Normalize URL to prevent duplicates
        normalized_url = normalize_url(match_url)
        if not normalized_url:
            return False
        
        date_discovered = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract match_id from URL
        # URL format: https://www.aiscore.com/tennis/match-players/MATCH_ID
        # Handle URLs like: /match-players/MATCH_ID or /match-players/MATCH_ID/h2h
        url_parts = normalized_url.split('/')
        match_id = url_parts[-1] if url_parts else ''
        
        # Make sure match_id is not 'h2h' or other invalid values
        if match_id.lower() in ['h2h', 'statistics', 'odds', 'predictions', 'lineups']:
            # Try to get the part before the invalid suffix
            if len(url_parts) >= 2:
                match_id = url_parts[-2]
            else:
                match_id = ''
        
        cursor.execute('''
            INSERT OR IGNORE INTO match_links 
            (match_url, match_id, source_date, date_discovered)
            VALUES (?, ?, ?, ?)
        ''', (normalized_url, match_id, source_date, date_discovered))
        conn.commit()
        return cursor.rowcount > 0  # Returns True if new row inserted
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return False


def remove_duplicate_links_by_date(cursor, conn, source_date):
    """Remove duplicate links for a specific date (keeps oldest one)"""
    try:
        # Find duplicates for this specific date
        cursor.execute('''
            SELECT match_url, COUNT(*) as count
            FROM match_links
            WHERE source_date = ?
            GROUP BY match_url
            HAVING COUNT(*) > 1
        ''', (source_date,))
        duplicates = cursor.fetchall()
        
        if not duplicates:
            logger.info(f"No duplicates found for date {source_date}")
            return 0
        
        logger.info(f"Found {len(duplicates)} duplicate URLs for date {source_date}")
        
        removed_count = 0
        for url, count in duplicates:
            # Keep the oldest one (lowest id), delete others for this date
            cursor.execute('''
                DELETE FROM match_links
                WHERE match_url = ?
                AND source_date = ?
                AND id NOT IN (
                    SELECT MIN(id) FROM match_links 
                    WHERE match_url = ? AND source_date = ?
                )
            ''', (url, source_date, url, source_date))
            removed_count += cursor.rowcount
        
        conn.commit()
        logger.info(f"Removed {removed_count} duplicate entries for date {source_date}")
        return removed_count
    except sqlite3.Error as e:
        logger.error(f"Error removing duplicates: {e}")
        return 0


def remove_invalid_links(cursor, conn):
    """Remove links with invalid match_id (h2h, statistics, etc.)"""
    try:
        # Remove links where match_id is invalid
        invalid_ids = ['h2h', 'statistics', 'odds', 'predictions', 'lineups']
        
        cursor.execute('''
            DELETE FROM match_links
            WHERE LOWER(match_id) IN ({})
        '''.format(','.join(['?' for _ in invalid_ids])), invalid_ids)
        
        removed_count = cursor.rowcount
        
        # Also remove links that contain /h2h in URL
        cursor.execute('''
            DELETE FROM match_links
            WHERE match_url LIKE '%/h2h%'
        ''')
        
        removed_count += cursor.rowcount
        
        conn.commit()
        if removed_count > 0:
            logger.info(f"Removed {removed_count} invalid links (h2h, statistics, etc.)")
        return removed_count
    except sqlite3.Error as e:
        logger.error(f"Error removing invalid links: {e}")
        return 0


def get_existing_links(cursor):
    """Get all existing links from database (normalized)"""
    cursor.execute('SELECT match_url FROM match_links')
    links = {normalize_url(row[0]) for row in cursor.fetchall()}
    return {url for url in links if url}  # Remove None values


def scrape_date(driver, cursor, conn, date_str, scroll_increment=500, scroll_pause_time=2.0, max_no_change=8):
    """
    Scrape match links for a specific date and save to database
    
    Args:
        driver: Selenium WebDriver instance
        cursor: Database cursor
        conn: Database connection
        date_str: Date in format 'YYYYMMDD' or 'YYYY-MM-DD'
        scroll_increment: Pixels to scroll each iteration
        scroll_pause_time: Seconds to wait between scrolls
        max_no_change: Stop after this many scrolls with no new links
        
    Returns:
        Number of new links found
    """
    # Format date
    date_formatted = date_str.replace('-', '')
    url = f"https://www.aiscore.com/tennis/{date_formatted}"
    
    # First navigate to main tennis page to bypass Cloudflare
    main_page = "https://www.aiscore.com/tennis/"
    logger.info(f"Opening main page first: {main_page}")
    driver.get(main_page)
    time.sleep(3)
    
    # Wait for main page to load
    wait_time = 0
    while wait_time < 20:
        time.sleep(2)
        wait_time += 2
        current_title = driver.title
        if "Just a moment" not in current_title and "Checking your browser" not in current_title:
            logger.info("Main page loaded")
            break
    
    # Now navigate to specific date page
    logger.info(f"Navigating to date page: {url}")
    driver.get(url)
    
    # Wait for Cloudflare protection to pass
    max_wait = 30
    wait_time = 0
    while wait_time < max_wait:
        time.sleep(2)
        wait_time += 2
        current_title = driver.title
        logger.info(f"Page title: {current_title}")
        
        # Check if Cloudflare passed
        if "Just a moment" not in current_title and "Checking your browser" not in current_title:
            logger.info("Cloudflare check passed")
            break
        
        if wait_time >= max_wait:
            logger.warning("Cloudflare check timeout - page may be blocked")
            return 0
    
    # Wait for page to fully load
    time.sleep(5)
    
    # Try to find at least one match link to confirm page loaded
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/tennis/match']"))
        )
        logger.info("Page loaded - match links found")
    except:
        logger.warning("No match links found on page - may need to wait longer or page structure changed")
        # Check page source for debugging
        if "tennis" in driver.page_source.lower():
            logger.info("Page contains 'tennis' keyword - page loaded but no matches found")
        else:
            logger.warning("Page may not have loaded correctly")
    
    # Track links found (for logging only)
    match_links_found = set()
    new_links_count = 0
    
    logger.info("Scrolling and saving links to database...")
    
    # Scrolling loop - insert links immediately after each scroll
    no_change_count = 0
    
    while no_change_count < max_no_change:
        # Get current position
        current_position = driver.execute_script("return window.pageYOffset;")
        
        # Collect and save links currently visible in DOM
        links_before = new_links_count
        
        for link in driver.find_elements(By.CSS_SELECTOR, "a[href*='/tennis/match']"):
            href = link.get_attribute('href')
            if href and is_valid_match_url(href):
                normalized_href = normalize_url(href)
                if normalized_href and normalized_href not in match_links_found:
                    match_links_found.add(normalized_href)
                    # Insert immediately into database
                    if insert_link(cursor, conn, normalized_href, date_formatted):
                        new_links_count += 1
        
        links_after = new_links_count
        new_links_found = links_after - links_before
        
        if new_links_found > 0:
            logger.info(f"Position: {current_position}px | Saved: {links_after} (+{new_links_found} new)")
        
        # Check if new links were found
        if new_links_found == 0:
            no_change_count += 1
        else:
            no_change_count = 0  # Reset counter if new links found
        
        # Scroll down by increment
        driver.execute_script(f"window.scrollBy(0, {scroll_increment});")
        time.sleep(scroll_pause_time)
        
        # Check if we've reached the actual bottom
        at_bottom = driver.execute_script(
            "return (window.innerHeight + window.pageYOffset) >= document.body.scrollHeight;"
        )
        if at_bottom and no_change_count >= 3:
            logger.info("Reached bottom of page")
            break
    
    # Final collection at the end
    for link in driver.find_elements(By.CSS_SELECTOR, "a[href*='/tennis/match']"):
        href = link.get_attribute('href')
        if href and is_valid_match_url(href):
            normalized_href = normalize_url(href)
            if normalized_href and normalized_href not in match_links_found:
                match_links_found.add(normalized_href)
                # Insert immediately
                if insert_link(cursor, conn, normalized_href, date_formatted):
                    new_links_count += 1
    
    logger.info(f"Finished scraping! Found {len(match_links_found)} unique links, saved {new_links_count} new links")
    
    # After scraping this date, check and remove duplicates for this specific date
    logger.info(f"Checking for duplicates for date {date_formatted}...")
    duplicate_removed = remove_duplicate_links_by_date(cursor, conn, date_formatted)
    
    if len(match_links_found) > 0 and new_links_count == 0:
        logger.warning(f"All {len(match_links_found)} links were already in database")
    elif len(match_links_found) == 0:
        logger.warning("No links found on page - check if date has matches")
    
    return new_links_count


def get_database_stats(cursor):
    """Get database statistics"""
    cursor.execute('SELECT COUNT(*) FROM match_links')
    total = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT source_date) FROM match_links')
    dates = cursor.fetchone()[0]
    
    cursor.execute('SELECT source_date, COUNT(*) FROM match_links GROUP BY source_date ORDER BY source_date DESC')
    by_date = cursor.fetchall()
    
    return {
        'total_links': total,
        'unique_dates': dates,
        'links_by_date': by_date
    }


def generate_month_dates(month_str):
    """Generate all dates in a month from YYYYMM format
    
    Args:
        month_str: Month in YYYYMM format (e.g., '202511')
    
    Returns:
        List of dates in YYYYMMDD format
    """
    try:
        # Parse month string
        if len(month_str) == 6:  # YYYYMM
            year = int(month_str[:4])
            month = int(month_str[4:6])
        elif len(month_str) == 7 and '-' in month_str:  # YYYY-MM
            year, month = map(int, month_str.split('-'))
        else:
            raise ValueError("Invalid month format")
        
        # Get number of days in month
        _, num_days = calendar.monthrange(year, month)
        
        # Generate all dates
        dates = []
        for day in range(1, num_days + 1):
            date_str = f"{year}{month:02d}{day:02d}"
            dates.append(date_str)
        
        return dates
    except Exception as e:
        logger.error(f"Error generating dates for month {month_str}: {e}")
        return []


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Scrape tennis match links and save to database',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'date',
        type=str,
        help='Date to scrape (YYYYMMDD, YYYY-MM-DD) or use --month for entire month'
    )
    
    parser.add_argument(
        '--month',
        action='store_true',
        default=False,
        help='Scrape entire month (date should be YYYYMM format, e.g., 202511)'
    )
    
    parser.add_argument(
        '--visible',
        action='store_true',
        default=False,
        help='Run browser in visible mode (default: headless)'
    )
    
    args = parser.parse_args()
    
    # Configuration
    DB_PATH = 'data/tennis_matches.db'
    HEADLESS = not args.visible  # Default is True (headless), use --visible to show browser
    
    # Determine dates to scrape
    if args.month:
        dates_to_scrape = generate_month_dates(args.date)
        if not dates_to_scrape:
            logger.error(f"Invalid month format: {args.date}. Use YYYYMM (e.g., 202511)")
            return
        logger.info(f"Scraping entire month: {args.date} ({len(dates_to_scrape)} dates)")
    else:
        dates_to_scrape = [args.date]
        logger.info(f"Scraping single date: {args.date}")
    
    logger.info(f"Headless mode: {HEADLESS}")
    
    # Initialize
    conn, cursor = init_database(DB_PATH)
    
    # Remove invalid links only (h2h, statistics, etc.) - no duplicate check before scraping
    remove_invalid_links(cursor, conn)
    
    driver = None
    total_new_links = 0
    
    try:
        # Create driver
        driver = create_driver(headless=HEADLESS)
        
        # Scrape each date
        for idx, date_to_scrape in enumerate(dates_to_scrape, 1):
            if len(dates_to_scrape) > 1:
                logger.info(f"\n{'='*60}")
                logger.info(f"Scraping date {idx}/{len(dates_to_scrape)}: {date_to_scrape}")
                logger.info(f"{'='*60}")
            
            new_links = scrape_date(driver, cursor, conn, date_to_scrape)
            total_new_links += new_links
            
            # Brief pause between dates to avoid triggering rate limits
            if idx < len(dates_to_scrape):
                logger.info("Waiting 5 seconds before next date...")
                time.sleep(5)
        
        # Show final statistics
        logger.info(f"\n{'='*60}")
        logger.info("FINAL STATISTICS")
        logger.info(f"{'='*60}")
        
        stats = get_database_stats(cursor)
        logger.info(f"Total links in database: {stats['total_links']}")
        logger.info(f"New links added: {total_new_links}")
        logger.info(f"Unique dates: {stats['unique_dates']}")
        
        logger.info(f"\nTop 10 dates by link count:")
        for date, count in stats['links_by_date'][:10]:
            logger.info(f"  {date}: {count} links")
        
        if total_new_links == 0:
            logger.warning("\nNo new links found. If Cloudflare is blocking, try:")
            logger.warning("  1. Run with --visible flag to show browser")
            logger.warning("  2. Use a VPN or different network")
            logger.warning("  3. Try again later")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if driver:
            driver.quit()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

