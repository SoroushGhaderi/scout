"""Scrape match details for unscraped links from database"""

import sqlite3
import time
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


def connect_database(db_path='data/tennis_matches.db'):
    """Connect to database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    return conn, cursor


def create_driver(headless=True):
    """Create Selenium driver"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    if headless:
        chrome_options.add_argument("--headless")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(10)
    
    return driver


def get_unscraped_links(cursor, limit=None, max_attempts=3):
    """
    Get links that haven't been scraped yet
    
    Args:
        cursor: Database cursor
        limit: Maximum number of links to return
        max_attempts: Only get links with fewer attempts than this
    
    Returns:
        List of tuples: (id, match_url, match_id, source_date, attempt_count)
    """
    query = '''
        SELECT id, match_url, match_id, source_date, attempt_count
        FROM match_links
        WHERE is_scraped = 0 
        AND (attempt_count < ? OR attempt_count IS NULL)
        ORDER BY created_at ASC
    '''
    
    if limit:
        query += f' LIMIT {limit}'
    
    cursor.execute(query, (max_attempts,))
    return cursor.fetchall()


def mark_scraping_attempt(cursor, conn, link_id, success=False, error_message=None):
    """
    Update scraping attempt status
    
    Args:
        cursor: Database cursor
        conn: Database connection
        link_id: ID of the link in match_links table
        success: Whether scraping was successful
        error_message: Error message if failed
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    if success:
        cursor.execute('''
            UPDATE match_links
            SET is_scraped = 1,
                scraped_at = ?,
                last_attempt_at = ?,
                attempt_count = attempt_count + 1,
                scraping_error = NULL,
                updated_at = ?
            WHERE id = ?
        ''', (now, now, now, link_id))
    else:
        cursor.execute('''
            UPDATE match_links
            SET last_attempt_at = ?,
                attempt_count = attempt_count + 1,
                scraping_error = ?,
                updated_at = ?
            WHERE id = ?
        ''', (now, error_message, now, link_id))
    
    conn.commit()


def save_match_details(cursor, conn, match_url, match_data):
    """
    Save scraped match details to database
    
    Args:
        cursor: Database cursor
        conn: Database connection
        match_url: Match URL
        match_data: Dictionary with match details
    """
    try:
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Convert complex data to JSON
        statistics_json = json.dumps(match_data.get('statistics', {}))
        h2h_json = json.dumps(match_data.get('h2h', {}))
        odds_json = json.dumps(match_data.get('odds', {}))
        
        cursor.execute('''
            INSERT OR REPLACE INTO match_details
            (match_url, match_id, player1, player2, tournament, score, status,
             start_time, round, surface, statistics, h2h_data, odds_data, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            match_url,
            match_data.get('match_id', ''),
            match_data.get('player1', ''),
            match_data.get('player2', ''),
            match_data.get('tournament', ''),
            match_data.get('score', ''),
            match_data.get('status', ''),
            match_data.get('start_time', ''),
            match_data.get('round', ''),
            match_data.get('surface', ''),
            statistics_json,
            h2h_json,
            odds_json,
            now
        ))
        
        conn.commit()
        return True
    
    except sqlite3.Error as e:
        print(f"❌ Database error saving details: {e}")
        return False


def scrape_match_details(driver, match_url):
    """
    Scrape details for a single match
    
    Args:
        driver: Selenium WebDriver
        match_url: URL of the match
    
    Returns:
        Dictionary with match data or None if failed
    """
    try:
        print(f"   Loading: {match_url}")
        driver.get(match_url)
        time.sleep(3)  # Wait for page to load
        
        # Get page source
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Extract match ID from URL
        match_id = match_url.split('/')[-1] if '/' in match_url else ''
        
        # Try to extract basic match info from page
        match_data = {
            'match_id': match_id,
            'match_url': match_url,
            'player1': extract_text(soup, ['home-name', 'player1', 'team-home']),
            'player2': extract_text(soup, ['away-name', 'player2', 'team-away']),
            'tournament': extract_text(soup, ['tournament-name', 'league-name', 'tournament']),
            'score': extract_text(soup, ['score', 'match-score']),
            'status': extract_text(soup, ['status', 'match-status', 'state']),
            'start_time': extract_text(soup, ['time', 'match-time', 'start-time']),
            'round': extract_text(soup, ['round']),
            'surface': extract_text(soup, ['surface']),
            'statistics': extract_statistics(soup),
            'h2h': extract_h2h(soup),
            'odds': extract_odds(soup)
        }
        
        return match_data
    
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return None


def extract_text(soup, class_names):
    """Extract text from element with multiple possible class names"""
    for class_name in class_names:
        element = soup.find(class_=class_name)
        if element:
            return element.get_text(strip=True)
    return ''


def extract_statistics(soup):
    """Extract match statistics"""
    stats = {}
    
    # Look for statistics section
    stats_section = soup.find('div', class_='statistics') or soup.find('div', id='statistics')
    
    if stats_section:
        stat_items = stats_section.find_all('div', class_='stat-item')
        for item in stat_items:
            name_elem = item.find(class_='stat-name')
            value_elem = item.find(class_='stat-value')
            
            if name_elem and value_elem:
                stats[name_elem.get_text(strip=True)] = value_elem.get_text(strip=True)
    
    return stats


def extract_h2h(soup):
    """Extract head-to-head data"""
    h2h_data = {'previous_matches': []}
    
    h2h_section = soup.find('div', class_='h2h') or soup.find('div', id='h2h')
    
    if h2h_section:
        matches = h2h_section.find_all('div', class_='match-item')
        for match in matches:
            h2h_data['previous_matches'].append({
                'date': extract_text(match, ['date']),
                'tournament': extract_text(match, ['tournament']),
                'score': extract_text(match, ['score'])
            })
    
    return h2h_data


def extract_odds(soup):
    """Extract betting odds"""
    odds = {}
    
    odds_section = soup.find('div', class_='odds') or soup.find('div', id='odds')
    
    if odds_section:
        bookmakers = odds_section.find_all('div', class_='bookmaker')
        for bm in bookmakers:
            name = extract_text(bm, ['bookmaker-name', 'name'])
            if name:
                odds[name] = {
                    'player1': extract_text(bm, ['player1-odds', 'home-odds']),
                    'player2': extract_text(bm, ['player2-odds', 'away-odds'])
                }
    
    return odds


def get_scraping_stats(cursor):
    """Get current scraping statistics"""
    cursor.execute('SELECT COUNT(*) FROM match_links WHERE is_scraped = 1')
    scraped_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM match_links WHERE is_scraped = 0')
    unscraped_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM match_links')
    total_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM match_details')
    details_count = cursor.fetchone()[0]
    
    return {
        'total_links': total_count,
        'scraped': scraped_count,
        'unscraped': unscraped_count,
        'details_saved': details_count,
        'progress': (scraped_count / total_count * 100) if total_count > 0 else 0
    }


def main():
    """Main execution function"""
    DB_PATH = 'data/tennis_matches.db'
    HEADLESS = True
    LIMIT = None
    DELAY = 3
    MAX_ATTEMPTS = 3
    
    conn, cursor = connect_database(DB_PATH)
    
    # Get initial stats
    stats = get_scraping_stats(cursor)
    print(f"📊 Status: {stats['scraped']}/{stats['total_links']} scraped ({stats['progress']:.1f}%)")
    
    # Get unscraped links
    links = get_unscraped_links(cursor, limit=LIMIT, max_attempts=MAX_ATTEMPTS)
    
    if not links:
        print("✅ No unscraped links found!")
        conn.close()
        return
    
    print(f"🎾 Found {len(links)} links to scrape\n")
    
    driver = None
    
    try:
        driver = create_driver(headless=HEADLESS)
        
        success_count = 0
        fail_count = 0
        
        for idx, (link_id, match_url, match_id, source_date, attempt_count) in enumerate(links, 1):
            print(f"[{idx}/{len(links)}] {match_id[:20]}...")
            
            try:
                match_data = scrape_match_details(driver, match_url)
                
                if match_data:
                    if save_match_details(cursor, conn, match_url, match_data):
                        mark_scraping_attempt(cursor, conn, link_id, success=True)
                        success_count += 1
                        print(f"   ✅ {match_data.get('player1', 'N/A')} vs {match_data.get('player2', 'N/A')}")
                    else:
                        mark_scraping_attempt(cursor, conn, link_id, success=False, error_message="Failed to save")
                        fail_count += 1
                else:
                    mark_scraping_attempt(cursor, conn, link_id, success=False, error_message="Failed to scrape")
                    fail_count += 1
                
            except Exception as e:
                error_msg = str(e)[:200]
                mark_scraping_attempt(cursor, conn, link_id, success=False, error_message=error_msg)
                fail_count += 1
                print(f"   ❌ Error: {e}")
            
            if idx < len(links):
                time.sleep(DELAY)
        
        print(f"\n✅ Done: {success_count} successful, {fail_count} failed")
        
        stats = get_scraping_stats(cursor)
        print(f"📊 Progress: {stats['scraped']}/{stats['total_links']} ({stats['progress']:.1f}%)")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if driver:
            driver.quit()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

