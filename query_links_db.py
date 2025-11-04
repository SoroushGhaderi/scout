"""
Database query utilities for tennis match links
Simple functions to query the database - no CSV exports
"""

import sqlite3


def connect_database(db_path='data/tennis_matches.db'):
    """Connect to database"""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except sqlite3.Error as e:
        print(f"❌ Database connection error: {e}")
        return None


def get_statistics(db_path='data/tennis_matches.db'):
    """Get comprehensive database statistics"""
    conn = connect_database(db_path)
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    # Total links
    cursor.execute('SELECT COUNT(*) FROM match_links')
    total = cursor.fetchone()[0]
    
    # Scraped vs unscraped
    cursor.execute('SELECT COUNT(*) FROM match_links WHERE is_scraped = 1')
    scraped = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM match_links WHERE is_scraped = 0')
    unscraped = cursor.fetchone()[0]
    
    # Failed attempts
    cursor.execute('SELECT COUNT(*) FROM match_links WHERE attempt_count >= 3 AND is_scraped = 0')
    failed = cursor.fetchone()[0]
    
    # Total match details
    cursor.execute('SELECT COUNT(*) FROM match_details')
    details_count = cursor.fetchone()[0]
    
    # Unique dates
    cursor.execute('SELECT COUNT(DISTINCT source_date) FROM match_links')
    unique_dates = cursor.fetchone()[0]
    
    # Links by date
    cursor.execute('''
        SELECT source_date, 
               COUNT(*) as total,
               SUM(CASE WHEN is_scraped = 1 THEN 1 ELSE 0 END) as scraped
        FROM match_links 
        GROUP BY source_date 
        ORDER BY source_date DESC
    ''')
    by_date = cursor.fetchall()
    
    conn.close()
    
    return {
        'total_links': total,
        'scraped': scraped,
        'unscraped': unscraped,
        'failed': failed,
        'details_count': details_count,
        'unique_dates': unique_dates,
        'links_by_date': by_date,
        'progress': (scraped / total * 100) if total > 0 else 0
    }


def print_statistics(db_path='data/tennis_matches.db'):
    """Print database statistics"""
    stats = get_statistics(db_path)
    
    if stats is None:
        print("⚠️  Could not retrieve statistics")
        return
    
    print(f"\n{'='*80}")
    print(f"📊 Database Statistics")
    print(f"{'='*80}")
    print(f"Total links: {stats['total_links']}")
    print(f"Unique dates: {stats['unique_dates']}")
    print(f"\n🎯 Scraping Progress:")
    print(f"   Scraped: {stats['scraped']} ({stats['progress']:.1f}%)")
    print(f"   Unscraped: {stats['unscraped']}")
    print(f"   Failed (3+ attempts): {stats['failed']}")
    print(f"   Match details saved: {stats['details_count']}")
    
    print(f"\n📅 Links by date:")
    print(f"{'-'*80}")
    for date, total, scraped in stats['links_by_date'][:20]:
        progress = (scraped / total * 100) if total > 0 else 0
        status_bar = '█' * int(progress / 5) + '░' * (20 - int(progress / 5))
        print(f"   {date}: {total:3d} links [{status_bar}] {scraped}/{total} scraped ({progress:.0f}%)")
    
    print(f"{'='*80}\n")


def get_unscraped_links(db_path='data/tennis_matches.db', limit=100):
    """Get links that haven't been scraped yet"""
    conn = connect_database(db_path)
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    query = '''
        SELECT id, match_url, match_id, source_date, attempt_count
        FROM match_links 
        WHERE is_scraped = 0
        AND (attempt_count < 3 OR attempt_count IS NULL)
        ORDER BY created_at ASC
    '''
    
    if limit:
        query += f' LIMIT {limit}'
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    conn.close()
    return results


def get_scraped_details(db_path='data/tennis_matches.db', limit=100):
    """Get scraped match details"""
    conn = connect_database(db_path)
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    query = '''
        SELECT match_url, player1, player2, tournament, score, status, scraped_at
        FROM match_details
        ORDER BY scraped_at DESC
    '''
    
    if limit:
        query += f' LIMIT {limit}'
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    conn.close()
    return results


def get_failed_links(db_path='data/tennis_matches.db'):
    """Get links that failed to scrape (3+ attempts)"""
    conn = connect_database(db_path)
    if not conn:
        return None
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT match_url, match_id, source_date, attempt_count, scraping_error
        FROM match_links 
        WHERE is_scraped = 0 AND attempt_count >= 3
        ORDER BY last_attempt_at DESC
    ''')
    
    results = cursor.fetchall()
    conn.close()
    return results


def reset_failed_links(db_path='data/tennis_matches.db'):
    """Reset failed links to retry scraping"""
    conn = connect_database(db_path)
    if not conn:
        return
    
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM match_links WHERE is_scraped = 0 AND attempt_count >= 3')
    count = cursor.fetchone()[0]
    
    if count == 0:
        print("⚠️  No failed links to reset")
        conn.close()
        return
    
    print(f"⚠️  About to reset {count} failed links")
    confirm = input("Type 'yes' to confirm: ")
    
    if confirm.lower() == 'yes':
        cursor.execute('''
            UPDATE match_links 
            SET attempt_count = 0, scraping_error = NULL 
            WHERE is_scraped = 0 AND attempt_count >= 3
        ''')
        conn.commit()
        print(f"✅ Reset {count} failed links")
    else:
        print("❌ Reset cancelled")
    
    conn.close()


def search_links(search_term, db_path='data/tennis_matches.db'):
    """Search for links containing specific text"""
    conn = connect_database(db_path)
    if not conn:
        return None
    
    cursor = conn.cursor()
    cursor.execute('''
        SELECT match_url, match_id, source_date, is_scraped
        FROM match_links 
        WHERE match_url LIKE ?
        ORDER BY created_at DESC
    ''', (f'%{search_term}%',))
    
    results = cursor.fetchall()
    conn.close()
    return results


def main():
    """Print statistics"""
    print_statistics()


if __name__ == "__main__":
    main()
