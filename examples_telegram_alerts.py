"""
Example: Sending Daily Metrics via Telegram

This script demonstrates how to use the new Telegram metrics reporting system.

Requirements:
1. Create a Telegram bot:
   - Message @BotFather on Telegram
   - Create a new bot and get the bot token
   
2. Get your Chat ID:
   - Forward the bot token to your chat
   - Visit: https://api.telegram.org/bot[YOUR_TOKEN]/getUpdates
   - Find your chat_id in the response

3. Add to .env:
   TELEGRAM_BOT_TOKEN=your_token_here
   TELEGRAM_CHAT_ID=your_chat_id_here

4. Call reports after scraping:
"""

from datetime import datetime
from src.utils.metrics_alerts import send_daily_report


def example_fotmob_report():
    """Example: FotMob daily report after scraping."""
    send_daily_report(
        scraper='fotmob',
        date='20260215',
        matches_scraped=150,
        errors=2,
        skipped=3,
        duration_seconds=3600,
        cache_hits=45,
        context={
            'database': 'ClickHouse (14 tables)',
            'storage': '2.3 GB TAR archives',
            'api_calls': '152 requests',
        }
    )


def example_aiscore_report():
    """Example: AIScore daily report after scraping."""
    send_daily_report(
        scraper='aiscore',
        date='20260215',
        matches_scraped=120,
        odds_scraped=118,
        errors=1,
        skipped=1,
        duration_seconds=5400,
        context={
            'database': 'ClickHouse (5 tables)',
            'storage': '1.8 GB TAR archives',
            'leagues_tracked': 95,
        }
    )


def example_integrated_in_pipeline():
    """
    Example: How to integrate into the main pipeline.
    
    In scripts/pipeline.py or similar:
    """
    from src.utils.metrics_alerts import send_daily_report
    from datetime import datetime
    import time
    
    # At the start of scraping
    start_time = time.time()
    
    # ... do FotMob scraping ...
    fotmob_matches = 150
    fotmob_errors = 2
    
    # ... do AIScore scraping ...
    aiscore_matches = 120
    aiscore_odds = 118
    
    # Send reports
    duration = time.time() - start_time
    
    # Send FotMob report
    send_daily_report(
        scraper='fotmob',
        matches_scraped=fotmob_matches,
        errors=fotmob_errors,
        duration_seconds=1800,  # FotMob duration
    )
    
    # Send AIScore report
    send_daily_report(
        scraper='aiscore',
        matches_scraped=aiscore_matches,
        odds_scraped=aiscore_odds,
        duration_seconds=duration - 1800,  # AIScore duration
    )


# Message Format Examples (what users will receive):
"""
═══════════════════════════════════════════

⚽ FotMob Daily Report - 20260215

✨ Matches Scraped: 150
📈 Success Rate: 98.7% ✅
❌ Errors: 2
⏭️ Skipped: 3
⏱️ Duration: 1.0h
💨 Cache Hits: 45

Additional Details:
🗄️ database: ClickHouse (14 tables)
💾 storage: 2.3 GB TAR archives
🌐 api_calls: 152 requests

✅ All matches scraped successfully!

═══════════════════════════════════════════

⚽ AIScore Daily Report - 20260215

✨ Matches Found: 120
💰 Odds Scraped: 118
📈 Success Rate: 98.3% ✅
❌ Errors: 1
⏭️ Skipped: 1
⏱️ Duration: 1.5h

Additional Details:
🗄️ database: ClickHouse (5 tables)
💾 storage: 1.8 GB TAR archives
🎲 leagues_tracked: 95

ℹ️ Status: Completed with issues

═══════════════════════════════════════════
"""


if __name__ == '__main__':
    print("Telegram Metrics Reporting Examples")
    print("=" * 50)
    print("\n1. FotMob Daily Report:")
    print("   example_fotmob_report()")
    print("\n2. AIScore Daily Report:")
    print("   example_aiscore_report()")
    print("\n3. Integration in Pipeline:")
    print("   example_integrated_in_pipeline()")
    print("\nSee CONFIG_GUIDE.md for Telegram setup instructions.")
