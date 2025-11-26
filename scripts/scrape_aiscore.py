"""
AIScore Bronze Layer Orchestrator
==================================

SCRAPER: AIScore
PURPOSE: Runs AIScore bronze layer processing sequentially:
         1. Link Scraping: Scrapes match links and saves to daily_listings.json
         2. Odds Scraping: Scrapes odds data for all matches in daily_listings.json

Usage:
    # Single date (full pipeline)
    python scripts/scrape_aiscore.py 20251113
    
    # Date range
    python scripts/scrape_aiscore.py 20251101 20251107
    
    # Monthly scraping
    python scripts/scrape_aiscore.py --month 202511
    
    # Links only
    python scripts/scrape_aiscore.py 20251113 --links-only
    
    # Odds only (requires daily_listings.json to exist)
    python scripts/scrape_aiscore.py 20251113 --odds-only
"""

import argparse
import sys
import logging
from pathlib import Path

# Add project root to path (using optimized utility)
sys.path.insert(0, str(Path(__file__).parent))
from utils import add_project_to_path, validate_date_format, generate_date_range, generate_month_dates, PerformanceTimer
add_project_to_path()

from src.utils.date_utils import extract_year_month, DATE_FORMAT_COMPACT
from src.utils.alerting import get_alert_manager, AlertLevel

# Lazy imports for better startup performance
_scraper_links_module = None
_scraper_odds_module = None
_config_module = None
_browser_module = None
_bronze_storage_module = None

logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='AIScore Bronze Layer Pipeline - Orchestrates Links → Odds scraping',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Full Pipeline:
    python scripts/scrape_aiscore.py 20251113              # Single date
    python scripts/scrape_aiscore.py 20251101 20251107      # Date range
    python scripts/scrape_aiscore.py --month 202511         # Entire month
  
  Layer-Specific:
    python scripts/scrape_aiscore.py 20251113 --links-only   # Links only
    python scripts/scrape_aiscore.py 20251113 --odds-only   # Odds only
  
  Options:
    python scripts/scrape_aiscore.py 20251113 --visible      # Visible browser
        """
    )
    
    # Date arguments
    parser.add_argument(
        'date',
        type=str,
        nargs='?',
        help='Date to scrape (YYYYMMDD). Required unless --month is used.'
    )
    
    parser.add_argument(
        'end_date',
        type=str,
        nargs='?',
        help='End date for range scraping (YYYYMMDD format)'
    )
    
    parser.add_argument(
        '--month',
        type=str,
        help='Scrape entire month (YYYYMM format, e.g., 202511 for November 2025)'
    )
    
    # Layer selection
    layer_group = parser.add_mutually_exclusive_group()
    layer_group.add_argument(
        '--links-only',
        action='store_true',
        help='Run link scraping only (skip odds)'
    )
    layer_group.add_argument(
        '--odds-only',
        action='store_true',
        help='Run odds scraping only (requires daily_listings.json to exist)'
    )
    
    # Options
    parser.add_argument(
        '--visible',
        action='store_true',
        help='Run browser in visible mode (not headless)'
    )
    
    # Config is read from .env file - no config parameter needed
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-scrape even if data exists'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.date and not args.month:
        parser.error("Either 'date' argument or '--month' option is required")
    
    if args.date and args.month:
        parser.error("Cannot use both 'date' and '--month' options. Use one or the other.")
    
    # Validate month format if provided
    if args.month:
        if len(args.month) != 6 or not args.month.isdigit():
            parser.error(f"Invalid month format: {args.month}. Use YYYYMM (e.g., 202511)")
        
        year_str, month_str = extract_year_month(args.month)
        year = int(year_str)
        month = int(month_str)
        
        if not (1 <= month <= 12):
            parser.error(f"Invalid month: {month}. Must be between 01 and 12")
        
        if not (2000 <= year <= 2100):
            parser.error(f"Invalid year: {year}. Must be between 2000 and 2100")
    
    return args


def _lazy_load_links_scraper():
    """Lazy load link scraper module for better performance."""
    global _scraper_links_module
    if _scraper_links_module is None:
        sys.path.insert(0, str(Path(__file__).parent / 'aiscore_scripts'))
        import scrape_links
        _scraper_links_module = scrape_links
    return _scraper_links_module


def _lazy_load_odds_scraper():
    """Lazy load odds scraper module for better performance."""
    global _scraper_odds_module
    if _scraper_odds_module is None:
        sys.path.insert(0, str(Path(__file__).parent / 'aiscore_scripts'))
        import scrape_odds
        _scraper_odds_module = scrape_odds
    return _scraper_odds_module


def _get_config():
    """Get or create cached config instance."""
    global _config_module
    if _config_module is None:
        from src.scrapers.aiscore.config import Config
        _config_module = Config()
    return _config_module


def _get_bronze_storage():
    """Get or create cached bronze storage instance."""
    global _bronze_storage_module
    if _bronze_storage_module is None:
        from src.scrapers.aiscore.bronze_storage import BronzeStorage
        config = _get_config()
        _bronze_storage_module = BronzeStorage(config.storage.bronze_path)
    return _bronze_storage_module


def run_links_scraping(args, date_str: str) -> int:
    """
    Run link scraping for a specific date using direct function call (no subprocess).
    PERFORMANCE OPTIMIZATION: Direct import eliminates subprocess overhead (~500ms saved per call).
    """
    print("=" * 80)
    print(f"STEP 1: LINK SCRAPING - {date_str}")
    print("=" * 80)

    try:
        # Import scraper module (lazy loaded)
        scrape_links = _lazy_load_links_scraper()

        # Get or create config
        config = _get_config()

        # Override config with CLI args
        if args.visible:
            config.browser.headless = False
            print("[INFO] Visible mode enabled")

        # Call scraping function directly (no subprocess)
        from src.scrapers.aiscore.browser import BrowserManager

        browser = None
        try:
            browser = BrowserManager(config)
            bronze_storage = _get_bronze_storage()

            # Run the scraping logic directly
            matches = scrape_links.scrape_match_links(date_str, browser, config)

            if matches:
                # Save to bronze storage
                match_ids = [m['match_id'] for m in matches]
                bronze_storage.save_daily_listing(date_str, match_ids)
                print(f"\n[OK] Link scraping completed: {len(matches)} matches found")
                return 0
            else:
                print(f"\n[WARNING] No matches found for {date_str}")
                return 0  # Not an error - just no matches

        finally:
            if browser:
                try:
                    browser.close()
                except Exception as e:
                    logger.debug(f"Error closing browser: {e}")

    except Exception as e:
        print(f"\n[ERROR] Link scraping failed: {e}")
        logger.error(f"Link scraping error: {e}", exc_info=True)
        return 1


def run_odds_scraping(args, date_str: str) -> int:
    """
    Run odds scraping for a specific date using direct function call (no subprocess).
    PERFORMANCE OPTIMIZATION: Direct import eliminates subprocess overhead (~500ms saved per call).
    """
    print("=" * 80)
    print(f"STEP 2: ODDS SCRAPING - {date_str}")
    print("=" * 80)

    try:
        # Import scraper module (lazy loaded)
        scrape_odds = _lazy_load_odds_scraper()

        # Get or create config
        config = _get_config()

        # Override config with CLI args
        if args.visible:
            config.browser.headless = False
            print("[INFO] Visible mode enabled")

        # Call scraping function directly (no subprocess)
        from src.scrapers.aiscore.browser import BrowserManager
        from src.scrapers.aiscore.odds_scraper import OddsScraper

        browser = None
        try:
            browser = BrowserManager(config)
            bronze_storage = _get_bronze_storage()

            # Create odds scraper
            scraper = OddsScraper(config, db=None, browser=browser)

            # Run the scraping logic directly
            with PerformanceTimer(f"Odds scraping for {date_str}", logger):
                results = scraper.scrape_from_daily_matches(date_str, bronze_storage)

            successful = results.get('successful', 0)
            failed = results.get('failed', 0)

            print(f"\n[OK] Odds scraping completed: {successful} successful, {failed} failed")

            if failed > 0:
                print(f"[WARNING] {failed} matches failed")
                return 1
            return 0

        finally:
            if browser:
                try:
                    browser.close()
                except Exception as e:
                    logger.debug(f"Error closing browser: {e}")

    except Exception as e:
        print(f"\n[ERROR] Odds scraping failed: {e}")
        logger.error(f"Odds scraping error: {e}", exc_info=True)
        return 1


def check_daily_listings_exist(date_str: str) -> bool:
    """
    Check if daily listings exist for a date (indicates link scraping was completed).
    Uses daily_listings/YYYYMMDD/matches.json file.
    PERFORMANCE OPTIMIZATION: Uses cached config and bronze storage instances.
    """
    bronze_storage = _get_bronze_storage()
    # Check if daily listing file exists (this indicates links scraping was done)
    return bronze_storage.daily_listing_exists(date_str)


def process_single_date(args, date_str: str) -> tuple[bool, bool]:
    """
    Process a single date through the bronze layer pipeline.
    
    Returns:
        Tuple of (links_success, odds_success)
    """
    links_success = False
    odds_success = False
    
    # Check if matches already exist for this date (indicates scraping was completed)
    matches_exist = check_daily_listings_exist(date_str)
    
    # Step 1: Link Scraping (MUST run first, unless matches already exist)
    if not args.odds_only:
        if matches_exist:
            print(f"\n[OK] Matches already exist for {date_str}")
            print(f"[INFO] Skipping link scraping (day already scraped)")
            links_success = True
        else:
            # No matches exist - need to scrape links
            if args.force:
                print(f"\n[INFO] Force mode: Re-scraping links for {date_str}...")
            else:
                print(f"\n[INFO] No matches found for {date_str}")
                print(f"[INFO] Starting link scraping...")
            
            links_exit_code = run_links_scraping(args, date_str)
            links_success = (links_exit_code == 0)
            
            if not links_success:
                print(f"\n[ERROR] Link scraping failed for {date_str}")
                print(f"[ERROR] Cannot proceed to odds scraping without links.")
                # Send email alert
                alert_manager = get_alert_manager()
                alert_manager.send_alert(
                    level=AlertLevel.ERROR,
                    title=f"AIScore Link Scraping Failed - {date_str}",
                    message=f"Link scraping failed for date {date_str}. Cannot proceed to odds scraping.",
                    context={"date": date_str, "step": "AIScore Link Scraping", "exit_code": links_exit_code}
                )
                if not args.force:
                    return False, False
            else:
                print(f"\n[OK] Link scraping completed for {date_str}")
                # Verify matches were created
                print(f"[INFO] Verifying matches were created...")
                if not check_daily_listings_exist(date_str):
                    print(f"[WARNING] No matches found after link scraping!")
                    print(f"[WARNING] This may be normal if no matches were found for this date")
                else:
                    print(f"[OK] Matches verified and ready for odds scraping")
    else:
        # Odds-only mode: check if matches exist
        print(f"\n[INFO] Odds-only mode: Checking for existing matches...")
        if matches_exist:
            print(f"\n[OK] Matches found for {date_str}, proceeding to odds scraping")
            links_success = True
        else:
            print(f"\n[ERROR] No matches found for {date_str}")
            print(f"[ERROR] Link scraping must run first before odds scraping.")
            script_path = Path(__file__).parent.parent / 'scripts' / 'aiscore_scripts' / 'scrape_links.py'
            print(f"[INFO] Run link scraping first: python {script_path} {date_str}")
            return False, False
    
    # Step 2: Odds Scraping (runs only after links succeed)
    if not args.links_only and links_success:
        # Check daily listing to see how many matches need odds scraping
        # PERFORMANCE OPTIMIZATION: Use cached instances
        bronze_storage = _get_bronze_storage()
        
        # Load daily listing to check match statuses
        daily_listing = bronze_storage.load_daily_listing(date_str)
        if daily_listing:
            matches = daily_listing.get("matches", [])
            if matches:
                # Filter matches that need odds scraping (status: n/a, failed, pending, failed_by_timeout)
                matches_to_scrape = [
                    m for m in matches 
                    if m.get("scrape_status") in ["n/a", "failed", "pending", "failed_by_timeout"]
                ]
                matches_with_odds = [
                    m for m in matches 
                    if m.get("scrape_status") in ["success", "partial"]
                ]
                
                if len(matches_to_scrape) == 0 and len(matches_with_odds) > 0:
                    print(f"\n[OK] Odds scraping already completed for {date_str}")
                    print(f"[INFO] All {len(matches)} matches already have odds data")
                    print(f"[INFO] Skipping odds scraping")
                    odds_success = True
                elif len(matches_to_scrape) > 0:
                    print(f"\n[INFO] Starting odds scraping for {date_str}...")
                    print(f"[INFO] Total matches: {len(matches)}")
                    print(f"[INFO] To scrape: {len(matches_to_scrape)} (status: n/a/failed/pending)")
                    print(f"[INFO] Already done: {len(matches_with_odds)} (status: success/partial)")
                    print(f"[INFO] Skipped: {len(matches) - len(matches_to_scrape) - len(matches_with_odds)} (forbidden)")
                    odds_exit_code = run_odds_scraping(args, date_str)
                    odds_success = (odds_exit_code == 0)
                    
                    if odds_success:
                        print(f"\n[OK] Odds scraping completed for {date_str}")
                    else:
                        print(f"\n[ERROR] Odds scraping failed for {date_str}")
                        # Send email alert
                        alert_manager = get_alert_manager()
                        alert_manager.send_alert(
                            level=AlertLevel.ERROR,
                            title=f"AIScore Odds Scraping Failed - {date_str}",
                            message=f"Odds scraping failed for date {date_str}.\n\nTotal matches: {len(matches)}\nTo scrape: {len(matches_to_scrape)}",
                            context={"date": date_str, "step": "AIScore Odds Scraping", "exit_code": odds_exit_code, "total_matches": len(matches), "matches_to_scrape": len(matches_to_scrape)}
                        )
                else:
                    print(f"\n[WARNING] No matches to scrape odds for {date_str}")
                    odds_success = True
            else:
                print(f"\n[WARNING] Daily listing exists but has no matches for {date_str}")
                odds_success = True
        else:
            print(f"\n[WARNING] No daily listing found for {date_str}, skipping odds scraping")
            odds_success = True  # No matches to scrape odds for
    elif args.links_only:
        odds_success = True  # Skipped by design
    
    return links_success, odds_success


# Note: generate_date_range and generate_month_dates now imported from utils module (see imports at top)


def main():
    """Main execution"""
    args = parse_arguments()
    
    # Determine date range
    if args.month:
        dates = generate_month_dates(args.month)
        year, month = extract_year_month(args.month)
        month_name = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][int(month) - 1]
        date_display = f"Month: {month_name} {year} ({args.month})"
    elif args.end_date:
        dates = generate_date_range(args.date, args.end_date)
        date_display = f"Range: {args.date} to {args.end_date}"
    else:
        dates = [args.date]
        date_display = f"Single: {args.date}"
    
    # Determine mode
    if args.links_only:
        mode = "Links only"
    elif args.odds_only:
        mode = "Odds only"
    else:
        mode = "Links → Odds (full pipeline)"
    
    # Print header
    print("\n" + "=" * 80)
    print("AIScore Bronze Layer Pipeline")
    print("=" * 80)
    print(f"Mode:            {mode}")
    print(f"Date(s):         {date_display}")
    print(f"Total dates:     {len(dates)}")
    print(f"Browser:         {'Visible' if args.visible else 'Headless'}")
    print(f"Config:          .env file")
    print("=" * 80 + "\n")
    
    # Process dates
    successful_dates = 0
    failed_dates = 0
    links_only_count = 0
    odds_only_count = 0
    
    for idx, date_str in enumerate(dates, 1):
        print(f"\n{'=' * 80}")
        print(f"Processing date {idx}/{len(dates)}: {date_str}")
        print(f"{'=' * 80}\n")
        
        links_success, odds_success = process_single_date(args, date_str)
        
        if args.links_only:
            if links_success:
                successful_dates += 1
                links_only_count += 1
            else:
                failed_dates += 1
        elif args.odds_only:
            if odds_success:
                successful_dates += 1
                odds_only_count += 1
            else:
                failed_dates += 1
        else:
            # Full pipeline
            if links_success and odds_success:
                successful_dates += 1
            else:
                failed_dates += 1
                if not links_success:
                    print(f"[WARNING] Skipping odds scraping for {date_str} (links failed)")
    
    # Print final summary
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"Total dates processed: {len(dates)}")
    print(f"Successful:           {successful_dates}")
    print(f"Failed:                {failed_dates}")
    
    if args.links_only:
        print(f"Links scraped:         {links_only_count}")
    elif args.odds_only:
        print(f"Odds scraped:          {odds_only_count}")
    else:
        print(f"Full pipeline:          {successful_dates}")
    
    print("=" * 80)
    
    # Show next steps
    if successful_dates > 0 and not args.odds_only:
        print("\nNext steps:")
        if args.links_only:
            print(f"  Scrape odds:  python scripts/scrape_aiscore.py {dates[0]} --odds-only")
        else:
            print(f"  View data:     Check data/aiscore/daily_listings/{dates[0]}/matches.json")
            print(f"  View odds:     Check data/aiscore/matches/{dates[0]}/")
    
    # Exit with appropriate code
    if failed_dates > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user. Exiting...")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

