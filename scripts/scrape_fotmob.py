"""
Bronze Layer Processing - Raw Data Scraping
============================================

SCRAPER: FotMob
PURPOSE: Scrapes raw match data from FotMob API and saves to Bronze layer.
         Supports single date, date range, and monthly scraping.

Usage:
    # Single date
    python scripts/scrape_fotmob.py 20250108
    
    # Date range (start and end)
    python scripts/scrape_fotmob.py 20250101 20250107
    
    # Date range (start + number of days)
    python scripts/scrape_fotmob.py 20250101 --days 7
    
    # Monthly scraping
    python scripts/scrape_fotmob.py --month 202511
    
    # With options
    python scripts/scrape_fotmob.py 20250108 --force --debug
"""

import argparse
from datetime import datetime, timedelta
from calendar import monthrange
from src.config import FotMobConfig
from src.orchestrator import FotMobOrchestrator
from src.utils.logging_utils import setup_logging
from src.utils.date_utils import extract_year_month, DATE_FORMAT_COMPACT
from src.utils.alerting import get_alert_manager, AlertLevel


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='FotMob Bronze Layer Processing - Scrape raw match data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=""" 
Examples:
  Single Date:
    python scripts/scrape_fotmob.py 20250108                   # Scrape Jan 8, 2025
    python scripts/scrape_fotmob.py 20250108 --force --debug   # Force + debug mode
  
  Date Range:
    python scripts/scrape_fotmob.py 20250101 20250107          # Scrape Jan 1-7
    python scripts/scrape_fotmob.py 20250101 --days 7          # 7 days from Jan 1
    python scripts/scrape_fotmob.py 20250101 --days 30 --force # Force 30 days
  
  Monthly Scraping:
    python scripts/scrape_fotmob.py --month 202511              # Scrape entire November 2025
    python scripts/scrape_fotmob.py --month 202511 --force       # Month with force re-scrape
        """
    )
    
    # Date arguments - mutually exclusive group
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        'start_date',
        type=str,
        nargs='?',
        help='Date (or start date) in YYYYMMDD format. Required unless --month is used.'
    )
    date_group.add_argument(
        '--month',
        type=str,
        help='Scrape entire month (YYYYMM format, e.g., 202511 for November 2025)'
    )
    
    # Optional: end date OR number of days (mutually exclusive, only if start_date is used)
    range_group = parser.add_mutually_exclusive_group()
    range_group.add_argument(
        'end_date',
        type=str,
        nargs='?',
        help='End date in YYYYMMDD format (for range scraping, requires start_date)'
    )
    range_group.add_argument(
        '--days',
        type=int,
        help='Number of days to scrape from start date (requires start_date)'
    )
    
    # Note: FotMob scraper runs single-threaded (sequential) by default
    # Parallel processing is disabled for stability and API rate limiting
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-scrape already processed matches'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    parser.add_argument(
        '--compress',
        action='store_true',
        help='Compress files after scraping (creates .tar archives)'
    )
    
    args = parser.parse_args()
    
    # Validate month format if provided
    if args.month:
        if len(args.month) != 6 or not args.month.isdigit():
            parser.error(f"Invalid month format: {args.month}. Use YYYYMM (e.g., 202511)")
        
        year_str, month_str = extract_year_month(args.month)
        year = int(year_str)
        month = int(month_str)
        
        if not (1 <= month <= 12):
            parser.error(f"Invalid month: {month}. Must be between 01 and 12")
        
        # Month mode - end_date and days are not allowed
        if args.end_date or args.days:
            parser.error("Cannot use --end_date or --days with --month option")
    
    # Validate start_date format if provided
    if args.start_date:
        try:
            datetime.strptime(args.start_date, DATE_FORMAT_COMPACT)
        except ValueError:
            parser.error(f"Invalid start date format: {args.start_date}. Use YYYYMMDD (e.g., 20250108)")
        
        # Validate end_date if provided
        if args.end_date:
            try:
                datetime.strptime(args.end_date, DATE_FORMAT_COMPACT)
            except ValueError:
                parser.error(f"Invalid end date format: {args.end_date}. Use YYYYMMDD")
            
            # Check that end_date >= start_date
            if args.end_date < args.start_date:
                parser.error(f"End date ({args.end_date}) cannot be before start date ({args.start_date})")
        
        # Validate days if provided
        if args.days and args.days < 1:
            parser.error(f"Number of days must be at least 1 (got: {args.days})")
    
    return args


def generate_date_range(start_date_str: str, end_date_str: str = None, num_days: int = None) -> list:
    """
    Generate list of dates in YYYYMMDD format.
    
    Args:
        start_date_str: Start date (YYYYMMDD)
        end_date_str: End date (YYYYMMDD) - optional
        num_days: Number of days from start - optional
    
    Returns:
        List of date strings in YYYYMMDD format
    """
    start_date = datetime.strptime(start_date_str, DATE_FORMAT_COMPACT)
    
    # Single date (no end_date or days specified)
    if not end_date_str and not num_days:
        return [start_date_str]
    
    # Calculate end date
    if end_date_str:
        end_date = datetime.strptime(end_date_str, DATE_FORMAT_COMPACT)
    else:  # num_days provided
        end_date = start_date + timedelta(days=num_days - 1)
    
    # Generate date list
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime(DATE_FORMAT_COMPACT))
        current_date += timedelta(days=1)
    
    return dates


def generate_month_dates(month_str: str) -> list:
    """
    Generate all dates in a month.
    
    Args:
        month_str: Month in YYYYMM format (e.g., 202511)
    
    Returns:
        List of date strings in YYYYMMDD format
    """
    year = int(month_str[:4])
    month = int(month_str[4:6])
    
    _, last_day = monthrange(year, month)
    dates = []
    
    for day in range(1, last_day + 1):
        date_str = f"{year}{month:02d}{day:02d}"
        dates.append(date_str)
    
    return dates


def main():
    """Main execution function"""
    args = parse_arguments()
    
    # Initialize configuration
    config = FotMobConfig()
    
    # Override log level if debug mode
    if args.debug:
        config.logging.level = 'DEBUG'
    
    # Ensure parallel processing is disabled (FotMob runs single-threaded)
    config.scraping.enable_parallel = False
    config.scraping.max_workers = 1
    
    # Generate list of dates to process
    if args.month:
        dates = generate_month_dates(args.month)
        year, month = extract_year_month(args.month)
        month_name = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][int(month) - 1]
        date_display = f"Month: {month_name} {year} ({args.month})"
        mode_text = f"Monthly ({len(dates)} days)"
    else:
        dates = generate_date_range(args.start_date, args.end_date, args.days)
        is_range = len(dates) > 1
        mode_text = f"Range ({len(dates)} days)" if is_range else "Single date"
        date_display = dates[0] + (f" to {dates[-1]}" if is_range else "")
    
    # Setup logging
    if args.month:
        log_suffix = args.month
    elif len(dates) == 1:
        log_suffix = dates[0]
    else:
        log_suffix = f"{dates[0]}_to_{dates[-1]}"
    
    logger = setup_logging(
        name="bronze_processing",
        log_dir=config.log_dir,
        log_level=config.log_level,
        date_suffix=log_suffix
    )
    
    # Print header
    print("\n" + "=" * 80)
    print("Bronze Layer Processing")
    print("=" * 80)
    print(f"Mode:             {mode_text}")
    print(f"Date(s):          {date_display}")
    print(f"Total dates:      {len(dates)}")
    print(f"Mode:             Single-threaded (sequential)")
    print(f"Force re-scrape:  {args.force}")
    print(f"Auto-compress:    {args.compress}")
    print("=" * 80 + "\n")
    
    # Track overall statistics
    total_stats = {
        'dates_processed': 0,
        'dates_failed': 0,
        'total_matches': 0,
        'total_successful': 0,
        'total_failed': 0,
        'total_skipped': 0
    }
    
    # Initialize orchestrator
    orchestrator = FotMobOrchestrator(config)
    
    # Process each date
    for idx, date_str in enumerate(dates, 1):
        try:
            logger.info(f"[{idx}/{len(dates)}] Processing date: {date_str}")
            print(f"\n[{idx}/{len(dates)}] Scraping {date_str}...")
            
            # Run scraping (compression and profiling are automatic)
            metrics = orchestrator.scrape_date(
                date_str=date_str,
                force_rescrape=args.force
            )
            
            # Update statistics
            total_stats['dates_processed'] += 1
            total_stats['total_matches'] += metrics.total_matches
            total_stats['total_successful'] += metrics.successful_matches
            total_stats['total_failed'] += metrics.failed_matches
            total_stats['total_skipped'] += metrics.skipped_matches
            
            # Show date summary
            print(f"  Matches:   {metrics.total_matches}")
            print(f"  Success:   {metrics.successful_matches}")
            print(f"  Failed:    {metrics.failed_matches}")
            print(f"  Skipped:   {metrics.skipped_matches}")
            
        except Exception as e:
            logger.error(f"Failed to process date {date_str}: {e}")
            print(f"  ERROR: {e}")
            total_stats['dates_failed'] += 1
            # Send email alert
            alert_manager = get_alert_manager()
            alert_manager.send_alert(
                level=AlertLevel.ERROR,
                title=f"FotMob Bronze Scraping Failed - {date_str}",
                message=f"Failed to scrape FotMob data for date {date_str}.\n\nError: {str(e)}",
                context={"date": date_str, "step": "FotMob Bronze Scraping", "error": str(e)}
            )
    
    # Print final summary
    print("\n" + "=" * 80)
    print("BRONZE LAYER COMPLETE")
    print("=" * 80)
    print(f"Dates processed:  {total_stats['dates_processed']}/{len(dates)}")
    print(f"Dates failed:     {total_stats['dates_failed']}")
    print("-" * 80)
    print(f"Total matches:    {total_stats['total_matches']}")
    print(f"Successful:       {total_stats['total_successful']}")
    print(f"Failed:           {total_stats['total_failed']}")
    print(f"Skipped:          {total_stats['total_skipped']}")
    print("=" * 80)
    
    # Show next steps
    if total_stats['total_successful'] > 0:
        print("\nNext steps:")
        print("  Load to ClickHouse:  python scripts/load_clickhouse.py --scraper fotmob --date <date>")
        print("  View profiling:  python manage.py bronze view-profiling")
    
    # Exit with appropriate code
    exit_code = 0 if total_stats['dates_failed'] == 0 else 1
    exit(exit_code)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user. Exiting...")
        exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)

