"""AIScore Bronze Layer Orchestrator.

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

# Load environment variables from .env file FIRST
import os
from dotenv import load_dotenv
load_dotenv()

import argparse
import sys
import json
import logging
import time
from pathlib import Path
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Tuple, Optional, List

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
from utils import (
    add_project_to_path,
    validate_date_format,
    generate_date_range,
    generate_month_dates,
    get_month_display_name,
    PerformanceTimer,
    print_header,
)

add_project_to_path()

from src.utils.date_utils import extract_year_month
from src.utils.alerting import get_alert_manager, AlertLevel
from src.utils.metrics_alerts import send_daily_report


# ============================================================================
# Module-level State (Lazy Loading)
# ============================================================================

_scraper_links_module = None
_scraper_odds_module = None
_config_module = None
_browser_module = None
_bronze_storage_module = None

logger = logging.getLogger(__name__)


# ============================================================================
# Lazy Loading Functions
# ============================================================================


def _lazy_load_links_scraper():
    """Lazy load link scraper module for better performance."""
    global _scraper_links_module
    if _scraper_links_module is None:
        sys.path.insert(0, str(Path(__file__).parent / "aiscore_scripts"))
        import scrape_links

        _scraper_links_module = scrape_links
    return _scraper_links_module


def _lazy_load_odds_scraper():
    """Lazy load odds scraper module for better performance."""
    global _scraper_odds_module
    if _scraper_odds_module is None:
        sys.path.insert(0, str(Path(__file__).parent / "aiscore_scripts"))
        import scrape_odds

        _scraper_odds_module = scrape_odds
    return _scraper_odds_module


def _get_config():
    """Get or create cached config instance."""
    global _config_module
    if _config_module is None:
        from config import AIScoreConfig as Config

        _config_module = Config()
    return _config_module


def _get_bronze_storage():
    """Get or create cached bronze storage instance."""
    global _bronze_storage_module
    if _bronze_storage_module is None:
        from src.storage.aiscore_storage import BronzeStorage

        config = _get_config()
        _bronze_storage_module = BronzeStorage(config.storage.bronze_path)
    return _bronze_storage_module


# ============================================================================
# Argument Parsing
# ============================================================================


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="AIScore Bronze Layer Pipeline - Orchestrates Links → Odds scraping",
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
        """,
    )

    _add_date_arguments(parser)
    _add_layer_arguments(parser)
    _add_option_arguments(parser)

    return parser


def _add_date_arguments(parser: argparse.ArgumentParser) -> None:
    """Add date-related arguments to parser."""
    parser.add_argument(
        "date",
        type=str,
        nargs="?",
        help="Date to scrape (YYYYMMDD). Required unless --month is used.",
    )
    parser.add_argument(
        "end_date",
        type=str,
        nargs="?",
        help="End date for range scraping (YYYYMMDD format)",
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Scrape entire month (YYYYMM format, e.g., 202511 for November 2025)",
    )


def _add_layer_arguments(parser: argparse.ArgumentParser) -> None:
    """Add layer selection arguments to parser."""
    layer_group = parser.add_mutually_exclusive_group()
    layer_group.add_argument(
        "--links-only", action="store_true", help="Run link scraping only (skip odds)"
    )
    layer_group.add_argument(
        "--odds-only",
        action="store_true",
        help="Run odds scraping only (requires daily_listings.json to exist)",
    )


def _add_option_arguments(parser: argparse.ArgumentParser) -> None:
    """Add option arguments to parser."""
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (not headless)",
    )
    parser.add_argument(
        "--force", action="store_true", help="Force re-scrape even if data exists"
    )


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    parser = create_argument_parser()
    args = parser.parse_args()

    _validate_arguments(parser, args)

    return args


def _validate_arguments(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    """Validate parsed arguments."""
    if not args.date and not args.month:
        parser.error("Either 'date' argument or '--month' option is required")

    if args.date and args.month:
        parser.error(
            "Cannot use both 'date' and '--month' options. Use one or the other."
        )

    if args.month:
        is_valid, error_msg = validate_date_format(args.month, "YYYYMM")
        if not is_valid:
            parser.error(error_msg)


# ============================================================================
# Logging Setup
# ============================================================================


def setup_scraper_logging(
    config, date_str: str, scraper_type: str
) -> Optional[RotatingFileHandler]:
    """
    Setup logging for scraper with date suffix.

    Args:
        config: AIScore config
        date_str: Date string for log file suffix
        scraper_type: 'links' or 'odds'

    Returns:
        File handler if created, None otherwise
    """
    log_file = Path(config.logging.file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Add date suffix to log filename
    if date_str:
        base_name = log_file.stem
        extension = log_file.suffix
        log_file = log_file.parent / f"{base_name}_{scraper_type}_{date_str}{extension}"

    # Clear existing handlers
    root_logger = logging.getLogger()
    _clear_logging_handlers(root_logger)

    module_logger = logging.getLogger(__name__)
    _clear_logging_handlers(module_logger)

    root_logger.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(config.logging.format))
    root_logger.addHandler(console_handler)

    # File handler
    file_handler = _create_file_handler(log_file, config)
    if file_handler:
        root_logger.addHandler(file_handler)

    # Configure module logger
    module_logger.propagate = True
    module_logger.setLevel(logging.DEBUG)

    # Log initialization
    _log_initialization(root_logger, log_file, file_handler)

    return file_handler


def _clear_logging_handlers(logger: logging.Logger) -> None:
    """Clear all handlers from a logger."""
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()


def _create_file_handler(log_file: Path, config) -> Optional[RotatingFileHandler]:
    """Create a rotating file handler."""
    try:
        file_handler = RotatingFileHandler(
            str(log_file),
            maxBytes=config.logging.max_bytes,
            backupCount=config.logging.backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(config.logging.format))
        file_handler.stream.write("")
        file_handler.stream.flush()
        return file_handler
    except Exception as e:
        print(f"WARNING: Failed to create file handler: {e}", file=sys.stderr)
        print(f"  Log file path: {log_file.absolute()}", file=sys.stderr)
        return None


def _log_initialization(
    root_logger: logging.Logger, log_file: Path, file_handler
) -> None:
    """Log initialization information."""
    root_logger.info("=" * 80)
    root_logger.info("📝 Logging initialized")
    root_logger.info(f"   Log file: {log_file.absolute()}")
    root_logger.info(f"   Console level: INFO and above")
    root_logger.info(f"   File level: INFO and above")
    if file_handler:
        root_logger.info("   File handler: ✓ Active")
        file_handler.flush()
    else:
        root_logger.warning("   File handler: ✗ Failed to initialize")
    root_logger.info("=" * 80)


# ============================================================================
# Links Scraping
# ============================================================================


def run_links_scraping(args: argparse.Namespace, date_str: str) -> int:
    """
    Run link scraping for a specific date.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("=" * 80)
    print(f"STEP 1: LINK SCRAPING - {date_str}")
    print("=" * 80)

    try:
        scrape_links = _lazy_load_links_scraper()
        config = _get_config()

        setup_scraper_logging(config, date_str, "links")

        if args.visible:
            config.browser.headless = False
            print("[INFO] Visible mode enabled")

        return _execute_links_scraping(date_str, scrape_links, config)

    except Exception as e:
        print(f"\n[ERROR] Link scraping failed: {e}")
        logger.error(f"Link scraping error: {e}", exc_info=True)
        return 1


def _execute_links_scraping(date_str: str, scrape_links, config) -> int:
    """Execute the actual links scraping."""
    from src.scrapers.aiscore.browser import BrowserManager

    browser = None
    try:
        browser = BrowserManager(config)
        bronze_storage = _get_bronze_storage()

        url_list = scrape_links.scrape_match_links(date_str, browser, config)

        daily_file = (
            Path(config.storage.bronze_path)
            / "daily_listings"
            / date_str
            / "matches.json"
        )

        if daily_file.exists():
            with open(daily_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                total_matches = data.get("total_matches", 0)

            print(f"\n[OK] Link scraping completed: {total_matches} matches found")
            print(f"     Data saved to: {daily_file}")
            return 0
        elif url_list:
            print(
                f"\n[WARNING] Links returned but JSON file not found: "
                f"{len(url_list)} URLs"
            )
            print(f"     Expected file: {daily_file}")
            return 0
        else:
            print(f"\n[WARNING] No matches found for {date_str}")
            return 0

    finally:
        _close_browser_safely(browser)


# ============================================================================
# Odds Scraping
# ============================================================================


def run_odds_scraping(args: argparse.Namespace, date_str: str) -> int:
    """
    Run odds scraping for a specific date.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("=" * 80)
    print(f"STEP 2: ODDS SCRAPING - {date_str}")
    print("=" * 80)

    try:
        scrape_odds = _lazy_load_odds_scraper()
        config = _get_config()

        setup_scraper_logging(config, date_str, "odds")

        if args.visible:
            config.browser.headless = False
            print("[INFO] Visible mode enabled")

        return _execute_odds_scraping(date_str, config)

    except Exception as e:
        print(f"\n[ERROR] Odds scraping failed: {e}")
        logger.error(f"Odds scraping error: {e}", exc_info=True)
        return 1


def _execute_odds_scraping(date_str: str, config) -> int:
    """Execute the actual odds scraping."""
    from src.scrapers.aiscore.browser import BrowserManager
    from src.scrapers.aiscore.odds_scraper import OddsScraper

    browser = None
    try:
        browser = BrowserManager(config)
        bronze_storage = _get_bronze_storage()

        scraper = OddsScraper(config, browser=browser)

        with PerformanceTimer(f"Odds scraping for {date_str}", logger):
            results = scraper.scrape_from_daily_matches(date_str, bronze_storage)

        successful = results.get("successful", 0)
        failed = results.get("failed", 0)

        print(
            f"\n[OK] Odds scraping completed: {successful} successful, "
            f"{failed} failed"
        )

        if failed > 0:
            print(
                f"[WARNING] {failed} matches failed (this is normal - "
                f"some matches may not have odds available)"
            )

        return 0

    finally:
        _close_browser_safely(browser)


def _close_browser_safely(browser) -> None:
    """Close browser with error handling."""
    if browser:
        try:
            browser.close()
        except Exception as e:
            logger.debug(f"Error closing browser: {e}")


# ============================================================================
# Daily Listings Check
# ============================================================================


def check_daily_listings_exist(date_str: str) -> bool:
    """
    Check if daily listings exist for a date.

    Args:
        date_str: Date in YYYYMMDD format

    Returns:
        True if listings exist, False otherwise
    """
    bronze_storage = _get_bronze_storage()
    return bronze_storage.daily_listing_exists(date_str)


def get_match_scraping_status(date_str: str) -> Tuple[int, int, int, int]:
    """
    Get match scraping status for a date.

    Returns:
        Tuple of (total, to_scrape, completed, forbidden)
    """
    bronze_storage = _get_bronze_storage()
    daily_listing = bronze_storage.load_daily_listing(date_str)

    if not daily_listing:
        return 0, 0, 0, 0

    matches = daily_listing.get("matches", [])
    if not matches:
        return 0, 0, 0, 0

    to_scrape = [
        m
        for m in matches
        if m.get("scrape_status") in [
            "n/a",
            "failed",
            "pending",
            "failed_by_timeout"
        ]
    ]
    completed = [
        m for m in matches
        if m.get("scrape_status") in ["success", "partial"]
    ]
    forbidden = len(matches) - len(to_scrape) - len(completed)

    return len(matches), len(to_scrape), len(completed), forbidden


# ============================================================================
# Single Date Processing
# ============================================================================


def _compress_date_files(date_str: str) -> None:
    """
    Compress JSON files for a date into .json.gz, then bundle into tar archive.
    
    Args:
        date_str: Date in YYYYMMDD format
    """
    try:
        bronze_storage = _get_bronze_storage()
        print(f"\n[INFO] Compressing files for {date_str}...")
        
        compression_stats = bronze_storage.compress_date_files(date_str)
        
        if compression_stats.get('status') == 'already_compressed':
            logger.debug(f"Files already compressed for {date_str}")
        elif compression_stats.get('status') == 'success':
            saved_mb = compression_stats.get('saved_mb', 0)
            saved_pct = compression_stats.get('saved_pct', 0)
            compressed = compression_stats.get('compressed', 0)
            logger.info(
                f"Compressed {compressed} files for {date_str}: "
                f"saved {saved_mb:.2f} MB ({saved_pct:.0f}% reduction)"
            )
        elif compression_stats.get('status') == 'no_files':
            logger.debug(f"No files to compress for {date_str}")
        else:
            logger.warning(f"Compression completed with status: {compression_stats.get('status')}")
            
    except Exception as e:
        logger.error(f"Error during compression for {date_str}: {e}")
        # Don't fail the entire pipeline for compression errors
        print(f"[WARNING] Compression failed but continuing: {e}")


def process_single_date(args: argparse.Namespace, date_str: str) -> Tuple[bool, bool]:
    """
    Process a single date through the bronze layer pipeline.

    Returns:
        Tuple of (links_success, odds_success)
    """
    links_success = False
    odds_success = False

    matches_exist = check_daily_listings_exist(date_str)

    # Step 1: Link Scraping
    if not args.odds_only:
        links_success = _process_links_step(args, date_str, matches_exist)
        if not links_success and not args.force:
            return False, False
    else:
        links_success = _handle_odds_only_mode(date_str, matches_exist)
        if not links_success:
            return False, False

    # Step 2: Odds Scraping
    if not args.links_only and links_success:
        odds_success = _process_odds_step(args, date_str)
    elif args.links_only:
        odds_success = True

    # Step 3: Compress files after successful scraping
    if odds_success and not args.links_only:
        _compress_date_files(date_str)

    return links_success, odds_success


def _process_links_step(
    args: argparse.Namespace, date_str: str, matches_exist: bool
) -> bool:
    """Process the links scraping step."""
    if matches_exist:
        print(f"\n[OK] Matches already exist for {date_str}")
        print("[INFO] Skipping link scraping (day already scraped)")
        return True

    if args.force:
        print(f"\n[INFO] Force mode: Re-scraping links for {date_str}...")
    else:
        print(f"\n[INFO] No matches found for {date_str}")
        print("[INFO] Starting link scraping...")

    exit_code = run_links_scraping(args, date_str)
    success = exit_code == 0

    if not success:
        print(f"\n[ERROR] Link scraping failed for {date_str}")
        print("[ERROR] Cannot proceed to odds scraping without links.")
        _send_links_failure_alert(date_str, exit_code)
    else:
        print(f"\n[OK] Link scraping completed for {date_str}")
        _verify_matches_created(date_str)

    return success


def _handle_odds_only_mode(date_str: str, matches_exist: bool) -> bool:
    """Handle odds-only mode validation."""
    print("\n[INFO] Odds-only mode: Checking for existing matches...")

    if matches_exist:
        print(f"\n[OK] Matches found for {date_str}, proceeding to odds scraping")
        return True

    print(f"\n[ERROR] No matches found for {date_str}")
    print("[ERROR] Link scraping must run first before odds scraping.")
    script_path = (
        Path(__file__).parent.parent
        / "scripts"
        / "aiscore_scripts"
        / "scrape_links.py"
    )
    print(f"[INFO] Run link scraping first: python {script_path} {date_str}")
    return False


def _verify_matches_created(date_str: str) -> None:
    """Verify matches were created after link scraping."""
    print("[INFO] Verifying matches were created...")
    if not check_daily_listings_exist(date_str):
        print("[WARNING] No matches found after link scraping!")
        print("[WARNING] This may be normal if no matches were found for this date")
    else:
        print("[OK] Matches verified and ready for odds scraping")


def _process_odds_step(args: argparse.Namespace, date_str: str) -> bool:
    """Process the odds scraping step."""
    total, to_scrape, completed, forbidden = get_match_scraping_status(date_str)

    if total == 0:
        print(
            f"\n[WARNING] No daily listing found for {date_str}, skipping odds scraping"
        )
        return True

    if to_scrape == 0 and completed > 0:
        print(f"\n[OK] Odds scraping already completed for {date_str}")
        print(f"[INFO] All {total} matches already have odds data")
        print("[INFO] Skipping odds scraping")
        return True

    if to_scrape > 0:
        print(f"\n[INFO] Starting odds scraping for {date_str}...")
        print(f"[INFO] Total matches: {total}")
        print(
            f"[INFO] To scrape: {to_scrape} "
            f"(status: n/a/failed/pending)"
        )
        print(
            f"[INFO] Already done: {completed} "
            f"(status: success/partial)"
        )
        print(f"[INFO] Skipped: {forbidden} (forbidden)")

        exit_code = run_odds_scraping(args, date_str)
        success = exit_code == 0

        if success:
            print(f"\n[OK] Odds scraping completed for {date_str}")
        else:
            print(f"\n[ERROR] Odds scraping failed for {date_str}")
            _send_odds_failure_alert(date_str, exit_code, total, to_scrape)

        return success

    print(f"\n[WARNING] No matches to scrape odds for {date_str}")
    return True


def _send_links_failure_alert(date_str: str, exit_code: int) -> None:
    """Send alert for links scraping failure."""
    alert_manager = get_alert_manager()
    alert_manager.send_alert(
        level=AlertLevel.ERROR,
        title=f"AIScore Link Scraping Failed - {date_str}",
        message=f"Link scraping failed for date {date_str}. Cannot proceed to odds scraping.",
        context={
            "date": date_str,
            "step": "AIScore Link Scraping",
            "exit_code": exit_code,
        },
    )


def _send_odds_failure_alert(
    date_str: str, exit_code: int, total: int, to_scrape: int
) -> None:
    """Send alert for odds scraping failure."""
    alert_manager = get_alert_manager()
    alert_manager.send_alert(
        level=AlertLevel.ERROR,
        title=f"AIScore Odds Scraping Failed - {date_str}",
        message=f"Odds scraping failed for date {date_str}.\n\nTotal matches: {total}\nTo scrape: {to_scrape}",
        context={
            "date": date_str,
            "step": "AIScore Odds Scraping",
            "exit_code": exit_code,
            "total_matches": total,
            "matches_to_scrape": to_scrape,
        },
    )


# ============================================================================
# Date Range Generation
# ============================================================================


def generate_dates(args: argparse.Namespace) -> Tuple[List[str], str]:
    """
    Generate list of dates from arguments.

    Returns:
        Tuple of (dates list, display text)
    """
    if args.month:
        dates = generate_month_dates(args.month)
        display_text = f"Month: {get_month_display_name(args.month)} ({args.month})"
    elif args.end_date:
        dates = generate_date_range(args.date, args.end_date)
        display_text = f"Range: {args.date} to {args.end_date}"
    else:
        dates = [args.date]
        display_text = f"Single: {args.date}"

    return dates, display_text


# ============================================================================
# Output Formatting
# ============================================================================


def get_mode_text(args: argparse.Namespace) -> str:
    """Get mode description text."""
    if args.links_only:
        return "Links only"
    elif args.odds_only:
        return "Odds only"
    return "Links → Odds (full pipeline)"


def print_pipeline_header(
    args: argparse.Namespace, dates: List[str], display_text: str
) -> None:
    """Print pipeline header."""
    mode = get_mode_text(args)

    print_header("AIScore Bronze Layer Pipeline")
    print(f"Mode:            {mode}")
    print(f"Date(s):         {display_text}")
    print(f"Total dates:     {len(dates)}")
    print(f"Browser:         {'Visible' if args.visible else 'Headless'}")
    print(f"Config:          .env file")
    print("=" * 80 + "\n")


def print_final_summary(
    dates: List[str],
    successful_dates: int,
    failed_dates: int,
    links_only_count: int,
    odds_only_count: int,
    args: argparse.Namespace,
) -> None:
    """Print final pipeline summary."""
    print_header("PIPELINE COMPLETE")
    print(f"Total dates processed: {len(dates)}")
    print(f"Successful:           {successful_dates}")
    print(f"Failed:              {failed_dates}")

    if args.links_only:
        print(f"Links scraped:         {links_only_count}")
    elif args.odds_only:
        print(f"Odds scraped:          {odds_only_count}")
    else:
        print(f"Full pipeline:          {successful_dates}")

    print("=" * 80)


def print_next_steps(
    successful_dates: int, dates: List[str], args: argparse.Namespace
) -> None:
    """Print next steps."""
    if successful_dates > 0 and not args.odds_only:
        print("\nNext steps:")
        if args.links_only:
            print(
                f"  Scrape odds:  python scripts/scrape_aiscore.py "
                f"{dates[0]} --odds-only"
            )
        else:
            print(
                f"  View data:     Check "
                f"data/aiscore/daily_listings/{dates[0]}/matches.json"
            )
            print(f"  View odds:     Check data/aiscore/matches/{dates[0]}/")


# ============================================================================
# Main Execution
# ============================================================================


def run_pipeline(args: argparse.Namespace) -> int:
    """
    Run the AIScore pipeline.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    dates, display_text = generate_dates(args)
    print_pipeline_header(args, dates, display_text)

    successful_dates = 0
    failed_dates = 0
    links_only_count = 0
    odds_only_count = 0

    for idx, date_str in enumerate(dates, 1):
        print(f"\n{'=' * 80}")
        print(f"Processing date {idx}/{len(dates)}: {date_str}")
        print(f"{'=' * 80}\n")

        start_time = time.time()
        links_success, odds_success = process_single_date(args, date_str)
        duration = time.time() - start_time

        if args.links_only:
            if links_success:
                successful_dates += 1
                links_only_count += 1
                # Send daily report for links-only mode
                total_matches, _, _, _ = get_match_scraping_status(date_str)
                send_daily_report(
                    scraper='aiscore',
                    date=date_str,
                    matches_scraped=total_matches,
                    duration_seconds=duration,
                    context={'mode': 'links-only'}
                )
            else:
                failed_dates += 1
        elif args.odds_only:
            if odds_success:
                successful_dates += 1
                odds_only_count += 1
                # Send daily report for odds-only mode
                total_matches, _, _, _ = get_match_scraping_status(date_str)
                send_daily_report(
                    scraper='aiscore',
                    date=date_str,
                    matches_scraped=total_matches,
                    odds_scraped=total_matches,
                    duration_seconds=duration,
                    context={'mode': 'odds-only'}
                )
            else:
                failed_dates += 1
        else:
            if links_success and odds_success:
                successful_dates += 1
                # Send daily report for full pipeline
                total_matches, _, _, _ = get_match_scraping_status(date_str)
                send_daily_report(
                    scraper='aiscore',
                    date=date_str,
                    matches_scraped=total_matches,
                    odds_scraped=total_matches,
                    duration_seconds=duration,
                )
            else:
                failed_dates += 1
                if not links_success:
                    print(
                        f"[WARNING] Skipping odds scraping for {date_str} "
                        f"(links failed)"
                    )

    print_final_summary(
        dates,
        successful_dates,
        failed_dates,
        links_only_count,
        odds_only_count,
        args,
    )
    print_next_steps(successful_dates, dates, args)

    return 1 if failed_dates > 0 else 0


def main() -> int:
    """Main execution function."""
    args = parse_arguments()
    return run_pipeline(args)


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user. Exiting...")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
