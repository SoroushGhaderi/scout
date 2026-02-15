"""Bronze Layer Processing - Raw Data Scraping.

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

# Load environment variables from .env file FIRST
import os
from dotenv import load_dotenv
load_dotenv()

from src.utils.alerting import get_alert_manager, AlertLevel
from src.utils.logging_utils import setup_logging
from src.utils.metrics_alerts import send_daily_report
from src.orchestrator import FotMobOrchestrator
from config import FotMobConfig
from utils import (
    add_project_to_path,
    validate_date_format,
    create_date_range_info,
    DateRangeInfo,
    PipelineStats,
    print_header,
    print_separator,
)
import argparse
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

add_project_to_path()


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class ScrapingStats:
    """Statistics for scraping operations."""

    dates_processed: int = 0
    dates_failed: int = 0
    total_matches: int = 0
    total_successful: int = 0
    total_failed: int = 0
    total_skipped: int = 0

    def update_from_metrics(self, metrics) -> None:
        """Update stats from orchestrator metrics."""
        self.dates_processed += 1
        self.total_matches += metrics.total_matches
        self.total_successful += metrics.successful_matches
        self.total_failed += metrics.failed_matches
        self.total_skipped += metrics.skipped_matches

    def record_failure(self) -> None:
        """Record a date processing failure."""
        self.dates_failed += 1


# ============================================================================
# Argument Parsing
# ============================================================================


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="FotMob Bronze Layer Processing - Scrape raw match data",
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
        """,
    )

    _add_date_arguments(parser)
    _add_option_arguments(parser)

    return parser


def _add_date_arguments(parser: argparse.ArgumentParser) -> None:
    """Add date-related arguments to parser."""
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "start_date",
        type=str,
        nargs="?",
        help="Date (or start date) in YYYYMMDD format. Required unless --month is used.",
    )
    date_group.add_argument(
        "--month",
        type=str,
        help="Scrape entire month (YYYYMM format, e.g., 202511 for November 2025)",
    )

    range_group = parser.add_mutually_exclusive_group()
    range_group.add_argument(
        "end_date",
        type=str,
        nargs="?",
        help="End date in YYYYMMDD format (for range scraping, requires start_date)",
    )
    range_group.add_argument(
        "--days",
        type=int,
        help="Number of days to scrape from start date (requires start_date)",
    )


def _add_option_arguments(parser: argparse.ArgumentParser) -> None:
    """Add option arguments to parser."""
    parser.add_argument(
        "--force", action="store_true", help="Force re-scrape already processed matches"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Compress files after scraping (creates .tar archives)",
    )


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    parser = create_argument_parser()
    args = parser.parse_args()

    _validate_month_argument(parser, args)
    _validate_start_date_argument(parser, args)

    return args


def _validate_month_argument(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    """Validate month argument if provided."""
    if not args.month:
        return

    is_valid, error_msg = validate_date_format(args.month, "YYYYMM")
    if not is_valid:
        parser.error(error_msg)

    if args.end_date or args.days:
        parser.error("Cannot use --end_date or --days with --month option")


def _validate_start_date_argument(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
    """Validate start date and related arguments."""
    if not args.start_date:
        return

    is_valid, error_msg = validate_date_format(args.start_date, "YYYYMMDD")
    if not is_valid:
        parser.error(error_msg)

    if args.end_date:
        is_valid, error_msg = validate_date_format(args.end_date, "YYYYMMDD")
        if not is_valid:
            parser.error(error_msg)

        if args.end_date < args.start_date:
            parser.error(
                f"End date ({args.end_date}) cannot be before start date ({args.start_date})"
            )

    if args.days and args.days < 1:
        parser.error(f"Number of days must be at least 1 (got: {args.days})")


# ============================================================================
# Date Range Generation
# ============================================================================


def create_date_info(args: argparse.Namespace) -> DateRangeInfo:
    """Create DateRangeInfo from parsed arguments."""
    return create_date_range_info(
        date=args.start_date if not args.end_date and not args.days else None,
        start_date=args.start_date if args.end_date or args.days else None,
        end_date=args.end_date,
        month=args.month,
        num_days=args.days,
    )


# ============================================================================
# Configuration
# ============================================================================


def create_config(args: argparse.Namespace) -> FotMobConfig:
    """Create FotMob configuration from arguments."""
    config = FotMobConfig()

    if args.debug:
        config.logging.level = "DEBUG"

    # FotMob runs single-threaded (sequential) by default
    config.scraping.enable_parallel = False
    config.scraping.max_workers = 1

    return config


# ============================================================================
# Output Formatting
# ============================================================================


def print_scraping_header(date_info: DateRangeInfo, args: argparse.Namespace) -> None:
    """Print scraping header."""
    print_header("Bronze Layer Processing")
    print(f"Mode:             {date_info.mode_text}")
    print(f"Date(s):          {date_info.display_text}")
    print(f"Total dates:      {len(date_info.dates)}")
    print(f"Mode:             Single-threaded (sequential)")
    print(f"Force re-scrape:  {args.force}")
    print(f"Auto-compress:    {args.compress}")
    print("=" * 80 + "\n")


def print_date_metrics(metrics) -> None:
    """Print metrics for a single date."""
    print(f"  Matches:   {metrics.total_matches}")
    print(f"  Success:   {metrics.successful_matches}")
    print(f"  Failed:    {metrics.failed_matches}")
    print(f"  Skipped:   {metrics.skipped_matches}")


def print_final_summary(stats: ScrapingStats, total_dates: int) -> None:
    """Print final scraping summary."""
    print_header("BRONZE LAYER COMPLETE")
    print(f"Dates processed:  {stats.dates_processed}/{total_dates}")
    print(f"Dates failed:     {stats.dates_failed}")
    print_separator()
    print(f"Total matches:    {stats.total_matches}")
    print(f"Successful:       {stats.total_successful}")
    print(f"Failed:           {stats.total_failed}")
    print(f"Skipped:          {stats.total_skipped}")
    print("=" * 80)


def print_next_steps(stats: ScrapingStats) -> None:
    """Print next steps if applicable."""
    if stats.total_successful > 0:
        print("\nNext steps:")
        print(
            "  Load to ClickHouse:  "
            "python scripts/load_clickhouse.py --scraper fotmob --date <date>"
        )
        print("  View profiling:  python manage.py bronze view-profiling")


# ============================================================================
# Scraping Execution
# ============================================================================


def process_single_date(
    date_str: str,
    orchestrator: FotMobOrchestrator,
    args: argparse.Namespace,
    logger,
    idx: int,
    total: int,
) -> Optional[object]:
    """
    Process a single date.

    Returns:
        Metrics object if successful, None if failed
    """
    logger.info(f"[{idx}/{total}] Processing date: {date_str}")
    print(f"\n[{idx}/{total}] Scraping {date_str}...")

    try:
        metrics = orchestrator.scrape_date(date_str=date_str, force_rescrape=args.force)
        return metrics
    except Exception as e:
        logger.error(f"Failed to process date {date_str}: {e}")
        print(f"  ERROR: {e}")
        _send_failure_alert(date_str, e)
        return None


def _send_failure_alert(date_str: str, error: Exception) -> None:
    """Send alert for date processing failure."""
    alert_manager = get_alert_manager()
    alert_manager.send_alert(
        level=AlertLevel.ERROR,
        title=f"FotMob Bronze Scraping Failed - {date_str}",
        message=f"Failed to scrape FotMob data for date {date_str}.\n\nError: {str(error)}",
        context={
            "date": date_str,
            "step": "FotMob Bronze Scraping",
            "error": str(error),
        },
    )


def run_scraping(args: argparse.Namespace) -> int:
    """
    Run the scraping process.

    Args:
        args: Parsed command-line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    config = create_config(args)
    date_info = create_date_info(args)

    # Setup logging
    logger = setup_logging(
        name="bronze_processing",
        log_dir=config.log_dir,
        log_level=config.log_level,
        date_suffix=date_info.log_suffix,
    )

    # Print header
    print_scraping_header(date_info, args)

    # Initialize
    stats = ScrapingStats()
    orchestrator = FotMobOrchestrator(config)

    # Process each date
    for idx, date_str in enumerate(date_info.dates, 1):
        start_time = time.time()
        metrics = process_single_date(
            date_str, orchestrator, args, logger, idx, len(date_info.dates)
        )
        duration = time.time() - start_time

        if metrics:
            stats.update_from_metrics(metrics)
            print_date_metrics(metrics)
            
            # Send daily Telegram report
            send_daily_report(
                scraper='fotmob',
                date=date_str,
                matches_scraped=metrics.successful_matches,
                errors=metrics.failed_matches,
                skipped=metrics.skipped_matches,
                duration_seconds=duration,
            )
        else:
            stats.record_failure()

    # Print summary
    print_final_summary(stats, len(date_info.dates))
    print_next_steps(stats)

    return 0 if stats.dates_failed == 0 else 1


# ============================================================================
# Main Entry Point
# ============================================================================


def main() -> int:
    """Main execution function."""
    args = parse_arguments()
    return run_scraping(args)


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user. Exiting...")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)
