"""Command-line interface for FotMob scraper.

SCRAPER: FotMob
PURPOSE: Command-line interface for FotMob scraper.
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path

from .config import load_config, FotMobConfig
from .orchestrator import FotMobOrchestrator
from .utils import setup_logging
from .utils.date_utils import DATE_FORMAT_COMPACT


def validate_date(date_str: str) -> bool:
    """Validate date format (YYYYMMDD)."""
    try:
        datetime.strptime(date_str, DATE_FORMAT_COMPACT)
        return True
    except ValueError:
        return False


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='FotMob Scraper - Production-ready football data scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape all matches for a specific date
  python -m src.cli --date 20250101

  # Force re-scrape already processed matches
  python -m src.cli --date 20250101 --force

  # Configuration is loaded from config.yaml (primary source) and .env (overrides)

  # Disable parallel processing
  python -m src.cli --date 20250101 --no-parallel

  # Note: Storage options removed - data goes directly to ClickHouse
        """
    )

    parser.add_argument(
        '-date', '--date',
        type=str,
        required=True,
        help='Date in YYYYMMDD format (e.g., 20250101)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-scrape of already processed matches'
    )

    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing'
    )

    parser.add_argument(
        '--max-workers',
        type=int,
        default=None,
        help='Maximum number of parallel workers (default: 5)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default=None,
        help='Logging level (overrides config)'
    )

    parser.add_argument(
        '--no-metrics',
        action='store_true',
        help='Disable metrics collection'
    )

    parser.add_argument(
        '--no-quality-checks',
        action='store_true',
        help='Disable data quality checks'
    )

    args = parser.parse_args()

    if not validate_date(args.date):
        print(f"Error: Invalid date format '{args.date}'. Use YYYYMMDD format (e.g., 20250101)")
        sys.exit(1)

    try:
        config = load_config()

        if args.no_parallel:
            config.enable_parallel = False
        if args.max_workers:
            config.max_workers = args.max_workers
        if args.log_level:
            config.log_level = args.log_level
        if args.no_metrics:
            config.enable_metrics = False
        if args.no_quality_checks:
            config.enable_data_quality_checks = False

        logger = setup_logging(
            name="fotmob_scraper",
            log_dir=config.log_dir,
            log_level=config.log_level,
            date_suffix=args.date
        )

        logger.info("=" * 80)
        logger.info(f"FotMob Scraper v2.0 - Starting scrape for {args.date}")
        logger.info("=" * 80)
        logger.info(f"Configuration:")
        logger.info(f"  Storage: {config.storage_type}")
        logger.info(f"  Parallel: {config.enable_parallel}")
        if config.enable_parallel:
            logger.info(f"  Max Workers: {config.max_workers}")
        logger.info(f"  Data Quality Checks: {config.enable_data_quality_checks}")
        logger.info(f"  Metrics: {config.enable_metrics}")
        logger.info("=" * 80)

        with FotMobOrchestrator(config) as orchestrator:
            metrics = orchestrator.scrape_date(
                date_str=args.date,
                force_rescrape=args.force
            )

        if metrics.failed_matches > 0:
            logger.warning(f"Completed with {metrics.failed_matches} failures")
            sys.exit(1)
        else:
            logger.info("[SUCCESS] Scraping completed")
            sys.exit(0)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(130)

    except Exception as e:
        print(f"\n[ERROR] Fatal error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
