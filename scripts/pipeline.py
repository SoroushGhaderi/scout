"""
Unified Pipeline Orchestrator
==============================

PURPOSE: Orchestrates the complete data pipeline:
        1. FotMob Bronze Layer (scraping)
        2. AIScore Bronze Layer (scraping)
        3. Load FotMob to ClickHouse
        4. Load AIScore to ClickHouse

This script runs all steps sequentially, handling errors gracefully
and providing a comprehensive summary at the end.

Usage:
    # Single date (full pipeline)
    python scripts/pipeline.py 20251113
    
    # Date range
    python scripts/pipeline.py --start-date 20251101 --end-date 20251107
    
    # Monthly scraping
    python scripts/pipeline.py --month 202511
    
    # Skip specific steps
    python scripts/pipeline.py 20251113 --skip-bronze
    python scripts/pipeline.py 20251113 --skip-clickhouse
    python scripts/pipeline.py 20251113 --bronze-only
"""

import argparse
import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from calendar import monthrange
from typing import List, Dict, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.date_utils import extract_year_month, DATE_FORMAT_COMPACT
from src.utils.alerting import get_alert_manager, AlertLevel
from src.utils.logging_utils import setup_logging


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Unified Pipeline Orchestrator - Runs Bronze scraping and ClickHouse loading for both scrapers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Full Pipeline:
    python scripts/pipeline.py 20251113                    # Single date
    python scripts/pipeline.py --start-date 20251101 --end-date 20251107  # Date range
    python scripts/pipeline.py --month 202511             # Entire month
  
  Partial Pipeline:
    python scripts/pipeline.py 20251113 --bronze-only     # Bronze scraping only
    python scripts/pipeline.py 20251113 --skip-bronze      # ClickHouse loading only
    python scripts/pipeline.py 20251113 --skip-fotmob      # Skip FotMob entirely
    python scripts/pipeline.py 20251113 --skip-aiscore     # Skip AIScore entirely
  
  Options:
    python scripts/pipeline.py 20251113 --force            # Force re-scrape/reload
    python scripts/pipeline.py 20251113 --visible          # Visible browser for AIScore
        """
    )
    
    # Date arguments - mutually exclusive group
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        'date',
        type=str,
        nargs='?',
        help='Date to process (YYYYMMDD format). Required unless --start-date or --month is used.'
    )
    date_group.add_argument(
        '--start-date',
        type=str,
        help='Start date for range processing (YYYYMMDD format)'
    )
    date_group.add_argument(
        '--month',
        type=str,
        help='Process entire month (YYYYMM format, e.g., 202511 for November 2025)'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for range processing (YYYYMMDD format). Required if --start-date is used.'
    )
    
    # Pipeline control
    parser.add_argument(
        '--bronze-only',
        action='store_true',
        help='Run bronze scraping only (skip ClickHouse loading)'
    )
    
    parser.add_argument(
        '--skip-bronze',
        action='store_true',
        help='Skip bronze scraping (run ClickHouse loading only)'
    )
    
    parser.add_argument(
        '--skip-fotmob',
        action='store_true',
        help='Skip FotMob processing (bronze + ClickHouse)'
    )
    
    parser.add_argument(
        '--skip-aiscore',
        action='store_true',
        help='Skip AIScore processing (bronze + ClickHouse)'
    )
    
    parser.add_argument(
        '--skip-clickhouse',
        action='store_true',
        help='Skip ClickHouse loading (run bronze scraping only)'
    )
    
    # Options
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-scrape/reload even if data exists'
    )
    
    parser.add_argument(
        '--visible',
        action='store_true',
        help='Run AIScore browser in visible mode (not headless)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
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
        
        if args.end_date:
            parser.error("Cannot use --end-date with --month option")
    
    # Validate date range
    if args.start_date:
        if not args.end_date:
            parser.error("--end-date is required when using --start-date")
        
        try:
            start = datetime.strptime(args.start_date, DATE_FORMAT_COMPACT)
            end = datetime.strptime(args.end_date, DATE_FORMAT_COMPACT)
            if end < start:
                parser.error(f"End date ({args.end_date}) cannot be before start date ({args.start_date})")
        except ValueError as e:
            parser.error(f"Invalid date format: {e}")
    
    # Validate single date format
    if args.date:
        try:
            datetime.strptime(args.date, DATE_FORMAT_COMPACT)
        except ValueError:
            parser.error(f"Invalid date format: {args.date}. Use YYYYMMDD (e.g., 20251113)")
    
    return args


def generate_dates(args) -> List[str]:
    """Generate list of dates to process based on arguments."""
    if args.month:
        year_str, month_str = extract_year_month(args.month)
        year = int(year_str)
        month = int(month_str)
        _, last_day = monthrange(year, month)
        dates = []
        for day in range(1, last_day + 1):
            dates.append(f"{year}{month:02d}{day:02d}")
        return dates
    elif args.start_date:
        start = datetime.strptime(args.start_date, DATE_FORMAT_COMPACT)
        end = datetime.strptime(args.end_date, DATE_FORMAT_COMPACT)
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime(DATE_FORMAT_COMPACT))
            current += timedelta(days=1)
        return dates
    else:
        return [args.date]


def run_step(
    name: str,
    cmd: List[str],
    project_root: Path,
    continue_on_error: bool = False,
    date_str: Optional[str] = None,
    log_file: Optional[Path] = None
) -> Dict[str, any]:
    """Run a pipeline step and return result."""
    logger = logging.getLogger("pipeline")
    
    logger.info("\n" + "=" * 80)
    logger.info(f"STEP: {name}")
    logger.info("=" * 80)
    logger.info(f"Command: {' '.join(cmd)}")
    logger.info("=" * 80 + "\n")
    
    start_time = time.time()
    
    # Redirect subprocess output to unified log file and console
    if log_file:
        # Use Popen with pipes to capture output and tee to both file and console
        log_f = open(log_file, 'a', encoding='utf-8')
        try:
            process = subprocess.Popen(
                cmd,
                cwd=project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr into stdout
                text=True,
                bufsize=1,  # Line buffered
                universal_newlines=True
            )
            
            # Read output line by line and write to both file and console
            for line in process.stdout:
                if line:
                    log_f.write(line)
                    log_f.flush()
                    sys.stdout.write(line)
                    sys.stdout.flush()
            
            process.wait()
            result = type('Result', (), {'returncode': process.returncode})()
        finally:
            log_f.close()
    else:
        # Fallback: just use console if no log file specified
        result = subprocess.run(cmd, cwd=project_root, text=True)
    
    elapsed_time = time.time() - start_time
    
    success = result.returncode == 0
    
    if not success:
        if continue_on_error:
            logger.warning(f"\n[WARNING] {name} failed (exit code: {result.returncode}) but continuing...")
        else:
            logger.error(f"\n[ERROR] {name} failed (exit code: {result.returncode})")
        
        # Send email alert for step failure
        alert_manager = get_alert_manager()
        # Note: stderr/stdout won't be available since we're not capturing output
        error_output = "Check logs for details"
        alert_manager.send_alert(
            level=AlertLevel.ERROR,
            title=f"Pipeline Step Failed: {name}",
            message=f"Step '{name}' failed with exit code {result.returncode}.\n\nElapsed time: {elapsed_time:.1f}s",
            context={
                "step_name": name,
                "exit_code": result.returncode,
                "elapsed_time": elapsed_time,
                "date": date_str,
                "error_output": error_output
            }
        )
    else:
        logger.info(f"\n[SUCCESS] {name} completed in {elapsed_time:.1f}s")
    
    return {
        'name': name,
        'success': success,
        'exit_code': result.returncode,
        'elapsed_time': elapsed_time
    }


def run_fotmob_bronze(date_str: str, args, project_root: Path, log_file: Optional[Path] = None) -> Dict[str, any]:
    """Run FotMob bronze scraping for a date."""
    script_path = project_root / 'scripts' / 'scrape_fotmob.py'
    cmd = [sys.executable, str(script_path), date_str]
    
    if args.force:
        cmd.append('--force')
    if args.debug:
        cmd.append('--debug')
    
    return run_step(f"FotMob Bronze - {date_str}", cmd, project_root, continue_on_error=True, date_str=date_str, log_file=log_file)


def run_aiscore_bronze(date_str: str, args, project_root: Path, log_file: Optional[Path] = None) -> Dict[str, any]:
    """Run AIScore bronze scraping for a date."""
    script_path = project_root / 'scripts' / 'scrape_aiscore.py'
    cmd = [sys.executable, str(script_path), date_str]
    
    if args.visible:
        cmd.append('--visible')
    if args.force:
        cmd.append('--force')
    
    return run_step(f"AIScore Bronze - {date_str}", cmd, project_root, continue_on_error=True, date_str=date_str, log_file=log_file)


def run_clickhouse_load(scraper: str, date_str: str, args, project_root: Path, log_file: Optional[Path] = None) -> Dict[str, any]:
    """Load data to ClickHouse for a scraper and date."""
    script_path = project_root / 'scripts' / 'load_clickhouse.py'
    cmd = [sys.executable, str(script_path), '--scraper', scraper, '--date', date_str]
    
    if args.force:
        cmd.append('--force')
    
    return run_step(f"ClickHouse Load - {scraper} - {date_str}", cmd, project_root, continue_on_error=True, date_str=date_str, log_file=log_file)


def run_clickhouse_load_month(scraper: str, month_str: str, args, project_root: Path, log_file: Optional[Path] = None) -> Dict[str, any]:
    """Load data to ClickHouse for a scraper and month."""
    script_path = project_root / 'scripts' / 'load_clickhouse.py'
    cmd = [sys.executable, str(script_path), '--scraper', scraper, '--month', month_str]
    
    if args.force:
        cmd.append('--force')
    
    return run_step(f"ClickHouse Load - {scraper} - Month {month_str}", cmd, project_root, continue_on_error=True, date_str=month_str, log_file=log_file)


def main():
    """Main execution function"""
    args = parse_arguments()
    project_root = Path(__file__).parent.parent
    
    # Generate dates to process
    dates = generate_dates(args)
    
    # Determine mode and log file date suffix
    if args.month:
        year, month = extract_year_month(args.month)
        month_name = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][int(month) - 1]
        date_display = f"Month: {month_name} {year} ({args.month})"
        log_date_suffix = args.month
    elif len(dates) > 1:
        date_display = f"Range: {dates[0]} to {dates[-1]}"
        log_date_suffix = f"{dates[0]}_to_{dates[-1]}"
    else:
        date_display = f"Date: {dates[0]}"
        log_date_suffix = dates[0]
    
    # Setup unified logging for pipeline
    log_file = project_root / "logs" / f"pipeline_{log_date_suffix}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    logger = setup_logging(
        name="pipeline",
        log_dir="logs",
        log_level="DEBUG" if args.debug else "INFO",
        date_suffix=None  # We'll set the filename manually
    )
    
    # Replace file handler with our custom filename
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            handler.close()
    
    # Add file handler with pipeline_{date}.log filename
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.info("\n" + "=" * 80)
    logger.info("UNIFIED PIPELINE ORCHESTRATOR")
    logger.info("=" * 80)
    logger.info(f"Mode:             {date_display}")
    logger.info(f"Total dates:      {len(dates)}")
    logger.info(f"Skip FotMob:      {args.skip_fotmob}")
    logger.info(f"Skip AIScore:     {args.skip_aiscore}")
    logger.info(f"Skip Bronze:      {args.skip_bronze}")
    logger.info(f"Skip ClickHouse:  {args.skip_clickhouse or args.bronze_only}")
    logger.info(f"Force mode:       {args.force}")
    logger.info(f"Log file:         {log_file}")
    logger.info("=" * 80)
    
    # Track results
    results = {
        'fotmob_bronze': [],
        'aiscore_bronze': [],
        'fotmob_clickhouse': [],
        'aiscore_clickhouse': []
    }
    
    pipeline_start = time.time()
    
    # Process each date (Bronze scraping)
    for idx, date_str in enumerate(dates, 1):
        logger.info(f"\n\n{'#' * 80}")
        logger.info(f"# Processing date {idx}/{len(dates)}: {date_str}")
        logger.info(f"{'#' * 80}\n")
        
        # Step 1: FotMob Bronze
        if not args.skip_fotmob and not args.skip_bronze:
            result = run_fotmob_bronze(date_str, args, project_root, log_file=log_file)
            results['fotmob_bronze'].append(result)
        
        # Step 2: AIScore Bronze
        if not args.skip_aiscore and not args.skip_bronze:
            result = run_aiscore_bronze(date_str, args, project_root, log_file=log_file)
            results['aiscore_bronze'].append(result)
        
        # Step 3 & 4: Load to ClickHouse (per-date mode only)
        if not args.month and not args.skip_clickhouse and not args.bronze_only:
            # Per-date loading (not monthly mode)
            if not args.skip_fotmob:
                result = run_clickhouse_load('fotmob', date_str, args, project_root, log_file=log_file)
                results['fotmob_clickhouse'].append(result)
            if not args.skip_aiscore:
                result = run_clickhouse_load('aiscore', date_str, args, project_root, log_file=log_file)
                results['aiscore_clickhouse'].append(result)
    
    # Step 3 & 4: Load to ClickHouse (monthly mode - after all bronze scraping)
    if args.month and not args.skip_clickhouse and not args.bronze_only:
        logger.info(f"\n\n{'#' * 80}")
        logger.info(f"# Loading to ClickHouse (Monthly Mode)")
        logger.info(f"{'#' * 80}\n")
        
        if not args.skip_fotmob:
            result = run_clickhouse_load_month('fotmob', args.month, args, project_root, log_file=log_file)
            results['fotmob_clickhouse'].append(result)
        if not args.skip_aiscore:
            result = run_clickhouse_load_month('aiscore', args.month, args, project_root, log_file=log_file)
            results['aiscore_clickhouse'].append(result)
    
    # Print summary
    pipeline_elapsed = time.time() - pipeline_start
    
    logger.info("\n\n" + "=" * 80)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total time: {pipeline_elapsed:.1f}s ({pipeline_elapsed/60:.1f} minutes)")
    logger.info(f"Dates processed: {len(dates)}")
    logger.info(f"Log file: {log_file}")
    logger.info("\nResults by step:")
    
    for step_name, step_results in results.items():
        if step_results:
            successful = sum(1 for r in step_results if r['success'])
            failed = len(step_results) - successful
            total_time = sum(r['elapsed_time'] for r in step_results)
            logger.info(f"\n  {step_name.replace('_', ' ').title()}:")
            logger.info(f"    Successful: {successful}/{len(step_results)}")
            logger.info(f"    Failed: {failed}")
            logger.info(f"    Total time: {total_time:.1f}s")
            if failed > 0:
                logger.info(f"    Failed dates: {[r['name'].split(' - ')[-1] for r in step_results if not r['success']]}")
    
    logger.info("\n" + "=" * 80)
    
    # Determine exit code
    all_successful = all(
        all(r['success'] for r in step_results)
        for step_results in results.values()
        if step_results
    )
    
    if all_successful:
        logger.info("✓ Pipeline completed successfully!")
        return 0
    else:
        logger.warning("⚠ Pipeline completed with some failures (see details above)")
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger = logging.getLogger("pipeline")
        logger.warning("\n\nPipeline interrupted by user. Exiting...")
        sys.exit(130)
    except Exception as e:
        logger = logging.getLogger("pipeline")
        logger.error(f"\nFatal error: {e}", exc_info=True)
        sys.exit(1)

