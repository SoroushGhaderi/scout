"""Unified Pipeline Orchestrator.

PURPOSE: Orchestrates the complete data pipeline:
    1. FotMob Bronze Layer (scraping)
    2. AIScore Bronze Layer (scraping)
    3. Load FotMob to ClickHouse
    4. Load AIScore to ClickHouse

This script runs all steps sequentially, handling errors gracefully
and providing a comprehensive summary at the end.

Usage:
    python scripts/pipeline.py 20251113
    python scripts/pipeline.py --start-date 20251101 --end-date 20251107
    python scripts/pipeline.py --month 202511
    python scripts/pipeline.py 20251113 --skip-bronze
    python scripts/pipeline.py 20251113 --skip-clickhouse
    python scripts/pipeline.py 20251113 --bronze-only
"""
import argparse
import subprocess
import sys
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Any

sys.path.insert(0, str(Path(__file__).parent))
from utils.script_utils import (
    get_project_root,
    add_project_to_path,
    StepResult,
    DateRangeInfo,
    create_date_range_info,
    validate_date_format,
    MONTH_NAMES,
    format_elapsed_time,
    log_header,
)
add_project_to_path()
from src.utils.alerting import get_alert_manager, AlertLevel
from src.utils.logging_utils import setup_logging
SCRIPT_NAMES = {
    "fotmob_bronze": "scrape_fotmob.py",
    "aiscore_bronze": "scrape_aiscore.py",
    "clickhouse_load": "load_clickhouse.py",
}

RESULT_CATEGORIES = {
    "fotmob_bronze": "FotMob Bronze",
    "aiscore_bronze": "AIScore Bronze",
    "fotmob_clickhouse": "FotMob ClickHouse",
    "aiscore_clickhouse": "AIScore ClickHouse",
}
@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""
    skip_fotmob: bool = False
    skip_aiscore: bool = False
    skip_bronze: bool = False
    skip_clickhouse: bool = False
    bronze_only: bool = False
    force: bool = False
    visible: bool = False
    debug: bool = False


@dataclass
class PipelineResults:
    """Results tracking for pipeline execution."""
    fotmob_bronze: List[StepResult] = field(default_factory=list)
    aiscore_bronze: List[StepResult] = field(default_factory=list)
    fotmob_clickhouse: List[StepResult] = field(default_factory=list)
    aiscore_clickhouse: List[StepResult] = field(default_factory=list)

    def add_result(self, category: str, result: StepResult) -> None:
        """Add a result to the appropriate category."""
        getattr(self, category).append(result)

    def all_successful(self) -> bool:
        """Check if all steps were successful."""
        categories = [
            "fotmob_bronze",
            "aiscore_bronze",
            "fotmob_clickhouse",
            "aiscore_clickhouse"
        ]
        for category in categories:
            results = getattr(self, category)
            if results and not all(r.success for r in results):
                return False
        return True

    def get_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get summary statistics for all categories."""
        summary = {}
        categories = [
            "fotmob_bronze",
            "aiscore_bronze",
            "fotmob_clickhouse",
            "aiscore_clickhouse"
        ]
        for category in categories:
            results = getattr(self, category)
            if results:
                successful = sum(1 for r in results if r.success)
                failed = len(results) - successful
                total_time = sum(r.elapsed_time for r in results)
                failed_dates = [
                    r.name.split(" - ")[-1]
                    for r in results
                    if not r.success
                ]
                summary[category] = {
                    "total": len(results),
                    "successful": successful,
                    "failed": failed,
                    "total_time": total_time,
                    "failed_dates": failed_dates,
                }
        return summary

def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description=(
            'Unified Pipeline Orchestrator - '
            'Runs Bronze scraping and ClickHouse loading for both scrapers'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Full Pipeline:
    python scripts/pipeline.py 20251113
    python scripts/pipeline.py --start-date 20251101 --end-date 20251107
    python scripts/pipeline.py --month 202511
  Partial Pipeline:
    python scripts/pipeline.py 20251113 --bronze-only
    python scripts/pipeline.py 20251113 --skip-bronze
    python scripts/pipeline.py 20251113 --skip-fotmob
    python scripts/pipeline.py 20251113 --skip-aiscore
  Options:
    python scripts/pipeline.py 20251113 --force
    python scripts/pipeline.py 20251113 --visible
        """
    )
    _add_date_arguments(parser)
    _add_pipeline_control_arguments(parser)
    _add_option_arguments(parser)
    return parser


def _add_date_arguments(parser: argparse.ArgumentParser) -> None:
    """Add date-related arguments to parser."""
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        'date',
        type=str,
        nargs='?',
        help=(
            'Date to process (YYYYMMDD format). '
            'Required unless --start-date or --month is used.'
        )
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
        help=(
            'End date for range processing (YYYYMMDD format). '
            'Required if --start-date is used.'
        )
    )


def _add_pipeline_control_arguments(parser: argparse.ArgumentParser) -> None:
    """Add pipeline control arguments to parser."""
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
        help='Skip AIscore processing (bronze + ClickHouse)'
    )
    parser.add_argument(
        '--skip-clickhouse',
        action='store_true',
        help='Skip ClickHouse loading (run bronze scraping only)'
    )


def _add_option_arguments(parser: argparse.ArgumentParser) -> None:
    """Add option arguments to parser."""
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-scrape/reload even if data exists'
    )
    parser.add_argument(
        '--visible',
        action='store_true',
        help='Run AIscore browser visible mode (not headless)'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )


def parse_arguments() -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    parser = create_argument_parser()
    args = parser.parse_args()
    _validate_month_argument(parser, args)
    _validate_date_range_arguments(parser, args)
    _validate_single_date_argument(parser, args)
    return args


def _validate_month_argument(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace
) -> None:
    """Validate month argument if provided."""
    if not args.month:
        return
    is_valid, error_msg = validate_date_format(args.month, "YYYYMM")
    if not is_valid:
        parser.error(error_msg)
    if args.end_date:
        parser.error("Cannot use --end-date with --month option")


def _validate_date_range_arguments(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace
) -> None:
    """Validate date range arguments."""
    if not args.start_date:
        return
    if not args.end_date:
        parser.error("--end-date is required when using --start-date")
    is_valid, error_msg = validate_date_format(args.start_date, "YYYYMMDD")
    if not is_valid:
        parser.error(error_msg)
    is_valid, error_msg = validate_date_format(args.end_date, "YYYYMMDD")
    if not is_valid:
        parser.error(error_msg)
    if args.end_date < args.start_date:
        parser.error(
            f"End date ({args.end_date}) cannot be before start date "
            f"({args.start_date})"
        )


def _validate_single_date_argument(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace
) -> None:
    """Validate single date argument."""
    if not args.date:
        return
    is_valid, error_msg = validate_date_format(args.date, "YYYYMMDD")
    if not is_valid:
        parser.error(error_msg)


def create_pipeline_config(args: argparse.Namespace) -> PipelineConfig:
    """Create PipelineConfig from parsed arguments."""
    return PipelineConfig(
        skip_fotmob=args.skip_fotmob,
        skip_aiscore=args.skip_aiscore,
        skip_bronze=args.skip_bronze,
        skip_clickhouse=args.skip_clickhouse,
        bronze_only=args.bronze_only,
        force=args.force,
        visible=args.visible,
        debug=args.debug,
    )


def create_date_info(args: argparse.Namespace) -> DateRangeInfo:
    """Create DateRangeInfo from parsed arguments."""
    return create_date_range_info(
        date=args.date,
        start_date=args.start_date,
        end_date=args.end_date,
        month=args.month,
    )


def run_step(
    name: str,
    cmd: List[str],
    project_root: Path,
    continue_on_error: bool = False,
    date_str: Optional[str] = None,
    log_file: Optional[Path] = None
) -> StepResult:
    """
    Run a pipeline step and return result.
    Args:
        name: Step name for logging
        cmd: Command to execute
        project_root: Project root directory
        continue_on_error: Whether to continue on failure
        date_str: Date being processed (for alerts)
        log_file: Optional log file path
    Returns:
        StepResult with execution details
    """
    logger = logging.getLogger("pipeline")
    _log_step_header(logger, name, cmd)
    start_time = time.time()
    result = _execute_command(cmd, project_root, log_file)
    elapsed_time = time.time() - start_time
    success = result.returncode == 0
    step_result = StepResult(
        name=name,
        success=success,
        exit_code=result.returncode,
        elapsed_time=elapsed_time,
        date_str=date_str,
    )
    _handle_step_result(logger, step_result, continue_on_error)
    return step_result


def _log_step_header(logger: logging.Logger, name: str, cmd: List[str]) -> None:
    """Log step header information."""
    logger.info("\n" + "=" * 80)
    logger.info(f"STEP: {name}")
    logger.info("=" * 80)
    logger.info(f"Command: {' '.join(cmd)}")
    logger.info("=" * 80 + "\n")


def _execute_command(
    cmd: List[str],
    project_root: Path,
    log_file: Optional[Path] = None
) -> Any:
    """Execute command with output handling."""
    if log_file:
        return _execute_with_logging(cmd, project_root, log_file)
    return subprocess.run(cmd, cwd=project_root, text=True)


def _execute_with_logging(
    cmd: List[str],
    project_root: Path,
    log_file: Path
) -> Any:
    """Execute command with output teed to log file."""
    with open(log_file, 'a', encoding='utf-8') as log_f:
        process = subprocess.Popen(
            cmd,
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        for line in process.stdout:
            if line:
                log_f.write(line)
                log_f.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
        process.wait()
    return type('Result', (), {'returncode': process.returncode})()


def _handle_step_result(
    logger: logging.Logger,
    result: StepResult,
    continue_on_error: bool
) -> None:
    """Handle step result logging and alerting."""
    if result.success:
        logger.info(
            f"\n[SUCCESS] {result.name} completed in "
            f"{result.elapsed_time:.1f}s"
        )
        return
    if continue_on_error:
        logger.warning(
            f"\n[WARNING] {result.name} failed (exit code: "
            f"{result.exit_code}) but continuing..."
        )
    else:
        logger.error(
            f"\n[ERROR] {result.name} failed (exit code: "
            f"{result.exit_code})"
        )
    _send_step_failure_alert(result)


def _send_step_failure_alert(result: StepResult) -> None:
    """Send alert for step failure."""
    alert_manager = get_alert_manager()
    alert_manager.send_alert(
        level=AlertLevel.ERROR,
        title=f"Pipeline Step Failed: {result.name}",
        message=(
            f"Step '{result.name}' failed with exit code "
            f"{result.exit_code}.\n\nElapsed time: "
            f"{result.elapsed_time:.1f}s"
        ),
        context={
            "step_name": result.name,
            "exit_code": result.exit_code,
            "elapsed_time": result.elapsed_time,
            "date": result.date_str,
            "error_output": "Check logs for details"
        }
    )


def run_fotmob_bronze(
    date_str: str,
    config: PipelineConfig,
    project_root: Path,
    log_file: Optional[Path] = None
) -> StepResult:
    """Run FotMob bronze scraping for a date."""
    script_path = project_root / 'scripts' / SCRIPT_NAMES["fotmob_bronze"]
    cmd = [sys.executable, str(script_path), date_str]
    if config.force:
        cmd.append('--force')
    if config.debug:
        cmd.append('--debug')
    return run_step(
        f"FotMob Bronze - {date_str}",
        cmd,
        project_root,
        continue_on_error=True,
        date_str=date_str,
        log_file=log_file
    )


def run_aiscore_bronze(
    date_str: str,
    config: PipelineConfig,
    project_root: Path,
    log_file: Optional[Path] = None
) -> StepResult:
    """Run AIscore bronze scraping for a date."""
    script_path = project_root / 'scripts' / SCRIPT_NAMES["aiscore_bronze"]
    cmd = [sys.executable, str(script_path), date_str]
    if config.visible:
        cmd.append('--visible')
    if config.force:
        cmd.append('--force')
    return run_step(
        f"AIscore Bronze - {date_str}",
        cmd,
        project_root,
        continue_on_error=True,
        date_str=date_str,
        log_file=log_file
    )


def run_clickhouse_load(
    scraper: str,
    date_str: str,
    config: PipelineConfig,
    project_root: Path,
    log_file: Optional[Path] = None
) -> StepResult:
    """Load data to ClickHouse for a scraper and date."""
    script_path = project_root / 'scripts' / SCRIPT_NAMES["clickhouse_load"]
    cmd = [
        sys.executable,
        str(script_path),
        '--scraper',
        scraper,
        '--date',
        date_str
    ]
    if config.force:
        cmd.append('--force')
    return run_step(
        f"ClickHouse Load - {scraper} - {date_str}",
        cmd,
        project_root,
        continue_on_error=True,
        date_str=date_str,
        log_file=log_file
    )


def run_clickhouse_load_month(
    scraper: str,
    month_str: str,
    config: PipelineConfig,
    project_root: Path,
    log_file: Optional[Path] = None
) -> StepResult:
    """Load data to ClickHouse for a scraper and month."""
    script_path = project_root / 'scripts' / SCRIPT_NAMES["clickhouse_load"]
    cmd = [
        sys.executable,
        str(script_path),
        '--scraper',
        scraper,
        '--month',
        month_str
    ]
    if config.force:
        cmd.append('--force')
    return run_step(
        f"ClickHouse Load - {scraper} - Month {month_str}",
        cmd,
        project_root,
        continue_on_error=True,
        date_str=month_str,
        log_file=log_file
    )


def setup_pipeline_logging(
    project_root: Path,
    date_info: DateRangeInfo,
    debug: bool = False
) -> tuple[logging.Logger, Path]:
    """Setup logging for pipeline execution."""
    log_file = project_root / "logs" / f"pipeline_{date_info.log_suffix}.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logging(
        name="pipeline",
        log_dir="logs",
        log_level="DEBUG" if debug else "INFO",
        date_suffix=None
    )
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
            handler.close()
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - '
        '%(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger, log_file


def log_pipeline_header(
    logger: logging.Logger,
    date_info: DateRangeInfo,
    config: PipelineConfig,
    log_file: Path
) -> None:
    """Log pipeline header information."""
    logger.info("\n" + "=" * 80)
    logger.info("UNIFIED PIPELINE ORCHESTRATOR")
    logger.info("=" * 80)
    logger.info(f"Mode:             {date_info.display_text}")
    logger.info(f"Total dates:      {len(date_info.dates)}")
    logger.info(f"Skip FotMob:      {config.skip_fotmob}")
    logger.info(f"Skip AIscore:     {config.skip_aiscore}")
    logger.info(f"Skip Bronze:      {config.skip_bronze}")
    logger.info(
        f"Skip ClickHouse:  "
        f"{config.skip_clickhouse or config.bronze_only}"
    )
    logger.info(f"Force mode:       {config.force}")
    logger.info(f"Log file:         {log_file}")
    logger.info("=" * 80)


def process_bronze_scraping(
    date_str: str,
    config: PipelineConfig,
    results: PipelineResults,
    project_root: Path,
    log_file: Path
) -> None:
    """Process bronze scraping for a single date."""
    if not config.skip_fotmob and not config.skip_bronze:
        result = run_fotmob_bronze(
            date_str, config, project_root, log_file=log_file
        )
        results.add_result("fotmob_bronze", result)
    if not config.skip_aiscore and not config.skip_bronze:
        result = run_aiscore_bronze(
            date_str, config, project_root, log_file=log_file
        )
        results.add_result("aiscore_bronze", result)


def process_clickhouse_loading_per_date(
    date_str: str,
    config: PipelineConfig,
    results: PipelineResults,
    project_root: Path,
    log_file: Path
) -> None:
    """Process ClickHouse loading for a single date."""
    if not config.skip_fotmob:
        result = run_clickhouse_load(
            'fotmob', date_str, config, project_root, log_file=log_file
        )
        results.add_result("fotmob_clickhouse", result)
    if not config.skip_aiscore:
        result = run_clickhouse_load(
            'aiscore', date_str, config, project_root, log_file=log_file
        )
        results.add_result("aiscore_clickhouse", result)


def process_clickhouse_loading_monthly(
    month_str: str,
    config: PipelineConfig,
    results: PipelineResults,
    project_root: Path,
    log_file: Path,
    logger: logging.Logger
) -> None:
    """Process ClickHouse loading for monthly mode."""
    logger.info(f"\n\n{'#' * 80}")
    logger.info("# Loading to ClickHouse (Monthly Mode)")
    logger.info(f"{'#' * 80}\n")
    if not config.skip_fotmob:
        result = run_clickhouse_load_month(
            'fotmob', month_str, config, project_root, log_file=log_file
        )
        results.add_result("fotmob_clickhouse", result)
    if not config.skip_aiscore:
        result = run_clickhouse_load_month(
            'aiscore', month_str, config, project_root, log_file=log_file
        )
        results.add_result("aiscore_clickhouse", result)


def log_pipeline_summary(
    logger: logging.Logger,
    results: PipelineResults,
    total_dates: int,
    elapsed_time: float,
    log_file: Path
) -> None:
    """Log pipeline summary."""
    logger.info("\n\n" + "=" * 80)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total time: {format_elapsed_time(elapsed_time)}")
    logger.info(f"Dates processed: {total_dates}")
    logger.info(f"Log file: {log_file}")
    logger.info("\nResults by step:")
    summary = results.get_summary()
    for step_name, stats in summary.items():
        display_name = RESULT_CATEGORIES.get(
            step_name,
            step_name.replace('_', ' ').title()
        )
        logger.info(f"\n  {display_name}:")
        logger.info(
            f"    Successful: {stats['successful']}/{stats['total']}"
        )
        logger.info(f"    Failed: {stats['failed']}")
        logger.info(f"    Total time: {stats['total_time']:.1f}s")
        if stats['failed'] > 0:
            logger.info(f"    Failed dates: {stats['failed_dates']}")
    logger.info("\n" + "=" * 80)


def run_pipeline(args: argparse.Namespace) -> int:
    """
    Run the complete pipeline.
    Args:
        args: Parsed command-line arguments
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    project_root = get_project_root()
    config = create_pipeline_config(args)
    date_info = create_date_info(args)
    logger, log_file = setup_pipeline_logging(
        project_root, date_info, config.debug
    )
    log_pipeline_header(logger, date_info, config, log_file)
    results = PipelineResults()
    pipeline_start = time.time()
    for idx, date_str in enumerate(date_info.dates, 1):
        logger.info(f"\n\n{'#' * 80}")
        logger.info(
            f"# Processing date {idx}/{len(date_info.dates)}: {date_str}"
        )
        logger.info(f"{'#' * 80}\n")
        process_bronze_scraping(
            date_str, config, results, project_root, log_file
        )
        if (not args.month and not config.skip_clickhouse
                and not config.bronze_only):
            process_clickhouse_loading_per_date(
                date_str, config, results, project_root, log_file
            )
    if args.month and not config.skip_clickhouse and not config.bronze_only:
        process_clickhouse_loading_monthly(
            args.month, config, results, project_root, log_file, logger
        )
    pipeline_elapsed = time.time() - pipeline_start
    log_pipeline_summary(
        logger, results, len(date_info.dates), pipeline_elapsed, log_file
    )
    if results.all_successful():
        logger.info("✓ Pipeline completed successfully!")
        return 0
    else:
        logger.warning(
            "⚠ Pipeline completed with some failures (see details above)"
        )
        return 1


def main() -> int:
    """Main execution function."""
    args = parse_arguments()
    return run_pipeline(args)


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
