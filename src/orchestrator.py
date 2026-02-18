"""
Orchestrator for parallel match scraping and processing.

SCRAPER: FotMob
PURPOSE: Orchestrate the entire scraping and processing pipeline.
         Saves to Bronze layer only. Use load_clickhouse.py to load to ClickHouse.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests

from .core import OrchestratorProtocol, OrchestratorError, StorageProtocol
from .core.constants import Defaults
from .config import FotMobConfig
from .processors import MatchProcessor
from .scrapers import MatchScraper, DailyScraper
from .storage import BronzeStorage, get_s3_uploader
from .utils import ScraperMetrics, DataQualityChecker, get_logger, get_alert_manager
from .utils.alerting import AlertLevel


class FotMobOrchestrator(OrchestratorProtocol):
    """
    Orchestrate the entire scraping and processing pipeline.

    SCRAPER: FotMob
    PURPOSE: Scrapes match data and saves to Bronze layer (JSON files).
             ClickHouse loading is done separately via load_clickhouse.py
    """

    def __init__(self, config: Optional[FotMobConfig] = None, bronze_only: bool = True):
        """
        Initialize the orchestrator.

        Args:
            config: FotMob configuration. If None, uses default config.
            bronze_only: If True, only scrape to Bronze layer (no processing).
                        Default is True. Processing to ClickHouse is done separately.
        """
        self.config = config or FotMobConfig()
        self.logger = get_logger()
        self.bronze_only = bronze_only
        self.alert_manager = get_alert_manager()

        # Safety: FotMob can rate-limit/ban aggressive clients. Force sequential scraping
        # unless you intentionally re-enable parallelism in code.
        try:
            if getattr(self.config, "scraping", None) is not None:
                if getattr(self.config.scraping, "enable_parallel", False):
                    self.logger.warning(
                        "FotMob parallel scraping is disabled for safety (rate-limit/ban risk). "
                        "Forcing enable_parallel=False, max_workers=1."
                    )
                self.config.scraping.enable_parallel = False
                self.config.scraping.max_workers = 1
        except Exception as e:
            # Never fail initialization due to config shape differences
            self.logger.debug(f"Could not enforce sequential FotMob scraping: {e}")

        self.bronze_storage = BronzeStorage(self.config.bronze_base_dir)
        self.logger.info("Bronze layer storage initialized")

        self.processor = None if bronze_only else MatchProcessor()

        self.logger.info(f"FotMob Orchestrator initialized (bronze_only={bronze_only})")
        if bronze_only:
            self.logger.info("Note: Use load_clickhouse.py to load data from bronze to ClickHouse")

    def scrape_date(
        self,
        date_str: str,
        force_rescrape: bool = False
    ) -> ScraperMetrics:
        """
        Scrape all matches for a specific date.

        Args:
            date_str: Date in YYYYMMDD format
            force_rescrape: If True, rescrape already processed matches

        Returns:
            ScraperMetrics object with results
        """

        metrics = ScraperMetrics(date=date_str)
        metrics.start()

        self.logger.info(f"Starting scrape for date: {date_str}")

        self.logger.info("Running automatic health check...")
        health_status = self.bronze_storage.health_check()

        if health_status['overall_status'] != 'HEALTHY':
            critical_failures = [
                check for check in health_status['checks']
                if check['status'] == 'FAIL' and check.get('critical', False)
            ]
            if critical_failures:
                error_msg = f"Critical health check failures: {[c['name'] for c in critical_failures]}"
                self.logger.error(error_msg)
                metrics.end()
                raise OrchestratorError(
                    error_msg,
                    details={'failed_checks': [c['name'] for c in critical_failures]}
                )
            self.logger.warning("Health check warnings detected, but continuing...")
        else:
            self.logger.info("[OK] Health check passed")

        try:
            scraped_match_ids = set()

            match_ids = self._fetch_match_ids(date_str)
            metrics.total_matches = len(match_ids)

            if not match_ids:
                self.logger.warning(f"No matches found for date: {date_str}")
                metrics.end()
                return metrics

            completion_pct = self.bronze_storage.get_completion_percentage(date_str)
            already_complete = completion_pct is not None and completion_pct >= 100.0
            
            if already_complete:
                self.logger.info(f"Date {date_str} already complete ({completion_pct:.0f}%), skipping scrape, proceeding with compression/S3")
                metrics.skipped_matches = metrics.total_matches
                metrics.successful_matches = metrics.total_matches
                self.logger.info(f"Set metrics: total={metrics.total_matches}, successful={metrics.successful_matches}, skipped={metrics.skipped_matches}")

            if already_complete:
                match_ids_to_scrape = []
            elif not force_rescrape:
                match_ids_to_scrape = [
                    str(m) for m in match_ids
                    if not self.bronze_storage.match_exists(str(m), date_str)
                ]
                skipped = len(match_ids) - len(match_ids_to_scrape)
                if skipped > 0:
                    self.logger.info(f"Skipping {skipped} already scraped matches in Bronze")
                for match_id in match_ids:
                    if self.bronze_storage.match_exists(str(match_id), date_str):
                        metrics.record_skip(str(match_id), "Already scraped in Bronze")
                        scraped_match_ids.add(str(match_id))
            else:
                match_ids_to_scrape = [str(m) for m in match_ids]

            if self.config.enable_parallel and len(match_ids_to_scrape) > 1:
                self._scrape_matches_parallel(match_ids_to_scrape, metrics, date_str, scraped_match_ids)
            else:
                self._scrape_matches_sequential(match_ids_to_scrape, metrics, date_str, scraped_match_ids)

        except Exception as e:
            self.logger.exception(f"Error during scraping: {e}")

        finally:
            metrics.end()

        all_matches_scraped = (
            metrics.total_matches > 0 and 
            metrics.successful_matches == metrics.total_matches and 
            metrics.failed_matches == 0
        )
        
        self.logger.info(f"Post-scrape: bronze_only={self.bronze_only}, successful={metrics.successful_matches}, total={metrics.total_matches}, failed={metrics.failed_matches}, all_scraped={all_matches_scraped}")

        if self.bronze_only and metrics.successful_matches > 0:
            self.logger.info(f"Compression check: all_matches_scraped={all_matches_scraped}")
            if not all_matches_scraped:
                missing_count = metrics.total_matches - metrics.successful_matches
                missing_reason = f"Missing {missing_count} of {metrics.total_matches} matches (failed: {metrics.failed_matches}, skipped: {metrics.skipped_matches})"
                self.logger.warning(f"Skipping compression for {date_str}: {missing_reason}")
                
                alert_manager = get_alert_manager()
                alert_manager.send_alert(
                    level=AlertLevel.WARNING,
                    title=f"FotMob Partial Scraping - {date_str}",
                    message=f"Only {metrics.successful_matches}/{metrics.total_matches} matches scraped. Compression and S3 backup skipped.\n\nReason: {missing_reason}",
                    context={
                        "date": date_str,
                        "total_matches": metrics.total_matches,
                        "successful": metrics.successful_matches,
                        "failed": metrics.failed_matches,
                        "skipped": metrics.skipped_matches,
                    }
                )
            else:
                try:
                    self.logger.debug(f"Starting compression for {date_str}")
                    compression_stats = self.bronze_storage.compress_date_files(date_str)

                    if compression_stats.get('status') == 'success' and compression_stats['compressed'] > 0:
                        saved_mb = compression_stats.get('saved_mb', 0)
                        saved_pct = compression_stats.get('saved_pct', 0)
                        compressed = compression_stats.get('compressed', 0)
                        self.logger.info(
                            f"Compressed {compressed} files for {date_str}: "
                            f"saved {saved_mb:.2f} MB ({saved_pct:.0f}% reduction)"
                        )
                except Exception as e:
                    self.logger.error(f"Error during compression for {date_str}: {e}")

        if self.bronze_only and all_matches_scraped:
            self.logger.info("Checking S3 backup...")
            s3_uploader = get_s3_uploader()
            if s3_uploader:
                bronze_dir = f"{self.config.bronze_base_dir}/matches/{date_str}"
                bronze_path = Path(bronze_dir)
                
                self.logger.info(f"Checking bronze directory: {bronze_dir}")
                
                if bronze_path.exists():
                    files = list(bronze_path.iterdir())
                    self.logger.info(f"Bronze directory has {len(files)} files")
                    
                    if files:
                        try:
                            if s3_uploader.upload_bronze_backup(bronze_dir, date_str, "fotmob"):
                                self.logger.info(f"S3 backup complete for {date_str}")
                            else:
                                self.logger.error(f"Failed to upload {date_str} to S3")
                        except Exception as e:
                            self.logger.error(f"Error uploading to S3 for {date_str}: {e}")
                    else:
                        self.logger.warning(f"Bronze directory {bronze_dir} is empty, skipping S3 upload")
                else:
                    self.logger.warning(f"Bronze directory {bronze_dir} does not exist, skipping S3 upload")
            else:
                self.logger.info("S3 uploader not available (not configured or boto3 not installed)")

        metrics.print_summary()

        return metrics

    def _fetch_match_ids(
        self, date_str: str, force_refetch: bool = False
    ) -> List[Union[int, str]]:
        """
        Fetch match IDs for a date and save daily listing to Bronze.
        Uses daily listing to prevent duplicate API requests.

        Args:
            date_str: Date string YYYYMMDD format
            force_refetch: If True, fetch from API even if daily listing exists

        Returns:
            List of match IDs (int for FotMob, str for AIScore)
        """

        if not force_refetch and self.bronze_storage.daily_listing_exists(date_str):
            self.logger.info(f"Daily listing exists for {date_str}, loading from storage")
            if match_ids := self.bronze_storage.get_match_ids_for_date(date_str):
                self.logger.info(f"Loaded {len(match_ids)} match IDs from daily listing for {date_str}")
                return match_ids
            self.logger.warning(f"Daily listing exists but is empty for {date_str}, fetching from API")

        self.logger.info(f"Fetching daily listing from API for {date_str}")

        max_retries = Defaults.MAX_FETCH_RETRIES

        for attempt in range(max_retries):
            try:
                with DailyScraper(self.config) as scraper:
                    match_ids = scraper.fetch_matches_for_date(date_str)

                if not match_ids:
                    self.logger.warning(f"Empty match list returned for {date_str}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        self.logger.info(f"Retrying {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        self.logger.error(f"Failed to fetch daily listing after {max_retries} attempts")
                        return []

                try:
                    self.bronze_storage.save_daily_listing(date_str, match_ids)
                    self.logger.info(f"Saved daily listing for {date_str}: {len(match_ids)} matches")
                except Exception as e:
                    self.logger.warning(f"Could not save daily listing for {date_str}: {e}")

                self.logger.info(f"Successfully fetched {len(match_ids)} match IDs for {date_str}")
                return match_ids

            except requests.exceptions.RequestException as network_error:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    self.logger.warning(
                        f"Network error fetching daily listing "
                        f"(attempt {attempt + 1}/{max_retries}): {network_error}. "
                        f"Retrying {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to fetch daily listing after {max_retries} attempts: {network_error}")
                    raise

            except Exception as error:
                self.logger.error(f"Unexpected error fetching daily listing: {error}")
                raise

        return []

    def _scrape_matches_sequential(
        self,
        match_ids: List[str],
        metrics: ScraperMetrics,
        date_str: str,
        scraped_match_ids: set
    ):
        """Scrape matches sequentially."""
        self.logger.info(f"Scraping {len(match_ids)} matches sequentially")

        completed_count = 0

        with MatchScraper(self.config) as scraper:
            for match_id in match_ids:
                if match_id in scraped_match_ids:
                    self.logger.debug(f"[SKIP]Skipping match {match_id} (already scraped in this session)")
                    continue

                try:
                    self._process_single_match(scraper, match_id, metrics, date_str, scraped_match_ids)
                except Exception as e:
                    self.logger.error(f"Error processing match {match_id}: {e}")
                    metrics.record_failure(match_id, str(e), type(e).__name__)

                completed_count += 1
                # Log progress after EVERY match
                progress_pct = (completed_count / len(match_ids)) * 100
                self.logger.info(
                    f"[PROGRESS] {completed_count}/{len(match_ids)} ({progress_pct:.1f}%) | "
                    f"Success: {metrics.successful_matches} | "
                    f"Failed: {metrics.failed_matches} | "
                    f"Skipped: {metrics.skipped_matches}"
                )

    def _scrape_matches_parallel(
        self,
        match_ids: List[str],
        metrics: ScraperMetrics,
        date_str: str,
        scraped_match_ids: set
    ):
        """Scrape matches in parallel using thread pool."""
        self.logger.info(
            f"Scraping {len(match_ids)} matches in parallel "
            f"(max_workers={self.config.max_workers})"
        )

        match_ids_to_scrape = [m for m in match_ids if m not in scraped_match_ids]
        if len(match_ids_to_scrape) < len(match_ids):
            skipped = len(match_ids) - len(match_ids_to_scrape)
            self.logger.info(f"Skipping {skipped} matches already scraped in this session")

        # IMPORTANT: In parallel mode, reuse one MatchScraper (and its underlying
        # requests.Session) per worker thread to avoid per-match connection/TLS overhead.
        thread_local = threading.local()
        created_scrapers: List[MatchScraper] = []
        created_scrapers_lock = threading.Lock()

        def _get_thread_scraper() -> MatchScraper:
            scraper = getattr(thread_local, "scraper", None)
            if scraper is None:
                scraper = MatchScraper(self.config)
                thread_local.scraper = scraper
                with created_scrapers_lock:
                    created_scrapers.append(scraper)
            return scraper

        def _worker(match_id: str) -> tuple[bool, Optional[str]]:
            scraper = _get_thread_scraper()
            return self._process_match_with_scraper(scraper, match_id, date_str)

        try:
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                future_to_match = {}
                for match_id in match_ids_to_scrape:
                    future = executor.submit(_worker, match_id)
                    future_to_match[future] = match_id

                completed_count = 0

                for future in as_completed(future_to_match):
                    match_id = future_to_match[future]
                    try:
                        success, error_msg = future.result()
                        if success:
                            scraped_match_ids.add(match_id)
                            metrics.record_success(match_id)
                            self.logger.info(f"[SUCCESS] Processed match {match_id}")
                        else:
                            metrics.record_failure(match_id, error_msg or "Unknown error")
                            self.logger.error(f"[FAILED] Match {match_id}")
                    except Exception as e:
                        self.logger.exception(f"Exception for match {match_id}: {e}")
                        metrics.record_failure(match_id, str(e), type(e).__name__)

                    completed_count += 1
                    # Log progress after EVERY match
                    progress_pct = (completed_count / len(match_ids_to_scrape)) * 100
                    self.logger.info(
                        f"[PROGRESS] {completed_count}/{len(match_ids_to_scrape)} ({progress_pct:.1f}%) | "
                        f"Success: {metrics.successful_matches} | "
                        f"Failed: {metrics.failed_matches} | "
                        f"Skipped: {metrics.skipped_matches}"
                    )
        finally:
            # Always close any sessions created for worker threads.
            for scraper in created_scrapers:
                try:
                    scraper.close()
                except Exception as e:
                    self.logger.debug(f"Failed to close worker scraper session: {e}")

    def _fetch_and_save_match(
        self,
        scraper: MatchScraper,
        match_id: str,
        date_str: str,
    ) -> tuple[bool, Optional[str], Optional[List[str]]]:
        """
        Core match processing logic - fetch data and save to Bronze storage.

        This method contains the shared logic used by both sequential and parallel
        processing modes. It intentionally does NOT handle metrics or alerting,
        allowing callers to handle those concerns appropriately.

        Args:
            scraper: MatchScraper instance to use for fetching
            match_id: Match ID to process
            date_str: Date string YYYYMMDD format

        Returns:
            Tuple of (success, error_message, quality_issues)
            - success: True if match was processed successfully
            - error_message: Error description if failed, None otherwise
            - quality_issues: List of data quality issues, or None if not checked
        """
        try:
            raw_data = scraper.fetch_match_details(match_id)
            if not raw_data:
                return False, "Failed to fetch match data", None

            self.bronze_storage.save_raw_match_data(match_id, raw_data, date_str)
            self.logger.debug(f"Saved raw data to bronze layer for match {match_id}")

            try:
                self.bronze_storage.mark_match_as_scraped(match_id, date_str)
            except Exception as e:
                self.logger.warning(f"Could not update daily listing for match {match_id}: {e}")

            if self.bronze_only:
                return True, None, None

            # Run data quality checks if enabled
            quality_issues = None
            if self.processor and self.config.enable_data_quality_checks:
                try:
                    dataframes = self.processor.process_all(raw_data)
                    validation_results = DataQualityChecker.validate_all_dataframes(dataframes)
                    quality_issues = [
                        issue
                        for result in validation_results.values()
                        if not result.get('passed', True)
                        for issue in result.get('issues', [])
                    ]

                    if quality_issues and self.config.fail_on_quality_issues:
                        return False, f"Data quality issues: {quality_issues}", quality_issues
                except Exception as e:
                    self.logger.warning(f"Data quality check failed for {match_id}: {e}")

            return True, None, quality_issues

        except Exception as e:
            self.logger.exception(f"Error processing match {match_id}: {e}")
            return False, str(e), None

    def _process_match_with_scraper(
        self,
        scraper: MatchScraper,
        match_id: str,
        date_str: str,
    ) -> tuple[bool, Optional[str]]:
        """
        Process a single match using an existing scraper instance.

        NOTE: This method intentionally does NOT create/close the scraper.
        In parallel mode we reuse a scraper (and HTTP session) per worker thread.

        Returns:
            Tuple of (success, error_message)
        """
        success, error_msg, _ = self._fetch_and_save_match(scraper, match_id, date_str)
        return success, error_msg

    def _process_single_match(
        self,
        scraper: MatchScraper,
        match_id: str,
        metrics: ScraperMetrics,
        date_str: str,
        scraped_match_ids: set
    ):
        """Process a single match (for sequential execution)."""
        self.logger.info(f"Processing match {match_id}")

        success, error_msg, quality_issues = self._fetch_and_save_match(
            scraper, match_id, date_str
        )

        if success:
            scraped_match_ids.add(match_id)

            if quality_issues:
                metrics.record_data_quality_issue(match_id, quality_issues)
                self.alert_manager.alert_data_quality_issue(
                    match_id=match_id,
                    issues=quality_issues,
                    context={"date": date_str}
                )

            metrics.record_success(match_id)
            self.logger.info(f"[SUCCESS] Scraped match {match_id} to Bronze")
        else:
            metrics.record_failure(match_id, error_msg or "Unknown error", "ProcessingError")
            self.alert_manager.alert_failed_scrape(
                match_id=match_id,
                error=error_msg or "Unknown error",
                error_type="ProcessingError",
                context={"date": date_str}
            )

    def get_match_data(
        self, match_id: int, date_str: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve raw match data from Bronze layer.

        Args:
            match_id: Match ID to retrieve
            date_str: Date string YYYYMMDD format

        Returns:
            Raw match data (dict) or None if not found

        Note: For processed data, use load_clickhouse.py to query ClickHouse.
        """

        return self.bronze_storage.load_raw_match_data(str(match_id), date_str)

    def close(self):
        """Clean up resources."""

        self.logger.info("Orchestrator closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
