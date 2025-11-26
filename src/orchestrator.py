"""
Orchestrator for parallel match scraping and processing.

SCRAPER: FotMob
PURPOSE: Orchestrate the entire scraping and processing pipeline.
         Saves to Bronze layer only. Use load_clickhouse.py to load to ClickHouse.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
from datetime import datetime
import time
import requests

from .config import FotMobConfig
from .scrapers import MatchScraper, DailyScraper
from .processors import MatchProcessor
from .storage import BronzeStorage
from .utils import ScraperMetrics, DataQualityChecker, get_logger, get_alert_manager


class FotMobOrchestrator:
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
        
        # Initialize Bronze layer (always needed for scraping)
        self.bronze_storage = BronzeStorage(self.config.bronze_base_dir)
        self.logger.info("Bronze layer storage initialized")
        
        # Processor for data quality checks (if needed)
        self.processor = MatchProcessor() if not bronze_only else None
        
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
        
        # AUTOMATIC HEALTH CHECK: Run before scraping starts
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
                raise RuntimeError(error_msg)
            else:
                self.logger.warning("Health check warnings detected, but continuing...")
        else:
            self.logger.info("[OK] Health check passed")
        
        try:
            # Track scraped match IDs in this session to prevent missing information
            scraped_match_ids = set()
            
            # Fetch all match IDs for the date
            match_ids = self._fetch_match_ids(date_str)
            metrics.total_matches = len(match_ids)
            
            if not match_ids:
                self.logger.warning(f"No matches found for date: {date_str}")
                metrics.end()
                return metrics
            
            # Filter out already processed matches (check if file exists)
            if not force_rescrape:
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
                            # Add to scraped set to track
                            scraped_match_ids.add(str(match_id))
            else:
                match_ids_to_scrape = [str(m) for m in match_ids]
            
            # Scrape matches (parallel or sequential)
            if self.config.enable_parallel and len(match_ids_to_scrape) > 1:
                self._scrape_matches_parallel(match_ids_to_scrape, metrics, date_str, scraped_match_ids)
            else:
                self._scrape_matches_sequential(match_ids_to_scrape, metrics, date_str, scraped_match_ids)
        
        except Exception as e:
            self.logger.exception(f"Error during scraping: {e}")
        
        finally:
            metrics.end()
            
            # Post-processing after scraping is complete (only in bronze_only mode)
            if self.bronze_only and metrics.successful_matches > 0:
                # 1. Compress files
                try:
                    self.logger.info(f"Compressing files for {date_str}...")
                    compression_stats = self.bronze_storage.compress_date_files(date_str)
                    
                    if compression_stats['compressed'] > 0:
                        self.logger.info(
                            f"Compression saved {compression_stats['saved_mb']} MB "
                            f"({compression_stats['saved_pct']}% reduction)"
                        )
                except Exception as e:
                    self.logger.error(f"Error during compression: {e}")
                    # Don't fail the whole scraping job if compression fails
                
                # Profiling removed - use data lineage for tracking instead
            
            # Print summary
            metrics.print_summary()
        
        return metrics
    
    def _fetch_match_ids(self, date_str: str, force_refetch: bool = False) -> List[int]:
        """
        Fetch match IDs for a date and save daily listing to Bronze.
        Uses daily listing to prevent duplicate API requests.
        
        Args:
            date_str: Date string in YYYYMMDD format
            force_refetch: If True, fetch from API even if daily listing exists
        
        Returns:
            List of match IDs
        """
        # Check if daily listing exists (prevent duplicate API requests)
        if not force_refetch and self.bronze_storage.daily_listing_exists(date_str):
            self.logger.info(f"Daily listing exists for {date_str}, loading from storage")
            match_ids = self.bronze_storage.get_match_ids_for_date(date_str)
            if match_ids:
                self.logger.info(f"Loaded {len(match_ids)} match IDs from daily listing for {date_str}")
                return match_ids
            else:
                self.logger.warning(f"Daily listing exists but is empty for {date_str}, fetching from API")
        
        # Fetch from API with retry logic
        self.logger.info(f"Fetching daily listing from API for {date_str}")
        
        max_retries = 5
        
        for attempt in range(max_retries):
            try:
                with DailyScraper(self.config) as scraper:
                    # Fetch and extract match IDs
                    match_ids = scraper.fetch_matches_for_date(date_str)
                    
                    # Validate response
                    if not match_ids:
                        self.logger.warning(f"Empty match list returned for {date_str}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt  # Exponential backoff
                            self.logger.info(f"Retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait_time)
                            continue
                        else:
                            self.logger.error(f"Failed to fetch daily listing after {max_retries} attempts")
                            return []
                    
                    # Save daily listing to prevent duplicate requests
                    try:
                        self.bronze_storage.save_daily_listing(date_str, match_ids)
                        self.logger.info(f"Saved daily listing for {date_str}: {len(match_ids)} matches")
                    except Exception as e:
                        self.logger.warning(f"Could not save daily listing for {date_str}: {e}")
                    
                    self.logger.info(f"Successfully fetched {len(match_ids)} match IDs for {date_str}")
                    return match_ids
                    
            except requests.exceptions.RequestException as network_error:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                    self.logger.warning(
                        f"Network error fetching daily listing (attempt {attempt + 1}/{max_retries}): {network_error}. "
                        f"Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Failed to fetch daily listing after {max_retries} attempts: {network_error}")
                    raise
            
            except Exception as error:
                # Unexpected error - log and re-raise
                self.logger.error(f"Unexpected error fetching daily listing: {error}")
                raise
        
        # Should never reach here
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
        update_interval = self.config.metrics_update_interval
        
        with MatchScraper(self.config) as scraper:
            for match_id in match_ids:
                # Skip if already scraped in this session
                if match_id in scraped_match_ids:
                    self.logger.debug(f"Skipping match {match_id} (already scraped in this session)")
                    continue
                
                try:
                    self._process_single_match(scraper, match_id, metrics, date_str, scraped_match_ids)
                except Exception as e:
                    self.logger.error(f"Error processing match {match_id}: {e}")
                    metrics.record_failure(match_id, str(e), type(e).__name__)
                
                # Update metrics every N matches
                completed_count += 1
                if completed_count % update_interval == 0:
                    self.logger.info(
                        f"[PROGRESS] {completed_count}/{len(match_ids)} matches processed"
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
        
        # Filter out already scraped matches
        match_ids_to_scrape = [m for m in match_ids if m not in scraped_match_ids]
        if len(match_ids_to_scrape) < len(match_ids):
            skipped = len(match_ids) - len(match_ids_to_scrape)
            self.logger.info(f"Skipping {skipped} matches already scraped in this session")
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_match = {}
            for match_id in match_ids_to_scrape:
                future = executor.submit(
                    self._process_match_with_scraper,
                    match_id,
                    date_str,
                    scraped_match_ids
                )
                future_to_match[future] = match_id
            
            # Process completed tasks with periodic metrics updates
            completed_count = 0
            update_interval = self.config.metrics_update_interval
            
            for future in as_completed(future_to_match):
                match_id = future_to_match[future]
                try:
                    success, error_msg = future.result()
                    if success:
                        # Add to scraped_match_ids immediately after successful scrape
                        scraped_match_ids.add(match_id)
                        metrics.record_success(match_id)
                        self.logger.info(f"[SUCCESS] Processed match {match_id}")
                    else:
                        metrics.record_failure(match_id, error_msg or "Unknown error")
                        self.logger.error(f"[FAILED] Match {match_id}")
                except Exception as e:
                    self.logger.exception(f"Exception for match {match_id}: {e}")
                    metrics.record_failure(match_id, str(e), type(e).__name__)
                
                # Update metrics every N matches
                completed_count += 1
                if completed_count % update_interval == 0:
                    self.logger.info(
                        f"[PROGRESS] {completed_count}/{len(match_ids_to_scrape)} matches processed"
                    )
    
    def _process_match_with_scraper(
        self,
        match_id: str,
        date_str: str,
        scraped_match_ids: set
    ) -> tuple[bool, Optional[str]]:
        """
        Process a single match (for parallel execution).
        
        Returns:
            Tuple of (success, error_message)
        """
        try:
            with MatchScraper(self.config) as scraper:
                # Fetch raw data
                raw_data = scraper.fetch_match_details(match_id)
                if not raw_data:
                    return False, "Failed to fetch match data"
                
                # Save to Bronze layer (raw JSON)
                self.bronze_storage.save_raw_match_data(match_id, raw_data, date_str)
                self.logger.debug(f"Saved raw data to bronze layer for match {match_id}")
                
                # Add to scraped_match_ids immediately after successful save to prevent missing information
                scraped_match_ids.add(match_id)
                
                # Update daily listing file: move match_id from missing_match_ids to scraped_match_ids
                try:
                    self.bronze_storage.mark_match_as_scraped(match_id, date_str)
                except Exception as e:
                    self.logger.warning(f"Could not update daily listing for match {match_id}: {e}")
                
                # If bronze_only mode, skip processing (ClickHouse loading done separately)
                if self.bronze_only:
                    return True, None
                
                # Optional: Data quality checks on raw data
                if self.processor and self.config.enable_data_quality_checks:
                    try:
                        dataframes = self.processor.process_all(raw_data)
                        validation_results = DataQualityChecker.validate_all_dataframes(dataframes)
                        issues = [
                            issue
                            for result in validation_results.values()
                            if not result.get('passed', True)
                            for issue in result.get('issues', [])
                        ]
                        
                        if issues and self.config.fail_on_quality_issues:
                            return False, f"Data quality issues: {issues}"
                    except Exception as e:
                        self.logger.warning(f"Data quality check failed for {match_id}: {e}")
                
                # Note: ClickHouse loading is done separately via load_clickhouse.py
                return True, None
        
        except Exception as e:
            self.logger.exception(f"Error processing match {match_id}: {e}")
            return False, str(e)
    
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
        
        try:
            # Fetch raw data
            raw_data = scraper.fetch_match_details(match_id)
            if not raw_data:
                metrics.record_failure(match_id, "Failed to fetch match data")
                return
            
            # Save to Bronze layer (raw JSON)
            self.bronze_storage.save_raw_match_data(match_id, raw_data, date_str)
            self.logger.debug(f"Saved raw data to bronze layer for match {match_id}")
            
            # Add to scraped_match_ids immediately after successful save to prevent missing information
            scraped_match_ids.add(match_id)
            
            # Update daily listing file: move match_id from missing_match_ids to scraped_match_ids
            try:
                self.bronze_storage.mark_match_as_scraped(match_id, date_str)
            except Exception as e:
                self.logger.warning(f"Could not update daily listing for match {match_id}: {e}")
            
            # If bronze_only mode, skip processing (ClickHouse loading done separately)
            if self.bronze_only:
                metrics.record_success(match_id)
                self.logger.info(f"[SUCCESS] Scraped match {match_id} to Bronze")
                return
            
            # Optional: Data quality checks on raw data
            if self.processor and self.config.enable_data_quality_checks:
                try:
                    dataframes = self.processor.process_all(raw_data)
                    validation_results = DataQualityChecker.validate_all_dataframes(dataframes)
                    issues = []
                    for df_name, result in validation_results.items():
                        if not result.get('passed', True):
                            issues.extend(result.get('issues', []))
                    
                    if issues:
                        metrics.record_data_quality_issue(match_id, issues)
                        # Send alert for data quality issues
                        self.alert_manager.alert_data_quality_issue(
                            match_id=match_id,
                            issues=issues,
                            context={"date": date_str}
                        )
                        if self.config.fail_on_quality_issues:
                            metrics.record_failure(match_id, f"Data quality issues: {issues}")
                            return
                except Exception as e:
                    self.logger.warning(f"Data quality check failed for {match_id}: {e}")
            
            # Note: Silver layer (ClickHouse) loading is done separately via load_clickhouse.py
            metrics.record_success(match_id)
            self.logger.info(f"[SUCCESS] Scraped match {match_id} to Bronze")
        
        except Exception as e:
            self.logger.exception(f"Error processing match {match_id}: {e}")
            metrics.record_failure(match_id, str(e), type(e).__name__)
            # Send alert for failed scrape
            self.alert_manager.alert_failed_scrape(
                match_id=match_id,
                error=str(e),
                error_type=type(e).__name__,
                context={"date": date_str}
            )
    
    def get_match_data(self, match_id: int, date_str: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve raw match data from Bronze layer.
        
        Args:
            match_id: Match ID to retrieve
            date_str: Date string in YYYYMMDD format
        
        Returns:
            Raw match data (dict) or None if not found
            
        Note: For processed data, use load_clickhouse.py to query ClickHouse.
        """
        return self.bronze_storage.load_raw_match_data(str(match_id), date_str)
    
    def close(self):
        """Clean up resources."""
        # No resources to close (BronzeStorage doesn't require cleanup)
        self.logger.info("Orchestrator closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

