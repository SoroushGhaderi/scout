"""
DEPRECATED: Daily Aggregation for Silver Layer (Parquet)

This module is DEPRECATED. Parquet storage has been removed.
Use ClickHouse queries and aggregations instead.
"""

import glob
import pandas as pd
from pathlib import Path
from typing import Dict
import logging
import warnings

from ..utils.logging_utils import get_logger
from ..utils.date_utils import format_date_compact_to_display_partial


class DailyAggregator:
    """
    Concatenates per-game Silver layer data into daily files.
    No complex aggregations - just simple concatenation for faster querying.
    """
    
    def __init__(self, silver_base_dir: str = "data/silver"):
        """
        Initialize daily aggregator.
        
        Args:
            silver_base_dir: Base directory of Silver layer
        """
        self.silver_base_dir = Path(silver_base_dir)
        self.matches_dir = self.silver_base_dir / "matches"
        self.aggregated_dir = self.silver_base_dir / "aggregated" / "daily"
        self.aggregated_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = get_logger()
    
    def aggregate_date(self, date_str: str, force: bool = False) -> Dict[str, int]:
        """
        Concatenate all parquet files for a specific date.
        
        Args:
            date_str: Date in YYYYMMDD format
            force: Force re-aggregation even if exists
        
        Returns:
            Dict with concatenation statistics
        """
        # Normalize date format
        try:
            date_partition = format_date_compact_to_display_partial(date_str)
        except (ValueError, IndexError):
            date_partition = date_str
        
        date_dir = self.matches_dir / date_partition
        
        # Verify Silver layer matches exist
        if not date_dir.exists():
            self.logger.error(f"ERROR: Cannot aggregate: Silver layer matches not found for {date_partition}")
            self.logger.error(f"   Expected location: {date_dir}")
            self.logger.info(f"   Run first: python process_silver.py {date_str}")
            return {"status": "error", "message": "Silver layer not found"}
        
        # Verify at least one dataframe type exists
        dataframe_dirs = [d for d in date_dir.iterdir() if d.is_dir()]
        if not dataframe_dirs:
            self.logger.error(f"ERROR: Cannot aggregate: No dataframes found in {date_dir}")
            self.logger.info(f"   The Silver layer appears empty. Process it first.")
            return {"status": "error", "message": "No dataframes found"}
        
        self.logger.info(f"SUCCESS: Verified Silver layer: {len(dataframe_dirs)} dataframe types found")
        
        # Check if already aggregated
        agg_date_dir = self.aggregated_dir / date_partition
        if agg_date_dir.exists() and not force:
            self.logger.info(f"Daily aggregates already exist for {date_partition} (use --force to re-aggregate)")
            return {"status": "skipped"}
        
        agg_date_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Concatenating data for {date_partition}")
        
        stats = {}
        
        # Concatenate each dataframe type
        for df_type in dataframe_dirs:
            df_name = df_type.name
            count = self._concatenate_dataframe(df_type, agg_date_dir, df_name)
            if count > 0:
                stats[df_name] = count
        
        total_records = sum(stats.values())
        self.logger.info(f"Completed concatenation for {date_partition}: {total_records} records in {len(stats)} dataframes")
        
        return stats
    
    def _concatenate_dataframe(self, source_dir: Path, output_dir: Path, df_name: str) -> int:
        """
        Simply concatenate all parquet files for a dataframe type.
        
        Args:
            source_dir: Source directory containing parquet files
            output_dir: Output directory for concatenated file
            df_name: Name of the dataframe type
        
        Returns:
            Number of records concatenated
        """
        try:
            # Find all parquet files
            pattern = f"{source_dir.as_posix()}/**/*.parquet"
            files = glob.glob(pattern, recursive=True)
            
            if not files:
                self.logger.debug(f"No files found for {df_name}")
                return 0
            
            # Concatenate all files
            dfs = []
            for file in files:
                try:
                    df = pd.read_parquet(file, engine='pyarrow')
                    if not df.empty:
                        dfs.append(df)
                except Exception as e:
                    self.logger.warning(f"Error reading {file}: {e}")
                    continue
            
            if not dfs:
                return 0
            
            # Concatenate with join='outer' to handle missing columns gracefully
            # Suppress FutureWarning about all-NA columns (we handle it by dropping after)
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=FutureWarning, message='.*empty or all-NA entries.*')
                combined_df = pd.concat(dfs, ignore_index=True, join='outer')
            
            # Drop columns that are entirely NA (result of schema inconsistencies)
            combined_df = combined_df.dropna(axis=1, how='all')
            
            # Save concatenated file
            output_file = output_dir / f"{df_name}.parquet"
            combined_df.to_parquet(output_file, index=False, engine='pyarrow', compression='snappy')
            
            self.logger.debug(f"Concatenated {len(combined_df)} {df_name} records from {len(files)} files")
            return len(combined_df)
        
        except Exception as e:
            self.logger.error(f"Error concatenating {df_name}: {e}")
            return 0
