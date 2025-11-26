"""
Load data from bronze layer JSON files into ClickHouse data warehouse.

SCRAPER: Both FotMob and AIScore
PURPOSE: Load processed data from bronze layer (JSON/JSON.gz) directly to ClickHouse

Usage:
    # Load FotMob data for a date
    python scripts/load_clickhouse.py --scraper fotmob --date 20251113
    
    # Load AIScore data for a date
    python scripts/load_clickhouse.py --scraper aiscore --date 20251113
    
    # Load date range
    python scripts/load_clickhouse.py --scraper fotmob --start-date 20251101 --end-date 20251107
    
    # Load entire month
    python scripts/load_clickhouse.py --scraper fotmob --month 202511
    
    # Show table statistics
    python scripts/load_clickhouse.py --scraper fotmob --stats
"""

import argparse
import sys
import os
import logging
import json
import gzip
import tarfile
import io
import warnings
from pathlib import Path
from datetime import datetime, timedelta, date
from calendar import monthrange
from typing import List, Optional, Dict, Any

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.bronze_storage import BronzeStorage as FotMobBronzeStorage
from src.scrapers.aiscore.bronze_storage import BronzeStorage as AIScoreBronzeStorage
from src.processors.match_processor import MatchProcessor
from src.config.fotmob_config import FotMobConfig
from src.config.aiscore_config import AIScoreConfig
from src.utils.logging_utils import get_logger, setup_logging
from src.utils.lineage import LineageTracker
from src.utils.date_utils import format_date_compact_to_display, extract_year_month, DATE_FORMAT_COMPACT
from src.utils.alerting import get_alert_manager, AlertLevel
from src.storage.dlq import DeadLetterQueue


def get_unique_key_columns(table_name: str) -> List[str]:
    """
    Get unique key columns for a table (used for deduplication).
    
    Args:
        table_name: Table name
    
    Returns:
        List of column names that form the unique key
    """
    # Define unique keys based on ORDER BY clauses in table definitions
    unique_keys = {
        'general': ['match_id'],
        'timeline': ['match_id'],
        'venue': ['match_id'],
        'player': ['match_id', 'player_id'],
        'shotmap': ['match_id', 'id'],
        'goal': ['match_id', 'event_id'],
        'cards': ['match_id', 'event_id'],
        'red_card': ['match_id', 'event_id'],
        'period': ['match_id', 'period'],
        'momentum': ['match_id', 'minute'],
        'starters': ['match_id', 'player_id'],
        'substitutes': ['match_id', 'player_id'],
        'coaches': ['match_id', 'id'],
        'team_form': ['match_id', 'team_id', 'form_position'],
        # AIScore tables
        'matches': ['game_date', 'match_id'],
        'odds_1x2': ['game_date', 'match_id', 'scraped_at', 'bookmaker'],
        'odds_asian_handicap': ['game_date', 'match_id', 'scraped_at', 'home_handicap', 'away_handicap'],
        'odds_over_under': ['game_date', 'match_id', 'scraped_at', 'bookmaker', 'total_line', 'market_type'],
        'daily_listings': ['scrape_date'],
    }
    return unique_keys.get(table_name, ['match_id'])


def table_exists(client: ClickHouseClient, table_name: str, database: str = "fotmob") -> bool:
    """
    Check if a table exists in ClickHouse.
    
    Args:
        client: ClickHouse client
        table_name: Table name to check
        database: Database name
    
    Returns:
        True if table exists, False otherwise
    """
    try:
        client.execute(f"DESCRIBE TABLE {database}.{table_name}")
        return True
    except Exception as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "unknown_table" in error_str:
            return False
        # Re-raise if it's a different error (connection issue, etc.)
        raise


def deduplicate_dataframe(
    client: ClickHouseClient,
    df: pd.DataFrame,
    table_name: str,
    database: str,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Remove duplicate records from dataframe by checking existing data in ClickHouse.
    
    Args:
        client: ClickHouse client
        df: DataFrame to deduplicate
        table_name: Table name
        database: Database name
        logger: Logger instance
    
    Returns:
        DataFrame with duplicates removed
    """
    if logger is None:
        logger = get_logger()
    
    if df.empty:
        return df
    
    try:
        unique_keys = get_unique_key_columns(table_name)
        
        # Check which columns exist in both dataframe and unique keys
        available_keys = [key for key in unique_keys if key in df.columns]
        
        if not available_keys:
            logger.warning(f"No unique key columns found in dataframe for {table_name}, skipping deduplication")
            return df
        
        # Build query to check existing records
        # Create a temporary table or use IN clause
        key_values = df[available_keys].drop_duplicates()
        
        if key_values.empty:
            return df
        
        # Build WHERE clause for checking existing records
        conditions = []
        for idx, row in key_values.iterrows():
            row_conditions = []
            for key in available_keys:
                value = row[key]
                if pd.isna(value):
                    row_conditions.append(f"{key} IS NULL")
                elif isinstance(value, (int, float)):
                    row_conditions.append(f"{key} = {value}")
                else:
                    # Escape string values
                    escaped_value = str(value).replace("'", "''")
                    row_conditions.append(f"{key} = '{escaped_value}'")
            conditions.append("(" + " AND ".join(row_conditions) + ")")
        
        if not conditions:
            return df
        
        # Query existing records
        where_clause = " OR ".join(conditions)
        query = f"SELECT {', '.join(available_keys)} FROM {database}.{table_name} WHERE {where_clause}"
        
        try:
            existing_result = client.execute(query)
            
            # Extract existing records
            existing_records = set()
            if hasattr(existing_result, 'result_rows') and existing_result.result_rows:
                for row in existing_result.result_rows:
                    # Create tuple of key values
                    key_tuple = tuple(row[i] if i < len(row) else None for i in range(len(available_keys)))
                    existing_records.add(key_tuple)
            elif isinstance(existing_result, (list, tuple)):
                for row in existing_result:
                    if isinstance(row, (list, tuple)):
                        key_tuple = tuple(row[i] if i < len(row) else None for i in range(len(available_keys)))
                        existing_records.add(key_tuple)
            
            if not existing_records:
                logger.debug(f"No existing records found for {table_name}, all {len(df)} rows are new")
                return df
            
            # Filter out duplicates
            def is_duplicate(row):
                key_tuple = tuple(row[key] if key in row else None for key in available_keys)
                return key_tuple in existing_records
            
            mask = df.apply(is_duplicate, axis=1)
            duplicates_count = mask.sum()
            
            if duplicates_count > 0:
                logger.info(f"Found {duplicates_count} duplicate records in {table_name}, filtering them out")
                df_filtered = df[~mask].copy()
                return df_filtered
            else:
                logger.debug(f"No duplicates found for {table_name}, all {len(df)} rows are new")
                return df
                
        except Exception as e:
            # If query fails (e.g., table doesn't exist or no data), assume all are new
            logger.debug(f"Could not check for duplicates in {table_name}: {e}, assuming all rows are new")
            return df
            
    except Exception as e:
        logger.warning(f"Error during deduplication for {table_name}: {e}, proceeding without deduplication")
        return df


def load_fotmob_data(
    client: ClickHouseClient,
    date_str: str,
    force: bool = False,
    logger: Optional[logging.Logger] = None
) -> Dict[str, int]:
    """
    Load FotMob data from bronze JSON files to ClickHouse.
    
    Args:
        client: ClickHouse client (connected to fotmob database)
        date_str: Date in YYYYMMDD format
        force: Force reload even if data exists
        logger: Logger instance
    
    Returns:
        Dictionary with loading statistics
    """
    if logger is None:
        logger = get_logger()
    
    stats = {}
    
    # Initialize lineage tracker
    lineage_tracker = LineageTracker()
    
    try:
        config = FotMobConfig()
        bronze_storage = FotMobBronzeStorage(config.storage.bronze_path)
        processor = MatchProcessor()
        
        # Use YYYYMMDD format directly (date_str is already in YYYYMMDD format)
        matches_dir = bronze_storage.matches_dir / date_str
        
        if not matches_dir.exists():
            logger.warning(f"No bronze files found for date {date_str} at {matches_dir}")
            return stats
        
        all_dataframes = {}
        
        archive_path = matches_dir / f"{date_str}_matches.tar"
        if archive_path.exists():
            logger.info(f"Found TAR archive: {archive_path}")
            try:
                with tarfile.open(archive_path, 'r') as tar:
                    json_gz_members = [m for m in tar.getmembers() if m.name.endswith('.json.gz')]
                    logger.info(f"Found {len(json_gz_members)} match files in TAR archive")
                    
                    for member in json_gz_members:
                        try:
                            match_id = member.name.replace('match_', '').replace('.json.gz', '')
                            f = tar.extractfile(member)
                            if f:
                                with gzip.open(io.BytesIO(f.read()), 'rt', encoding='utf-8') as gz:
                                    file_data = json.load(gz)
                                raw_data = file_data.get('data', file_data)
                                dataframes = processor.process_all(raw_data)
                                for table_name, df in dataframes.items():
                                    if table_name not in all_dataframes:
                                        all_dataframes[table_name] = []
                                    if isinstance(df, pd.DataFrame) and not df.empty:
                                        all_dataframes[table_name].append(df)
                        except Exception as e:
                            logger.error(f"Error processing {member.name} from TAR: {e}", exc_info=True)
                            continue
            except Exception as e:
                logger.error(f"Error reading TAR archive {archive_path}: {e}", exc_info=True)
        
        if not all_dataframes:
            json_gz_files = list(matches_dir.glob("match_*.json.gz"))
            if json_gz_files:
                logger.info(f"Found {len(json_gz_files)} .json.gz files for {date_str}")
                for json_gz_file in json_gz_files:
                    try:
                        match_id = json_gz_file.stem.replace('match_', '').replace('.json', '')
                        with gzip.open(json_gz_file, 'rt', encoding='utf-8') as f:
                            file_data = json.load(f)
                        raw_data = file_data.get('data', file_data)
                        dataframes = processor.process_all(raw_data)
                        for table_name, df in dataframes.items():
                            if table_name not in all_dataframes:
                                all_dataframes[table_name] = []
                            if isinstance(df, pd.DataFrame) and not df.empty:
                                all_dataframes[table_name].append(df)
                    except Exception as e:
                        logger.error(f"Error processing {json_gz_file}: {e}", exc_info=True)
                        continue
        
        if not all_dataframes:
            json_files = list(matches_dir.glob("match_*.json"))
            if json_files:
                logger.info(f"Found {len(json_files)} JSON files for {date_str}")
                for json_file in json_files:
                    try:
                        match_id = json_file.stem.replace('match_', '')
                        with open(json_file, 'r', encoding='utf-8') as f:
                            file_data = json.load(f)
                        raw_data = file_data.get('data', file_data)
                        dataframes = processor.process_all(raw_data)
                        for table_name, df in dataframes.items():
                            if table_name not in all_dataframes:
                                all_dataframes[table_name] = []
                            if isinstance(df, pd.DataFrame) and not df.empty:
                                all_dataframes[table_name].append(df)
                    except Exception as e:
                        logger.error(f"Error processing {json_file}: {e}", exc_info=True)
                        continue
        
        if not all_dataframes:
            logger.warning(f"No match files found in {matches_dir} (checked TAR archive, .json.gz, and .json files)")
            return stats
        
        logger.info(f"Processed {sum(len(df_list) for df_list in all_dataframes.values())} match files for {date_str}")
        
        logger.info(f"Accumulated dataframes summary:")
        for table_name, df_list in all_dataframes.items():
            total_rows = sum(len(df) for df in df_list) if df_list else 0
            logger.info(f"  {table_name}: {len(df_list)} dataframes, {total_rows} total rows")
        
        # Tables that are handled separately (starters, substitutes, coaches)
        # Skip them in the main loop to avoid double processing
        tables_handled_separately = ['starters', 'substitutes', 'coaches']
        
        for table_name, df_list in all_dataframes.items():
            # Skip tables that are handled separately
            if table_name in tables_handled_separately:
                continue
                
            try:
                if not df_list:
                    continue
                
                non_empty_dfs = [df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty]
                if not non_empty_dfs:
                    continue
                
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', category=FutureWarning, message='.*DataFrame concatenation.*')
                    combined_df = pd.concat(non_empty_dfs, ignore_index=True, sort=False)
                
                clickhouse_table = table_name
                if table_name == 'cards_only':
                    clickhouse_table = 'cards'
                elif table_name == 'lineup_data':
                    continue
                
                # Check if table exists before processing
                if not table_exists(client, clickhouse_table, database="fotmob"):
                    logger.error(
                        f"Table fotmob.{clickhouse_table} does not exist. "
                        f"Please run 'python scripts/setup_clickhouse.py' to create all required tables."
                    )
                    stats[table_name] = 0
                    continue
                
                column_renames = {}
                if clickhouse_table == 'player':
                    if 'id' in combined_df.columns:
                        column_renames['id'] = 'player_id'
                    if 'name' in combined_df.columns:
                        column_renames['name'] = 'player_name'
                
                if column_renames:
                    combined_df = combined_df.rename(columns=column_renames)
                    logger.debug(f"Renamed columns for {clickhouse_table}: {column_renames}")
                
                int64_columns = {
                    'goal': ['event_id', 'shot_event_id'],
                    'cards': ['event_id'],
                    'red_card': ['event_id'],
                    'starters': ['player_id'],
                    'substitutes': ['player_id'],
                    'coaches': ['id'],
                    'team_form': ['team_id'],
                    'shotmap': ['id'],
                }
                
                if clickhouse_table in int64_columns:
                    for col_name in int64_columns[clickhouse_table]:
                        if col_name in combined_df.columns:
                            combined_df[col_name] = pd.to_numeric(combined_df[col_name], errors='coerce')
                            combined_df[col_name] = combined_df[col_name].astype('Int64')
                
                # Check table schema to see if inserted_at column exists
                table_has_inserted_at = False
                try:
                    table_info = client.execute(f"DESCRIBE TABLE fotmob.{clickhouse_table}")
                    
                    rows = []
                    if hasattr(table_info, 'result_rows') and table_info.result_rows:
                        rows = table_info.result_rows
                    elif hasattr(table_info, 'result_columns') and table_info.result_columns:
                        num_cols = len(table_info.result_columns)
                        num_rows = len(table_info.result_columns[0]) if table_info.result_columns[0] else 0
                        rows = [[table_info.result_columns[i][j] for i in range(num_cols)] for j in range(num_rows)]
                    elif isinstance(table_info, (list, tuple)):
                        rows = table_info
                    
                    # Check if inserted_at column exists in table schema
                    for row in rows:
                        if not row or len(row) < 2:
                            continue
                        col_name = row[0] if isinstance(row, (list, tuple)) else str(row)
                        if col_name == 'inserted_at':
                            table_has_inserted_at = True
                            break
                    
                    non_nullable_int32_cols = []
                    non_nullable_string_cols = []
                    for row in rows:
                        if not row or len(row) < 2:
                            continue
                        col_name = row[0] if isinstance(row, (list, tuple)) else str(row)
                        col_type = str(row[1] if isinstance(row, (list, tuple)) else row)
                        
                        if 'Nullable' not in col_type and col_name in combined_df.columns:
                            if 'Int32' in col_type:
                                non_nullable_int32_cols.append(col_name)
                            elif 'String' in col_type:
                                non_nullable_string_cols.append(col_name)
                    
                    int32_max, int32_min = 2147483647, -2147483648
                    for col in non_nullable_int32_cols + non_nullable_string_cols:
                        null_mask = combined_df[col].isna()
                        if null_mask.any():
                            logger.error(f"Found {null_mask.sum()} NULL values in non-nullable column {clickhouse_table}.{col}. Setting defaults.")
                            combined_df.loc[null_mask, col] = 0 if col in non_nullable_int32_cols else ''
                    
                    for col in non_nullable_int32_cols:
                        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce').fillna(0)
                        overflow_mask = (combined_df[col] < int32_min) | (combined_df[col] > int32_max)
                        if overflow_mask.any():
                            logger.error(f"Found {overflow_mask.sum()} Int32 overflow values in {clickhouse_table}.{col}. Clipping.")
                            combined_df.loc[overflow_mask & (combined_df[col] > int32_max), col] = int32_max
                            combined_df.loc[overflow_mask & (combined_df[col] < int32_min), col] = int32_min
                                
                except Exception as e:
                    logger.debug(f"Schema validation skipped for {clickhouse_table}: {e}")
                    # If we can't check schema, assume table doesn't have inserted_at (safer)
                    table_has_inserted_at = False
                
                # Add inserted_at column only if table has this column in its schema
                if table_has_inserted_at and 'inserted_at' not in combined_df.columns:
                    combined_df['inserted_at'] = datetime.now()
                elif not table_has_inserted_at and 'inserted_at' in combined_df.columns:
                    # Remove inserted_at if table doesn't have it (e.g., coaches table)
                    combined_df = combined_df.drop(columns=['inserted_at'])
                
                # Insert data directly - ClickHouse will handle deduplication via ReplacingMergeTree
                if combined_df.empty:
                    logger.info(f"No rows to insert for {clickhouse_table}")
                    stats[clickhouse_table] = 0
                    continue
                
                # Initialize DLQ for failed inserts
                dlq = DeadLetterQueue()
                
                try:
                    rows_inserted = client.insert_dataframe(clickhouse_table, combined_df, database="fotmob")
                    stats[clickhouse_table] = rows_inserted
                    logger.info(f"Loaded {rows_inserted} rows into fotmob.{clickhouse_table}")
                except Exception as insert_error:
                    # Send failed insert to DLQ
                    dlq.send_to_dlq(
                        table=clickhouse_table,
                        data=combined_df,
                        error=str(insert_error),
                        context={
                            "date": date_str,
                            "database": "fotmob",
                            "source_files_count": len(df_list)
                        }
                    )
                    logger.error(f"Failed to insert {clickhouse_table}, sent to DLQ: {insert_error}")
                    stats[clickhouse_table] = 0
                    raise  # Re-raise to maintain existing error handling
                
                # Force merge to optimize table immediately
                try:
                    logger.info(f"Optimizing fotmob.{clickhouse_table} table...")
                    client.execute(f"OPTIMIZE TABLE fotmob.{clickhouse_table} FINAL")
                    logger.info(f"Successfully optimized fotmob.{clickhouse_table} table")
                except Exception as e:
                    logger.error(f"Failed to optimize {clickhouse_table} table: {e}", exc_info=True)
                
                # Record data lineage for table-level load
                try:
                    lineage_tracker.record_load(
                        scraper="fotmob",
                        source_id=f"batch_{date_str}",
                        date=date_str,
                        destination_table=clickhouse_table,
                        metadata={
                            "rows_inserted": rows_inserted,
                            "source_files_count": len(df_list),
                            "table_name": clickhouse_table
                        }
                    )
                except Exception as e:
                    logger.warning(f"Could not record lineage for {clickhouse_table}: {e}")
                
            except Exception as e:
                error_str = str(e).lower()
                if "does not exist" in error_str or "unknown_table" in error_str:
                    logger.error(
                        f"Table fotmob.{clickhouse_table} does not exist. "
                        f"Please run 'python scripts/setup_clickhouse.py' to create all required tables."
                    )
                else:
                    logger.error(f"Error loading {table_name}: {e}", exc_info=True)
                stats[table_name] = 0
        
        for table_name in ['starters', 'substitutes', 'coaches']:
            if table_name in all_dataframes and all_dataframes[table_name]:
                non_empty_dfs = [df for df in all_dataframes[table_name] if isinstance(df, pd.DataFrame) and not df.empty]
                if non_empty_dfs:
                    # Check if table exists before attempting to load
                    try:
                        client.execute(f"DESCRIBE TABLE fotmob.{table_name}")
                    except Exception as e:
                        error_str = str(e).lower()
                        if "does not exist" in error_str or "unknown_table" in error_str:
                            logger.error(
                                f"Table fotmob.{table_name} does not exist. "
                                f"Please run 'python scripts/setup_clickhouse.py' to create all required tables."
                            )
                            stats[table_name] = 0
                            continue
                        else:
                            raise
                    
                    with warnings.catch_warnings():
                        warnings.filterwarnings('ignore', category=FutureWarning, message='.*DataFrame concatenation.*')
                        combined_df = pd.concat(non_empty_dfs, ignore_index=True, sort=False)
                    
                    int64_columns = {
                        'starters': ['player_id'],
                        'substitutes': ['player_id'],
                        'coaches': ['id'],
                    }
                    if table_name in int64_columns:
                        for col_name in int64_columns[table_name]:
                            if col_name in combined_df.columns:
                                combined_df[col_name] = pd.to_numeric(combined_df[col_name], errors='coerce').astype('Int64')
                    
                    # Add inserted_at column only if table has this column
                    # Note: coaches table does NOT have inserted_at column
                    tables_with_inserted_at = ['starters', 'substitutes']  # coaches excluded
                    if table_name in tables_with_inserted_at and 'inserted_at' not in combined_df.columns:
                        combined_df['inserted_at'] = datetime.now()
                    
                    # Insert data directly - ClickHouse will handle deduplication via ReplacingMergeTree
                    if combined_df.empty:
                        logger.info(f"No rows to insert for {table_name}")
                        stats[table_name] = 0
                        continue
                    
                    # Initialize DLQ for failed inserts
                    dlq = DeadLetterQueue()
                    
                    try:
                        rows_inserted = client.insert_dataframe(table_name, combined_df, database="fotmob")
                        stats[table_name] = rows_inserted
                        logger.info(f"Loaded {rows_inserted} rows into fotmob.{table_name}")
                    except Exception as insert_error:
                        # Send failed insert to DLQ
                        dlq.send_to_dlq(
                            table=table_name,
                            data=combined_df,
                            error=str(insert_error),
                            context={
                                "date": date_str,
                                "database": "fotmob"
                            }
                        )
                        logger.error(f"Failed to insert {table_name}, sent to DLQ: {insert_error}")
                        stats[table_name] = 0
                        raise  # Re-raise to maintain existing error handling
                    
                    # Force merge to optimize table immediately
                    try:
                        logger.info(f"Optimizing fotmob.{table_name} table...")
                        client.execute(f"OPTIMIZE TABLE fotmob.{table_name} FINAL")
                        logger.info(f"Successfully optimized fotmob.{table_name} table")
                    except Exception as e:
                        logger.error(f"Failed to optimize {table_name} table: {e}", exc_info=True)
                    
                    # Record data lineage
                    try:
                        lineage_tracker.record_load(
                            scraper="fotmob",
                            source_id=f"batch_{date_str}",
                            date=date_str,
                            destination_table=table_name,
                            metadata={
                                "rows_inserted": rows_inserted,
                                "source_files_count": len(non_empty_dfs),
                                "table_name": table_name
                            }
                        )
                    except Exception as e:
                        logger.warning(f"Could not record lineage for {table_name}: {e}")
        
        # Optimize all tables at the end to ensure optimization
        logger.info("Optimizing all FotMob tables...")
        fotmob_tables = ['general', 'timeline', 'venue', 'player', 'shotmap', 'goal', 
                         'cards', 'red_card', 'period', 'momentum', 'starters', 
                         'substitutes', 'coaches', 'team_form']
        optimized_count = 0
        for table in fotmob_tables:
            try:
                logger.info(f"Optimizing fotmob.{table} table...")
                client.execute(f"OPTIMIZE TABLE fotmob.{table} FINAL")
                logger.info(f"Successfully optimized fotmob.{table} table")
                optimized_count += 1
            except Exception as e:
                logger.error(f"Failed to optimize {table} table: {e}", exc_info=True)
        logger.info(f"Optimized {optimized_count}/{len(fotmob_tables)} FotMob tables")
        
    except Exception as e:
        logger.error(f"Error loading FotMob data: {e}", exc_info=True)
    
    return stats


def load_aiscore_data(
    client: ClickHouseClient,
    date_str: str,
    force: bool = False,
    logger: Optional[logging.Logger] = None
) -> Dict[str, int]:
    """
    Load AIScore data from Bronze JSON.gz files to ClickHouse.
    
    Args:
        client: ClickHouse client (connected to aiscore database)
        date_str: Date in YYYYMMDD format
        force: Force reload even if data exists
        logger: Logger instance
    
    Returns:
        Dictionary with loading statistics
    """
    if logger is None:
        logger = get_logger()
    
    stats = {}
    
    # Initialize lineage tracker
    lineage_tracker = LineageTracker()
    
    try:
        required_tables = ['matches', 'odds_1x2', 'odds_asian_handicap', 'odds_over_under', 'daily_listings']
        missing_tables = []
        
        for table in required_tables:
            try:
                client.execute(f"SELECT 1 FROM aiscore.{table} LIMIT 1")
            except Exception as e:
                error_str = str(e).lower()
                if any(keyword in error_str for keyword in ['doesn\'t exist', 'does not exist', 'unknown table', 'table not found']):
                    missing_tables.append(table)
        
        if missing_tables:
            logger.error(f"AIScore tables are missing: {', '.join(missing_tables)}")
            logger.error("Please run the table creation script first:")
            logger.error("  docker-compose exec scraper python scripts/setup_clickhouse.py")
            raise RuntimeError(f"Missing AIScore tables: {', '.join(missing_tables)}. Run setup_clickhouse.py first.")
        
        config = AIScoreConfig()
        bronze_storage = AIScoreBronzeStorage(config.storage.bronze_path)
        
        # Try to load from daily listings file first (new structure)
        daily_listing = bronze_storage.load_daily_listing(date_str)
        if not daily_listing:
            # Fallback to deprecated read_daily_list (reads from matches directory)
            daily_data = bronze_storage.read_daily_list(date_str)
            if not daily_data:
                logger.warning(f"No daily listings found for {date_str}")
                return stats
            matches_list = daily_data.get('matches', [])
        else:
            # Check if daily_listing has 'matches' array (old format) or 'match_ids' (new format)
            if 'matches' in daily_listing:
                # Format with matches array - use directly (has full info from links scraper)
                matches_list = daily_listing.get('matches', [])
                logger.info(f"Using matches array from daily listing ({len(matches_list)} matches)")
            elif 'match_ids' in daily_listing:
                # New format: has match_ids list
                match_ids = daily_listing.get('match_ids', [])
                if not match_ids:
                    logger.warning(f"No match IDs found in daily listings for {date_str}")
                    return stats
                
                # Convert match_ids to match dictionaries for compatibility
                matches_list = []
                for match_id in match_ids:
                    # Try to load match data to get full info
                    match_data = bronze_storage.load_raw_match_data(str(match_id), date_str)
                    if match_data:
                        data = match_data.get("data", {})
                        matches_list.append({
                            "match_id": str(match_id),
                            "match_url": data.get("match_url", ""),
                            "game_date": data.get("game_date", date_str),
                            "scrape_timestamp": match_data.get("scraped_at"),
                            "scrape_status": data.get("scrape_status", "unknown"),
                            "teams": data.get("teams", {}),
                            "league": data.get("league")
                        })
                    else:
                        # If match data doesn't exist, create minimal entry from daily listing
                        matches_list.append({
                            "match_id": str(match_id),
                            "game_date": date_str,
                            "scrape_status": "unknown"
                        })
            else:
                logger.warning(f"Daily listing for {date_str} has unexpected format")
                return stats
        
        if not matches_list:
            logger.warning(f"No matches found for {date_str}")
            return stats
        
        logger.info(f"Found {len(matches_list)} matches for {date_str}")
        
        # date_str is in YYYYMMDD format, convert to date object
        scrape_date_obj = datetime.strptime(date_str, DATE_FORMAT_COMPACT).date()
        
        matches_data = []
        odds_1x2_data = []
        odds_asian_handicap_data = []
        odds_over_under_data = []
        
        # Track source file paths for lineage
        match_file_paths = {}
        
        for match in matches_list:
            match_id = match.get('match_id')
            if not match_id:
                continue
            
            game_date = match.get('game_date', date_str)
            # Use new structure - load from matches/YYYYMMDD/match_*.json
            match_data = bronze_storage.load_raw_match_data(match_id, date_str)
            
            if not match_data:
                # Match data file doesn't exist yet (odds scraper hasn't run)
                # Use data from daily listing if available
                if 'match_url' in match or 'teams' in match:
                    # Create minimal match data from daily listing
                    data = {
                        "match_id": match_id,
                        "match_url": match.get('match_url', ''),
                        "game_date": game_date,
                        "scrape_status": match.get('scrape_status', 'links_only'),
                        "teams": match.get('teams', {}),
                        "league": match.get('league', {}),
                        "odds_1x2": [],
                        "odds_asian_handicap": [],
                        "odds_over_under": []
                    }
                    # Create a mock match_data dict to avoid None errors later
                    match_data = {
                        "data": data,
                        "scraped_at": match.get('scrape_timestamp') or datetime.now().isoformat()
                    }
                    logger.debug(f"Using daily listing data for {match_id} (match file not found)")
                else:
                    # No data available at all, skip
                    logger.debug(f"Match data not found for {match_id}, skipping")
                    continue
            else:
                data = match_data.get("data", {})
            
            # Get source file path for lineage tracking (date_str is already in YYYYMMDD format)
            odds_path = bronze_storage.matches_dir / date_str / f"match_{match_id}.json"
            if odds_path.exists():
                match_file_paths[match_id] = odds_path
            
            # Extract data in old format for compatibility
            manifest_data = {
                "match_id": match_id,
                "game_date": game_date,
                "scrape_status": data.get("scrape_status", "unknown"),
                "teams": data.get("teams", {}),
                "match_result": data.get("match_result"),
                "league": data.get("league")
            }
            odds_data = {
                "match_id": match_id,
                "match_url": data.get("match_url", ""),
                "game_date": game_date,
                "odds_1x2": data.get("odds_1x2", []),
                "odds_asian_handicap": data.get("odds_asian_handicap", []),
                "odds_over_under": data.get("odds_over_under", [])
            }
            
            # game_date might be in YYYYMMDD or YYYY-MM-DD format
            if isinstance(game_date, str):
                if len(game_date) == 8 and game_date.isdigit():
                    # YYYYMMDD format
                    game_date_obj = datetime.strptime(game_date, DATE_FORMAT_COMPACT).date()
                elif len(game_date) == 10 and '-' in game_date:
                    # YYYY-MM-DD format
                    game_date_obj = date.fromisoformat(game_date)
                else:
                    # Try to parse as-is
                    game_date_obj = datetime.strptime(game_date, DATE_FORMAT_COMPACT).date()
            else:
                game_date_obj = game_date
            
            odds_counts = data.get("odds_counts", {})
            # Ensure match_data is a dict (should be set above, but safety check)
            if not isinstance(match_data, dict):
                match_data = {
                    "data": data,
                    "scraped_at": match.get('scrape_timestamp') or datetime.now().isoformat()
                }
            
            match_record = {
                'match_id': match_id,
                'match_url': data.get('match_url', match.get('match_url', '')),
                'game_date': game_date_obj,
                'scrape_date': scrape_date_obj,
                'scrape_timestamp': datetime.fromisoformat(match_data.get('scraped_at')) if match_data and match_data.get('scraped_at') else datetime.now(),
                'scrape_status': data.get('scrape_status', match.get('scrape_status', 'unknown')),
                'scrape_duration': data.get('scrape_duration'),
                'home_team': data.get('teams', match.get('teams', {})).get('home', ''),
                'away_team': data.get('teams', match.get('teams', {})).get('away', ''),
                'match_result': data.get('match_result'),
                'league': data.get('league') or match.get('league'),
                'country': match.get('country'),
                'odds_1x2_count': odds_counts.get('odds_1x2', 0),
                'odds_asian_handicap_count': odds_counts.get('odds_asian_handicap', 0),
                'odds_over_under_goals_count': odds_counts.get('odds_over_under_goals', 0),
                'odds_over_under_corners_count': odds_counts.get('odds_over_under_corners', 0),
                'total_odds_count': odds_counts.get('total_odds', 0),
                'links_scraping_complete': 0,  # No longer tracked
                'links_scraping_completed_at': None,
                'odds_scraping_complete': 0,  # No longer tracked
                'odds_scraping_completed_at': None,
            }
            matches_data.append(match_record)
            
            # Extract odds data - check if any odds lists have data
            odds_1x2_list = odds_data.get('odds_1x2', [])
            odds_ah_list = odds_data.get('odds_asian_handicap', [])
            odds_ou_list = odds_data.get('odds_over_under', [])
            
            # Log what we found in the data
            logger.debug(f"Match {match_id}: odds_1x2={len(odds_1x2_list)}, odds_asian_handicap={len(odds_ah_list)}, odds_over_under={len(odds_ou_list)}")
            
            has_odds = len(odds_1x2_list) > 0 or len(odds_ah_list) > 0 or len(odds_ou_list) > 0
            
            if has_odds:
                logger.info(f"Found odds for match {match_id}: {len(odds_1x2_list)} 1X2, {len(odds_ah_list)} AH, {len(odds_ou_list)} OU")
                
                for odds in odds_1x2_list:
                    odds_1x2_data.append({
                        'match_id': match_id,
                        'match_url': odds.get('match_url', data.get('match_url', '')),
                        'game_date': game_date_obj,
                        'scrape_date': scrape_date_obj,
                        'bookmaker': odds.get('bookmaker'),
                        'home_odds': odds.get('home_odds'),
                        'draw_odds': odds.get('draw_odds'),
                        'away_odds': odds.get('away_odds'),
                        'scraped_at': datetime.fromisoformat(odds.get('scraped_at')) if odds.get('scraped_at') else datetime.now(),
                    })
                
                for odds in odds_ah_list:
                    odds_asian_handicap_data.append({
                        'match_id': match_id,
                        'match_url': odds.get('match_url', data.get('match_url', '')),
                        'game_date': game_date_obj,
                        'scrape_date': scrape_date_obj,
                        'match_time': odds.get('match_time'),
                        'moment_result': odds.get('moment_result'),
                        'home_handicap': odds.get('home_handicap'),
                        'home_odds': odds.get('home_odds'),
                        'away_handicap': odds.get('away_handicap'),
                        'away_odds': odds.get('away_odds'),
                        'scraped_at': datetime.fromisoformat(odds.get('scraped_at')) if odds.get('scraped_at') else datetime.now(),
                    })
                
                for odds in odds_ou_list:
                    odds_over_under_data.append({
                        'match_id': match_id,
                        'match_url': odds.get('match_url', data.get('match_url', '')),
                        'game_date': game_date_obj,
                        'scrape_date': scrape_date_obj,
                        'bookmaker': odds.get('bookmaker'),
                        'total_line': odds.get('total_line'),
                        'over_odds': odds.get('over_odds'),
                        'under_odds': odds.get('under_odds'),
                        'market_type': odds.get('market_type', 'goals'),
                        'scraped_at': datetime.fromisoformat(odds.get('scraped_at')) if odds.get('scraped_at') else datetime.now(),
                    })
            else:
                logger.debug(f"No odds data found for match {match_id} (odds lists are empty). Scrape status: {data.get('scrape_status', 'unknown')}")
        
        if matches_data:
            matches_df = pd.DataFrame(matches_data)
            
            # Use scrape_timestamp as inserted_at for ReplacingMergeTree deduplication
            # ReplacingMergeTree keeps the row with the highest inserted_at value
            if 'inserted_at' not in matches_df.columns:
                if 'scrape_timestamp' in matches_df.columns:
                    matches_df['inserted_at'] = pd.to_datetime(matches_df['scrape_timestamp'])
                else:
                    matches_df['inserted_at'] = datetime.now()
            
            # ClickHouse ReplacingMergeTree handles deduplication automatically
            # Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
            # Initialize DLQ for failed inserts
            dlq = DeadLetterQueue()
            
            try:
                stats['matches'] = client.insert_dataframe('matches', matches_df, database="aiscore")
            except Exception as insert_error:
                # Send failed insert to DLQ
                dlq.send_to_dlq(
                    table='matches',
                    data=matches_df,
                    error=str(insert_error),
                    context={
                        "date": date_str,
                        "database": "aiscore"
                    }
                )
                logger.error(f"Failed to insert matches, sent to DLQ: {insert_error}")
                stats['matches'] = 0
                raise  # Re-raise to maintain existing error handling
            
            # Force merge to deduplicate immediately
            try:
                client.execute(f"OPTIMIZE TABLE aiscore.matches FINAL")
                logger.debug("Optimized aiscore.matches table to remove duplicates")
            except Exception as e:
                logger.warning(f"Could not optimize matches table: {e}")
            
            # Record lineage for each match
            for match_record in matches_data:
                match_id = match_record['match_id']
                source_path = match_file_paths.get(match_id)
                try:
                    # Get parent lineage IDs (scrape records)
                    scrape_lineage = lineage_tracker.get_lineage("aiscore", date_str, match_id)
                    parent_ids = [l.lineage_id for l in scrape_lineage if l.transformation == "scrape"]
                    
                    lineage_tracker.record_load(
                        scraper="aiscore",
                        source_id=match_id,
                        date=date_str,
                        destination_table="matches",
                        destination_id=match_id,
                        source_path=source_path,
                        parent_lineage_ids=parent_ids,
                        metadata={
                            "game_date": str(match_record.get('game_date', '')),
                            "scrape_status": match_record.get('scrape_status', 'unknown')
                        }
                    )
                except Exception as e:
                    logger.warning(f"Could not record lineage for match {match_id}: {e}")
        else:
            logger.warning("No matches data to insert")
        
        if odds_1x2_data:
            odds_1x2_df = pd.DataFrame(odds_1x2_data)
            
            # Use scraped_at as inserted_at for ReplacingMergeTree deduplication
            # ReplacingMergeTree keeps the row with the highest inserted_at value
            if 'inserted_at' not in odds_1x2_df.columns:
                if 'scraped_at' in odds_1x2_df.columns:
                    odds_1x2_df['inserted_at'] = odds_1x2_df['scraped_at']
                else:
                    odds_1x2_df['inserted_at'] = datetime.now()
            
            # ClickHouse ReplacingMergeTree handles deduplication automatically
            # Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
            if not odds_1x2_df.empty:
                # Initialize DLQ for failed inserts
                dlq = DeadLetterQueue()
                
                try:
                    stats['odds_1x2'] = client.insert_dataframe('odds_1x2', odds_1x2_df, database="aiscore")
                except Exception as insert_error:
                    # Send failed insert to DLQ
                    dlq.send_to_dlq(
                        table='odds_1x2',
                        data=odds_1x2_df,
                        error=str(insert_error),
                        context={
                            "date": date_str,
                            "database": "aiscore"
                        }
                    )
                    logger.error(f"Failed to insert odds_1x2, sent to DLQ: {insert_error}")
                    stats['odds_1x2'] = 0
                    raise  # Re-raise to maintain existing error handling
                
                # Force merge to deduplicate immediately
                try:
                    client.execute(f"OPTIMIZE TABLE aiscore.odds_1x2 FINAL")
                    logger.debug("Optimized aiscore.odds_1x2 table to remove duplicates")
                except Exception as e:
                    logger.warning(f"Could not optimize odds_1x2 table: {e}")
            else:
                logger.info("No new 1X2 odds to insert after deduplication")
                stats['odds_1x2'] = 0
            
            # Record lineage for odds tables (batch per match)
            processed_matches = set()
            for odds_record in odds_1x2_data:
                match_id = odds_record['match_id']
                if match_id not in processed_matches:
                    processed_matches.add(match_id)
                    source_path = match_file_paths.get(match_id)
                    try:
                        scrape_lineage = lineage_tracker.get_lineage("aiscore", date_str, match_id)
                        parent_ids = [l.lineage_id for l in scrape_lineage if l.transformation == "scrape"]
                        
                        lineage_tracker.record_load(
                            scraper="aiscore",
                            source_id=match_id,
                            date=date_str,
                            destination_table="odds_1x2",
                            destination_id=match_id,
                            source_path=source_path,
                            parent_lineage_ids=parent_ids,
                            metadata={"odds_type": "1x2"}
                        )
                    except Exception as e:
                        logger.warning(f"Could not record lineage for odds_1x2 match {match_id}: {e}")
        else:
            logger.info("No 1X2 odds data to insert")
        
        if odds_asian_handicap_data:
            odds_ah_df = pd.DataFrame(odds_asian_handicap_data)
            logger.debug(f"Asian Handicap DataFrame shape: {odds_ah_df.shape}")
            logger.debug(f"Sample home_handicap values: {odds_ah_df['home_handicap'].head().tolist()}")
            
            # Use scraped_at as inserted_at for ReplacingMergeTree deduplication
            # ReplacingMergeTree keeps the row with the highest inserted_at value
            if 'inserted_at' not in odds_ah_df.columns:
                if 'scraped_at' in odds_ah_df.columns:
                    odds_ah_df['inserted_at'] = odds_ah_df['scraped_at']
                else:
                    odds_ah_df['inserted_at'] = datetime.now()
            
            # ClickHouse ReplacingMergeTree handles deduplication automatically
            # Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
            if not odds_ah_df.empty:
                # Initialize DLQ for failed inserts
                dlq = DeadLetterQueue()
                
                try:
                    stats['odds_asian_handicap'] = client.insert_dataframe('odds_asian_handicap', odds_ah_df, database="aiscore")
                except Exception as insert_error:
                    # Send failed insert to DLQ
                    dlq.send_to_dlq(
                        table='odds_asian_handicap',
                        data=odds_ah_df,
                        error=str(insert_error),
                        context={
                            "date": date_str,
                            "database": "aiscore"
                        }
                    )
                    logger.error(f"Failed to insert odds_asian_handicap, sent to DLQ: {insert_error}")
                    stats['odds_asian_handicap'] = 0
                    raise  # Re-raise to maintain existing error handling
                
                # Force merge to deduplicate immediately
                try:
                    client.execute(f"OPTIMIZE TABLE aiscore.odds_asian_handicap FINAL")
                    logger.debug("Optimized aiscore.odds_asian_handicap table to remove duplicates")
                except Exception as e:
                    logger.warning(f"Could not optimize odds_asian_handicap table: {e}")
            else:
                logger.info("No new Asian Handicap odds to insert after deduplication")
                stats['odds_asian_handicap'] = 0
            
            # Record lineage for Asian Handicap odds
            processed_matches = set()
            for odds_record in odds_asian_handicap_data:
                match_id = odds_record['match_id']
                if match_id not in processed_matches:
                    processed_matches.add(match_id)
                    source_path = match_file_paths.get(match_id)
                    try:
                        scrape_lineage = lineage_tracker.get_lineage("aiscore", date_str, match_id)
                        parent_ids = [l.lineage_id for l in scrape_lineage if l.transformation == "scrape"]
                        
                        lineage_tracker.record_load(
                            scraper="aiscore",
                            source_id=match_id,
                            date=date_str,
                            destination_table="odds_asian_handicap",
                            destination_id=match_id,
                            source_path=source_path,
                            parent_lineage_ids=parent_ids,
                            metadata={"odds_type": "asian_handicap"}
                        )
                    except Exception as e:
                        logger.warning(f"Could not record lineage for odds_asian_handicap match {match_id}: {e}")
        else:
            logger.info("No Asian Handicap odds data to insert")
        
        if odds_over_under_data:
            odds_ou_df = pd.DataFrame(odds_over_under_data)
            
            # Use scraped_at as inserted_at for ReplacingMergeTree deduplication
            # ReplacingMergeTree keeps the row with the highest inserted_at value
            if 'inserted_at' not in odds_ou_df.columns:
                if 'scraped_at' in odds_ou_df.columns:
                    odds_ou_df['inserted_at'] = odds_ou_df['scraped_at']
                else:
                    odds_ou_df['inserted_at'] = datetime.now()
            
            # ClickHouse ReplacingMergeTree handles deduplication automatically
            # Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
            if not odds_ou_df.empty:
                # Initialize DLQ for failed inserts
                dlq = DeadLetterQueue()
                
                try:
                    stats['odds_over_under'] = client.insert_dataframe('odds_over_under', odds_ou_df, database="aiscore")
                except Exception as insert_error:
                    # Send failed insert to DLQ
                    dlq.send_to_dlq(
                        table='odds_over_under',
                        data=odds_ou_df,
                        error=str(insert_error),
                        context={
                            "date": date_str,
                            "database": "aiscore"
                        }
                    )
                    logger.error(f"Failed to insert odds_over_under, sent to DLQ: {insert_error}")
                    stats['odds_over_under'] = 0
                    raise  # Re-raise to maintain existing error handling
                
                # Force merge to deduplicate immediately
                try:
                    client.execute(f"OPTIMIZE TABLE aiscore.odds_over_under FINAL")
                    logger.debug("Optimized aiscore.odds_over_under table to remove duplicates")
                except Exception as e:
                    logger.warning(f"Could not optimize odds_over_under table: {e}")
            else:
                logger.info("No new Over/Under odds to insert after deduplication")
                stats['odds_over_under'] = 0
            
            # Record lineage for Over/Under odds
            processed_matches = set()
            for odds_record in odds_over_under_data:
                match_id = odds_record['match_id']
                if match_id not in processed_matches:
                    processed_matches.add(match_id)
                    source_path = match_file_paths.get(match_id)
                    try:
                        scrape_lineage = lineage_tracker.get_lineage("aiscore", date_str, match_id)
                        parent_ids = [l.lineage_id for l in scrape_lineage if l.transformation == "scrape"]
                        
                        lineage_tracker.record_load(
                            scraper="aiscore",
                            source_id=match_id,
                            date=date_str,
                            destination_table="odds_over_under",
                            destination_id=match_id,
                            source_path=source_path,
                            parent_lineage_ids=parent_ids,
                            metadata={"odds_type": "over_under", "market_type": odds_record.get('market_type', 'goals')}
                        )
                    except Exception as e:
                        logger.warning(f"Could not record lineage for odds_over_under match {match_id}: {e}")
        else:
            logger.info("No Over/Under odds data to insert")
        
        # Get daily listing data for metadata
        daily_listing_meta = daily_listing if daily_listing else None
        if not daily_listing_meta:
            # Try to get from deprecated read_daily_list
            daily_data_fallback = bronze_storage.read_daily_list(date_str)
            if daily_data_fallback:
                daily_listing_meta = daily_data_fallback
        
        daily_listings_record = {
            'scrape_date': scrape_date_obj,
            'total_matches': len(matches_list),
            'links_scraping_complete': 1 if daily_listing_meta and daily_listing_meta.get('links_scraping_complete', False) else 0,
            'links_scraping_completed_at': datetime.fromisoformat(daily_listing_meta['links_scraping_completed_at']) if daily_listing_meta and daily_listing_meta.get('links_scraping_completed_at') else None,
            'odds_scraping_complete': 1 if daily_listing_meta and daily_listing_meta.get('odds_scraping_complete', False) else 0,
            'odds_scraping_completed_at': datetime.fromisoformat(daily_listing_meta['odds_scraping_completed_at']) if daily_listing_meta and daily_listing_meta.get('odds_scraping_completed_at') else None,
        }
        daily_df = pd.DataFrame([daily_listings_record])
        
        # Use scrape_date timestamp as inserted_at for ReplacingMergeTree deduplication
        # ReplacingMergeTree keeps the row with the highest inserted_at value
        if 'inserted_at' not in daily_df.columns:
            # Use links_scraping_completed_at or odds_scraping_completed_at if available
            if 'links_scraping_completed_at' in daily_df.columns and daily_df['links_scraping_completed_at'].notna().any():
                daily_df['inserted_at'] = pd.to_datetime(daily_df['links_scraping_completed_at'])
            elif 'odds_scraping_completed_at' in daily_df.columns and daily_df['odds_scraping_completed_at'].notna().any():
                daily_df['inserted_at'] = pd.to_datetime(daily_df['odds_scraping_completed_at'])
            else:
                daily_df['inserted_at'] = datetime.now()
        
        # ClickHouse ReplacingMergeTree handles deduplication automatically
        # Duplicates are removed during merge operations (use FINAL in SELECT for immediate deduplication)
        
        if not daily_df.empty:
            # Initialize DLQ for failed inserts
            dlq = DeadLetterQueue()
            
            try:
                stats['daily_listings'] = client.insert_dataframe('daily_listings', daily_df, database="aiscore")
            except Exception as insert_error:
                # Send failed insert to DLQ
                dlq.send_to_dlq(
                    table='daily_listings',
                    data=daily_df,
                    error=str(insert_error),
                    context={
                        "date": date_str,
                        "database": "aiscore"
                    }
                )
                logger.error(f"Failed to insert daily_listings, sent to DLQ: {insert_error}")
                stats['daily_listings'] = 0
                raise  # Re-raise to maintain existing error handling
            
            # Force merge to deduplicate immediately
            try:
                client.execute(f"OPTIMIZE TABLE aiscore.daily_listings FINAL")
                logger.debug("Optimized aiscore.daily_listings table to remove duplicates")
            except Exception as e:
                logger.warning(f"Could not optimize daily_listings table: {e}")
        else:
            logger.info("No new daily listings to insert after deduplication")
            stats['daily_listings'] = 0
        
        # Optimize all tables at the end to ensure deduplication
        logger.info("Optimizing all AIScore tables to remove duplicates...")
        aiscore_tables = ['matches', 'odds_1x2', 'odds_asian_handicap', 'odds_over_under', 'daily_listings']
        for table in aiscore_tables:
            try:
                client.execute(f"OPTIMIZE TABLE aiscore.{table} FINAL")
                logger.debug(f"Optimized aiscore.{table} table")
            except Exception as e:
                logger.warning(f"Could not optimize {table} table: {e}")
        
    except Exception as e:
        logger.error(f"Error loading AIScore data: {e}", exc_info=True)
    
    return stats


def generate_date_range(start_date: str, end_date: str) -> List[str]:
    """Generate list of dates between start and end (inclusive)."""
    start = datetime.strptime(start_date, DATE_FORMAT_COMPACT)
    end = datetime.strptime(end_date, DATE_FORMAT_COMPACT)
    
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime(DATE_FORMAT_COMPACT))
        current += timedelta(days=1)
    
    return dates


def generate_month_dates(month_str: str) -> List[str]:
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
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Load data from bronze layer JSON files into ClickHouse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load FotMob data for a date
  python scripts/load_clickhouse.py --scraper fotmob --date 20251113
  
  # Load AIScore data for a date
  python scripts/load_clickhouse.py --scraper aiscore --date 20251113
  
  # Load date range
  python scripts/load_clickhouse.py --scraper fotmob --start-date 20251101 --end-date 20251107
  
  # Load entire month
  python scripts/load_clickhouse.py --scraper fotmob --month 202511
  python scripts/load_clickhouse.py --scraper aiscore --month 202511
  
  # Show table statistics
  python scripts/load_clickhouse.py --scraper fotmob --stats
  
  # Truncate and reload
  python scripts/load_clickhouse.py --scraper aiscore --date 20251113 --truncate
        """
    )
    
    parser.add_argument(
        '--scraper',
        type=str,
        choices=['fotmob', 'aiscore'],
        required=True,
        help='Scraper to load data for (fotmob or aiscore)'
    )
    
    # Date arguments - mutually exclusive group
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        '--date',
        type=str,
        help='Date to load (YYYYMMDD format). Required unless --start-date or --month is used.'
    )
    date_group.add_argument(
        '--start-date',
        type=str,
        help='Start date for range loading (YYYYMMDD format)'
    )
    date_group.add_argument(
        '--month',
        type=str,
        help='Load entire month (YYYYMM format, e.g., 202511 for November 2025)'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date for range loading (YYYYMMDD format). Required if --start-date is used.'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        help='ClickHouse host (default: from CLICKHOUSE_HOST env var or localhost)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.getenv('CLICKHOUSE_PORT', '8123')),
        help='ClickHouse HTTP port (default: from CLICKHOUSE_PORT env var or 8123)'
    )
    
    parser.add_argument(
        '--username',
        type=str,
        default=os.getenv('CLICKHOUSE_USER', 'fotmob_user'),
        help='ClickHouse username (default: from CLICKHOUSE_USER env var or fotmob_user)'
    )
    
    parser.add_argument(
        '--password',
        type=str,
        default=os.getenv('CLICKHOUSE_PASSWORD', 'fotmob_pass'),
        help='ClickHouse password (default: from CLICKHOUSE_PASSWORD env var or fotmob_pass)'
    )
    
    parser.add_argument(
        '--truncate',
        action='store_true',
        help='Truncate tables before loading (fresh start)'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show table statistics and exit'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force reload even if data exists'
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
        
        # Month mode - end_date is not allowed
        if args.end_date:
            parser.error("Cannot use --end-date with --month option")
    
    # Determine database
    database = args.scraper
    
    # Determine date suffix for logging (before connecting to ClickHouse)
    if args.stats:
        # For stats, no date needed - use default
        date_suffix = None
    elif args.month:
        date_suffix = args.month
    elif args.start_date:
        date_suffix = f"{args.start_date}_to_{args.end_date}"
    elif args.date:
        date_suffix = args.date
    else:
        date_suffix = None
    
    # Setup logging with date suffix
    logger = setup_logging(
        name="clickhouse_loader",
        log_dir="logs",
        log_level="INFO",
        date_suffix=date_suffix
    )
    
    # Connect to ClickHouse
    client = ClickHouseClient(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        database=database
    )
    
    if not client.connect():
        logger.error("Failed to connect to ClickHouse")
        sys.exit(1)
    
    try:
        # Define table lists
        FOTMOB_TABLES = ['general', 'timeline', 'venue', 'player', 'shotmap', 'goal', 
                         'cards', 'red_card', 'period', 'momentum', 'starters', 
                         'substitutes', 'coaches', 'team_form']
        AISCORE_TABLES = ['matches', 'odds_1x2', 'odds_asian_handicap', 'odds_over_under', 'daily_listings']
        
        # Show statistics
        if args.stats:
            logger.info(f"\n=== {database.upper()} Database Statistics ===\n")
            tables = FOTMOB_TABLES if database == 'fotmob' else AISCORE_TABLES
            
            for table in tables:
                stats = client.get_table_stats(table, database=database)
                if 'error' not in stats:
                    logger.info(f"{table}: {stats.get('row_count', 0):,} rows, {stats.get('size', '0 B')}")
                else:
                    logger.warning(f"{table}: {stats.get('error', 'Unknown error')}")
            
            return
        
        # Determine dates to process
        if args.month:
            dates = generate_month_dates(args.month)
            year, month = extract_year_month(args.month)
            month_name = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][int(month) - 1]
            date_display = f"Month: {month_name} {year} ({args.month})"
            logger.info(f"\n{'='*80}")
            logger.info(f"Monthly Loading Mode: {date_display}")
            logger.info(f"Total dates: {len(dates)}")
            logger.info(f"{'='*80}\n")
        elif args.start_date:
            dates = generate_date_range(args.start_date, args.end_date)
        elif args.date:
            dates = [args.date]
        else:
            parser.error("Either --date, --start-date, or --month must be provided")
            year, month = extract_year_month(args.month)
            month_name = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][int(month) - 1]
            date_display = f"Month: {month_name} {year} ({args.month})"
            logger.info(f"\n{'='*80}")
            logger.info(f"Monthly Loading Mode: {date_display}")
            logger.info(f"Total dates: {len(dates)}")
            logger.info(f"{'='*80}\n")
        
        # Truncate if requested
        if args.truncate:
            logger.warning("Truncating tables before loading...")
            tables = FOTMOB_TABLES if database == 'fotmob' else AISCORE_TABLES
            for table in tables:
                try:
                    client.truncate_table(table, database=database)
                except Exception as e:
                    logger.warning(f"Could not truncate {table}: {e}")
        
        # Load data for each date
        total_stats = {}
        for date_str in dates:
            logger.info(f"\n{'='*80}")
            logger.info(f"Loading {database} data for {date_str}")
            logger.info(f"{'='*80}\n")
            
            try:
                if database == 'fotmob':
                    stats = load_fotmob_data(client, date_str, args.force, logger)
                else:  # aiscore
                    stats = load_aiscore_data(client, date_str, args.force, logger)
                
                # Accumulate stats
                for table, count in stats.items():
                    total_stats[table] = total_stats.get(table, 0) + count
            except Exception as e:
                logger.error(f"Failed to load {database} data for {date_str}: {e}", exc_info=True)
                # Send email alert
                alert_manager = get_alert_manager()
                alert_manager.send_alert(
                    level=AlertLevel.ERROR,
                    title=f"ClickHouse Loading Failed - {database.upper()} - {date_str}",
                    message=f"Failed to load {database} data to ClickHouse for date {date_str}.\n\nError: {str(e)}",
                    context={"date": date_str, "scraper": database, "step": f"ClickHouse Loading - {database}", "error": str(e)}
                )
                # Continue with next date
                continue
        
        # Print summary
        logger.info(f"\n{'='*80}")
        logger.info("LOADING SUMMARY")
        logger.info(f"{'='*80}\n")
        logger.info(f"Dates processed: {len(dates)}")
        logger.info(f"Total rows loaded by table:")
        for table, count in sorted(total_stats.items()):
            logger.info(f"  {table}: {count:,} rows")
        
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
