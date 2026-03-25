"""Load raw FotMob bronze files into ClickHouse bronze tables.

SCRAPER: FotMob
PURPOSE: Transform raw bronze files (JSON/JSON.gz/TAR) into ClickHouse bronze tables

Usage:
    # Load FotMob data for a date
    python scripts/load_clickhouse.py --scraper fotmob --date 20251113

    # Load date range
    python scripts/load_clickhouse.py --scraper fotmob --start-date 20251101 --end-date 20251107

    # Load entire month
    python scripts/load_clickhouse.py --scraper fotmob --month 202511

    # Show table statistics
    python scripts/load_clickhouse.py --scraper fotmob --stats

    Note:
    Table optimization is handled separately via SQL scripts.
    Run clickhouse/bronze/02_optimize.sql to optimize and deduplicate tables.
"""

import argparse
import gzip
import io
import json
import logging
import os
import sys
import tarfile
import warnings
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd

from config import FotMobConfig
from src.processors.bronze.match_processor import FotMobBronzeMatchProcessor
from src.storage.bronze.fotmob import FotMobBronzeStorage
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.dlq import DeadLetterQueue
from src.utils.alerting import AlertLevel, get_alert_manager
from src.utils.date_utils import (
    DATE_FORMAT_COMPACT,
    extract_year_month,
    format_date_compact_to_display,
)
from src.utils.lineage import LineageTracker
from src.utils.logging_utils import get_logger, setup_logging

# ============================================================================
# Constants
# ============================================================================

FOTMOB_TABLES = [
    "general",
    "timeline",
    "venue",
    "player",
    "shotmap",
    "goal",
    "cards",
    "red_card",
    "period",
    "momentum",
    "starters",
    "substitutes",
    "coaches",
    "team_form",
]

TABLES_HANDLED_SEPARATELY = ["starters", "substitutes", "coaches"]

TABLES_WITH_INSERTED_AT = ["starters", "substitutes"]  # coaches excluded

# Unique keys for deduplication
UNIQUE_KEY_COLUMNS = {
    "general": ["match_id"],
    "timeline": ["match_id"],
    "venue": ["match_id"],
    "player": ["match_id", "player_id"],
    "shotmap": ["match_id", "id"],
    "goal": ["match_id", "event_id"],
    "cards": ["match_id", "event_id"],
    "red_card": ["match_id", "event_id"],
    "period": ["match_id", "period"],
    "momentum": ["match_id", "minute"],
    "starters": ["match_id", "player_id"],
    "substitutes": ["match_id", "player_id"],
    "coaches": ["match_id", "id"],
    "team_form": ["match_id", "team_id", "form_position"],
}

INT64_COLUMNS = {
    "goal": ["event_id", "shot_event_id"],
    "cards": ["event_id"],
    "red_card": ["event_id"],
    "starters": ["player_id"],
    "substitutes": ["player_id"],
    "coaches": ["id"],
    "team_form": ["team_id"],
    "shotmap": ["id"],
}

INT32_RANGE = (2147483647, -2147483648)


def to_bronze_table_name(logical_table_name: str) -> str:
    """Map logical table names to physical bronze table names."""
    return f"bronze_{logical_table_name}"


# ============================================================================
# Data Classes
# ============================================================================


@dataclass
class LoadingStats:
    """Statistics for data loading."""

    tables: Dict[str, int] = field(default_factory=dict)

    def add(self, table_name: str, count: int) -> None:
        """Add count for a table."""
        self.tables[table_name] = self.tables.get(table_name, 0) + count

    def get(self, table_name: str) -> int:
        """Get count for a table."""
        return self.tables.get(table_name, 0)


# ============================================================================
# Utility Functions
# ============================================================================


def get_unique_key_columns(table_name: str) -> List[str]:
    """Get unique key columns for a table (used for deduplication)."""
    return UNIQUE_KEY_COLUMNS.get(table_name, ["match_id"])


def table_exists(client: ClickHouseClient, table_name: str, database: str = "fotmob") -> bool:
    """Check if a table exists in ClickHouse."""
    try:
        client.execute(f"DESCRIBE TABLE {database}.{table_name}")
        return True
    except Exception as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "unknown_table" in error_str:
            return False
        raise


# ============================================================================
# DataFrame Processing
# ============================================================================


def prepare_int64_columns(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Convert specified columns to Int64 type."""
    if table_name not in INT64_COLUMNS:
        return df

    for col_name in INT64_COLUMNS[table_name]:
        if col_name in df.columns:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            df[col_name] = df[col_name].astype("Int64")

    return df


def prepare_nullable_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all float columns that contain only integers to Int64."""
    for col in df.columns:
        if df[col].dtype in ["float64", "float32"]:
            try:
                non_null_values = df[col].dropna()
                if (
                    len(non_null_values) > 0
                    and (non_null_values == non_null_values.astype(int)).all()
                ):
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            except (ValueError, TypeError):
                pass
    return df


def rename_columns_for_table(
    df: pd.DataFrame, table_name: str, logger: logging.Logger
) -> pd.DataFrame:
    """Rename columns for specific tables."""
    column_renames = {}

    if table_name == "player":
        if "id" in df.columns:
            column_renames["id"] = "player_id"
        if "name" in df.columns:
            column_renames["name"] = "player_name"

    if column_renames:
        df = df.rename(columns=column_renames)
        logger.debug(f"Renamed columns for {table_name}: {column_renames}")

    return df


def check_table_has_inserted_at(client: ClickHouseClient, table_name: str, database: str) -> bool:
    """Check if table has inserted_at column."""
    try:
        table_info = client.execute(f"DESCRIBE TABLE {database}.{table_name}")
        rows = _extract_describe_rows(table_info)

        for row in rows:
            if not row or len(row) < 2:
                continue
            col_name = row[0] if isinstance(row, (list, tuple)) else str(row)
            if col_name == "inserted_at":
                return True
        return False
    except Exception:
        return False


def _extract_describe_rows(table_info) -> List:
    """Extract rows from DESCRIBE TABLE result."""
    if hasattr(table_info, "result_rows") and table_info.result_rows:
        return table_info.result_rows
    elif hasattr(table_info, "result_columns") and table_info.result_columns:
        num_cols = len(table_info.result_columns)
        num_rows = len(table_info.result_columns[0]) if table_info.result_columns[0] else 0
        return [[table_info.result_columns[i][j] for i in range(num_cols)] for j in range(num_rows)]
    elif isinstance(table_info, (list, tuple)):
        return table_info
    return []


def validate_and_fix_schema(
    df: pd.DataFrame,
    client: ClickHouseClient,
    table_name: str,
    database: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Validate DataFrame against table schema and fix issues."""
    try:
        table_info = client.execute(f"DESCRIBE TABLE {database}.{table_name}")
        rows = _extract_describe_rows(table_info)

        non_nullable_int32_cols = []
        non_nullable_string_cols = []
        nullable_int_cols = []

        for row in rows:
            if not row or len(row) < 2:
                continue
            col_name = row[0] if isinstance(row, (list, tuple)) else str(row)
            col_type = str(row[1] if isinstance(row, (list, tuple)) else row)

            if col_name not in df.columns:
                continue

            # Track nullable integer columns
            if "Nullable" in col_type and "Int" in col_type:
                nullable_int_cols.append(col_name)
            elif "Nullable" not in col_type and col_name in df.columns:
                if "Int32" in col_type:
                    non_nullable_int32_cols.append(col_name)
                elif "String" in col_type:
                    non_nullable_string_cols.append(col_name)

        int32_max, int32_min = INT32_RANGE

        # Handle non-nullable columns
        for col in non_nullable_int32_cols + non_nullable_string_cols:
            null_mask = df[col].isna()
            if null_mask.any():
                logger.error(
                    f"Found {null_mask.sum()} NULL values in non-nullable column {table_name}.{col}. Setting defaults."
                )
                df.loc[null_mask, col] = 0 if col in non_nullable_int32_cols else ""

        for col in non_nullable_int32_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            overflow_mask = (df[col] < int32_min) | (df[col] > int32_max)
            if overflow_mask.any():
                logger.error(
                    f"Found {overflow_mask.sum()} Int32 overflow values in {table_name}.{col}. Clipping."
                )
                df.loc[overflow_mask & (df[col] > int32_max), col] = int32_max
                df.loc[overflow_mask & (df[col] < int32_min), col] = int32_min

        # Handle nullable integer columns - convert all types to Int64 to properly handle NaN/None
        for col in nullable_int_cols:
            try:
                # Convert to numeric (handles strings, floats, ints, None)
                # This will turn unparseable values and None into NaN
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                # Convert to nullable Int64 type, which properly preserves NaN as <NA>
                df[col] = numeric_col.astype("Int64")
            except Exception as col_err:
                logger.debug(f"Could not convert nullable int column {col}: {col_err}")

    except Exception as e:
        logger.debug(f"Schema validation skipped for {table_name}: {e}")

    return df


def add_inserted_at_column(df: pd.DataFrame, table_has_inserted_at: bool) -> pd.DataFrame:
    """Add or remove inserted_at column based on table schema."""
    if table_has_inserted_at and "inserted_at" not in df.columns:
        df["inserted_at"] = datetime.now()
    elif not table_has_inserted_at and "inserted_at" in df.columns:
        df = df.drop(columns=["inserted_at"])
    return df


# ============================================================================
# Data Insertion
# ============================================================================


def insert_dataframe_with_dlq(
    client: ClickHouseClient,
    df: pd.DataFrame,
    table_name: str,
    database: str,
    date_str: str,
    logger: logging.Logger,
    context: Optional[Dict] = None,
) -> int:
    """Insert DataFrame with DLQ fallback for failures."""
    if df.empty:
        logger.info(f"No rows to insert for {table_name}")
        return 0

    dlq = DeadLetterQueue()

    try:
        rows_inserted = client.insert_dataframe(table_name, df, database=database)
        logger.info(f"Loaded {rows_inserted} rows into {database}.{table_name}")
        return rows_inserted
    except Exception as insert_error:
        dlq.send_to_dlq(
            table=table_name,
            data=df,
            error=str(insert_error),
            context={"date": date_str, "database": database, **(context or {})},
        )
        logger.error(f"Failed to insert {table_name}, sent to DLQ: {insert_error}")
        raise


def record_lineage(
    lineage_tracker: LineageTracker,
    scraper: str,
    date_str: str,
    table_name: str,
    rows_inserted: int,
    source_files_count: int,
    logger: logging.Logger,
) -> None:
    """Record data lineage for a table load."""
    try:
        lineage_tracker.record_load(
            scraper=scraper,
            source_id=f"batch_{date_str}",
            date=date_str,
            destination_table=table_name,
            metadata={
                "rows_inserted": rows_inserted,
                "source_files_count": source_files_count,
                "table_name": table_name,
            },
        )
    except Exception as e:
        logger.warning(f"Could not record lineage for {table_name}: {e}")


# ============================================================================
# FotMob Data Loading
# ============================================================================


def load_match_files_from_tar(
    archive_path: Path, processor: MatchProcessor, logger: logging.Logger
) -> Dict[str, List]:
    """Load match files from TAR archive."""
    all_dataframes = {}

    logger.info(f"Found TAR archive: {archive_path}")
    try:
        with tarfile.open(archive_path, "r") as tar:
            json_gz_members = [m for m in tar.getmembers() if m.name.endswith(".json.gz")]
            logger.info(f"Found {len(json_gz_members)} match files in TAR archive")

            for member in json_gz_members:
                try:
                    f = tar.extractfile(member)
                    if f:
                        with gzip.open(io.BytesIO(f.read()), "rt", encoding="utf-8") as gz:
                            file_data = json.load(gz)
                        raw_data = file_data.get("data", file_data)
                        dataframes, _ = processor.process_all(raw_data)
                        _add_processed_dataframes(dataframes, all_dataframes)
                except Exception as e:
                    logger.error(f"Error processing {member.name} from TAR: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error reading TAR archive {archive_path}: {e}", exc_info=True)

    return all_dataframes


def load_match_files_from_json_gz(
    matches_dir: Path, processor: FotMobBronzeMatchProcessor, logger: logging.Logger
) -> Dict[str, List]:
    """Load match files from JSON.gz files."""
    all_dataframes = {}
    json_gz_files = list(matches_dir.glob("match_*.json.gz"))

    if json_gz_files:
        logger.info(f"Found {len(json_gz_files)} .json.gz files")
        for json_gz_file in json_gz_files:
            try:
                with gzip.open(json_gz_file, "rt", encoding="utf-8") as f:
                    file_data = json.load(f)
                raw_data = file_data.get("data", file_data)
                dataframes, _ = processor.process_all(raw_data)
                _add_processed_dataframes(dataframes, all_dataframes)
            except Exception as e:
                logger.error(f"Error processing {json_gz_file}: {e}", exc_info=True)

    return all_dataframes


def load_match_files_from_json(
    matches_dir: Path, processor: FotMobBronzeMatchProcessor, logger: logging.Logger
) -> Dict[str, List]:
    """Load match files from JSON files."""
    all_dataframes = {}
    json_files = list(matches_dir.glob("match_*.json"))

    if json_files:
        logger.info(f"Found {len(json_files)} JSON files")
        for json_file in json_files:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    file_data = json.load(f)
                raw_data = file_data.get("data", file_data)
                dataframes, _ = processor.process_all(raw_data)
                _add_processed_dataframes(dataframes, all_dataframes)
            except Exception as e:
                logger.error(f"Error processing {json_file}: {e}", exc_info=True)

    return all_dataframes


def _add_processed_dataframes(dataframes: Dict, all_dataframes: Dict) -> None:
    """Add processed dataframes to collection."""
    for table_name, df in dataframes.items():
        if table_name not in all_dataframes:
            all_dataframes[table_name] = []
        if isinstance(df, pd.DataFrame) and not df.empty:
            all_dataframes[table_name].append(df)


def load_fotmob_match_files(
    matches_dir: Path, date_str: str, processor: FotMobBronzeMatchProcessor, logger: logging.Logger
) -> Dict[str, List]:
    """Load all FotMob match files from a directory."""
    all_dataframes = {}

    # Try TAR archive first
    archive_path = matches_dir / f"{date_str}_matches.tar"
    if archive_path.exists():
        all_dataframes = load_match_files_from_tar(archive_path, processor, logger)

    # Try JSON.gz files
    if not all_dataframes:
        all_dataframes = load_match_files_from_json_gz(matches_dir, processor, logger)

    # Try plain JSON files
    if not all_dataframes:
        all_dataframes = load_match_files_from_json(matches_dir, processor, logger)

    return all_dataframes


def process_fotmob_table(
    client: ClickHouseClient,
    table_name: str,
    df_list: List[pd.DataFrame],
    date_str: str,
    lineage_tracker: LineageTracker,
    logger: logging.Logger,
) -> int:
    """Process and load a single FotMob table."""
    if not df_list:
        return 0

    non_empty_dfs = [df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty]
    if not non_empty_dfs:
        return 0

    # Determine ClickHouse table name
    clickhouse_table = table_name
    if table_name == "cards_only":
        clickhouse_table = "cards"
    elif table_name == "lineup_data":
        return 0

    physical_table = to_bronze_table_name(clickhouse_table)

    # Check table exists
    if not table_exists(client, physical_table, database="fotmob"):
        logger.error(
            f"Table fotmob.{physical_table} does not exist. "
            f"Please run 'python scripts/setup_clickhouse.py' to create all required tables."
        )
        return 0

    # Combine dataframes
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=FutureWarning, message=".*DataFrame concatenation.*"
        )
        combined_df = pd.concat(non_empty_dfs, ignore_index=True, sort=False)

    # Process dataframe
    combined_df = rename_columns_for_table(combined_df, clickhouse_table, logger)
    combined_df = prepare_int64_columns(combined_df, clickhouse_table)
    combined_df = prepare_nullable_numeric_columns(combined_df)

    table_has_inserted_at = check_table_has_inserted_at(client, physical_table, "fotmob")
    combined_df = validate_and_fix_schema(combined_df, client, physical_table, "fotmob", logger)
    combined_df = add_inserted_at_column(combined_df, table_has_inserted_at)

    # Insert data
    try:
        rows_inserted = insert_dataframe_with_dlq(
            client,
            combined_df,
            physical_table,
            "fotmob",
            date_str,
            logger,
            {"source_files_count": len(df_list)},
        )

        record_lineage(
            lineage_tracker,
            "fotmob",
            date_str,
            physical_table,
            rows_inserted,
            len(df_list),
            logger,
        )

        return rows_inserted
    except Exception as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "unknown_table" in error_str:
            logger.error(
                f"Table fotmob.{physical_table} does not exist. "
                f"Please run 'python scripts/setup_clickhouse.py' to create all required tables."
            )
        else:
            logger.error(f"Error loading {table_name}: {e}", exc_info=True)
        return 0


def load_fotmob_data(
    client: ClickHouseClient,
    date_str: str,
    force: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, int]:
    """Load FotMob data from bronze JSON files to ClickHouse."""
    if logger is None:
        logger = get_logger()

    stats = {}
    lineage_tracker = LineageTracker()

    try:
        config = FotMobConfig()
        bronze_storage = FotMobBronzeStorage(config.storage.bronze_path)
        processor = FotMobBronzeMatchProcessor()

        matches_dir = bronze_storage.matches_dir / date_str

        if not matches_dir.exists():
            logger.warning(f"No bronze files found for date {date_str} at {matches_dir}")
            return stats

        # Load all match files
        all_dataframes = load_fotmob_match_files(matches_dir, date_str, processor, logger)

        if not all_dataframes:
            logger.warning(f"No match files found in {matches_dir}")
            return stats

        logger.info(
            f"Processed {sum(len(df_list) for df_list in all_dataframes.values())} match files for {date_str}"
        )

        # Log summary
        logger.info("Accumulated dataframes summary:")
        for table_name, df_list in all_dataframes.items():
            total_rows = sum(len(df) for df in df_list) if df_list else 0
            logger.info(f"  {table_name}: {len(df_list)} dataframes, {total_rows} total rows")

        # Process main tables
        for table_name, df_list in all_dataframes.items():
            if table_name in TABLES_HANDLED_SEPARATELY:
                continue
            stats[table_name] = process_fotmob_table(
                client, table_name, df_list, date_str, lineage_tracker, logger
            )

        # Process separately handled tables
        for table_name in TABLES_HANDLED_SEPARATELY:
            if table_name in all_dataframes and all_dataframes[table_name]:
                stats[table_name] = _process_special_fotmob_table(
                    client,
                    table_name,
                    all_dataframes[table_name],
                    date_str,
                    lineage_tracker,
                    logger,
                )

    except Exception as e:
        logger.error(f"Error loading FotMob data: {e}", exc_info=True)

    return stats


def _process_special_fotmob_table(
    client: ClickHouseClient,
    table_name: str,
    df_list: List[pd.DataFrame],
    date_str: str,
    lineage_tracker: LineageTracker,
    logger: logging.Logger,
) -> int:
    """Process special FotMob tables (starters, substitutes, coaches)."""
    non_empty_dfs = [df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty]
    if not non_empty_dfs:
        return 0

    # Check table exists
    try:
        physical_table = to_bronze_table_name(table_name)
        client.execute(f"DESCRIBE TABLE fotmob.{physical_table}")
    except Exception as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "unknown_table" in error_str:
            logger.error(
                f"Table fotmob.{physical_table} does not exist. "
                f"Please run 'python scripts/setup_clickhouse.py' to create all required tables."
            )
            return 0
        raise

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", category=FutureWarning, message=".*DataFrame concatenation.*"
        )
        combined_df = pd.concat(non_empty_dfs, ignore_index=True, sort=False)

    combined_df = prepare_int64_columns(combined_df, table_name)
    combined_df = prepare_nullable_numeric_columns(combined_df)
    combined_df = validate_and_fix_schema(combined_df, client, physical_table, "fotmob", logger)

    # Add inserted_at only for tables that have it
    if table_name in TABLES_WITH_INSERTED_AT and "inserted_at" not in combined_df.columns:
        combined_df["inserted_at"] = datetime.now()

    if combined_df.empty:
        logger.info(f"No rows to insert for {table_name}")
        return 0

    try:
        rows_inserted = insert_dataframe_with_dlq(
            client, combined_df, physical_table, "fotmob", date_str, logger
        )

        record_lineage(
            lineage_tracker,
            "fotmob",
            date_str,
            physical_table,
            rows_inserted,
            len(non_empty_dfs),
            logger,
        )

        return rows_inserted
    except Exception:
        return 0


# ============================================================================
# Date Range Generation
# ============================================================================


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
    """Generate all dates in a month."""
    year = int(month_str[:4])
    month = int(month_str[4:6])

    _, last_day = monthrange(year, month)
    return [f"{year}{month:02d}{day:02d}" for day in range(1, last_day + 1)]


# ============================================================================
# Argument Parsing
# ============================================================================


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Load data from bronze layer JSON files into ClickHouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load FotMob data for a date
  python scripts/load_clickhouse.py --scraper fotmob --date 20251113
  
  # Load date range
  python scripts/load_clickhouse.py --scraper fotmob --start-date 20251101 --end-date 20251107
  
  # Load entire month
  python scripts/load_clickhouse.py --scraper fotmob --month 202511
  
  # Show table statistics
  python scripts/load_clickhouse.py --scraper fotmob --stats
  
  # Truncate and reload
  python scripts/load_clickhouse.py --scraper fotmob --date 20251113 --truncate
        """,
    )

    _add_scraper_argument(parser)
    _add_date_arguments(parser)
    _add_connection_arguments(parser)
    _add_option_arguments(parser)

    return parser


def _add_scraper_argument(parser: argparse.ArgumentParser) -> None:
    """Add scraper argument."""
    parser.add_argument(
        "--scraper",
        type=str,
        choices=["fotmob"],
        required=True,
        help="Scraper to load data for (fotmob)",
    )


def _add_date_arguments(parser: argparse.ArgumentParser) -> None:
    """Add date-related arguments."""
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--date", type=str, help="Date to load (YYYYMMDD format)")
    date_group.add_argument(
        "--start-date", type=str, help="Start date for range loading (YYYYMMDD format)"
    )
    date_group.add_argument("--month", type=str, help="Load entire month (YYYYMM format)")

    parser.add_argument("--end-date", type=str, help="End date for range loading (YYYYMMDD format)")


def _add_connection_arguments(parser: argparse.ArgumentParser) -> None:
    """Add ClickHouse connection arguments."""
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("CLICKHOUSE_HOST", "localhost"),
        help="ClickHouse host",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        help="ClickHouse HTTP port",
    )
    parser.add_argument(
        "--username",
        type=str,
        default=os.getenv("CLICKHOUSE_USER", "fotmob_user"),
        help="ClickHouse username",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=os.getenv("CLICKHOUSE_PASSWORD", "fotmob_pass"),
        help="ClickHouse password",
    )


def _add_option_arguments(parser: argparse.ArgumentParser) -> None:
    """Add option arguments."""
    parser.add_argument("--truncate", action="store_true", help="Truncate tables before loading")
    parser.add_argument("--stats", action="store_true", help="Show table statistics and exit")
    parser.add_argument("--force", action="store_true", help="Force reload even if data exists")


def validate_arguments(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Validate parsed arguments."""
    if args.month:
        if len(args.month) != 6 or not args.month.isdigit():
            parser.error(f"Invalid month format: {args.month}. Use YYYYMM")

        year, month = int(args.month[:4]), int(args.month[4:6])
        if not (1 <= month <= 12):
            parser.error(f"Invalid month: {month}")

        if args.end_date:
            parser.error("Cannot use --end-date with --month option")


# ============================================================================
# Main Execution
# ============================================================================


def show_statistics(client: ClickHouseClient, database: str, logger: logging.Logger) -> None:
    """Show table statistics."""
    logger.info(f"\n=== {database.upper()} Database Statistics ===\n")
    tables = FOTMOB_TABLES

    for table in tables:
        physical_table = to_bronze_table_name(table)
        stats = client.get_table_stats(physical_table, database=database)
        if "error" not in stats:
            logger.info(
                f"{physical_table}: {stats.get('row_count', 0):,} rows, {stats.get('size', '0 B')}"
            )
        else:
            logger.warning(f"{physical_table}: {stats.get('error', 'Unknown error')}")


def get_dates_to_process(args: argparse.Namespace, logger: logging.Logger) -> List[str]:
    """Get list of dates to process from arguments."""
    if args.month:
        dates = generate_month_dates(args.month)
        year, month = extract_year_month(args.month)
        month_names = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        month_name = month_names[int(month) - 1]
        logger.info(f"\n{'='*80}")
        logger.info(f"Monthly Loading Mode: Month: {month_name} {year} ({args.month})")
        logger.info(f"Total dates: {len(dates)}")
        logger.info(f"{'='*80}\n")
        return dates
    elif args.start_date:
        return generate_date_range(args.start_date, args.end_date)
    elif args.date:
        return [args.date]
    return []


def main():
    """Main entry point."""
    parser = create_argument_parser()
    args = parser.parse_args()

    validate_arguments(parser, args)

    database = args.scraper

    # Determine date suffix for logging
    if args.stats:
        date_suffix = None
    elif args.month:
        date_suffix = args.month
    elif args.start_date:
        date_suffix = f"{args.start_date}_to_{args.end_date}"
    elif args.date:
        date_suffix = args.date
    else:
        date_suffix = None

    logger = setup_logging(
        name="clickhouse_loader",
        log_dir="logs",
        log_level="INFO",
        date_suffix=date_suffix,
    )

    # Connect to ClickHouse
    client = ClickHouseClient(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        database=database,
    )

    if not client.connect():
        logger.error("Failed to connect to ClickHouse")
        sys.exit(1)

    try:
        # Show statistics if requested
        if args.stats:
            show_statistics(client, database, logger)
            return

        # Get dates to process
        dates = get_dates_to_process(args, logger)
        if not dates:
            parser.error("Either --date, --start-date, or --month must be provided")

        # Truncate if requested
        if args.truncate:
            logger.warning("Truncating tables before loading...")
            tables = FOTMOB_TABLES
            for table in tables:
                try:
                    client.truncate_table(to_bronze_table_name(table), database=database)
                except Exception as e:
                    logger.warning(f"Could not truncate {table}: {e}")

        # Load data for each date
        total_stats: Dict[str, int] = {}
        for date_str in dates:
            logger.info(f"\n{'='*80}")
            logger.info(f"Loading {database} data for {date_str}")
            logger.info(f"{'='*80}\n")

            try:
                if database == "fotmob":
                    stats = load_fotmob_data(client, date_str, args.force, logger)
                else:
                    logger.error(f"Unknown scraper: {database}")
                    continue

                for table, count in stats.items():
                    total_stats[table] = total_stats.get(table, 0) + count
            except Exception as e:
                logger.error(f"Failed to load {database} data for {date_str}: {e}", exc_info=True)
                alert_manager = get_alert_manager()
                alert_manager.send_alert(
                    level=AlertLevel.ERROR,
                    title=f"ClickHouse Loading Failed - {database.upper()} - {date_str}",
                    message=f"Failed to load {database} data to ClickHouse for date {date_str}.\n\nError: {str(e)}",
                    context={
                        "date": date_str,
                        "scraper": database,
                        "step": f"ClickHouse Loading - {database}",
                        "error": str(e),
                    },
                )

        # Print summary
        logger.info(f"\n{'='*80}")
        logger.info("LOADING SUMMARY")
        logger.info(f"{'='*80}\n")
        logger.info(f"Dates processed: {len(dates)}")
        logger.info("Total rows loaded by table:")
        for table, count in sorted(total_stats.items()):
            logger.info(f"  {table}: {count:,} rows")

    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
