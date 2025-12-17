"""Load data from bronze layer JSON files into ClickHouse data warehouse.

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

    Note:
    Table optimization is handled separately via SQL scripts.
    Run clickhouse/init/03_optimize_tables.sql to optimize and deduplicate tables.
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
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Set

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.bronze_storage import BronzeStorage as FotMobBronzeStorage
from src.storage.aiscore_storage import AIScoreBronzeStorage
from src.processors.match_processor import MatchProcessor
from src.config.fotmob_config import FotMobConfig
from src.config.aiscore_config import AIScoreConfig
from src.utils.logging_utils import get_logger, setup_logging
from src.utils.lineage import LineageTracker
from src.utils.date_utils import (
    format_date_compact_to_display,
    extract_year_month,
    DATE_FORMAT_COMPACT,
)
from src.utils.alerting import get_alert_manager, AlertLevel
from src.storage.dlq import DeadLetterQueue


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

AISCORE_TABLES = [
    "matches",
    "odds_1x2",
    "odds_asian_handicap",
    "odds_over_under",
    "daily_listings",
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
    "matches": ["game_date", "match_id"],
    "odds_1x2": ["game_date", "match_id", "scraped_at", "bookmaker"],
    "odds_asian_handicap": [
        "game_date",
        "match_id",
        "scraped_at",
        "home_handicap",
        "away_handicap",
    ],
    "odds_over_under": [
        "game_date",
        "match_id",
        "scraped_at",
        "bookmaker",
        "total_line",
        "market_type",
    ],
    "daily_listings": ["scrape_date"],
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


def table_exists(
    client: ClickHouseClient, table_name: str, database: str = "fotmob"
) -> bool:
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


def check_table_has_inserted_at(
    client: ClickHouseClient, table_name: str, database: str
) -> bool:
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
        num_rows = (
            len(table_info.result_columns[0]) if table_info.result_columns[0] else 0
        )
        return [
            [table_info.result_columns[i][j] for i in range(num_cols)]
            for j in range(num_rows)
        ]
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

        for row in rows:
            if not row or len(row) < 2:
                continue
            col_name = row[0] if isinstance(row, (list, tuple)) else str(row)
            col_type = str(row[1] if isinstance(row, (list, tuple)) else row)

            if "Nullable" not in col_type and col_name in df.columns:
                if "Int32" in col_type:
                    non_nullable_int32_cols.append(col_name)
                elif "String" in col_type:
                    non_nullable_string_cols.append(col_name)

        int32_max, int32_min = INT32_RANGE

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

    except Exception as e:
        logger.debug(f"Schema validation skipped for {table_name}: {e}")

    return df


def add_inserted_at_column(
    df: pd.DataFrame, table_has_inserted_at: bool
) -> pd.DataFrame:
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
            json_gz_members = [
                m for m in tar.getmembers() if m.name.endswith(".json.gz")
            ]
            logger.info(f"Found {len(json_gz_members)} match files in TAR archive")

            for member in json_gz_members:
                try:
                    f = tar.extractfile(member)
                    if f:
                        with gzip.open(
                            io.BytesIO(f.read()), "rt", encoding="utf-8"
                        ) as gz:
                            file_data = json.load(gz)
                        raw_data = file_data.get("data", file_data)
                        dataframes, _ = processor.process_all(raw_data)
                        _add_processed_dataframes(dataframes, all_dataframes)
                except Exception as e:
                    logger.error(
                        f"Error processing {member.name} from TAR: {e}", exc_info=True
                    )
    except Exception as e:
        logger.error(f"Error reading TAR archive {archive_path}: {e}", exc_info=True)

    return all_dataframes


def load_match_files_from_json_gz(
    matches_dir: Path, processor: MatchProcessor, logger: logging.Logger
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
    matches_dir: Path, processor: MatchProcessor, logger: logging.Logger
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
    matches_dir: Path, date_str: str, processor: MatchProcessor, logger: logging.Logger
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

    non_empty_dfs = [
        df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty
    ]
    if not non_empty_dfs:
        return 0

    # Determine ClickHouse table name
    clickhouse_table = table_name
    if table_name == "cards_only":
        clickhouse_table = "cards"
    elif table_name == "lineup_data":
        return 0

    # Check table exists
    if not table_exists(client, clickhouse_table, database="fotmob"):
        logger.error(
            f"Table fotmob.{clickhouse_table} does not exist. "
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

    table_has_inserted_at = check_table_has_inserted_at(
        client, clickhouse_table, "fotmob"
    )
    combined_df = validate_and_fix_schema(
        combined_df, client, clickhouse_table, "fotmob", logger
    )
    combined_df = add_inserted_at_column(combined_df, table_has_inserted_at)

    # Insert data
    try:
        rows_inserted = insert_dataframe_with_dlq(
            client,
            combined_df,
            clickhouse_table,
            "fotmob",
            date_str,
            logger,
            {"source_files_count": len(df_list)},
        )

        record_lineage(
            lineage_tracker,
            "fotmob",
            date_str,
            clickhouse_table,
            rows_inserted,
            len(df_list),
            logger,
        )

        return rows_inserted
    except Exception as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "unknown_table" in error_str:
            logger.error(
                f"Table fotmob.{clickhouse_table} does not exist. "
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
        processor = MatchProcessor()

        matches_dir = bronze_storage.matches_dir / date_str

        if not matches_dir.exists():
            logger.warning(
                f"No bronze files found for date {date_str} at {matches_dir}"
            )
            return stats

        # Load all match files
        all_dataframes = load_fotmob_match_files(
            matches_dir, date_str, processor, logger
        )

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
            logger.info(
                f"  {table_name}: {len(df_list)} dataframes, {total_rows} total rows"
            )

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
    non_empty_dfs = [
        df for df in df_list if isinstance(df, pd.DataFrame) and not df.empty
    ]
    if not non_empty_dfs:
        return 0

    # Check table exists
    try:
        client.execute(f"DESCRIBE TABLE fotmob.{table_name}")
    except Exception as e:
        error_str = str(e).lower()
        if "does not exist" in error_str or "unknown_table" in error_str:
            logger.error(
                f"Table fotmob.{table_name} does not exist. "
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

    # Add inserted_at only for tables that have it
    if (
        table_name in TABLES_WITH_INSERTED_AT
        and "inserted_at" not in combined_df.columns
    ):
        combined_df["inserted_at"] = datetime.now()

    if combined_df.empty:
        logger.info(f"No rows to insert for {table_name}")
        return 0

    try:
        rows_inserted = insert_dataframe_with_dlq(
            client, combined_df, table_name, "fotmob", date_str, logger
        )

        record_lineage(
            lineage_tracker,
            "fotmob",
            date_str,
            table_name,
            rows_inserted,
            len(non_empty_dfs),
            logger,
        )

        return rows_inserted
    except Exception:
        return 0


# ============================================================================
# AIScore Data Loading
# ============================================================================


def verify_aiscore_tables(client: ClickHouseClient, logger: logging.Logger) -> None:
    """Verify all required AIScore tables exist."""
    missing_tables = []

    for table in AISCORE_TABLES:
        try:
            client.execute(f"SELECT 1 FROM aiscore.{table} LIMIT 1")
        except Exception as e:
            error_str = str(e).lower()
            if any(
                keyword in error_str
                for keyword in [
                    "doesn't exist",
                    "does not exist",
                    "unknown table",
                    "table not found",
                ]
            ):
                missing_tables.append(table)

    if missing_tables:
        logger.error(f"AIScore tables are missing: {', '.join(missing_tables)}")
        logger.error("Please run the table creation script first:")
        logger.error("  docker-compose exec scraper python scripts/setup_clickhouse.py")
        raise RuntimeError(
            f"Missing AIScore tables: {', '.join(missing_tables)}. Run setup_clickhouse.py first."
        )


def parse_game_date(game_date: Any, default_date: str) -> date:
    """Parse game date from various formats."""
    if isinstance(game_date, str):
        if len(game_date) == 8 and game_date.isdigit():
            return datetime.strptime(game_date, DATE_FORMAT_COMPACT).date()
        elif len(game_date) == 10 and "-" in game_date:
            return date.fromisoformat(game_date)
        else:
            return datetime.strptime(game_date, DATE_FORMAT_COMPACT).date()
    return game_date


def load_aiscore_matches_data(
    matches_list: List[Dict],
    date_str: str,
    bronze_storage: AIScoreBronzeStorage,
    logger: logging.Logger,
) -> tuple[List[Dict], List[Dict], List[Dict], List[Dict], Dict[str, Path]]:
    """Load match data and extract odds information."""
    scrape_date_obj = datetime.strptime(date_str, DATE_FORMAT_COMPACT).date()

    matches_data = []
    odds_1x2_data = []
    odds_asian_handicap_data = []
    odds_over_under_data = []
    match_file_paths = {}

    for match in matches_list:
        match_id = match.get("match_id")
        if not match_id:
            continue

        game_date = match.get("game_date", date_str)
        match_data = bronze_storage.load_raw_match_data(match_id, date_str)

        if not match_data:
            match_data = _create_minimal_match_data(match, game_date, date_str)
            if not match_data:
                logger.debug(f"Match data not found for {match_id}, skipping")
                continue

        data = match_data.get("data", {})

        # Track source file path
        odds_path = bronze_storage.matches_dir / date_str / f"match_{match_id}.json"
        if odds_path.exists():
            match_file_paths[match_id] = odds_path

        game_date_obj = parse_game_date(game_date, date_str)

        # Create match record
        match_record = _create_match_record(
            match, match_data, data, game_date_obj, scrape_date_obj
        )
        matches_data.append(match_record)

        # Extract odds data
        odds_1x2_list = data.get("odds_1x2", [])
        odds_ah_list = data.get("odds_asian_handicap", [])
        odds_ou_list = data.get("odds_over_under", [])

        logger.debug(
            f"Match {match_id}: odds_1x2={len(odds_1x2_list)}, odds_asian_handicap={len(odds_ah_list)}, odds_over_under={len(odds_ou_list)}"
        )

        if odds_1x2_list or odds_ah_list or odds_ou_list:
            logger.info(
                f"Found odds for match {match_id}: {len(odds_1x2_list)} 1X2, {len(odds_ah_list)} AH, {len(odds_ou_list)} OU"
            )

            _extract_odds_records(
                match_id,
                data,
                game_date_obj,
                scrape_date_obj,
                odds_1x2_list,
                odds_ah_list,
                odds_ou_list,
                odds_1x2_data,
                odds_asian_handicap_data,
                odds_over_under_data,
            )
        else:
            logger.debug(f"No odds data found for match {match_id}")

    return (
        matches_data,
        odds_1x2_data,
        odds_asian_handicap_data,
        odds_over_under_data,
        match_file_paths,
    )


def _create_minimal_match_data(
    match: Dict, game_date: str, date_str: str
) -> Optional[Dict]:
    """Create minimal match data from daily listing."""
    if "match_url" not in match and "teams" not in match:
        return None

    data = {
        "match_id": match.get("match_id"),
        "match_url": match.get("match_url", ""),
        "game_date": game_date,
        "scrape_status": match.get("scrape_status", "links_only"),
        "teams": match.get("teams", {}),
        "league": match.get("league", {}),
        "odds_1x2": [],
        "odds_asian_handicap": [],
        "odds_over_under": [],
    }
    return {
        "data": data,
        "scraped_at": match.get("scrape_timestamp") or datetime.now().isoformat(),
    }


def _create_match_record(
    match: Dict,
    match_data: Dict,
    data: Dict,
    game_date_obj: date,
    scrape_date_obj: date,
) -> Dict:
    """Create a match record for insertion."""
    odds_counts = data.get("odds_counts", {})

    return {
        "match_id": match.get("match_id"),
        "match_url": data.get("match_url", match.get("match_url", "")),
        "game_date": game_date_obj,
        "scrape_date": scrape_date_obj,
        "scrape_timestamp": (
            datetime.fromisoformat(match_data.get("scraped_at"))
            if match_data.get("scraped_at")
            else datetime.now()
        ),
        "scrape_status": data.get(
            "scrape_status", match.get("scrape_status", "unknown")
        ),
        "scrape_duration": data.get("scrape_duration"),
        "home_team": data.get("teams", match.get("teams", {})).get("home", ""),
        "away_team": data.get("teams", match.get("teams", {})).get("away", ""),
        "match_result": data.get("match_result"),
        "league": data.get("league") or match.get("league"),
        "country": match.get("country"),
        "odds_1x2_count": odds_counts.get("odds_1x2", 0),
        "odds_asian_handicap_count": odds_counts.get("odds_asian_handicap", 0),
        "odds_over_under_goals_count": odds_counts.get("odds_over_under_goals", 0),
        "odds_over_under_corners_count": odds_counts.get("odds_over_under_corners", 0),
        "total_odds_count": odds_counts.get("total_odds", 0),
        "links_scraping_complete": 0,
        "links_scraping_completed_at": None,
        "odds_scraping_complete": 0,
        "odds_scraping_completed_at": None,
    }


def _extract_odds_records(
    match_id: str,
    data: Dict,
    game_date_obj: date,
    scrape_date_obj: date,
    odds_1x2_list: List,
    odds_ah_list: List,
    odds_ou_list: List,
    odds_1x2_data: List,
    odds_asian_handicap_data: List,
    odds_over_under_data: List,
) -> None:
    """Extract odds records from odds lists."""
    match_url = data.get("match_url", "")

    for odds in odds_1x2_list:
        odds_1x2_data.append(
            {
                "match_id": match_id,
                "match_url": odds.get("match_url", match_url),
                "game_date": game_date_obj,
                "scrape_date": scrape_date_obj,
                "bookmaker": odds.get("bookmaker"),
                "home_odds": odds.get("home_odds"),
                "draw_odds": odds.get("draw_odds"),
                "away_odds": odds.get("away_odds"),
                "scraped_at": (
                    datetime.fromisoformat(odds.get("scraped_at"))
                    if odds.get("scraped_at")
                    else datetime.now()
                ),
            }
        )

    for odds in odds_ah_list:
        odds_asian_handicap_data.append(
            {
                "match_id": match_id,
                "match_url": odds.get("match_url", match_url),
                "game_date": game_date_obj,
                "scrape_date": scrape_date_obj,
                "match_time": odds.get("match_time"),
                "moment_result": odds.get("moment_result"),
                "home_handicap": odds.get("home_handicap"),
                "home_odds": odds.get("home_odds"),
                "away_handicap": odds.get("away_handicap"),
                "away_odds": odds.get("away_odds"),
                "scraped_at": (
                    datetime.fromisoformat(odds.get("scraped_at"))
                    if odds.get("scraped_at")
                    else datetime.now()
                ),
            }
        )

    for odds in odds_ou_list:
        odds_over_under_data.append(
            {
                "match_id": match_id,
                "match_url": odds.get("match_url", match_url),
                "game_date": game_date_obj,
                "scrape_date": scrape_date_obj,
                "bookmaker": odds.get("bookmaker"),
                "total_line": odds.get("total_line"),
                "over_odds": odds.get("over_odds"),
                "under_odds": odds.get("under_odds"),
                "market_type": odds.get("market_type", "goals"),
                "scraped_at": (
                    datetime.fromisoformat(odds.get("scraped_at"))
                    if odds.get("scraped_at")
                    else datetime.now()
                ),
            }
        )


def insert_aiscore_table(
    client: ClickHouseClient,
    data_list: List[Dict],
    table_name: str,
    date_str: str,
    lineage_tracker: LineageTracker,
    match_file_paths: Dict[str, Path],
    logger: logging.Logger,
    timestamp_column: str = "scraped_at",
) -> int:
    """Insert data into an AIScore table."""
    if not data_list:
        logger.info(f"No {table_name} data to insert")
        return 0

    df = pd.DataFrame(data_list)

    # Add inserted_at column
    if "inserted_at" not in df.columns:
        if timestamp_column in df.columns:
            df["inserted_at"] = pd.to_datetime(df[timestamp_column])
        else:
            df["inserted_at"] = datetime.now()

    if df.empty:
        logger.info(f"No new {table_name} to insert")
        return 0

    try:
        rows_inserted = insert_dataframe_with_dlq(
            client, df, table_name, "aiscore", date_str, logger
        )

        # Record lineage
        _record_aiscore_lineage(
            data_list, table_name, date_str, lineage_tracker, match_file_paths, logger
        )

        return rows_inserted
    except Exception:
        return 0


def _record_aiscore_lineage(
    data_list: List[Dict],
    table_name: str,
    date_str: str,
    lineage_tracker: LineageTracker,
    match_file_paths: Dict[str, Path],
    logger: logging.Logger,
) -> None:
    """Record lineage for AIScore data."""
    processed_matches: Set[str] = set()

    for record in data_list:
        match_id = record.get("match_id")
        if match_id and match_id not in processed_matches:
            processed_matches.add(match_id)
            source_path = match_file_paths.get(match_id)
            try:
                scrape_lineage = lineage_tracker.get_lineage(
                    "aiscore", date_str, match_id
                )
                parent_ids = [
                    l.lineage_id for l in scrape_lineage if l.transformation == "scrape"
                ]

                lineage_tracker.record_load(
                    scraper="aiscore",
                    source_id=match_id,
                    date=date_str,
                    destination_table=table_name,
                    destination_id=match_id,
                    source_path=source_path,
                    parent_lineage_ids=parent_ids,
                    metadata={"table": table_name},
                )
            except Exception as e:
                logger.warning(
                    f"Could not record lineage for {table_name} match {match_id}: {e}"
                )


def load_aiscore_data(
    client: ClickHouseClient,
    date_str: str,
    force: bool = False,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, int]:
    """Load AIScore data from Bronze JSON.gz files to ClickHouse."""
    if logger is None:
        logger = get_logger()

    stats = {}
    lineage_tracker = LineageTracker()

    try:
        verify_aiscore_tables(client, logger)

        config = AIScoreConfig()
        bronze_storage = AIScoreBronzeStorage(config.storage.bronze_path)

        # Load daily listings
        matches_list = _load_aiscore_daily_listings(bronze_storage, date_str, logger)
        if not matches_list:
            return stats

        logger.info(f"Found {len(matches_list)} matches for {date_str}")

        # Load match data
        matches_data, odds_1x2_data, odds_ah_data, odds_ou_data, match_file_paths = (
            load_aiscore_matches_data(matches_list, date_str, bronze_storage, logger)
        )

        # Insert matches
        if matches_data:
            stats["matches"] = insert_aiscore_table(
                client,
                matches_data,
                "matches",
                date_str,
                lineage_tracker,
                match_file_paths,
                logger,
                "scrape_timestamp",
            )
        else:
            logger.warning("No matches data to insert")

        # Insert odds tables
        stats["odds_1x2"] = insert_aiscore_table(
            client,
            odds_1x2_data,
            "odds_1x2",
            date_str,
            lineage_tracker,
            match_file_paths,
            logger,
        )
        stats["odds_asian_handicap"] = insert_aiscore_table(
            client,
            odds_ah_data,
            "odds_asian_handicap",
            date_str,
            lineage_tracker,
            match_file_paths,
            logger,
        )
        stats["odds_over_under"] = insert_aiscore_table(
            client,
            odds_ou_data,
            "odds_over_under",
            date_str,
            lineage_tracker,
            match_file_paths,
            logger,
        )

        # Insert daily listings
        stats["daily_listings"] = _insert_daily_listings(
            client, date_str, matches_list, bronze_storage, logger
        )

    except Exception as e:
        logger.error(f"Error loading AIScore data: {e}", exc_info=True)

    return stats


def _load_aiscore_daily_listings(
    bronze_storage: AIScoreBronzeStorage, date_str: str, logger: logging.Logger
) -> List[Dict]:
    """Load daily listings from bronze storage."""
    daily_listing = bronze_storage.load_daily_listing(date_str)

    if not daily_listing:
        daily_data = bronze_storage.read_daily_list(date_str)
        if not daily_data:
            logger.warning(f"No daily listings found for {date_str}")
            return []
        return daily_data.get("matches", [])

    if "matches" in daily_listing:
        matches = daily_listing.get("matches", [])
        logger.info(f"Using matches array from daily listing ({len(matches)} matches)")
        return matches

    if "match_ids" in daily_listing:
        match_ids = daily_listing.get("match_ids", [])
        if not match_ids:
            logger.warning(f"No match IDs found in daily listings for {date_str}")
            return []

        matches_list = []
        for match_id in match_ids:
            match_data = bronze_storage.load_raw_match_data(str(match_id), date_str)
            if match_data:
                data = match_data.get("data", {})
                matches_list.append(
                    {
                        "match_id": str(match_id),
                        "match_url": data.get("match_url", ""),
                        "game_date": data.get("game_date", date_str),
                        "scrape_timestamp": match_data.get("scraped_at"),
                        "scrape_status": data.get("scrape_status", "unknown"),
                        "teams": data.get("teams", {}),
                        "league": data.get("league"),
                    }
                )
            else:
                matches_list.append(
                    {
                        "match_id": str(match_id),
                        "game_date": date_str,
                        "scrape_status": "unknown",
                    }
                )
        return matches_list

    logger.warning(f"Daily listing for {date_str} has unexpected format")
    return []


def _insert_daily_listings(
    client: ClickHouseClient,
    date_str: str,
    matches_list: List[Dict],
    bronze_storage: AIScoreBronzeStorage,
    logger: logging.Logger,
) -> int:
    """Insert daily listings record."""
    scrape_date_obj = datetime.strptime(date_str, DATE_FORMAT_COMPACT).date()
    daily_listing_meta = bronze_storage.load_daily_listing(date_str)

    if not daily_listing_meta:
        daily_listing_meta = bronze_storage.read_daily_list(date_str) or {}

    daily_listings_record = {
        "scrape_date": scrape_date_obj,
        "total_matches": len(matches_list),
        "links_scraping_complete": (
            1 if daily_listing_meta.get("links_scraping_complete", False) else 0
        ),
        "links_scraping_completed_at": (
            datetime.fromisoformat(daily_listing_meta["links_scraping_completed_at"])
            if daily_listing_meta.get("links_scraping_completed_at")
            else None
        ),
        "odds_scraping_complete": (
            1 if daily_listing_meta.get("odds_scraping_complete", False) else 0
        ),
        "odds_scraping_completed_at": (
            datetime.fromisoformat(daily_listing_meta["odds_scraping_completed_at"])
            if daily_listing_meta.get("odds_scraping_completed_at")
            else None
        ),
    }

    daily_df = pd.DataFrame([daily_listings_record])

    if "inserted_at" not in daily_df.columns:
        if (
            "links_scraping_completed_at" in daily_df.columns
            and daily_df["links_scraping_completed_at"].notna().any()
        ):
            daily_df["inserted_at"] = pd.to_datetime(
                daily_df["links_scraping_completed_at"]
            )
        elif (
            "odds_scraping_completed_at" in daily_df.columns
            and daily_df["odds_scraping_completed_at"].notna().any()
        ):
            daily_df["inserted_at"] = pd.to_datetime(
                daily_df["odds_scraping_completed_at"]
            )
        else:
            daily_df["inserted_at"] = datetime.now()

    if daily_df.empty:
        logger.info("No new daily listings to insert")
        return 0

    try:
        rows_inserted = insert_dataframe_with_dlq(
            client, daily_df, "daily_listings", "aiscore", date_str, logger
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
        choices=["fotmob", "aiscore"],
        required=True,
        help="Scraper to load data for (fotmob or aiscore)",
    )


def _add_date_arguments(parser: argparse.ArgumentParser) -> None:
    """Add date-related arguments."""
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--date", type=str, help="Date to load (YYYYMMDD format)")
    date_group.add_argument(
        "--start-date", type=str, help="Start date for range loading (YYYYMMDD format)"
    )
    date_group.add_argument(
        "--month", type=str, help="Load entire month (YYYYMM format)"
    )

    parser.add_argument(
        "--end-date", type=str, help="End date for range loading (YYYYMMDD format)"
    )


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
    parser.add_argument(
        "--truncate", action="store_true", help="Truncate tables before loading"
    )
    parser.add_argument(
        "--stats", action="store_true", help="Show table statistics and exit"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force reload even if data exists"
    )


def validate_arguments(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> None:
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


def show_statistics(
    client: ClickHouseClient, database: str, logger: logging.Logger
) -> None:
    """Show table statistics."""
    logger.info(f"\n=== {database.upper()} Database Statistics ===\n")
    tables = FOTMOB_TABLES if database == "fotmob" else AISCORE_TABLES

    for table in tables:
        stats = client.get_table_stats(table, database=database)
        if "error" not in stats:
            logger.info(
                f"{table}: {stats.get('row_count', 0):,} rows, {stats.get('size', '0 B')}"
            )
        else:
            logger.warning(f"{table}: {stats.get('error', 'Unknown error')}")


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
            tables = FOTMOB_TABLES if database == "fotmob" else AISCORE_TABLES
            for table in tables:
                try:
                    client.truncate_table(table, database=database)
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
                    stats = load_aiscore_data(client, date_str, args.force, logger)

                for table, count in stats.items():
                    total_stats[table] = total_stats.get(table, 0) + count
            except Exception as e:
                logger.error(
                    f"Failed to load {database} data for {date_str}: {e}", exc_info=True
                )
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