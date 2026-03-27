"""Process FotMob silver layer in ClickHouse."""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
scripts_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from config.settings import settings
from src.processors.silver.fotmob import FotMobSilverProcessor
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.silver.fotmob import FotMobSilverStorage
from src.utils.logging_utils import get_logger
from utils.script_utils import validate_date_format

logger = get_logger()


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run silver transformations for FotMob")
    parser.add_argument("--date", type=str, help="Optional date (YYYYMMDD) for logging/context")
    parser.add_argument("--month", type=str, help="Optional month (YYYYMM) for logging/context")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.date:
        is_valid, error_msg = validate_date_format(args.date, "YYYYMMDD")
        if not is_valid:
            logger.error(error_msg)
            return 1
    if args.month:
        is_valid, error_msg = validate_date_format(args.month, "YYYYMM")
        if not is_valid:
            logger.error(error_msg)
            return 1
    if args.date:
        logger.info("Running silver transformations for date=%s", args.date)
    if args.month:
        logger.info("Running silver transformations for month=%s", args.month)

    client = ClickHouseClient(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database="default",
    )

    if not client.connect():
        logger.error("Failed to connect to ClickHouse")
        return 1

    try:
        sql_dir = project_root / "clickhouse" / "silver"
        processor = FotMobSilverProcessor(sql_dir=sql_dir)
        storage = FotMobSilverStorage(client, database=settings.clickhouse_db_fotmob)

        sql_files = processor.sql_files()
        if not sql_files:
            logger.error("No silver SQL files found in %s", sql_dir)
            return 1

        storage.execute_sql_files(sql_files)
        logger.info("Silver processing completed successfully")
        return 0
    finally:
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
