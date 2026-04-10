"""Run the scenario_one_man_army gold query against ClickHouse."""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(project_root))

from config.settings import settings
from src.storage.clickhouse_client import ClickHouseClient
from src.utils.logging_utils import get_logger

logger = get_logger()


SQL_FILE = project_root / "clickhouse" / "gold" / "scenario" / "scenario_one_man_army.sql"
TARGET_TABLE = "gold.scenario_one_man_army"


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one-man-army scenario query from gold SQL folder"
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    parse_args(argv)

    if not SQL_FILE.exists():
        logger.error("SQL file not found: %s", SQL_FILE)
        return 1

    insert_query = SQL_FILE.read_text(encoding="utf-8").strip().rstrip(";")

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
        client.execute(insert_query)
        logger.info("scenario_one_man_army insert completed successfully")

        optimize_sql = f"OPTIMIZE TABLE {TARGET_TABLE} FINAL DEDUPLICATE"
        client.execute(optimize_sql)
        logger.info("Optimization completed for %s", TARGET_TABLE)

        return 0
    finally:
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
