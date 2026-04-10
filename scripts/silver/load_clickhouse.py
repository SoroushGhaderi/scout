"""Load FotMob silver data into ClickHouse tables."""

import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
scripts_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from config.settings import settings
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.clickhouse_sql_executor import split_sql_statements
from src.utils.logging_utils import get_logger

logger = get_logger()


def _load_jobs() -> list[tuple[str, str]]:
    load_sql_dir = project_root / "clickhouse" / "silver" / "load"
    jobs: list[tuple[str, str]] = []
    for sql_path in sorted(path for path in load_sql_dir.glob("*.sql") if path.is_file()):
        stem_parts = sql_path.stem.split("_", 1)
        if len(stem_parts) != 2:
            logger.warning("Skipping silver load SQL with unexpected name: %s", sql_path.name)
            continue
        table_name = stem_parts[1]
        jobs.append((sql_path.name, f"silver.{table_name}"))
    return jobs


def _run_load_sql(client: ClickHouseClient, sql_filename: str, target_table: str) -> int:
    load_sql_dir = project_root / "clickhouse" / "silver" / "load"
    sql_file = load_sql_dir / sql_filename
    if not sql_file.exists():
        logger.error("Load SQL file not found: %s", sql_file)
        return 1

    sql_content = sql_file.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_content)
    if not statements:
        logger.error("No executable SQL found in %s", sql_file)
        return 1

    for statement in statements:
        client.execute(statement)
    logger.info("Load insert completed for %s", target_table)

    client.execute(f"OPTIMIZE TABLE {target_table} FINAL DEDUPLICATE")
    logger.info("Optimization completed for %s", target_table)
    return 0


def _run_load_jobs(client: ClickHouseClient) -> int:
    load_jobs = _load_jobs()
    if not load_jobs:
        logger.warning("No silver load SQL files found in %s", project_root / "clickhouse" / "silver" / "load")
        return 0

    total_jobs = len(load_jobs)
    for index, (sql_file, target_table) in enumerate(load_jobs, start=1):
        logger.info(
            "Running silver load job %s/%s: %s -> %s",
            index,
            total_jobs,
            sql_file,
            target_table,
        )
        started_at = time.perf_counter()
        result = _run_load_sql(client, sql_file, target_table)
        elapsed_seconds = time.perf_counter() - started_at
        if result != 0:
            logger.error(
                "Silver load job failed %s/%s: %s -> %s (exit code %s) after %.2f seconds",
                index,
                total_jobs,
                sql_file,
                target_table,
                result,
                elapsed_seconds,
            )
            return 1
        logger.info(
            "Completed silver load job %s/%s: %s -> %s in %.2f seconds",
            index,
            total_jobs,
            sql_file,
            target_table,
            elapsed_seconds,
        )
    return 0


def main() -> int:
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
        load_exit_code = _run_load_jobs(client)
        if load_exit_code != 0:
            return load_exit_code

        logger.info("Silver load completed successfully")
        return 0
    finally:
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
