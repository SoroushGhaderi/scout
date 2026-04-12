"""Load FotMob silver data into ClickHouse tables."""

import argparse
import sys
import time
from pathlib import Path
from typing import Optional

project_root = Path(__file__).resolve().parents[2]
scripts_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from config.settings import settings
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.clickhouse_sql_executor import execute_sql_statements, split_sql_statements
from src.utils.layer_contracts import LayerContractError, assert_silver_layer_contracts
from src.utils.logging_utils import get_logger

logger = get_logger()


def _load_sql_dirs() -> list[Path]:
    silver_root = project_root / "clickhouse" / "silver"
    dml_dir = silver_root / "dml"
    load_dir = silver_root / "load"
    sql_dirs = [path for path in (dml_dir, load_dir) if path.exists() and path.is_dir()]
    if dml_dir.exists():
        if load_dir.exists():
            logger.info("Using silver DML SQL from both %s (preferred) and %s (fallback)", dml_dir, load_dir)
        else:
            logger.info("Using silver DML SQL from %s", dml_dir)
    elif load_dir.exists():
        logger.warning("Using legacy silver load SQL directory: %s (consider migrating to dml/)", load_dir)
    return sql_dirs


def _load_jobs() -> list[tuple[Path, str]]:
    sql_by_name: dict[str, Path] = {}
    for sql_dir in _load_sql_dirs():
        for sql_path in sorted(path for path in sql_dir.glob("*.sql") if path.is_file()):
            if sql_path.name in sql_by_name:
                logger.warning(
                    "Skipping duplicate silver DML SQL %s from %s; using %s",
                    sql_path.name,
                    sql_dir,
                    sql_by_name[sql_path.name],
                )
                continue
            sql_by_name[sql_path.name] = sql_path

    jobs: list[tuple[Path, str]] = []
    for sql_path in sorted(sql_by_name.values(), key=lambda path: path.name):
        stem_parts = sql_path.stem.split("_", 1)
        if len(stem_parts) != 2:
            logger.warning("Skipping silver load SQL with unexpected name: %s", sql_path.name)
            continue
        table_name = stem_parts[1]
        jobs.append((sql_path, f"silver.{table_name}"))
    return jobs


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load FotMob silver SQL into ClickHouse")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview silver load jobs without executing SQL or optimizing tables",
    )
    return parser.parse_args(argv)


def _run_load_sql(client: ClickHouseClient, sql_file: Path, target_table: str, dry_run: bool = False) -> int:
    if not sql_file.exists():
        logger.error("Load SQL file not found: %s", sql_file)
        return 1

    sql_content = sql_file.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_content)
    if not statements:
        logger.error("No executable SQL found in %s", sql_file)
        return 1

    if dry_run:
        logger.info(
            "[dry-run] Would execute %s SQL statement(s) and optimize %s using %s",
            len(statements),
            target_table,
            sql_file.name,
        )
        return 0

    execute_sql_statements(
        client=client,
        statements=statements,
        layer_name="silver_load",
        source_name=sql_file.name,
    )

    client.execute(f"OPTIMIZE TABLE {target_table} FINAL DEDUPLICATE")
    return 0


def _run_load_jobs(client: Optional[ClickHouseClient], dry_run: bool = False) -> int:
    load_jobs = _load_jobs()
    if not load_jobs:
        logger.warning("No silver DML SQL files found in %s", project_root / "clickhouse" / "silver")
        return 0

    if dry_run:
        logger.info("[dry-run] Planned silver load jobs: %s", len(load_jobs))

    total_jobs = len(load_jobs)
    for index, (sql_path, target_table) in enumerate(load_jobs, start=1):
        logger.info(
            "Running silver load job %s/%s: %s -> %s",
            index,
            total_jobs,
            sql_path.name,
            target_table,
        )
        started_at = time.perf_counter()
        if not dry_run and client is None:
            logger.error("ClickHouse client is required when not running dry-run")
            return 1
        result = _run_load_sql(client, sql_path, target_table, dry_run=dry_run)
        elapsed_seconds = time.perf_counter() - started_at
        if result != 0:
            logger.error(
                "Silver load job failed %s/%s: %s -> %s (exit code %s) after %.2f seconds",
                index,
                total_jobs,
                sql_path.name,
                target_table,
                result,
                elapsed_seconds,
            )
            return 1
        logger.info(
            "Completed silver load job %s/%s: %s -> %s in %.2f seconds",
            index,
            total_jobs,
            sql_path.name,
            target_table,
            elapsed_seconds,
        )
    return 0


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        logger.info("Running silver loader in dry-run mode (no SQL will be executed)")
        load_exit_code = _run_load_jobs(None, dry_run=True)
        if load_exit_code == 0:
            logger.info("Silver dry-run completed successfully")
        return load_exit_code

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
        load_exit_code = _run_load_jobs(client, dry_run=False)
        if load_exit_code != 0:
            return load_exit_code

        assert_silver_layer_contracts(client, database="silver", log=logger)
        logger.info("Silver load completed successfully")
        return 0
    except LayerContractError as contract_error:
        logger.error("Silver layer contract assertion failed", error=str(contract_error))
        return 1
    finally:
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
