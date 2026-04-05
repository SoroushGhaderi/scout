"""Process FotMob gold layer in ClickHouse."""

import subprocess
import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
scripts_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from config.settings import settings
from src.processors.gold.fotmob import FotMobGoldProcessor
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.gold.fotmob import FotMobGoldStorage
from src.utils.logging_utils import get_logger

logger = get_logger()


def _scenario_scripts() -> list[Path]:
    gold_dir = Path(__file__).resolve().parent
    return sorted(path for path in gold_dir.glob("scenario*.py") if path.is_file())


def _build_command(script_path: Path) -> list[str]:
    return [sys.executable, str(script_path)]


def _run_scenario_scripts() -> int:
    scenario_scripts = _scenario_scripts()
    if not scenario_scripts:
        logger.warning("No gold scenario scripts found in %s", Path(__file__).resolve().parent)
        return 0

    total_scripts = len(scenario_scripts)
    for index, script_path in enumerate(scenario_scripts, start=1):
        command = _build_command(script_path)
        logger.info(
            "Running gold scenario script %s/%s: %s",
            index,
            total_scripts,
            script_path.name,
        )
        script_start = time.perf_counter()
        result = subprocess.run(command, cwd=project_root)
        elapsed_seconds = time.perf_counter() - script_start
        if result.returncode != 0:
            logger.error(
                "Gold scenario script failed %s/%s: %s (exit code %s) after %.2f seconds",
                index,
                total_scripts,
                script_path.name,
                result.returncode,
                elapsed_seconds,
            )
            return 1
        logger.info(
            "Completed gold scenario script %s/%s: %s in %.2f seconds",
            index,
            total_scripts,
            script_path.name,
            elapsed_seconds,
        )
    return 0


def main(argv=None) -> int:
    _ = argv
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
        sql_dir = project_root / "clickhouse" / "gold"
        processor = FotMobGoldProcessor(sql_dir=sql_dir)
        storage = FotMobGoldStorage(client, database="gold")

        sql_files = processor.sql_files()
        if not sql_files:
            logger.error("No gold SQL files found in %s", sql_dir)
            return 1

        storage.execute_sql_files(sql_files)
        scenario_exit_code = _run_scenario_scripts()
        if scenario_exit_code != 0:
            return scenario_exit_code
        logger.info("Gold processing completed successfully")
        return 0
    finally:
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
