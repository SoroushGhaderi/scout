"""Process FotMob gold layer in ClickHouse."""

import argparse
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
from src.utils.layer_completion_alerts import send_layer_completion_alert
from src.utils.layer_contracts import LayerContractError, assert_gold_layer_contracts
from src.utils.logging_utils import get_logger, setup_logging

logger = get_logger()


def _scenario_scripts() -> list[Path]:
    scenario_dir = Path(__file__).resolve().parent / "scenario"
    return sorted(path for path in scenario_dir.glob("scenario*.py") if path.is_file())


def _build_command(script_path: Path) -> list[str]:
    return [sys.executable, str(script_path)]


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process FotMob gold layer in ClickHouse")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview gold SQL/scenario jobs without executing SQL or subprocesses",
    )
    return parser.parse_args(argv)


def _run_scenario_scripts(dry_run: bool = False) -> tuple[int, int, list[str]]:
    scenario_scripts = _scenario_scripts()
    if not scenario_scripts:
        logger.warning("No gold scenario scripts found in %s", Path(__file__).resolve().parent / "scenario")
        return 0, 0, []

    if dry_run:
        logger.info("[dry-run] Planned gold scenario scripts: %s", len(scenario_scripts))

    total_scripts = len(scenario_scripts)
    success_count = 0
    failed_scenarios: list[str] = []

    for index, script_path in enumerate(scenario_scripts, start=1):
        logger.info(
            "Running gold scenario script %s/%s: %s",
            index,
            total_scripts,
            script_path.name,
        )
        if dry_run:
            logger.info("[dry-run] Would execute scenario script: %s", script_path)
            success_count += 1
            continue
        command = _build_command(script_path)
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
            failed_scenarios.append(script_path.name)
            continue
        success_count += 1
        logger.info(
            "Completed gold scenario script %s/%s: %s in %.2f seconds",
            index,
            total_scripts,
            script_path.name,
            elapsed_seconds,
        )

    failed_count = len(failed_scenarios)
    logger.info(
        "Gold scenario execution report | total=%s success=%s failed=%s",
        total_scripts,
        success_count,
        failed_count,
    )
    if failed_scenarios:
        logger.error("Failed scenarios: %s", ", ".join(failed_scenarios))

    return success_count, failed_count, failed_scenarios


def main(argv=None) -> int:
    global logger
    stage_start = time.perf_counter()
    args = parse_args(argv)
    logger = setup_logging(
        name="clickhouse_gold_loader",
        log_dir=settings.log_dir,
        log_level=settings.log_level,
    )
    if args.dry_run:
        logger.info("Running gold loader in dry-run mode (no SQL will be executed)")
        sql_dir = project_root / "clickhouse" / "gold"
        processor = FotMobGoldProcessor(sql_dir=sql_dir)
        sql_files = processor.sql_files()
        if not sql_files:
            logger.error("No gold SQL files found in %s", sql_dir)
            return 1
        logger.info("[dry-run] Planned gold SQL files: %s", len(sql_files))
        for sql_file in sql_files:
            logger.info("[dry-run] Would execute SQL file: %s", sql_file)
        _, failed_count, _ = _run_scenario_scripts(dry_run=True)
        total_scenarios = len(_scenario_scripts())
        scenario_success_rate = (
            ((total_scenarios - failed_count) / total_scenarios * 100) if total_scenarios > 0 else 0
        )
        send_layer_completion_alert(
            layer="gold",
            summary_message="Gold SQL/scenario dry-run finished.",
            scope="dry-run",
            success=failed_count == 0,
            duration_seconds=time.perf_counter() - stage_start,
            detail_lines=[
                f"SQL files planned: <b>{len(sql_files)}</b>",
                f"Scenario failures: <b>{failed_count}</b>",
                "Contract checks: <b>skipped (dry-run)</b>",
            ],
            insight_lines=[
                f"Scenario pass projection: <b>{scenario_success_rate:.1f}%</b>",
                "Dry-run signal: <b>SQL + scenario execution path validated</b>",
            ],
            action_lines=[
                "Run without --dry-run to populate gold tables and validate contracts.",
            ],
        )
        if failed_count > 0:
            return 1
        logger.info("Gold dry-run completed successfully")
        return 0

    client = ClickHouseClient(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database="default",
    )

    if not client.connect():
        logger.error("Failed to connect to ClickHouse")
        send_layer_completion_alert(
            layer="gold",
            summary_message="Gold processing finished with connection failure.",
            scope="runtime",
            success=False,
            duration_seconds=time.perf_counter() - stage_start,
            detail_lines=[
                "SQL files executed: <b>0</b>",
                "Scenario failures: <b>0</b>",
                "Contract checks: <b>not run</b>",
            ],
        )
        return 1

    sql_file_count = 0
    scenario_success_count = 0
    scenario_failed_count = 0
    contracts_checked = False
    exit_code = 0
    try:
        sql_dir = project_root / "clickhouse" / "gold"
        processor = FotMobGoldProcessor(sql_dir=sql_dir)
        storage = FotMobGoldStorage(client, database="gold")

        sql_files = processor.sql_files()
        if not sql_files:
            logger.error("No gold SQL files found in %s", sql_dir)
            exit_code = 1
            return exit_code

        sql_file_count = len(sql_files)
        storage.execute_sql_files(sql_files)
        scenario_success_count, scenario_failed_count, _ = _run_scenario_scripts(dry_run=False)
        if scenario_failed_count > 0:
            logger.error("Gold processing completed with failed scenarios")
            exit_code = 1
            return exit_code
        assert_gold_layer_contracts(client, database="gold", log=logger)
        contracts_checked = True
        logger.info("Gold processing completed successfully")
        return exit_code
    except LayerContractError as contract_error:
        logger.error("Gold layer contract assertion failed", error=str(contract_error))
        exit_code = 1
        return exit_code
    finally:
        client.disconnect()
        total_scenarios = scenario_success_count + scenario_failed_count
        scenario_success_rate = (
            (scenario_success_count / total_scenarios * 100) if total_scenarios > 0 else 0
        )
        send_layer_completion_alert(
            layer="gold",
            summary_message="Gold SQL + scenario processing finished.",
            scope="runtime",
            success=exit_code == 0,
            duration_seconds=time.perf_counter() - stage_start,
            detail_lines=[
                f"SQL files executed: <b>{sql_file_count}</b>",
                f"Scenarios succeeded: <b>{scenario_success_count}</b>",
                f"Scenario failures: <b>{scenario_failed_count}</b>",
                f"Contract checks: <b>{'passed' if contracts_checked else 'failed or skipped'}</b>",
            ],
            insight_lines=[
                f"Scenario success rate: <b>{scenario_success_rate:.1f}%</b>",
                (
                    "Analytics quality signal: <b>gold contracts passed</b>"
                    if contracts_checked
                    else "Analytics quality signal: <b>contract check failed or was skipped</b>"
                ),
            ],
            action_lines=[
                "If any scenarios failed, inspect failed script names in logs and rerun selectively.",
                "If contracts passed, downstream consumers can safely query gold outputs.",
            ],
        )


if __name__ == "__main__":
    raise SystemExit(main())
