"""Process FotMob silver layer in ClickHouse."""

import subprocess
import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
scripts_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from src.utils.logging_utils import get_logger

logger = get_logger()


def _scenario_scripts() -> list[Path]:
    silver_dir = Path(__file__).resolve().parent
    return sorted(
        path for path in silver_dir.glob("scenario*.py") if path.is_file()
    )


def _build_command(script_path: Path) -> list[str]:
    return [sys.executable, str(script_path)]


def main() -> int:
    scenario_scripts = _scenario_scripts()
    if not scenario_scripts:
        logger.error("No scenario scripts found in %s", Path(__file__).resolve().parent)
        return 1

    total_scripts = len(scenario_scripts)
    for index, script_path in enumerate(scenario_scripts, start=1):
        command = _build_command(script_path)
        logger.info(
            "Running silver scenario script %s/%s: %s",
            index,
            total_scripts,
            script_path.name,
        )
        script_start = time.perf_counter()
        result = subprocess.run(command, cwd=project_root)
        elapsed_seconds = time.perf_counter() - script_start
        if result.returncode != 0:
            logger.error(
                "Silver scenario script failed %s/%s: %s (exit code %s) after %.2f seconds",
                index,
                total_scripts,
                script_path.name,
                result.returncode,
                elapsed_seconds,
            )
            return 1
        logger.info(
            "Completed silver scenario script %s/%s: %s in %.2f seconds",
            index,
            total_scripts,
            script_path.name,
            elapsed_seconds,
        )

    logger.info("Silver processing completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
