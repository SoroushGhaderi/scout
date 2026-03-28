"""Process FotMob silver layer in ClickHouse."""

import subprocess
import sys
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

    for script_path in scenario_scripts:
        command = _build_command(script_path)
        logger.info("Running silver scenario script: %s", script_path.name)
        result = subprocess.run(command, cwd=project_root)
        if result.returncode != 0:
            logger.error(
                "Silver scenario script failed: %s (exit code %s)",
                script_path.name,
                result.returncode,
            )
            return 1

    logger.info("Silver processing completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
