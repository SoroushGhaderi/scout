"""Process FotMob silver layer in ClickHouse."""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
scripts_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from src.utils.logging_utils import get_logger

logger = get_logger()


def main() -> int:
    logger.info(
        "Silver processing completed successfully (scenario classifications now run in gold layer)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
