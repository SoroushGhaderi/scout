"""Create the FotMob bronze layer in ClickHouse."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from clickhouse_setup_common import run_clickhouse_layer_setup


def main() -> int:
    return run_clickhouse_layer_setup("bronze")


if __name__ == "__main__":
    sys.exit(main())
