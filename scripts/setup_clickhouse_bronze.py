"""Create the FotMob bronze layer in ClickHouse."""

import sys

from clickhouse_setup_common import run_clickhouse_layer_setup


def main() -> int:
    return run_clickhouse_layer_setup("bronze")


if __name__ == "__main__":
    sys.exit(main())
