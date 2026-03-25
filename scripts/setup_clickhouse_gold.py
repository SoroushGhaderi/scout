"""Create the FotMob gold layer in ClickHouse."""

import sys

from clickhouse_setup_common import run_clickhouse_layer_setup


def main() -> int:
    return run_clickhouse_layer_setup("gold")


if __name__ == "__main__":
    sys.exit(main())
