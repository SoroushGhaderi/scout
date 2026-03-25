"""Create the FotMob bronze, silver, and gold ClickHouse layers."""

import sys

from clickhouse_setup_common import LAYER_ORDER, run_clickhouse_layers


def main() -> int:
    return run_clickhouse_layers(LAYER_ORDER)


if __name__ == "__main__":
    sys.exit(main())
