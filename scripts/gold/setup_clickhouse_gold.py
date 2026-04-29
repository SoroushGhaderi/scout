"""Create the FotMob gold layer in ClickHouse."""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from clickhouse_setup_common import run_clickhouse_layer_setup


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create Gold ClickHouse schema/tables")
    parser.add_argument(
        "--part",
        choices=("all", "scenarios", "signals"),
        default="all",
        help="Which gold DDL to execute: scenarios, signals, or all (default: all)",
    )
    return parser.parse_args(argv)


def _gold_sql_filter(part: str):
    def _matches(sql_path: Path) -> bool:
        name = sql_path.name.lower()
        if "create_database" in name:
            return True
        if part == "scenarios":
            return "create" in name and "scenario" in name
        if part == "signals":
            return "create" in name and "signal" in name
        return True

    return _matches


def main(argv=None) -> int:
    args = _parse_args(argv)
    return run_clickhouse_layer_setup("gold", sql_file_filter=_gold_sql_filter(args.part))


if __name__ == "__main__":
    sys.exit(main())
