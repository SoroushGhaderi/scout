"""Layered entrypoint for Silver ClickHouse schema setup."""

from pathlib import Path
import runpy


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[2]
    runpy.run_path(str(root / "scripts" / "setup_clickhouse_silver.py"), run_name="__main__")

