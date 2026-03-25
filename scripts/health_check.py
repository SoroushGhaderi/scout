"""Run system health checks for ClickHouse, storage, and disk space."""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.health_check import health_check
from src.utils.logging_utils import setup_logging


def parse_arguments() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="System Health Check",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/health_check.py
  python scripts/health_check.py --no-clickhouse
  python scripts/health_check.py --storage-only
  python scripts/health_check.py --storage data data/fotmob
  python scripts/health_check.py --json
        """,
    )
    parser.add_argument("--clickhouse-host", type=str, default=None)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-username", type=str, default="default")
    parser.add_argument("--clickhouse-password", type=str, default="")
    parser.add_argument("--clickhouse-database", type=str, default="default")
    parser.add_argument("--no-clickhouse", action="store_true", help="Skip ClickHouse health check")
    parser.add_argument("--storage", nargs="+", default=None, help="Storage paths to check")
    parser.add_argument("--storage-only", action="store_true", help="Skip ClickHouse and use only storage/disk checks")
    parser.add_argument("--disk-path", type=str, default=".")
    parser.add_argument("--disk-threshold", type=float, default=1.0)
    parser.add_argument("--json", action="store_true", help="Output JSON")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def _print_component(component_name: str, component_result: Dict[str, Any]) -> None:
    """Print one health-check component."""
    print(f"\n{component_name.upper().replace('_', ' ')}")
    print("-" * 80)

    if "status" in component_result:
        status = component_result.get("status", "unknown")
        symbol = {
            "healthy": "OK",
            "warning": "WARN",
            "error": "ERR",
            "critical": "ERR",
            "skipped": "SKIP",
        }.get(status, "?")
        print(f"Status: {symbol} {status.upper()}")
        message = component_result.get("message")
        if message:
            print(f"Message: {message}")
        for key, value in component_result.items():
            if key not in {"status", "message"}:
                print(f"{key}: {value}")
        return

    for path_name, path_result in component_result.items():
        if not isinstance(path_result, dict):
            continue
        status = path_result.get("status", "unknown")
        symbol = {
            "healthy": "OK",
            "warning": "WARN",
            "error": "ERR",
            "critical": "ERR",
        }.get(status, "?")
        print(f"{path_name}: {symbol} {status.upper()}")
        if "message" in path_result:
            print(f"  {path_result['message']}")


def print_health_results(results: Dict[str, Any], json_output: bool = False) -> int:
    """Print health check results and return a process exit code."""
    if json_output:
        print(json.dumps(results, indent=2))
    else:
        print("=" * 80)
        print("SYSTEM HEALTH CHECK")
        print("=" * 80)
        print(f"Timestamp: {results['timestamp']}")
        print(f"Overall Status: {results['overall_status'].upper()}")
        for component_name, component_result in results["components"].items():
            if isinstance(component_result, dict):
                _print_component(component_name, component_result)
        print("\n" + "=" * 80)

    if results["overall_status"] == "healthy":
        return 0
    if results["overall_status"] == "degraded":
        return 1
    return 2


def main() -> int:
    """Main entry point."""
    args = parse_arguments()
    setup_logging(
        name="health_check",
        log_dir="logs",
        log_level="DEBUG" if args.verbose else "INFO",
    )

    clickhouse_host = args.clickhouse_host or os.getenv("CLICKHOUSE_HOST")
    if clickhouse_host == "localhost":
        clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")

    clickhouse_username = os.getenv("CLICKHOUSE_USER", args.clickhouse_username)
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", args.clickhouse_password)

    storage_paths = list(args.storage or ["data"])
    fotmob_bronze_path = Path("data/fotmob")
    if fotmob_bronze_path.exists() and str(fotmob_bronze_path) not in storage_paths:
        storage_paths.append(str(fotmob_bronze_path))

    if args.storage_only:
        clickhouse_host = None

    results = health_check(
        clickhouse_host=None if args.no_clickhouse else clickhouse_host,
        clickhouse_port=args.clickhouse_port,
        clickhouse_username=clickhouse_username,
        clickhouse_password=clickhouse_password,
        clickhouse_database=args.clickhouse_database,
        storage_paths=storage_paths,
        disk_path=args.disk_path,
        disk_threshold_gb=args.disk_threshold,
    )
    return print_health_results(results, json_output=args.json)


if __name__ == "__main__":
    raise SystemExit(main())
