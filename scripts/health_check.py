"""
System Health Check Script
==========================

Checks the health of all system components:
- ClickHouse connection
- Storage access
- Disk space

Usage:
    # Check all components
    python scripts/health_check.py
    
    # Check specific components
    python scripts/health_check.py --no-clickhouse
    python scripts/health_check.py --storage-only
    
    # Custom storage paths
    python scripts/health_check.py --storage data/fotmob data/aiscore
"""

import argparse
import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.health_check import health_check
from src.utils.logging_utils import get_logger, setup_logging


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='System Health Check',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full health check
  python scripts/health_check.py
  
  # Check without ClickHouse
  python scripts/health_check.py --no-clickhouse
  
  # Check only storage
  python scripts/health_check.py --storage-only
  
  # Custom storage paths
  python scripts/health_check.py --storage data/fotmob data/aiscore
  
  # JSON output
  python scripts/health_check.py --json
        """
    )
    
    # ClickHouse options
    parser.add_argument(
        '--clickhouse-host',
        type=str,
        default=None,
        help='ClickHouse host (default: from CLICKHOUSE_HOST env var or localhost)'
    )
    parser.add_argument(
        '--clickhouse-port',
        type=int,
        default=8123,
        help='ClickHouse port (default: 8123)'
    )
    parser.add_argument(
        '--clickhouse-username',
        type=str,
        default='default',
        help='ClickHouse username (default: default)'
    )
    parser.add_argument(
        '--clickhouse-password',
        type=str,
        default='',
        help='ClickHouse password (default: empty)'
    )
    parser.add_argument(
        '--clickhouse-database',
        type=str,
        default='default',
        help='ClickHouse database (default: default)'
    )
    parser.add_argument(
        '--no-clickhouse',
        action='store_true',
        help='Skip ClickHouse health check'
    )
    
    # Storage options
    parser.add_argument(
        '--storage',
        nargs='+',
        default=None,
        help='Storage paths to check (default: data)'
    )
    parser.add_argument(
        '--storage-only',
        action='store_true',
        help='Check only storage (skip ClickHouse and disk space)'
    )
    
    # Disk space options
    parser.add_argument(
        '--disk-path',
        type=str,
        default='.',
        help='Path to check disk space for (default: current directory)'
    )
    parser.add_argument(
        '--disk-threshold',
        type=float,
        default=1.0,
        help='Disk space warning threshold in GB (default: 1.0)'
    )
    
    # Output options
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose output'
    )
    
    return parser.parse_args()


def print_health_results(results: dict, json_output: bool = False):
    """Print health check results in a readable format."""
    if json_output:
        print(json.dumps(results, indent=2))
        return
    
    # Print header
    print("=" * 80)
    print("SYSTEM HEALTH CHECK")
    print("=" * 80)
    print(f"Timestamp: {results['timestamp']}")
    print(f"Overall Status: {results['overall_status'].upper()}")
    print()
    
    # Print component results
    for component_name, component_result in results['components'].items():
        print(f"\n{component_name.upper().replace('_', ' ')}")
        print("-" * 80)
        
        if isinstance(component_result, dict):
            if "status" in component_result:
                # Single component result
                status = component_result["status"]
                status_symbol = {
                    "healthy": "✓",
                    "warning": "⚠",
                    "error": "✗",
                    "critical": "✗",
                    "skipped": "-"
                }.get(status, "?")
                
                print(f"Status: {status_symbol} {status.upper()}")
                
                # Print details
                for key, value in component_result.items():
                    if key not in ["status", "message"]:
                        print(f"  {key}: {value}")
                
                if "message" in component_result:
                    print(f"  Message: {component_result['message']}")
            else:
                # Nested results (e.g., storage paths)
                for path, path_result in component_result.items():
                    if isinstance(path_result, dict):
                        status = path_result.get("status", "unknown")
                        status_symbol = {
                            "healthy": "✓",
                            "warning": "⚠",
                            "error": "✗",
                            "critical": "✗"
                        }.get(status, "?")
                        
                        print(f"  {path}: {status_symbol} {status.upper()}")
                        if "message" in path_result:
                            print(f"    {path_result['message']}")
    
    print()
    print("=" * 80)
    
    # Exit code based on overall status
    if results['overall_status'] == "healthy":
        print("✓ All systems healthy")
        return 0
    elif results['overall_status'] == "degraded":
        print("⚠ System is degraded (warnings present)")
        return 1
    else:
        print("✗ System is unhealthy (errors present)")
        return 2


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    logger = setup_logging(
        name="health_check",
        log_dir="logs",
        log_level=log_level
    )
    
    # Determine ClickHouse host
    import os
    clickhouse_host = args.clickhouse_host
    if not clickhouse_host and not args.no_clickhouse:
        clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        if clickhouse_host == 'localhost':
            # Try docker-compose service name
            clickhouse_host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    
    # Override from environment if not provided
    if not args.no_clickhouse:
        clickhouse_host = clickhouse_host or os.getenv('CLICKHOUSE_HOST', 'clickhouse')
        args.clickhouse_username = os.getenv('CLICKHOUSE_USER', args.clickhouse_username)
        args.clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', args.clickhouse_password)
    
    # Determine storage paths
    storage_paths = args.storage
    if storage_paths is None:
        # Default storage paths
        storage_paths = ["data"]
        # Check if scraper-specific paths exist
        fotmob_bronze = Path("data/fotmob")
        aiscore_bronze = Path("data/aiscore")
        if fotmob_bronze.exists():
            storage_paths.append(str(fotmob_bronze))
        if aiscore_bronze.exists():
            storage_paths.append(str(aiscore_bronze))
    
    # Perform health check
    if args.storage_only:
        # Only check storage
        clickhouse_host = None
        disk_path = None
    else:
        disk_path = args.disk_path
    
    results = health_check(
        clickhouse_host=clickhouse_host if not args.no_clickhouse else None,
        clickhouse_port=args.clickhouse_port,
        clickhouse_username=args.clickhouse_username,
        clickhouse_password=args.clickhouse_password,
        clickhouse_database=args.clickhouse_database,
        storage_paths=storage_paths,
        disk_path=disk_path if not args.storage_only else ".",
        disk_threshold_gb=args.disk_threshold
    )
    
    # Print results
    exit_code = print_health_results(results, json_output=args.json)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()

