"""
Drop ClickHouse Databases
==========================

⚠️  WARNING: This script will DELETE all data in fotmob and aiscore databases!
This action cannot be undone!

Usage:
    docker-compose exec scraper python scripts/drop_clickhouse_databases.py
    
    # Skip confirmation prompt (use with caution!)
    docker-compose exec scraper python scripts/drop_clickhouse_databases.py --yes
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.storage.clickhouse_client import ClickHouseClient
from src.utils.logging_utils import get_logger

logger = get_logger()


def get_database_stats(client: ClickHouseClient, database: str) -> dict:
    """Get statistics about a database."""
    try:
        # Get table count
        tables_result = client.execute(f"SHOW TABLES FROM {database}")
        tables = []
        if hasattr(tables_result, 'result_rows') and tables_result.result_rows:
            tables = [row[0] for row in tables_result.result_rows]
        elif hasattr(tables_result, 'result_columns') and tables_result.result_columns:
            tables = list(tables_result.result_columns[0])
        
        # Get row count and size
        stats_result = client.execute(f"""
            SELECT 
                formatReadableQuantity(sum(rows)) as rows,
                formatReadableSize(sum(bytes)) as size
            FROM system.parts
            WHERE database = '{database}' AND active
        """)
        
        rows = "0"
        size = "0 B"
        if hasattr(stats_result, 'result_rows') and stats_result.result_rows:
            rows = stats_result.result_rows[0][0] if stats_result.result_rows[0][0] else "0"
            size = stats_result.result_rows[0][1] if stats_result.result_rows[0][1] else "0 B"
        
        return {
            "tables": len(tables),
            "table_names": tables,
            "rows": rows,
            "size": size
        }
    except Exception as e:
        logger.warning(f"Could not get stats for {database}: {e}")
        return {"tables": 0, "table_names": [], "rows": "0", "size": "0 B"}


def drop_databases(confirm: bool = False) -> int:
    """Drop fotmob and aiscore databases."""
    # Get connection details from environment variables
    host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    username = os.getenv('CLICKHOUSE_USER', 'fotmob_user')
    password = os.getenv('CLICKHOUSE_PASSWORD', 'fotmob_pass')
    
    logger.warning("=" * 80)
    logger.warning("⚠️  DROP CLICKHOUSE DATABASES")
    logger.warning("=" * 80)
    logger.warning(f"Host: {host}:{port}")
    logger.warning(f"User: {username}")
    logger.warning("=" * 80)
    
    # Connect to ClickHouse
    client = ClickHouseClient(
        host=host,
        port=port,
        username=username,
        password=password,
        database="default"
    )
    
    if not client.connect():
        logger.error("❌ Failed to connect to ClickHouse")
        logger.error("Please check your connection settings")
        return 1
    
    logger.info("✅ Connected to ClickHouse")
    
    # Get database statistics before dropping
    databases_to_drop = ['fotmob', 'aiscore']
    stats = {}
    
    logger.info("\n" + "=" * 80)
    logger.info("Database Statistics (BEFORE DROP)")
    logger.info("=" * 80)
    
    for db_name in databases_to_drop:
        try:
            client.execute(f"USE {db_name}")
            stats[db_name] = get_database_stats(client, db_name)
            logger.warning(f"\n📊 Database: {db_name}")
            logger.warning(f"   Tables: {stats[db_name]['tables']}")
            logger.warning(f"   Rows: {stats[db_name]['rows']}")
            logger.warning(f"   Size: {stats[db_name]['size']}")
            if stats[db_name]['table_names']:
                logger.warning(f"   Table names: {', '.join(stats[db_name]['table_names'][:5])}")
                if len(stats[db_name]['table_names']) > 5:
                    logger.warning(f"   ... and {len(stats[db_name]['table_names']) - 5} more")
        except Exception as e:
            logger.warning(f"Database {db_name} may not exist: {e}")
            stats[db_name] = {"tables": 0, "rows": "0", "size": "0 B"}
    
    # Confirmation prompt
    if not confirm:
        logger.warning("\n" + "=" * 80)
        logger.warning("⚠️  WARNING: This will DELETE all data in the following databases:")
        for db_name in databases_to_drop:
            logger.warning(f"   - {db_name} ({stats[db_name]['tables']} tables, {stats[db_name]['rows']} rows, {stats[db_name]['size']})")
        logger.warning("=" * 80)
        logger.warning("⚠️  This action CANNOT be undone!")
        logger.warning("=" * 80)
        
        response = input("\nType 'YES' to confirm (or 'yes' to confirm): ").strip()
        if response.upper() != 'YES':
            logger.info("❌ Operation cancelled by user")
            return 0
    
    # Drop databases
    logger.warning("\n" + "=" * 80)
    logger.warning("DROPPING DATABASES...")
    logger.warning("=" * 80)
    
    dropped_count = 0
    failed_count = 0
    
    for db_name in databases_to_drop:
        try:
            logger.warning(f"\nDropping database '{db_name}'...")
            client.execute(f"DROP DATABASE IF EXISTS {db_name}")
            logger.warning(f"✅ Database '{db_name}' dropped successfully")
            dropped_count += 1
        except Exception as e:
            logger.error(f"❌ Failed to drop database '{db_name}': {e}")
            failed_count += 1
    
    # Verify
    logger.info("\n" + "=" * 80)
    logger.info("Verification")
    logger.info("=" * 80)
    
    try:
        result = client.execute("SHOW DATABASES")
        remaining_dbs = set()
        
        if hasattr(result, 'result_rows') and result.result_rows:
            remaining_dbs = {row[0] for row in result.result_rows}
        elif hasattr(result, 'result_columns') and result.result_columns:
            remaining_dbs = set(result.result_columns[0])
        
        for db_name in databases_to_drop:
            if db_name in remaining_dbs:
                logger.error(f"❌ Database '{db_name}' still exists!")
            else:
                logger.info(f"✅ Database '{db_name}' successfully removed")
        
        logger.info("\n" + "=" * 80)
        if failed_count == 0:
            logger.info(f"✅ Successfully dropped {dropped_count} database(s)")
            logger.info("=" * 80)
            logger.info("\n💡 Next steps:")
            logger.info("   To recreate databases and tables, run:")
            logger.info("   docker-compose exec scraper python scripts/setup_clickhouse.py")
            return 0
        else:
            logger.error(f"❌ Failed to drop {failed_count} database(s)")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Error verifying databases: {e}")
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Drop ClickHouse databases (fotmob and aiscore)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
⚠️  WARNING: This will DELETE all data in fotmob and aiscore databases!
This action cannot be undone!

Examples:
  # Interactive mode (will ask for confirmation)
  python scripts/drop_clickhouse_databases.py
  
  # Skip confirmation (use with caution!)
  python scripts/drop_clickhouse_databases.py --yes
        """
    )
    
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Skip confirmation prompt (use with caution!)'
    )
    
    args = parser.parse_args()
    
    exit_code = drop_databases(confirm=args.yes)
    
    if exit_code == 0:
        logger.info("\n✅ Operation completed")
    else:
        logger.error("\n❌ Operation failed")
        logger.error("Please check the errors above and try again")
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

