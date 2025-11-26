"""
Create ClickHouse tables for FotMob and AIScore databases.

SCRAPER: Both FotMob and AIScore
PURPOSE: Initialize ClickHouse databases and tables from SQL scripts

Usage:
    python scripts/setup_clickhouse.py
"""

import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.storage.clickhouse_client import ClickHouseClient
from src.utils.logging_utils import get_logger

logger = get_logger()


def check_databases_exist(client: ClickHouseClient, databases: list) -> bool:
    """
    Check if databases exist in ClickHouse.
    
    Args:
        client: ClickHouse client connected to default database
        databases: List of database names to check
        
    Returns:
        True if all databases exist, False otherwise
    """
    try:
        result = client.execute("SHOW DATABASES")
        existing_dbs = set()
        
        # Handle different result types from clickhouse_connect
        if hasattr(result, 'result_rows'):
            existing_dbs = {row[0] for row in result.result_rows}
        elif hasattr(result, 'result_columns'):
            # Alternative result format
            if result.result_columns and len(result.result_columns) > 0:
                existing_dbs = set(result.result_columns[0])
        elif isinstance(result, (list, tuple)):
            existing_dbs = {row[0] if isinstance(row, (list, tuple)) else str(row) for row in result}
        else:
            # Fallback: try to access each database directly
            logger.debug("Using fallback method to check databases")
            for db in databases:
                try:
                    client.execute(f"SELECT 1 FROM {db}.system.tables LIMIT 1")
                    existing_dbs.add(db)
                except:
                    pass
        
        # Check if all required databases exist
        missing_dbs = [db for db in databases if db not in existing_dbs]
        if missing_dbs:
            logger.info(f"Missing databases: {', '.join(missing_dbs)}")
            return False
        
        logger.info(f"All databases exist: {', '.join(databases)}")
        return True
    except Exception as e:
        logger.warning(f"Error checking databases: {e}")
        # If check fails, assume databases don't exist to be safe
        return False


def execute_sql_file(client: ClickHouseClient, sql_file: Path, database: str = None):
    """Execute SQL file statement by statement."""
    try:
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Switch to database if specified (before processing statements)
        if database:
            try:
                client.execute(f"USE {database}")
                logger.debug(f"Switched to database: {database}")
                # Also update the client's database attribute to maintain context
                client.database = database
            except Exception as e:
                logger.error(f"Failed to switch to database {database}: {e}")
                return False
        
        statements = []
        current_statement = []
        
        for line in sql_content.split('\n'):
            if line.strip().startswith('--'):
                continue
            
            if '--' in line:
                line = line[:line.index('--')]
            
            line = line.strip()
            if not line:
                continue
            
            # Skip USE statements if we already switched databases via parameter
            if database and line.upper().startswith('USE '):
                logger.debug(f"Skipping USE statement (already using {database}): {line}")
                continue
            
            current_statement.append(line)
            
            if line.rstrip().endswith(';'):
                statement = ' '.join(current_statement).rstrip(';').strip()
                if statement:
                    statements.append(statement)
                current_statement = []
        
        if current_statement:
            statement = ' '.join(current_statement).strip()
            if statement:
                statements.append(statement)
        
        executed_count = 0
        failed_statements = []
        for i, statement in enumerate(statements, 1):
            if not statement:
                continue
            
            # For CREATE TABLE statements, explicitly specify the database if provided
            if database and statement.upper().strip().startswith('CREATE TABLE'):
                # Check if table name already has a database prefix
                import re
                # Pattern to match: CREATE TABLE [IF NOT EXISTS] [database.]table_name
                if 'IF NOT EXISTS' in statement.upper():
                    # Pattern: CREATE TABLE IF NOT EXISTS [database.]table_name
                    pattern = r'CREATE TABLE IF NOT EXISTS\s+(\w+\.)?(\w+)'
                    replacement = f'CREATE TABLE IF NOT EXISTS {database}.\\2'
                else:
                    # Pattern: CREATE TABLE [database.]table_name
                    pattern = r'CREATE TABLE\s+(\w+\.)?(\w+)'
                    replacement = f'CREATE TABLE {database}.\\2'
                
                # Only replace if table name doesn't already have a database prefix
                if not re.search(r'CREATE TABLE\s+(?:IF NOT EXISTS\s+)?\w+\.\w+', statement, re.IGNORECASE):
                    statement = re.sub(pattern, replacement, statement, count=1, flags=re.IGNORECASE)
            
            try:
                logger.debug(f"Executing statement {i}/{len(statements)} from {sql_file.name}: {statement[:100]}...")
                client.execute(statement)
                executed_count += 1
                logger.debug(f"Statement {i} executed successfully")
            except Exception as e:
                error_str = str(e).lower()
                if "already exists" in error_str:
                    logger.debug(f"Table/database already exists (expected): {statement[:50]}...")
                    executed_count += 1
                else:
                    logger.error(f"Error executing statement {i} from {sql_file.name}: {e}")
                    logger.error(f"Full statement: {statement}")
                    failed_statements.append((i, statement, str(e)))
        
        if failed_statements:
            logger.warning(f"Failed to execute {len(failed_statements)} statements:")
            for stmt_num, stmt, error in failed_statements:
                logger.warning(f"  Statement {stmt_num}: {error}")
                logger.warning(f"    {stmt[:200]}...")
        
        logger.info(f"Successfully executed {executed_count}/{len(statements)} statements from {sql_file.name}")
        if len(statements) > 0 and executed_count == 0:
            logger.error(f"No statements were executed successfully from {sql_file.name}!")
            return False
        return executed_count > 0
        
    except Exception as e:
        logger.error(f"Error reading/executing {sql_file}: {e}", exc_info=True)
        return False


def create_user_if_not_exists(client: ClickHouseClient, username: str, password: str):
    """
    Create ClickHouse user if it doesn't exist.
    
    Args:
        client: ClickHouse client connected with admin privileges (default user)
        username: Username to create
        password: Password for the user
    """
    try:
        # Check if user exists
        result = client.execute(f"SELECT name FROM system.users WHERE name = '{username}'")
        user_exists = False
        
        if hasattr(result, 'result_rows') and result.result_rows:
            user_exists = True
        elif hasattr(result, 'result_columns') and result.result_columns:
            user_exists = len(result.result_columns[0]) > 0
        
        if user_exists:
            logger.info(f"User '{username}' already exists, skipping creation")
            return True
        
        # Create user
        logger.info(f"Creating user '{username}'...")
        create_user_sql = f"CREATE USER IF NOT EXISTS {username} IDENTIFIED BY '{password}'"
        client.execute(create_user_sql)
        
        # Grant permissions
        logger.info(f"Granting permissions to '{username}'...")
        grant_queries = [
            f"GRANT ALL ON fotmob.* TO {username}",
            f"GRANT ALL ON aiscore.* TO {username}",
            f"GRANT CREATE DATABASE ON *.* TO {username}",
        ]
        
        for grant_query in grant_queries:
            try:
                client.execute(grant_query)
                logger.debug(f"Executed: {grant_query}")
            except Exception as e:
                logger.warning(f"Could not grant permission (may already exist): {e}")
        
        logger.info(f"User '{username}' created and permissions granted")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create user '{username}': {e}")
        return False


def main():
    """Main entry point."""
    # Get connection details from environment variables
    host = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    username = os.getenv('CLICKHOUSE_USER', 'fotmob_user')
    password = os.getenv('CLICKHOUSE_PASSWORD', 'fotmob_pass')
    
    logger.info(f"Connecting to ClickHouse at {host}:{port}")
    
    # First, try to connect with the specified user
    client = ClickHouseClient(
        host=host,
        port=port,
        username=username,
        password=password,
        database="default"
    )
    
    connection_success = client.connect()
    
    # If connection failed, try with default user to create the custom user
    if not connection_success:
        logger.warning(f"Failed to connect with user '{username}', trying default user...")
        
        # Try default user with empty password first
        default_client = ClickHouseClient(
            host=host,
            port=port,
            username="default",
            password="",  # Default user typically has no password
            database="default"
        )
        
        default_connected = default_client.connect()
        
        # If that fails, try with no password parameter (some ClickHouse setups)
        if not default_connected:
            logger.debug("Trying default user connection without password parameter...")
            try:
                import clickhouse_connect
                default_client.client = clickhouse_connect.get_client(
                    host=host,
                    port=port,
                    username="default",
                    database="default"
                )
                default_client.client.query("SELECT 1")
                default_connected = True
                logger.info("Connected with default user (no password)")
            except Exception as e:
                logger.debug(f"Default user connection without password also failed: {e}")
        
        if default_connected:
            logger.info("Connected with default user, creating custom user...")
            if create_user_if_not_exists(default_client, username, password):
                # Now try connecting again with the custom user
                logger.info(f"Retrying connection with user '{username}'...")
                connection_success = client.connect()
                if connection_success:
                    default_client.disconnect()
                else:
                    logger.error(f"Still failed to connect with user '{username}' after creation")
                    logger.error("You may need to restart ClickHouse container for user changes to take effect")
                    default_client.disconnect()
                    sys.exit(1)
            else:
                logger.error("Failed to create user")
                default_client.disconnect()
                sys.exit(1)
        else:
            logger.error("Failed to connect even with default user")
            logger.error("Please check ClickHouse container status and logs")
            logger.error("You may need to manually create the user or check ClickHouse configuration")
            sys.exit(1)
    
    if not connection_success:
        logger.error("Failed to connect to ClickHouse")
        sys.exit(1)
    
    try:
        # Get SQL files - try multiple possible locations
        # 1. Absolute path in container (from volume mount)
        init_dir = Path("/app/clickhouse/init")
        
        # 2. Relative to project root (when running locally)
        if not init_dir.exists():
            init_dir = project_root / "clickhouse" / "init"
        
        # 3. If still not found, try from current working directory
        if not init_dir.exists():
            init_dir = Path("clickhouse/init")
        
        logger.info(f"Looking for SQL files in: {init_dir}")
        
        if not init_dir.exists():
            logger.error(f"ClickHouse init directory not found. Tried:")
            logger.error(f"  - /app/clickhouse/init")
            logger.error(f"  - {project_root / 'clickhouse' / 'init'}")
            logger.error(f"  - clickhouse/init")
            logger.error(f"\nNote: Make sure the clickhouse directory is mounted in docker-compose.yml")
            logger.error(f"and the scraper container has been restarted after adding the volume.")
            sys.exit(1)
        
        logger.info(f"Found ClickHouse init directory: {init_dir}")
        
        # Required tables for each database
        required_fotmob_tables = ['general', 'timeline', 'venue', 'player', 'shotmap', 'goal', 
                                  'cards', 'red_card', 'period', 'momentum', 'starters', 
                                  'substitutes', 'coaches', 'team_form']
        required_aiscore_tables = ['matches', 'odds_1x2', 'odds_asian_handicap', 'odds_over_under', 'daily_listings']
        
        fotmob_tables_exist = False
        aiscore_tables_exist = False
        
        databases_to_check = ["fotmob", "aiscore"]
        databases_exist = check_databases_exist(client, databases_to_check)
        
        if databases_exist:
            
            try:
                result = client.execute("SHOW TABLES FROM fotmob")
                tables = []
                
                # Handle different result types
                if hasattr(result, 'result_rows'):
                    tables = [row[0] for row in result.result_rows]
                elif hasattr(result, 'result_columns'):
                    if result.result_columns and len(result.result_columns) > 0:
                        tables = list(result.result_columns[0])
                elif isinstance(result, (list, tuple)):
                    tables = [row[0] if isinstance(row, (list, tuple)) else str(row) for row in result]
                
                # Check if ALL required tables exist
                missing_fotmob = [t for t in required_fotmob_tables if t not in tables]
                fotmob_tables_exist = len(missing_fotmob) == 0
                if tables:
                    logger.info(f"FotMob database has {len(tables)} tables: {', '.join(tables)}")
                    if missing_fotmob:
                        logger.info(f"Missing FotMob tables: {', '.join(missing_fotmob)}")
            except Exception as e:
                logger.debug(f"Could not check tables in fotmob database: {e}")
            
            try:
                result = client.execute("SHOW TABLES FROM aiscore")
                tables = []
                
                # Handle different result types
                if hasattr(result, 'result_rows'):
                    tables = [row[0] for row in result.result_rows]
                elif hasattr(result, 'result_columns'):
                    if result.result_columns and len(result.result_columns) > 0:
                        tables = list(result.result_columns[0])
                elif isinstance(result, (list, tuple)):
                    tables = [row[0] if isinstance(row, (list, tuple)) else str(row) for row in result]
                
                # Check if ALL required tables exist
                missing_aiscore = [t for t in required_aiscore_tables if t not in tables]
                aiscore_tables_exist = len(missing_aiscore) == 0
                if tables:
                    logger.info(f"AIScore database has {len(tables)} tables: {', '.join(tables)}")
                    if missing_aiscore:
                        logger.info(f"Missing AIScore tables: {', '.join(missing_aiscore)}")
            except Exception as e:
                logger.debug(f"Could not check tables in aiscore database: {e}")
            
            finally:
                client.database = "default"
            
            if fotmob_tables_exist and aiscore_tables_exist:
                logger.info("All required tables already exist for both FotMob and AIScore. Skipping creation.")
                return
            elif fotmob_tables_exist and not aiscore_tables_exist:
                logger.info("FotMob tables complete but AIScore tables are missing. Creating AIScore tables...")
            elif aiscore_tables_exist and not fotmob_tables_exist:
                logger.info("AIScore tables complete but FotMob tables are missing. Creating FotMob tables...")
            else:
                logger.info("Some tables are missing. Proceeding with table creation...")
        else:
            logger.info("Databases do not exist. Proceeding with database and table creation...")
        
        db_file = init_dir / "00_create_databases_fotmob_and_aiscore.sql"
        if not db_file.exists():
            db_file = init_dir / "01_create_database.sql"
        
        if db_file.exists():
            logger.info("=" * 60)
            logger.info("STEP 1: Creating databases (fotmob and aiscore)...")
            logger.info("=" * 60)
            if not execute_sql_file(client, db_file):
                logger.error("Failed to create databases!")
                sys.exit(1)
            logger.info("[SUCCESS] Databases created\n")
        else:
            logger.error(f"Database creation script not found. Tried:")
            logger.error(f"  - {init_dir / '00_create_databases_fotmob_and_aiscore.sql'}")
            logger.error(f"  - {init_dir / '01_create_database.sql'}")
            sys.exit(1)
        
        if not fotmob_tables_exist:
            fotmob_file = init_dir / "01_create_fotmob_tables.sql"
            if not fotmob_file.exists():
                fotmob_file = init_dir / "02_create_fotmob_tables.sql"
            
            if fotmob_file.exists():
                logger.info("=" * 60)
                logger.info("STEP 2: Creating FotMob tables...")
                logger.info("=" * 60)
                if not execute_sql_file(client, fotmob_file, database="fotmob"):
                    logger.error("Failed to create FotMob tables!")
                    sys.exit(1)
                logger.info("[SUCCESS] FotMob tables created\n")
            else:
                logger.error(f"FotMob table creation script not found. Tried:")
                logger.error(f"  - {init_dir / '01_create_fotmob_tables.sql'}")
                logger.error(f"  - {init_dir / '02_create_fotmob_tables.sql'}")
                sys.exit(1)
        else:
            logger.info("Skipping FotMob table creation (tables already exist)")
        
        if not aiscore_tables_exist:
            aiscore_file = init_dir / "02_create_aiscore_tables.sql"
            if not aiscore_file.exists():
                aiscore_file = init_dir / "03_create_aiscore_tables.sql"
            
            if aiscore_file.exists():
                logger.info("=" * 60)
                logger.info("STEP 3: Creating AIScore tables...")
                logger.info("=" * 60)
                if not execute_sql_file(client, aiscore_file, database="aiscore"):
                    logger.error("Failed to create AIScore tables!")
                    sys.exit(1)
                logger.info("[SUCCESS] AIScore tables created\n")
            else:
                logger.error(f"AIScore table creation script not found. Tried:")
                logger.error(f"  - {init_dir / '02_create_aiscore_tables.sql'}")
                logger.error(f"  - {init_dir / '03_create_aiscore_tables.sql'}")
                sys.exit(1)
        else:
            logger.info("Skipping AIScore table creation (tables already exist)")
        
        logger.info("\n=== Verifying tables ===")
        
        try:
            result = client.execute("SHOW TABLES FROM fotmob")
            if hasattr(result, 'result_rows'):
                tables = [row[0] for row in result.result_rows]
                logger.info(f"FotMob database has {len(tables)} tables: {', '.join(tables)}")
            else:
                logger.info("FotMob tables created (unable to verify count)")
        except Exception as e:
            logger.warning(f"Could not verify FotMob tables: {e}")
        
        try:
            result = client.execute("SHOW TABLES FROM aiscore")
            if hasattr(result, 'result_rows'):
                tables = [row[0] for row in result.result_rows]
                logger.info(f"AIScore database has {len(tables)} tables: {', '.join(tables)}")
            else:
                logger.info("AIScore tables created (unable to verify count)")
        except Exception as e:
            logger.warning(f"Could not verify AIScore tables: {e}")
        
        logger.info("\n[SUCCESS] ClickHouse tables created")
        
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()

