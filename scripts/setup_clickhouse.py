"""Create ClickHouse tables for FotMob database.

SCRAPER: FotMob
PURPOSE: Initialize ClickHouse database and tables from SQL scripts

Usage:
    python scripts/setup_clickhouse.py
"""

import os
import sys
from pathlib import Path

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

        if hasattr(result, "result_rows"):
            existing_dbs = {row[0] for row in result.result_rows}
        elif hasattr(result, "result_columns"):
            if result.result_columns and len(result.result_columns) > 0:
                existing_dbs = set(result.result_columns[0])
        elif isinstance(result, (list, tuple)):
            existing_dbs = {
                row[0] if isinstance(row, (list, tuple)) else str(row) for row in result
            }
        else:
            logger.debug("Using fallback method to check databases")
            for db in databases:
                try:
                    client.execute(f"SELECT 1 FROM {db}.system.tables LIMIT 1")
                    existing_dbs.add(db)
                except:
                    pass

        missing_dbs = [db for db in databases if db not in existing_dbs]
        if missing_dbs:
            logger.info(f"Missing databases: {', '.join(missing_dbs)}")
            return False

        logger.info(f"All databases exist: {', '.join(databases)}")
        return True
    except Exception as e:
        logger.warning(f"Error checking databases: {e}")
        return False


def execute_sql_file(client: ClickHouseClient, sql_file: Path, database: str = None):
    """Execute SQL file statement by statement."""
    try:
        with open(sql_file, "r", encoding="utf-8") as f:
            sql_content = f.read()

        if database:
            try:
                client.execute(f"USE {database}")
                logger.debug(f"Switched to database: {database}")
                client.database = database
            except Exception as e:
                logger.error(f"Failed to switch to database {database}: {e}")
                return False

        statements = []
        current_statement = []

        for line in sql_content.split("\n"):
            if line.strip().startswith("--"):
                continue

            if "--" in line:
                line = line[: line.index("--")]

            line = line.strip()
            if not line:
                continue

            if database and line.upper().startswith("USE "):
                logger.debug(f"Skipping USE statement (already using {database}): {line}")
                continue

            current_statement.append(line)

            if line.rstrip().endswith(";"):
                statement = " ".join(current_statement).rstrip(";").strip()
                if statement:
                    statements.append(statement)
                current_statement = []

        if current_statement:
            statement = " ".join(current_statement).strip()
            if statement:
                statements.append(statement)

        executed_count = 0
        failed_statements = []
        for i, statement in enumerate(statements, 1):
            if not statement:
                continue

            if database and statement.upper().strip().startswith("CREATE TABLE"):
                import re

                if "IF NOT EXISTS" in statement.upper():
                    pattern = r"CREATE TABLE IF NOT EXISTS\s+(\w+\.)?(\w+)"
                    replacement = f"CREATE TABLE IF NOT EXISTS {database}.\\2"
                else:
                    pattern = r"CREATE TABLE\s+(\w+\.)?(\w+)"
                    replacement = f"CREATE TABLE {database}.\\2"

                if not re.search(
                    r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?\w+\.\w+", statement, re.IGNORECASE
                ):
                    statement = re.sub(
                        pattern, replacement, statement, count=1, flags=re.IGNORECASE
                    )

            try:
                logger.debug(
                    f"Executing statement {i}/{len(statements)} from {sql_file.name}: {statement[:100]}..."
                )
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

        logger.info(
            f"Successfully executed {executed_count}/{len(statements)} statements from {sql_file.name}"
        )
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
        result = client.execute(f"SELECT name FROM system.users WHERE name = '{username}'")
        user_exists = False

        if hasattr(result, "result_rows") and result.result_rows:
            user_exists = True
        elif hasattr(result, "result_columns") and result.result_columns:
            user_exists = len(result.result_columns[0]) > 0

        if user_exists:
            logger.info(f"User '{username}' already exists, skipping creation")
            return True

        logger.info(f"Creating user '{username}'...")
        create_user_sql = f"CREATE USER IF NOT EXISTS {username} IDENTIFIED BY '{password}'"
        client.execute(create_user_sql)

        logger.info(f"Granting permissions to '{username}'...")
        grant_queries = [
            f"GRANT ALL ON fotmob.* TO {username}",
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
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    username = os.getenv("CLICKHOUSE_USER", "fotmob_user")
    password = os.getenv("CLICKHOUSE_PASSWORD", "fotmob_pass")

    logger.info(f"Connecting to ClickHouse at {host}:{port}")

    client = ClickHouseClient(
        host=host, port=port, username=username, password=password, database="default"
    )

    connection_success = client.connect()

    if not connection_success:
        logger.warning(f"Failed to connect with user '{username}', trying default user...")

        default_client = ClickHouseClient(
            host=host, port=port, username="default", password="", database="default"
        )

        default_connected = default_client.connect()

        if not default_connected:
            logger.debug("Trying default user connection without password parameter...")
            try:
                import clickhouse_connect

                default_client.client = clickhouse_connect.get_client(
                    host=host, port=port, username="default", database="default"
                )
                default_client.client.query("SELECT 1")
                default_connected = True
                logger.info("Connected with default user (no password)")
            except Exception as e:
                logger.debug(f"Default user connection without password also failed: {e}")

        if default_connected:
            logger.info("Connected with default user, creating custom user...")
            if create_user_if_not_exists(default_client, username, password):
                logger.info(f"Retrying connection with user '{username}'...")
                connection_success = client.connect()
                if connection_success:
                    default_client.disconnect()
                else:
                    logger.error(f"Still failed to connect with user '{username}' after creation")
                    logger.error(
                        "You may need to restart ClickHouse container for user changes to take effect"
                    )
                    default_client.disconnect()
                    sys.exit(1)
            else:
                logger.error("Failed to create user")
                default_client.disconnect()
                sys.exit(1)
        else:
            logger.error("Failed to connect even with default user")
            logger.error("Please check ClickHouse container status and logs")
            logger.error(
                "You may need to manually create the user or check ClickHouse configuration"
            )
            sys.exit(1)

    if not connection_success:
        logger.error("Failed to connect to ClickHouse")
        sys.exit(1)

    try:
        clickhouse_root_candidates = [
            Path("/app/clickhouse"),
            project_root / "clickhouse",
            Path("clickhouse"),
        ]
        clickhouse_root = next((p for p in clickhouse_root_candidates if p.exists()), None)

        if not clickhouse_root:
            logger.error("ClickHouse SQL directory not found. Tried:")
            for candidate in clickhouse_root_candidates:
                logger.error(f"  - {candidate}")
            sys.exit(1)

        layer_dirs = {
            "bronze": clickhouse_root / "bronze",
            "silver": clickhouse_root / "silver",
            "gold": clickhouse_root / "gold",
        }

        missing_layers = [name for name, layer_dir in layer_dirs.items() if not layer_dir.exists()]
        if missing_layers:
            logger.error(f"Missing SQL directories for layers: {', '.join(missing_layers)}")
            sys.exit(1)

        logger.info(f"Using ClickHouse SQL root: {clickhouse_root}")

        databases_to_check = ["fotmob"]
        if check_databases_exist(client, databases_to_check):
            logger.info("FotMob database already exists (scripts are idempotent, continuing)")

        step_number = 1
        for layer_name in ("bronze", "silver", "gold"):
            layer_dir = layer_dirs[layer_name]
            sql_files = sorted(layer_dir.glob("*.sql"))
            if not sql_files:
                logger.error(f"No SQL files found in {layer_dir}")
                sys.exit(1)

            logger.info("=" * 60)
            logger.info(f"STEP {step_number}: Executing {layer_name.upper()} SQL scripts...")
            logger.info("=" * 60)

            for sql_file in sql_files:
                logger.info(f"Running {sql_file.name}")
                if not execute_sql_file(client, sql_file):
                    logger.error(f"Failed while executing {sql_file}")
                    sys.exit(1)

            logger.info(f"[SUCCESS] {layer_name.upper()} SQL executed\n")
            step_number += 1

        logger.info("\n[SUCCESS] ClickHouse medallion setup completed")

    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
