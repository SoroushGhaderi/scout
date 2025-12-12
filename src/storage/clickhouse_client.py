"""ClickHouse client for loading data from silver layer to ClickHouse data warehouse.

Supports both FotMob and AIScore scrapers with separate databases.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List

import clickhouse_connect

from ..utils.health_check import check_clickhouse_connection
from ..utils.logging_utils import get_logger


class ClickHouseClient:
    """ClickHouse client for data warehouse operations."""

    ALLOWED_TABLES = {
        'general',
        'timeline',
        'venue',
        'player',
        'shotmap',
        'goal',
        'cards',
        'red_card',
        'period',
        'momentum',
        'starters',
        'substitutes',
        'coaches',
        'team_form',

        'matches',
        'odds_1x2',
        'odds_asian_handicap',
        'odds_over_under',
        'daily_listings'
    }

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "default"
    ):
        """Initialize ClickHouse client.

        Args:
            host: ClickHouse server host
            port: ClickHouse HTTP port
            username: ClickHouse username
            password: ClickHouse password
            database: Database name
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.logger = get_logger()
        self.client: Optional[clickhouse_connect.driver.Client] = None

    def _validate_table_name(self, table: str) -> str:
        """Validate table name against whitelist to prevent SQL injection.

        Args:
            table: Table name to validate

        Returns:
            Validated table name

        Raises:
            ValueError: If table name not in whitelist
        """
        if table not in self.ALLOWED_TABLES:
            raise ValueError(
                f"Invalid table name: '{table}'. "
                f"Allowed tables: {', '.join(sorted(self.ALLOWED_TABLES))}"
            )
        return table

    def connect(self) -> bool:
        """Connect to ClickHouse server."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database
            )

            result = self.client.query("SELECT 1")
            self.logger.info(
                f"Connected to ClickHouse: {self.host}:{self.port}/{self.database}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to ClickHouse: {e}")
            return False

    def disconnect(self):
        """Disconnect from ClickHouse."""
        if self.client:
            self.client.close()
            self.client = None

    def execute(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Execute a query."""
        if not self.client:
            raise RuntimeError("Not connected to ClickHouse. Call connect() first.")

        try:
            if parameters:
                return self.client.query(query, parameters=parameters)
            else:
                return self.client.query(query)
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}\nQuery: {query[:200]}")
            raise

    def insert_dataframe(self, table: str, df, database: Optional[str] = None) -> int:
        """Insert pandas DataFrame to ClickHouse table with SQL injection protection.

        Args:
            table: Table name (validated against whitelist)
            df: Pandas DataFrame
            database: Optional database name (overrides default)

        Returns:
            Number of rows inserted

        Raises:
            ValueError: If table name not in whitelist
        """
        if not self.client:
            raise RuntimeError("Not connected to ClickHouse. Call connect() first.")

        table = self._validate_table_name(table)

        if df.empty:
            self.logger.warning(f"DataFrame is empty, skipping insert to {table}")
            return 0

        try:
            db = database or self.database
            full_table = f"{db}.{table}" if db else table

            self.client.insert_df(full_table, df)
            rows_inserted = len(df)
            self.logger.info(f"Inserted {rows_inserted} rows to {full_table}")
            return rows_inserted

        except Exception as e:
            self.logger.error(f"Failed to insert to {table}: {e}")
            raise

    def get_table_stats(
        self, table: str, database: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get table statistics with SQL injection protection.

        Args:
            table: Table name (validated against whitelist)
            database: Optional database name

        Returns:
            Dictionary with table statistics

        Raises:
            ValueError: If table name not in whitelist
        """
        if not self.client:
            raise RuntimeError("Not connected to ClickHouse. Call connect() first.")

        table = self._validate_table_name(table)

        db = database or self.database
        full_table = f"{db}.{table}" if db else table

        try:
            count_result = self.execute(f"SELECT COUNT(*) as count FROM {full_table}")
            row_count = count_result.result_rows[0][0] if count_result.result_rows else 0

            size_query = (
                f"SELECT formatReadableSize(sum(bytes)) as size, sum(rows) as rows "
                f"FROM system.parts WHERE database = '{db}' AND table = '{table}' AND active"
            )
            size_result = self.execute(size_query)
            size_info = (
                size_result.result_rows[0] if size_result.result_rows else ("0 B", 0)
            )

            return {
                "table": full_table,
                "row_count": row_count,
                "size": size_info[0],
                "rows_in_parts": size_info[1]
            }

        except Exception as e:
            self.logger.error(f"Failed to get stats for {table}: {e}")
            return {"table": full_table, "error": str(e)}

    def truncate_table(self, table: str, database: Optional[str] = None):
        """Truncate a table with SQL injection protection.

        Args:
            table: Table name (validated against whitelist)
            database: Optional database name

        Raises:
            ValueError: If table name not in whitelist
        """
        if not self.client:
            raise RuntimeError("Not connected to ClickHouse. Call connect() first.")

        table = self._validate_table_name(table)

        db = database or self.database
        full_table = f"{db}.{table}" if db else table

        try:
            self.execute(f"TRUNCATE TABLE {full_table}")
            self.logger.info(f"Truncated table {full_table}")
        except Exception as e:
            self.logger.error(f"Failed to truncate {table}: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def health_check(self) -> Dict[str, Any]:
        """Check ClickHouse connection health.

        Returns:
            Dictionary with health check results
        """
        return check_clickhouse_connection(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database
        )
