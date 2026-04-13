"""Run FotMob silver SQL transformations in ClickHouse."""
from pathlib import Path
from typing import Iterable

from ..clickhouse_client import ClickHouseClient
from ..clickhouse_sql_executor import execute_sql_script


class FotMobSilverStorage:
    """Executes silver SQL scripts for FotMob."""

    def __init__(self, client: ClickHouseClient, database: str = "silver"):
        self.client = client
        self.database = database

    def execute_sql_files(self, sql_files: Iterable[Path]) -> None:
        """Execute SQL files sequentially."""
        for sql_file in sql_files:
            self._execute_sql_file(sql_file)

    def _execute_sql_file(self, sql_file: Path) -> None:
        execute_sql_script(self.client, sql_file, layer_name="silver")
