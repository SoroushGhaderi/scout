"""Run FotMob silver SQL transformations in ClickHouse."""

import logging
from pathlib import Path
from typing import Iterable

from ..clickhouse_client import ClickHouseClient

logger = logging.getLogger(__name__)


class FotMobSilverStorage:
    """Executes silver SQL scripts for FotMob."""

    def __init__(self, client: ClickHouseClient, database: str = "fotmob"):
        self.client = client
        self.database = database

    def execute_sql_files(self, sql_files: Iterable[Path]) -> None:
        """Execute SQL files sequentially."""
        for sql_file in sql_files:
            self._execute_sql_file(sql_file)

    def _execute_sql_file(self, sql_file: Path) -> None:
        sql_content = sql_file.read_text(encoding="utf-8")
        statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]
        for statement in statements:
            logger.info("Executing silver SQL from %s", sql_file.name)
            self.client.execute(statement)
