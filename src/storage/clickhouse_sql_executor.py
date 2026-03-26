"""Shared SQL execution helpers for ClickHouse-backed medallion layers."""

import logging
from pathlib import Path

from .clickhouse_client import ClickHouseClient

logger = logging.getLogger(__name__)


def split_sql_statements(sql_content: str) -> list[str]:
    """Split SQL content into executable statements while skipping comments."""
    statements: list[str] = []
    current_statement: list[str] = []

    for line in sql_content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue

        if "--" in line:
            line = line[: line.index("--")]

        stripped = line.strip()
        if not stripped:
            continue

        current_statement.append(stripped)
        if stripped.endswith(";"):
            statement = " ".join(current_statement).rstrip(";").strip()
            if statement:
                statements.append(statement)
            current_statement = []

    if current_statement:
        statement = " ".join(current_statement).strip()
        if statement:
            statements.append(statement)

    return statements


def execute_sql_script(
    client: ClickHouseClient,
    sql_file: Path,
    layer_name: str,
) -> None:
    """Execute one layer SQL file statement by statement."""
    sql_content = sql_file.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_content)

    if not statements:
        logger.warning("No executable %s SQL found in %s", layer_name, sql_file.name)
        return

    logger.info(
        "Executing %s SQL from %s (%s statements)",
        layer_name,
        sql_file.name,
        len(statements),
    )
    for statement in statements:
        client.execute(statement)
