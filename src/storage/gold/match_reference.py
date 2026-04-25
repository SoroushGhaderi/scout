"""Refresh gold match reference tables for content availability."""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from ..clickhouse_client import ClickHouseClient

ContentPart = Literal["signals", "scenarios"]

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_SQL_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "gold" / "reference"


@dataclass(frozen=True)
class MatchReferenceConfig:
    """Configuration for one gold match reference table."""

    part: ContentPart
    table_prefix: str
    target_table: str
    item_label: str
    refresh_sql_file: str
    empty_refresh_sql_file: str


_REFERENCE_CONFIGS: dict[ContentPart, MatchReferenceConfig] = {
    "signals": MatchReferenceConfig(
        part="signals",
        table_prefix="sig_",
        target_table="match_signal_reference",
        item_label="signal",
        refresh_sql_file="refresh_match_signal_reference.sql",
        empty_refresh_sql_file="refresh_match_signal_reference_empty.sql",
    ),
    "scenarios": MatchReferenceConfig(
        part="scenarios",
        table_prefix="scenario_",
        target_table="match_scenario_reference",
        item_label="scenario",
        refresh_sql_file="refresh_match_scenario_reference.sql",
        empty_refresh_sql_file="refresh_match_scenario_reference_empty.sql",
    ),
}


def _safe_identifier(identifier: str) -> str:
    if not _SAFE_IDENTIFIER.match(identifier):
        raise ValueError(f"Unsafe ClickHouse identifier: {identifier}")
    return identifier


def _read_sql_template(file_name: str) -> str:
    return (_SQL_DIR / file_name).read_text(encoding="utf-8").strip()


def _render_sql_template(template: str, variables: dict[str, str]) -> str:
    rendered = template
    for key, value in variables.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    unresolved_placeholders = re.findall(r"\{\{[a-zA-Z_][a-zA-Z0-9_]*\}\}", rendered)
    if unresolved_placeholders:
        raise ValueError(
            "Unresolved SQL template placeholder(s): "
            + ", ".join(sorted(set(unresolved_placeholders)))
        )
    return rendered


def _render_sql_file(file_name: str, variables: dict[str, str]) -> str:
    return _render_sql_template(_read_sql_template(file_name), variables)


def _list_content_tables(
    client: ClickHouseClient,
    *,
    database: str,
    table_prefix: str,
    excluded_tables: set[str],
) -> list[str]:
    """Return content tables with a match_id column for availability checks."""
    db = _safe_identifier(database)
    query = _read_sql_template("list_content_tables.sql")
    result = client.execute(
        query,
        {"database": db, "table_prefix": table_prefix},
        log_query=False,
    )
    tables = [str(row[0]) for row in result.result_rows if row]
    return [_safe_identifier(table) for table in tables if table not in excluded_tables]


def _build_available_items_join(
    *,
    database: str,
    table_names: list[str],
    item_label: str,
) -> str:
    db = _safe_identifier(database)
    label = _safe_identifier(item_label)
    item_column = f"{label}_id"
    source_template = _read_sql_template("available_item_source.sql")
    item_sources = [
        _render_sql_template(
            source_template,
            {
                "database": db,
                "table_name": _safe_identifier(table_name),
                "item_id": _safe_identifier(table_name),
                "item_column": item_column,
            },
        )
        for table_name in table_names
    ]
    separator = "\n" + _read_sql_template("available_item_sources_separator.sql") + "\n"
    return _render_sql_file(
        "available_items_join.sql",
        {
            "item_column": item_column,
            "available_column": f"available_{label}_ids",
            "available_item_sources": separator.join(item_sources),
        },
    )


def refresh_match_reference(
    client: ClickHouseClient,
    *,
    part: ContentPart,
    database: str = "gold",
) -> int:
    """Refresh a gold match reference table and return discovered content count."""
    config = _REFERENCE_CONFIGS[part]
    excluded_tables = {config.target_table}
    item_ids = _list_content_tables(
        client,
        database=database,
        table_prefix=config.table_prefix,
        excluded_tables=excluded_tables,
    )
    if item_ids:
        insert_sql = _render_sql_file(
            config.refresh_sql_file,
            {
                "database": _safe_identifier(database),
                "available_items_join": _build_available_items_join(
                    database=database,
                    table_names=item_ids,
                    item_label=config.item_label,
                ),
            },
        )
        client.execute(insert_sql, {"all_item_ids": item_ids})
    else:
        insert_sql = _render_sql_file(
            config.empty_refresh_sql_file,
            {"database": _safe_identifier(database)},
        )
        client.execute(insert_sql)

    optimize_sql = _render_sql_file(
        "optimize_match_reference.sql",
        {
            "database": _safe_identifier(database),
            "target_table": _safe_identifier(config.target_table),
        },
    )
    client.execute(optimize_sql)
    return len(item_ids)
