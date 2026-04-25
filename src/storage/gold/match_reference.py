"""Refresh gold match reference tables for content availability."""

import re
from dataclasses import dataclass
from typing import Literal

from ..clickhouse_client import ClickHouseClient

ContentPart = Literal["signals", "scenarios"]

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

_BRONZE_MATCH_COLUMNS = [
    "match_id",
    "match_date",
    "match_time_utc",
    "match_time_utc_date",
    "match_round",
    "coverage_level",
    "league_id",
    "league_name",
    "league_round_name",
    "parent_league_id",
    "parent_league_name",
    "parent_league_season",
    "parent_league_tournament_id",
    "country_code",
    "home_team_id",
    "home_team_name",
    "away_team_id",
    "away_team_name",
    "match_started",
    "match_finished",
    "full_score",
    "home_score",
    "away_score",
]


@dataclass(frozen=True)
class MatchReferenceConfig:
    """Configuration for one gold match reference table."""

    part: ContentPart
    table_prefix: str
    target_table: str
    item_label: str


_REFERENCE_CONFIGS: dict[ContentPart, MatchReferenceConfig] = {
    "signals": MatchReferenceConfig(
        part="signals",
        table_prefix="sig_",
        target_table="match_signal_reference",
        item_label="signal",
    ),
    "scenarios": MatchReferenceConfig(
        part="scenarios",
        table_prefix="scenario_",
        target_table="match_scenario_reference",
        item_label="scenario",
    ),
}


def _safe_identifier(identifier: str) -> str:
    if not _SAFE_IDENTIFIER.match(identifier):
        raise ValueError(f"Unsafe ClickHouse identifier: {identifier}")
    return identifier


def _sql_string_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _array_literal(values: list[str]) -> str:
    if not values:
        return "CAST([], 'Array(String)')"
    return "[" + ", ".join(_sql_string_literal(value) for value in values) + "]"


def _list_content_tables(
    client: ClickHouseClient,
    *,
    database: str,
    table_prefix: str,
    excluded_tables: set[str],
) -> list[str]:
    """Return content tables with a match_id column for availability checks."""
    db = _safe_identifier(database)
    query = """
        SELECT t.name
        FROM system.tables AS t
        INNER JOIN system.columns AS c
            ON t.database = c.database
           AND t.name = c.table
        WHERE t.database = %(database)s
          AND startsWith(t.name, %(table_prefix)s)
          AND c.name = 'match_id'
        ORDER BY t.name
    """
    result = client.execute(
        query,
        {"database": db, "table_prefix": table_prefix},
        log_query=False,
    )
    tables = [str(row[0]) for row in result.result_rows if row]
    return [_safe_identifier(table) for table in tables if table not in excluded_tables]


def _build_available_items_subquery(
    *,
    database: str,
    table_names: list[str],
    item_label: str,
) -> str:
    db = _safe_identifier(database)
    item_column = f"{item_label}_id"
    selects = [
        (
            f"SELECT match_id, {_sql_string_literal(table_name)} AS {item_column} "
            f"FROM {db}.{table_name} FINAL "
            "WHERE match_id > 0 "
            f"GROUP BY match_id, {item_column}"
        )
        for table_name in table_names
    ]
    union_query = "\n            UNION ALL\n            ".join(selects)
    return f"""
        SELECT
            match_id,
            groupUniqArray({item_column}) AS available_{item_label}_ids
        FROM (
            {union_query}
        )
        GROUP BY match_id
    """


def build_match_reference_insert_sql(
    *,
    database: str,
    target_table: str,
    item_label: str,
    item_ids: list[str],
) -> str:
    """Build the INSERT that refreshes one gold match reference table."""
    db = _safe_identifier(database)
    target = _safe_identifier(target_table)
    label = _safe_identifier(item_label)
    all_items_expr = _array_literal(item_ids)
    empty_array_expr = "CAST([], 'Array(String)')"
    available_expr = f"ifNull(available.available_{label}_ids, {empty_array_expr})"

    insert_columns = _BRONZE_MATCH_COLUMNS + [
        f"all_{label}_ids",
        f"available_{label}_ids",
        f"unavailable_{label}_ids",
        f"{label}_count",
        f"available_{label}_count",
        f"has_any_{label}",
    ]
    bronze_select_columns = [f"br.{column}" for column in _BRONZE_MATCH_COLUMNS]

    if item_ids:
        available_join = (
            "LEFT JOIN (\n"
            + _build_available_items_subquery(
                database=db,
                table_names=item_ids,
                item_label=label,
            )
            + "\n    ) AS available ON br.match_id = available.match_id"
        )
        availability_select = [
            f"{all_items_expr} AS all_{label}_ids",
            f"arraySort({available_expr}) AS available_{label}_ids",
            (
                "arraySort(arrayFilter("
                f"{label}_id -> NOT has({available_expr}, {label}_id), "
                f"{all_items_expr}"
                f")) AS unavailable_{label}_ids"
            ),
            f"toUInt16(length({all_items_expr})) AS {label}_count",
            f"toUInt16(length({available_expr})) AS available_{label}_count",
            f"toUInt8(length({available_expr}) > 0) AS has_any_{label}",
        ]
    else:
        available_join = ""
        availability_select = [
            f"{empty_array_expr} AS all_{label}_ids",
            f"{empty_array_expr} AS available_{label}_ids",
            f"{empty_array_expr} AS unavailable_{label}_ids",
            f"toUInt16(0) AS {label}_count",
            f"toUInt16(0) AS available_{label}_count",
            f"toUInt8(0) AS has_any_{label}",
        ]

    select_columns = bronze_select_columns + availability_select
    return f"""
        INSERT INTO {db}.{target} (
            {", ".join(insert_columns)}
        )
        SELECT
            {", ".join(select_columns)}
        FROM bronze.match_reference FINAL AS br
        {available_join}
        WHERE br.match_id > 0
    """


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
    insert_sql = build_match_reference_insert_sql(
        database=database,
        target_table=config.target_table,
        item_label=config.item_label,
        item_ids=item_ids,
    )
    client.execute(insert_sql)
    client.execute(f"OPTIMIZE TABLE {database}.{config.target_table} FINAL DEDUPLICATE")
    return len(item_ids)
