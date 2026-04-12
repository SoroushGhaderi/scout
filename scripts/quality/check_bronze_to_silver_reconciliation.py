"""Check bronze-to-silver entity coverage in ClickHouse.

This script reports whether entities present in bronze are missing in silver,
using eligibility filters aligned with silver DML rules to avoid false positives.
"""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

project_root = Path(__file__).resolve().parents[2]
scripts_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from config.settings import settings
from src.storage.clickhouse_client import ClickHouseClient
from src.utils.logging_utils import get_logger

logger = get_logger()


@dataclass(frozen=True)
class CheckSpec:
    """Defines one bronze->silver parity check."""

    name: str
    bronze_count_sql: str
    silver_count_sql: str
    missing_count_sql: str
    sample_sql: str
    sample_fields: List[str]


def _result_rows(result: Any) -> List[List[Any]]:
    if hasattr(result, "result_rows") and result.result_rows:
        return result.result_rows
    if isinstance(result, list):
        return result
    return []


def _query_scalar(client: ClickHouseClient, sql: str) -> int:
    result = client.execute(sql, log_query=False)
    rows = _result_rows(result)
    if not rows:
        return 0
    return int(rows[0][0])


def _query_rows(client: ClickHouseClient, sql: str) -> List[List[Any]]:
    return _result_rows(client.execute(sql, log_query=False))


def _format_row(row: List[Any]) -> str:
    return ", ".join(str(value) for value in row)


def _build_specs(sample_limit: int) -> Dict[str, CheckSpec]:
    return {
        "match": CheckSpec(
            name="match",
            bronze_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id
                    FROM bronze.general FINAL
                )
            """,
            silver_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id
                    FROM silver.match FINAL
                )
            """,
            missing_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id
                    FROM bronze.general FINAL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id
                    FROM silver.match FINAL
                ) AS s USING (match_id)
                WHERE s.match_id IS NULL
            """,
            sample_sql=f"""
                SELECT b.match_id
                FROM (
                    SELECT DISTINCT match_id
                    FROM bronze.general FINAL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id
                    FROM silver.match FINAL
                ) AS s USING (match_id)
                WHERE s.match_id IS NULL
                ORDER BY b.match_id
                LIMIT {sample_limit}
            """,
            sample_fields=["match_id"],
        ),
        "player": CheckSpec(
            name="player",
            bronze_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, player_id
                    FROM bronze.player FINAL
                    WHERE team_id IS NOT NULL
                )
            """,
            silver_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, player_id
                    FROM silver.player_match_stat FINAL
                )
            """,
            missing_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, player_id
                    FROM bronze.player FINAL
                    WHERE team_id IS NOT NULL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, player_id
                    FROM silver.player_match_stat FINAL
                ) AS s USING (match_id, player_id)
                WHERE s.match_id IS NULL
            """,
            sample_sql=f"""
                SELECT b.match_id, b.player_id
                FROM (
                    SELECT DISTINCT match_id, player_id
                    FROM bronze.player FINAL
                    WHERE team_id IS NOT NULL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, player_id
                    FROM silver.player_match_stat FINAL
                ) AS s USING (match_id, player_id)
                WHERE s.match_id IS NULL
                ORDER BY b.match_id, b.player_id
                LIMIT {sample_limit}
            """,
            sample_fields=["match_id", "player_id"],
        ),
        "shot": CheckSpec(
            name="shot",
            bronze_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, shot_id
                    FROM bronze.shotmap FINAL
                )
            """,
            silver_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, shot_id
                    FROM silver.shot FINAL
                )
            """,
            missing_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, shot_id
                    FROM bronze.shotmap FINAL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, shot_id
                    FROM silver.shot FINAL
                ) AS s USING (match_id, shot_id)
                WHERE s.match_id IS NULL
            """,
            sample_sql=f"""
                SELECT b.match_id, b.shot_id
                FROM (
                    SELECT DISTINCT match_id, shot_id
                    FROM bronze.shotmap FINAL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, shot_id
                    FROM silver.shot FINAL
                ) AS s USING (match_id, shot_id)
                WHERE s.match_id IS NULL
                ORDER BY b.match_id, b.shot_id
                LIMIT {sample_limit}
            """,
            sample_fields=["match_id", "shot_id"],
        ),
        "card": CheckSpec(
            name="card",
            bronze_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, event_id
                    FROM bronze.cards FINAL
                )
            """,
            silver_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, event_id
                    FROM silver.card FINAL
                )
            """,
            missing_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, event_id
                    FROM bronze.cards FINAL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, event_id
                    FROM silver.card FINAL
                ) AS s USING (match_id, event_id)
                WHERE s.match_id IS NULL
            """,
            sample_sql=f"""
                SELECT b.match_id, b.event_id
                FROM (
                    SELECT DISTINCT match_id, event_id
                    FROM bronze.cards FINAL
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, event_id
                    FROM silver.card FINAL
                ) AS s USING (match_id, event_id)
                WHERE s.match_id IS NULL
                ORDER BY b.match_id, b.event_id
                LIMIT {sample_limit}
            """,
            sample_fields=["match_id", "event_id"],
        ),
        "personnel": CheckSpec(
            name="personnel",
            bronze_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, team_side, role, person_id
                    FROM (
                        SELECT
                            match_id,
                            team_side,
                            'starter' AS role,
                            player_id AS person_id
                        FROM bronze.starters FINAL
                        WHERE match_id > 0
                          AND player_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0

                        UNION ALL

                        SELECT
                            match_id,
                            team_side,
                            'substitute' AS role,
                            player_id AS person_id
                        FROM bronze.substitutes FINAL
                        WHERE match_id > 0
                          AND player_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0

                        UNION ALL

                        SELECT
                            match_id,
                            team_side,
                            'coach' AS role,
                            coach_id AS person_id
                        FROM bronze.coaches FINAL
                        WHERE match_id > 0
                          AND coach_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0
                    )
                )
            """,
            silver_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, team_side, role, person_id
                    FROM silver.match_personnel FINAL
                )
            """,
            missing_count_sql="""
                SELECT count()
                FROM (
                    SELECT DISTINCT match_id, team_side, role, person_id
                    FROM (
                        SELECT
                            match_id,
                            team_side,
                            'starter' AS role,
                            player_id AS person_id
                        FROM bronze.starters FINAL
                        WHERE match_id > 0
                          AND player_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0

                        UNION ALL

                        SELECT
                            match_id,
                            team_side,
                            'substitute' AS role,
                            player_id AS person_id
                        FROM bronze.substitutes FINAL
                        WHERE match_id > 0
                          AND player_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0

                        UNION ALL

                        SELECT
                            match_id,
                            team_side,
                            'coach' AS role,
                            coach_id AS person_id
                        FROM bronze.coaches FINAL
                        WHERE match_id > 0
                          AND coach_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0
                    )
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, team_side, role, person_id
                    FROM silver.match_personnel FINAL
                ) AS s USING (match_id, team_side, role, person_id)
                WHERE s.match_id IS NULL
            """,
            sample_sql=f"""
                SELECT b.match_id, b.team_side, b.role, b.person_id
                FROM (
                    SELECT DISTINCT match_id, team_side, role, person_id
                    FROM (
                        SELECT
                            match_id,
                            team_side,
                            'starter' AS role,
                            player_id AS person_id
                        FROM bronze.starters FINAL
                        WHERE match_id > 0
                          AND player_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0

                        UNION ALL

                        SELECT
                            match_id,
                            team_side,
                            'substitute' AS role,
                            player_id AS person_id
                        FROM bronze.substitutes FINAL
                        WHERE match_id > 0
                          AND player_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0

                        UNION ALL

                        SELECT
                            match_id,
                            team_side,
                            'coach' AS role,
                            coach_id AS person_id
                        FROM bronze.coaches FINAL
                        WHERE match_id > 0
                          AND coach_id > 0
                          AND length(trim(BOTH ' ' FROM team_side)) > 0
                    )
                ) AS b
                LEFT JOIN (
                    SELECT DISTINCT match_id, team_side, role, person_id
                    FROM silver.match_personnel FINAL
                ) AS s USING (match_id, team_side, role, person_id)
                WHERE s.match_id IS NULL
                ORDER BY b.match_id, b.team_side, b.role, b.person_id
                LIMIT {sample_limit}
            """,
            sample_fields=["match_id", "team_side", "role", "person_id"],
        ),
    }


def parse_args(argv: List[str] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check bronze->silver coverage for matches and match entities"
    )
    parser.add_argument(
        "--checks",
        default="all",
        help=(
            "Comma-separated checks: match,player,shot,card,personnel or all "
            "(default: all)"
        ),
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=100,
        help="Maximum number of missing-key samples to print per check (default: 100)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 if any missing entity is found",
    )
    return parser.parse_args(argv)


def _resolve_requested_checks(raw_value: str, available: List[str]) -> List[str]:
    if raw_value.strip().lower() == "all":
        return available

    requested = [item.strip().lower() for item in raw_value.split(",") if item.strip()]
    unknown = [item for item in requested if item not in available]
    if unknown:
        raise ValueError(f"Unknown checks: {unknown}. Allowed: {available} or all")
    return requested


def main(argv: List[str] = None) -> int:
    args = parse_args(argv)

    if args.sample_limit <= 0:
        logger.error("sample-limit must be a positive integer", sample_limit=args.sample_limit)
        return 2

    specs = _build_specs(sample_limit=args.sample_limit)

    try:
        selected_checks = _resolve_requested_checks(args.checks, list(specs.keys()))
    except ValueError as error:
        logger.error(str(error))
        return 2

    client = ClickHouseClient(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database="default",
    )

    if not client.connect():
        logger.error("Failed to connect to ClickHouse")
        return 1

    failures = 0

    try:
        logger.info(
            "Running bronze->silver parity checks",
            checks=selected_checks,
            sample_limit=args.sample_limit,
            strict=args.strict,
        )

        for check_name in selected_checks:
            spec = specs[check_name]
            bronze_count = _query_scalar(client, spec.bronze_count_sql)
            silver_count = _query_scalar(client, spec.silver_count_sql)
            missing_count = _query_scalar(client, spec.missing_count_sql)
            matched_count = max(0, bronze_count - missing_count)
            coverage_pct = round((matched_count / bronze_count) * 100, 2) if bronze_count else 100.0
            extra_in_silver = max(0, silver_count - bronze_count)

            logger.info(
                "Record count summary",
                check=spec.name,
                bronze_eligible_count=bronze_count,
                silver_count=silver_count,
                missing_from_silver=missing_count,
                matched_count=matched_count,
                coverage_pct=coverage_pct,
                extra_in_silver=extra_in_silver,
            )

            if missing_count == 0:
                logger.info("[OK] No missing entities", check=spec.name)
                continue

            failures += 1
            logger.warning(
                "[FAIL] Missing entities found",
                check=spec.name,
                missing_count=missing_count,
                sample_fields=spec.sample_fields,
            )

            sample_rows = _query_rows(client, spec.sample_sql)
            logger.warning(
                "Printed missing-key samples",
                check=spec.name,
                shown_samples=len(sample_rows),
                requested_limit=args.sample_limit,
            )
            for row in sample_rows:
                logger.warning(
                    "Missing key sample",
                    check=spec.name,
                    sample=_format_row(row),
                )

        if failures == 0:
            logger.info("Bronze->silver parity check passed")
            return 0

        logger.warning(
            "Bronze->silver parity check completed with missing entities",
            failed_checks=failures,
            total_checks=len(selected_checks),
        )
        return 1 if args.strict else 0
    finally:
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
