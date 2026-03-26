"""Process FotMob gold layer in ClickHouse."""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
scripts_dir = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(scripts_dir))

from config.settings import settings
from src.processors.gold.fotmob import FotMobGoldProcessor
from src.storage.clickhouse_client import ClickHouseClient
from src.storage.gold.fotmob import FotMobGoldStorage
from src.utils.logging_utils import get_logger
from utils.script_utils import validate_date_format

logger = get_logger()


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run gold aggregations for FotMob")
    parser.add_argument("--date", type=str, help="Optional date (YYYYMMDD) filter")
    parser.add_argument("--month", type=str, help="Optional month (YYYYMM) filter")
    return parser.parse_args(argv)


def _build_match_filter(args: argparse.Namespace) -> str:
    if args.date:
        return f"toDate(match_time_utc_date) = toDate('{args.date[:4]}-{args.date[4:6]}-{args.date[6:8]}')"
    if args.month:
        return f"toYYYYMM(toDate(match_time_utc_date)) = {args.month}"
    return "1 = 1"


def refresh_gold_tables(client: ClickHouseClient, args: argparse.Namespace) -> None:
    match_filter = _build_match_filter(args)

    client.execute(
        f"""
        INSERT INTO fotmob.gold_player_match_stats
        SELECT
            p.match_id,
            toInt32(p.player_id) AS player_id,
            coalesce(p.player_name, 'unknown') AS player_name,
            p.team_id,
            p.team_name,
            toInt32(p.goals) AS goals,
            toInt32(p.assists) AS assists,
            toFloat32(p.rating) AS rating,
            toInt32(p.minutes_played) AS minutes_played,
            toInt32(countIf(s.id IS NOT NULL)) AS shot_events,
            toFloat32(sum(s.expected_goals)) AS xg,
            toFloat32(sum(s.expected_goals_on_target)) AS xgot,
            now() AS inserted_at
        FROM fotmob.silver_player p
        LEFT JOIN fotmob.silver_shotmap s
            ON p.match_id = s.match_id
            AND p.player_id = s.player_id
        INNER JOIN fotmob.silver_general g
            ON p.match_id = g.match_id
        WHERE {match_filter}
        GROUP BY
            p.match_id,
            p.player_id,
            p.player_name,
            p.team_id,
            p.team_name,
            p.goals,
            p.assists,
            p.rating,
            p.minutes_played
        """
    )

    client.execute(
        f"""
        INSERT INTO fotmob.gold_match_summary
        SELECT
            g.match_id,
            g.league_id,
            g.league_name,
            g.home_team_id,
            g.away_team_id,
            g.home_team_name,
            g.away_team_name,
            g.full_score,
            g.match_time_utc,
            v.attendance,
            v.referee_name,
            toFloat32(maxIf(p.expected_goals_home, p.period = 'All')) AS expected_goals_home,
            toFloat32(maxIf(p.expected_goals_away, p.period = 'All')) AS expected_goals_away,
            now() AS inserted_at
        FROM fotmob.silver_general g
        LEFT JOIN fotmob.silver_venue v ON g.match_id = v.match_id
        LEFT JOIN fotmob.silver_period p ON g.match_id = p.match_id
        WHERE {match_filter}
        GROUP BY
            g.match_id,
            g.league_id,
            g.league_name,
            g.home_team_id,
            g.away_team_id,
            g.home_team_name,
            g.away_team_name,
            g.full_score,
            g.match_time_utc,
            v.attendance,
            v.referee_name
        """
    )

    client.execute(
        """
        INSERT INTO fotmob.gold_team_season_stats
        SELECT
            toInt32(g.league_id) AS league_id,
            toInt32(p.team_id) AS team_id,
            anyLast(coalesce(p.team_name, 'unknown')) AS team_name,
            toUInt32(countDistinct(p.match_id)) AS matches,
            toInt32(sum(p.goals)) AS total_goals,
            toInt32(sum(p.assists)) AS total_assists,
            toFloat32(avg(p.rating)) AS avg_rating,
            toInt64(sum(p.minutes_played)) AS total_minutes,
            min(toDate(g.match_time_utc_date)) AS season_first_seen,
            max(toDate(g.match_time_utc_date)) AS season_last_seen,
            now() AS inserted_at
        FROM fotmob.silver_player p
        INNER JOIN fotmob.silver_general g ON p.match_id = g.match_id
        WHERE g.league_id IS NOT NULL AND p.team_id IS NOT NULL
        GROUP BY
            g.league_id,
            p.team_id
        """
    )


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.date:
        is_valid, error_msg = validate_date_format(args.date, "YYYYMMDD")
        if not is_valid:
            logger.error(error_msg)
            return 1
    if args.month:
        is_valid, error_msg = validate_date_format(args.month, "YYYYMM")
        if not is_valid:
            logger.error(error_msg)
            return 1

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

    try:
        sql_dir = project_root / "clickhouse" / "gold"
        processor = FotMobGoldProcessor(sql_dir=sql_dir)
        storage = FotMobGoldStorage(client, database=settings.clickhouse_db_fotmob)

        sql_files = processor.sql_files()
        if not sql_files:
            logger.error("No gold SQL files found in %s", sql_dir)
            return 1

        storage.execute_sql_files(sql_files)
        refresh_gold_tables(client, args)
        logger.info("Gold processing completed successfully")
        return 0
    finally:
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
