# Scripts Layout

## Canonical Entry Points

Use these for all new documentation, automation, and daily runs:

- `scripts/bronze/scrape_fotmob.py`
- `scripts/bronze/load_clickhouse.py`
- `scripts/bronze/setup_clickhouse.py`
- `scripts/silver/process.py`
- `scripts/silver/setup_clickhouse.py`
- `scripts/gold/process.py`
- `scripts/gold/setup_clickhouse.py`
- `scripts/orchestration/pipeline.py`
- `scripts/orchestration/setup_clickhouse.py`

## Operational Utility Scripts

- `scripts/ensure_directories.py`
- `scripts/health_check.py`
- `scripts/refresh_turnstile.py`
- `scripts/check_logging_style.py`

## Scenario Scripts

- `scripts/silver/scenario_demolition.py`
- `scripts/silver/scenario_clean_sheet_dominance.py`
- `scripts/silver/scenario_underdog_heist.py`
- `scripts/silver/scenario_dead_ball_dominance.py`
- `scripts/silver/scenario_low_block_heist.py`
- `scripts/silver/scenario_tactical_stalemate.py`
- `scripts/silver/scenario_great_escape.py`
- `scripts/silver/scenario_one_man_army.py`
- `scripts/silver/scenario_last_gasp.py`
- `scripts/silver/scenarios_catalog.md`
