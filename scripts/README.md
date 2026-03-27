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
