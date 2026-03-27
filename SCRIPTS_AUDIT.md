# Scripts Audit

Updated: 2026-03-27

This inventory is for cleanup clarity. It does not change runtime logic.

## Canonical (Layered)

| Script | Purpose | Status |
|---|---|---|
| `scripts/bronze/scrape_fotmob.py` | Bronze scrape entrypoint | Keep |
| `scripts/bronze/load_clickhouse.py` | Bronze ClickHouse load entrypoint | Keep |
| `scripts/bronze/setup_clickhouse.py` | Bronze schema setup entrypoint | Keep |
| `scripts/silver/process.py` | Silver build entrypoint | Keep |
| `scripts/silver/setup_clickhouse.py` | Silver schema setup entrypoint | Keep |
| `scripts/gold/process.py` | Gold build entrypoint | Keep |
| `scripts/gold/setup_clickhouse.py` | Gold schema setup entrypoint | Keep |
| `scripts/orchestration/pipeline.py` | Full orchestration entrypoint | Keep |
| `scripts/orchestration/setup_clickhouse.py` | All-layer schema setup entrypoint | Keep |

## Utility (Operational)

| Script | Purpose | Status |
|---|---|---|
| `scripts/ensure_directories.py` | Prepare local folders | Keep |
| `scripts/health_check.py` | Environment/storage/clickhouse checks | Keep |
| `scripts/refresh_turnstile.py` | Token refresh helper | Keep |
| `scripts/check_logging_style.py` | Logging style lint helper | Keep |
| `scripts/clickhouse_setup_common.py` | Shared setup logic module | Keep |
| `scripts/utils/*` | Shared script utilities | Keep |

## Cleanup Policy

1. New workflows must use canonical layered entrypoints.
2. Root-level layer entrypoints are removed.
3. New root-level layer scripts are not allowed.
