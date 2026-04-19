# PitchWise Orbit: Development & Architecture Guide

This document is the current source of truth for how Orbit is built and operated.
It replaces older mixed guidance with an implementation-accurate view of the `pitchwise_orbit` project.

## 1. Scope

PitchWise Orbit is a FotMob-only medallion pipeline with explicit layer boundaries:

1. Bronze: raw API payloads on local storage + warehouse raw tables in ClickHouse
2. Silver: cleaned/conformed analytical tables in ClickHouse
3. Gold: scenario tables for downstream product and analytics use

## 2. Current Architecture Snapshot

```text
FotMob API
  -> Bronze filesystem (data/fotmob)
  -> ClickHouse bronze.*
  -> ClickHouse silver.*
  -> ClickHouse gold.*
```

### Layer boundaries

1. Bronze is the only filesystem-backed layer.
2. Silver and Gold are ClickHouse-only layers.
3. Bronze preserves source fidelity with minimal transformation.
4. Silver standardizes keys/types and builds reusable entities.
5. Gold materializes scenario outcomes for product/BI use.

## 3. Canonical Entry Points (Current)

Use these script paths as the command surface.

### Setup

1. `python scripts/orchestration/setup_clickhouse.py`
2. `python scripts/bronze/setup_clickhouse.py`
3. `python scripts/silver/setup_clickhouse.py`
4. `python scripts/gold/setup_clickhouse_gold.py`

### Bronze

1. `python scripts/bronze/scrape_fotmob.py 20251208`
2. `python scripts/bronze/load_clickhouse.py --date 20251208`
3. `python scripts/bronze/drop_clickhouse.py --dry-run`

### Silver

1. `python scripts/silver/load_clickhouse.py`
2. `python scripts/silver/load_clickhouse.py --dry-run`
3. `python scripts/silver/drop_clickhouse.py --dry-run`

### Gold

1. `python scripts/gold/load_clickhouse_scenarios.py`
2. `python scripts/gold/load_clickhouse_scenarios.py --dry-run`
3. `python scripts/gold/drop_clickhouse_scenarios.py --dry-run`

### Orchestration and quality

1. `python scripts/orchestration/pipeline.py 20251208`
2. `python scripts/quality/check_bronze_to_silver_reconciliation.py --strict`
3. `python scripts/quality/check_logging_style.py`

### Ops utilities

1. `python scripts/health_check.py`
2. `python scripts/ensure_directories.py`
3. `python scripts/refresh_turnstile.py`

## 4. SQL and Code Layout

### ClickHouse SQL

```text
clickhouse/
  bronze/
    00_create_database.sql
    01_*.sql ... 14_*.sql
    99_optimize_tables.sql

  silver/
    ddl/
      00_create_database.sql
      01_*.sql ...
    dml/
      01_*.sql ...
    load/  # legacy fallback (supported by silver loader if present)

  gold/
    00_create_database.sql
    01_create_scenario_tables.sql
    scenario/
      team/scenario_*.sql
      player/scenario_*.sql
```

### Python architecture

1. `src/scrapers/fotmob/`: API fetchers and request behavior
2. `src/storage/bronze|silver|gold/`: persistence and execution
3. `src/processors/bronze|silver|gold/`: transformation wiring
4. `src/utils/`: logging, contracts, alerts, metrics, health checks
5. `scripts/`: operational CLI entry points

## 5. New Things In Orbit (Implemented)

These are concrete capabilities present in the current repo.

1. Orbit now has a clear script surface by layer under `scripts/bronze`, `scripts/silver`, `scripts/gold`, `scripts/orchestration`, and `scripts/quality`.
2. Silver processing is split into explicit `ddl/` and `dml/` stages, with controlled fallback support for legacy `load/` SQL.
3. Gold scenarios are now a structured system with SQL + runner pairs and catalog/contract docs (currently 48 scenario SQL files and 48 scenario runners).
4. Scenario SQL discovery supports recursive folders (`team/`, `player/`) while runner naming stays stable.
5. Bronze loading includes DLQ fallback (`src/storage/dlq.py`) for failed inserts and preserves failure context for replay.
6. Layer contracts are enforced in runtime (`assert_bronze_layer_contracts`, `assert_silver_layer_contracts`, `assert_gold_layer_contracts`).
7. Bronze-to-Silver reconciliation quality checks now support strict mode and coverage reporting by entity.
8. Silver and Gold loaders support `--dry-run` planning mode.
9. Standardized layer completion alerts are sent via `send_layer_completion_alert`.
10. Bronze runtime adds turnstile refresh automation support via `scripts/refresh_turnstile.py` and scrape-time refresh checks.
11. Bronze storage supports completion tracking (`daily_listings`) plus date-level compression and optional S3 backup upload.
12. Structured logging is unified around `structlog` in `src/utils/logging_utils.py`.

## 6. Improvements Already Available In This Project

Use these immediately to improve reliability/operations without adding new code.

1. Pre-flight safety checks before runs:
`python scripts/health_check.py --json`
2. Non-destructive planning for transformations:
`python scripts/silver/load_clickhouse.py --dry-run`
`python scripts/gold/load_clickhouse_scenarios.py --dry-run`
3. Stronger data quality gating:
`python scripts/quality/check_bronze_to_silver_reconciliation.py --strict`
4. Logging hygiene validation for runtime messages:
`python scripts/quality/check_logging_style.py`
5. Safe teardown preview before destructive actions:
`python scripts/bronze/drop_clickhouse.py --dry-run`
`python scripts/silver/drop_clickhouse.py --dry-run`
`python scripts/gold/drop_clickhouse_scenarios.py --dry-run`
6. Bronze durability and rerun friendliness: ReplacingMergeTree in bronze tables, post-load optimization SQL (`clickhouse/bronze/99_optimize_tables.sql`), and DLQ capture for failed records.
7. Operator-friendly recovery for token/cookie failures:
`python scripts/refresh_turnstile.py`

## 7. Pipeline Modes and Runbook

### Standard flows

1. Full day:
`python scripts/orchestration/pipeline.py 20251208`
2. Date range:
`python scripts/orchestration/pipeline.py --start-date 20251201 --end-date 20251207`
3. Month:
`python scripts/orchestration/pipeline.py --month 202512`

### Partial modes

1. Bronze only:
`python scripts/orchestration/pipeline.py 20251208 --bronze-only`
2. Silver only:
`python scripts/orchestration/pipeline.py 20251208 --silver-only`
3. Gold only:
`python scripts/orchestration/pipeline.py 20251208 --gold-only`
4. Reuse existing Bronze files:
`python scripts/orchestration/pipeline.py 20251208 --skip-bronze`

### Incident handling

1. Identify failing stage and SQL/script name from logs.
2. Re-run only the affected layer (or specific date range).
3. Run reconciliation and contract checks.
4. If insertion failures occurred, inspect DLQ files under `data/dlq`.

## 8. Engineering Standards (Current)

1. SQL contains transformation/business logic; Python handles orchestration and execution.
2. Keep SQL files deterministic and rerunnable.
3. Keep naming stable: Silver uses `NN_<entity>.sql`, and Gold scenario uses `scenario_<name>.sql`.
4. Use schema-qualified tables (`bronze.*`, `silver.*`, `gold.*`).
5. New/changed scenario work must update SQL file, runner file, gold scenario DDL, and `scripts/gold/scenario/SCENARIOS_CATALOG.md`.

## 9. Current Backlog (Recommended Next)

1. Add automated tests for script entry points and SQL discovery order.
2. Add CI gates for `check_logging_style` and reconciliation strict mode.
3. Add run summary artifacts (machine-readable JSON) for each layer execution.
4. Integrate quality checks as an explicit default stage in the orchestration flow.
5. Align package CLI command mapping (`src/cli.py`) with the layered script layout.

---

If architecture boundaries or script surfaces change, update this file in the same PR.
