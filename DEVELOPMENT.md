# Scout Development Guide

This document defines the engineering standard for Bronze, Silver, and Gold layers in Scout, with focus on professional SQL organization and Python execution patterns for Silver and Gold.

## 1. Architecture Overview

### Medallion Flow

1. Bronze: raw ingestion and normalization
2. Silver: cleaned, conformed, reusable analytical entities
3. Gold: business-level aggregates and serving tables

Current pipeline entrypoint: `scripts/pipeline.py`

Current layer runners:
- `scripts/process_silver.py`
- `scripts/process_gold.py`

Current shared SQL execution helper:
- `src/storage/clickhouse_sql_executor.py`

## 2. Layer Responsibility Boundaries

### Bronze
- Ingest raw/semi-structured source data
- Preserve source fidelity and traceability
- Minimal transformation

### Silver
- Clean and standardize fields
- Resolve data types, null handling, key consistency
- Build stable entities/views for downstream use

### Gold
- Build aggregate and domain-ready metrics
- Optimize for BI/reporting/product use cases
- Keep business logic explicit and testable

## 3. Professional SQL Folder Structure

Use layer-specific query folders, separated by intent.

```text
clickhouse/
  silver/
    ddl/
      000_create_database.sql
      010_create_or_replace_views.sql
    dml/
      100_backfill_entities.sql
      110_refresh_incremental.sql
  gold/
    ddl/
      000_create_database.sql
      010_create_tables.sql
    dml/
      100_refresh_match_summary.sql
      110_refresh_player_stats.sql
```

### Naming Standard

- Format: `NNN_<domain>_<action>.sql`
- `NNN` controls order, lexical sort is execution order
- Keep one concern per file (avoid very large mixed scripts)

### SQL Authoring Rules

- Prefer idempotent statements:
  - `CREATE TABLE IF NOT EXISTS`
  - `CREATE OR REPLACE VIEW`
- Keep DDL and DML separated by folder
- Keep SQL deterministic and re-runnable
- Use comments for business intent, not obvious syntax

## 4. Python Runner Standard (Silver/Gold)

Python is orchestration, SQL is transformation logic.

### Runner Responsibilities

- Discover SQL files by layer and stage (`ddl`, `dml`)
- Execute in deterministic order
- Log query file, statement count, elapsed time, success/failure
- Stop on failure with clear context

### Runner Must Not

- Embed large SQL business logic inside Python strings
- Build unsafe dynamic SQL from raw user input
- Mix orchestration concerns with transformation definitions

### Dynamic Query Policy

Default policy is static SQL files.

If runtime behavior is needed (date/month/window):
1. Prefer pre-defined SQL variants in separate files
2. If dynamic injection is unavoidable, only allow strict allowlisted values in Python before execution
3. Never directly concatenate unsanitized input into SQL

## 5. Recommended Code Structure in Current Repo

Keep your existing shape and evolve it incrementally.

### Keep

- `src/processors/silver/fotmob.py` and `src/processors/gold/fotmob.py` for SQL discovery
- `src/storage/silver/fotmob.py` and `src/storage/gold/fotmob.py` for execution wiring
- `src/storage/clickhouse_sql_executor.py` for shared execution logic

### Improve Next

1. Update processors to discover SQL in both `ddl/` and `dml/` folders
2. Add optional `--dry-run` in `scripts/process_silver.py` and `scripts/process_gold.py`
3. Add execution summary object (files run, statements run, elapsed seconds, failed file)
4. Add query-level metrics/logging for better observability

## 6. How to Add a New Silver Query

1. Identify query type:
- Schema/view change -> `clickhouse/silver/ddl/`
- Data refresh/backfill -> `clickhouse/silver/dml/`

2. Add file with next ordered prefix:
- Example: `120_player_quality_rules.sql`

3. Ensure idempotency and safe rerun behavior

4. Run locally:
- `python scripts/process_silver.py --date YYYYMMDD`

5. Validate outputs with explicit checks in ClickHouse

6. Add or update tests if transformation behavior changed

## 7. How to Add a New Gold Query

1. Decide object type:
- New serving table/materialization -> `clickhouse/gold/ddl/`
- Aggregate population/refresh -> `clickhouse/gold/dml/`

2. Add ordered SQL file:
- Example: `130_refresh_team_form.sql`

3. Ensure rerun strategy is explicit:
- Full refresh, partition refresh, or incremental append

4. Run locally:
- `python scripts/process_gold.py --month YYYYMM`

5. Validate row counts, keys, and metric sanity

## 8. Testing and Validation Standard

### Minimum Automated Coverage

- SQL discovery order tests
- SQL splitting and statement execution tests
- Runner failure behavior tests (fail-fast with file context)
- CLI argument validation tests (`--date`, `--month`)

### Data Quality Checks

- Null rate checks on critical columns
- Duplicate key checks on expected unique keys
- Freshness checks for latest processed dates
- Aggregate reconciliation checks (Silver vs Gold)

## 9. Operational Runbook

### Core Commands

```bash
# Full pipeline
python scripts/pipeline.py 20251113

# Silver only
python scripts/pipeline.py 20251113 --silver-only

# Gold only
python scripts/pipeline.py 20251113 --gold-only

# Direct layer runs
python scripts/process_silver.py --date 20251113
python scripts/process_gold.py --month 202511
```

### Incident Basics

1. Identify failing SQL file from logs
2. Re-run layer for same date/month after fix
3. Validate target tables/views with row count and sample checks
4. Record root cause and preventive action

## 10. Prioritized Engineering Backlog (Non-Redundant)

### P0

- Introduce `ddl/` and `dml/` folder split for Silver and Gold
- Standardize SQL naming and ordering convention
- Keep transformation logic in SQL files, not embedded Python

### P1

- Add `--dry-run` support for Silver/Gold runners
- Add query execution summaries and consistent structured logs
- Add tests around SQL discovery and failure semantics

### P2

- Add lightweight data contracts and quality assertions per layer
- Add incremental refresh strategy per Gold table
- Add layer-level performance dashboards (duration, success rate)

## 11. Final Standards Checklist

Use this checklist for each PR touching Silver/Gold:

- Query file is in correct layer and stage (`ddl` or `dml`)
- Naming follows `NNN_<domain>_<action>.sql`
- SQL is idempotent or rerun strategy is explicit
- Python changes are orchestration-only
- Tests updated for behavior changes
- Documentation updated when structure changes

---

This guide is now the single source of truth for Silver/Gold development workflow in Scout.
