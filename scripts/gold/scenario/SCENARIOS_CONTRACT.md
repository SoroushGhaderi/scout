# Gold Scenarios Contract

This document defines the stable contract for Gold scenario jobs in Scout.

## Scope

This contract applies to:

- `clickhouse/gold/scenario/scenario_*.sql`
- `scripts/gold/scenario/scenario_*.py`
- `scripts/gold/load_clickhouse_scenarios.py`
- `scripts/gold/scenario/SCENARIOS_CATALOG.md`

## Scenario Unit Contract

Each scenario is a 4-part unit:

1. SQL transformation file  
   `clickhouse/gold/scenario/scenario_<name>.sql`
2. Python runner  
   `scripts/gold/scenario/scenario_<name>.py`
3. Target table  
   `gold.scenario_<name>`
4. Catalog entry in  
   `scripts/gold/scenario/SCENARIOS_CATALOG.md`

All four parts are required for a production-ready scenario.

## Naming Contract

1. Scenario ID format: `scenario_<name>` (snake_case).
2. SQL and Python filenames must match exactly by `<name>`.
3. Target table must be `gold.scenario_<name>`.
4. Runner constants must point to matching SQL/table:
   - `SQL_FILE = ... / scenario_<name>.sql`
   - `TARGET_TABLE = "gold.scenario_<name>"`

## SQL Contract

1. Scenario SQL must be `INSERT INTO gold.scenario_<name> ... SELECT ...`.
2. Scenario SQL must not include DDL (`CREATE`, `ALTER`, `DROP`).
3. Source tables must be schema-qualified (`bronze.*`, `silver.*`, `gold.*`).
4. `match_id` must be produced and valid (`> 0`, non-null in final rows).
   This is required by `assert_gold_layer_contracts`.
5. SQL must be re-runnable safely (ReplacingMergeTree + dedup model).
6. Use explicit aliases and deterministic filters for reproducibility.

## Runner Contract

1. Runner must:
   - connect via `ClickHouseClient`
   - read SQL from file
   - execute insert SQL
   - run `OPTIMIZE TABLE <target> FINAL DEDUPLICATE`
   - return non-zero on failure
2. Runner should not embed business SQL in Python strings.
3. Runner must only execute its own scenario SQL file.

## Bulk Execution Contract

`scripts/gold/load_clickhouse_scenarios.py` is the canonical bulk runner.

1. Executes base gold SQL files from `clickhouse/gold/*.sql`.
2. Discovers and executes `scripts/gold/scenario/scenario*.py` in sorted order.
3. Supports `--dry-run` for plan/preview mode.
4. Runs `assert_gold_layer_contracts` after scenario execution.

## Catalog Contract

Each scenario entry in `SCENARIOS_CATALOG.md` must include:

1. Purpose
2. Tactical/statistical logic (threshold rationale)
3. Technical assets:
   - SQL file path
   - Python runner path
   - target table
4. Example execution command

## Validation Gate

Minimum operational checks:

1. `python scripts/gold/load_clickhouse_scenarios.py --dry-run`
2. `python scripts/gold/load_clickhouse_scenarios.py`
3. Verify no gold contract failures (`invalid match_id`, missing scenario tables).

## Change Management Rules

1. Any new scenario must update:
   - `clickhouse/gold/01_create_scenario_tables.sql` (or active DDL file set)
   - `scripts/gold/scenario/SCENARIOS_CATALOG.md`
2. Renaming/deleting a scenario requires coordinated changes to:
   - SQL file
   - Python runner
   - target table DDL
   - catalog entry
3. No breaking renames without documentation updates in:
   - `scripts/README.md`
   - `DEVELOPMENT_ARCHITECTURE.md` (if boundaries or command surface changed)
