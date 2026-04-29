# Gold Signal Execution Contract (Codex Low)

This contract defines the low-variance implementation part of Gold signals:

1. Creating and maintaining table DDL
2. Writing runner scripts and standard orchestration glue
3. Enforcing deterministic execution and release checks

This document is intended for routine implementation work where structure and consistency matter most.

## Normative Language

- `MUST`: mandatory for production readiness.
- `SHOULD`: strong recommendation; exceptions require clear rationale.
- `MAY`: optional.

## Scope

This contract applies to:

- `clickhouse/gold/02_create_signal_tables.sql` (or active signal DDL set)
- `scripts/gold/signal/runners/sig_*.py`
- `scripts/gold/load_clickhouse_scenarios.py`
- cross-asset wiring among SQL, runner, table, and catalog files

Compatibility note:

- The bulk loader currently supports both `sig_*.py` and legacy `signal_*.py` runner prefixes.
- New work MUST use `sig_` naming. Legacy `signal_` support exists only for backward compatibility.

## Repository Layout Contract

`scripts/gold/signal/` MUST contain:

1. `runners/` for executable signal jobs (`sig_*.py`)
2. `catalogs/` for per-signal documentation (`sig_*.md`)
3. `SIGNAL_CONTRACT.md` as the top-level index to active contracts
4. `SIGNAL_CORE_CONTRACT.md` for creative/high-value logic contract
5. `SIGNAL_EXECUTION_CONTRACT.md` for routine execution contract

Operational compatibility:

- `runners/` MAY temporarily include legacy `signal_*.py` files while old jobs are being migrated.
- New or renamed runners MUST use `sig_*.py`.

## Signal Package Contract

Each signal MUST ship as one 5-part package:

1. SQL transform: `clickhouse/gold/signal/sig_<name>.sql`
2. Python runner: `scripts/gold/signal/runners/sig_<name>.py`
3. Target table: `gold.sig_<name>`
4. Catalog: `scripts/gold/signal/catalogs/sig_<name>.md`
5. Catalog index entry in `scripts/gold/signal/catalogs/README.md`

No package is complete unless all 5 parts are present and consistent.

## Naming and Consistency Contract

1. Signal IDs MUST follow `sig_<name>` in `snake_case`.
2. Prefix MUST be `sig_` only for new work.
3. SQL filename, runner filename, and table suffix MUST match exactly by `<name>`.
4. Runner constants MUST reference matching assets:
   - `TARGET_TABLE = "gold.sig_<name>"`
   - SQL resolution MUST deterministically map runner stem to SQL stem (`sig_<name>.py` -> `sig_<name>.sql`), whether direct-path or controlled recursive lookup is used.
5. Catalog filename MUST be `catalogs/sig_<name>.md`.

## Runner Contract

1. Each runner MUST:
   - initialize `ClickHouseClient`
   - load SQL from its file
   - execute the insert query
   - run `OPTIMIZE TABLE <target> FINAL DEDUPLICATE`
   - exit non-zero on failure
2. Runner logic MUST NOT embed business SQL inline.
3. A runner MUST execute only its own signal SQL file.
4. Runner SQL discovery MUST be deterministic and fail fast when the resolved SQL file is missing.
5. Any SQL used by shared signal orchestration helpers MUST live in `.sql` files. Python MAY render validated SQL-template placeholders and pass query parameters, but MUST NOT inline business or reference queries.

## Bulk Execution Contract

`scripts/gold/load_clickhouse_scenarios.py` is the canonical orchestrator.

1. MUST execute base Gold SQL from `clickhouse/gold/*.sql`.
2. MUST discover and run `scripts/gold/scenario/scenario*.py` in sorted order.
3. MUST discover and run `scripts/gold/signal/runners/sig*.py` in sorted order. It MAY also include legacy `signal*.py` during migration.
4. MUST support `--dry-run` plan mode.
5. MUST run `assert_gold_layer_contracts` after scenario and signal execution.

## Validation and Release Gate

Before merge or release, run:

1. `python3 scripts/gold/load_clickhouse_scenarios.py --dry-run`
2. `python3 scripts/gold/load_clickhouse_scenarios.py`
3. Verify no Gold-layer contract failures, including:
   - invalid `match_id`
   - missing signal tables
   - runner execution failures

Recommended focused checks:

1. `python3 scripts/gold/load_clickhouse_scenarios.py --part signals --dry-run`
2. `python3 scripts/gold/load_clickhouse_scenarios.py --part signals`

## Change Management

1. Adding a new signal MUST also update:
   - `clickhouse/gold/02_create_signal_tables.sql` (or active DDL set)
   - `scripts/gold/signal/catalogs/sig_<name>.md`
   - `scripts/gold/signal/catalogs/README.md`
2. Renaming or deleting a signal MUST update all linked assets together:
   - SQL file
   - runner
   - table DDL
   - catalog file
   - catalog index
3. Breaking renames MUST be documented in:
   - `scripts/README.md`
   - `DEVELOPMENT_ARCHITECTURE.md` when boundary or command-surface behavior changes
