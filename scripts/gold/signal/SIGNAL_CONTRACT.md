# Gold Signal Contract

This contract defines required production and analytics standards for Gold-layer signals in Scout.
It is written to be executable by data engineers and auditable by analysts.

## Normative Language

- `MUST`: mandatory for production readiness.
- `SHOULD`: strong recommendation; exceptions require clear rationale.
- `MAY`: optional.

## Scope

This contract applies to:

- `clickhouse/gold/signal/sig_*.sql`
- `scripts/gold/signal/runners/sig_*.py`
- `scripts/gold/load_clickhouse_scenarios.py`
- `scripts/gold/signal/catalogs/*.md`

## Core Principles

1. Reproducibility: deterministic logic, explicit filters, stable naming.
2. Idempotence: safe reruns with deduplication semantics.
3. Interpretability: triggered behavior and context are easy to explain.
4. Analytical fairness: triggered-side and opponent metrics are symmetric unless intentionally net/delta.
5. Operational simplicity: readable SQL and predictable execution.

## Repository Layout Contract

`scripts/gold/signal/` MUST contain:

1. `runners/` for executable signal jobs (`sig_*.py`)
2. `catalogs/` for per-signal documentation (`sig_*.md`)
3. `SIGNAL_CONTRACT.md` as the governing specification

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
2. Prefix MUST be `sig_` only; `signal_` is not allowed.
3. SQL filename, runner filename, and table suffix MUST match exactly by `<name>`.
4. Runner constants MUST reference matching assets:
   - `SQL_FILE = .../sig_<name>.sql`
   - `TARGET_TABLE = "gold.sig_<name>"`
5. Catalog filename MUST be `catalogs/sig_<name>.md`.

## Production SQL Contract

File: `clickhouse/gold/signal/sig_<name>.sql`

1. SQL MUST be `INSERT INTO gold.sig_<name> (...) SELECT ...`.
2. SQL MUST NOT include DDL (`CREATE`, `ALTER`, `DROP`).
3. All source tables MUST be schema-qualified (`bronze.*`, `silver.*`, `gold.*`).
4. `match_id` MUST be present in final rows and MUST be valid (`NOT NULL`, `> 0`).
5. Queries joining `silver.match` MUST filter `m.match_finished = 1`, unless a signal explicitly models non-finished states.
6. Null-sensitive arithmetic and aggregations SHOULD use `coalesce(col, 0)` (or equivalent explicit handling).
7. Nullable keys in `GROUP BY` or `ORDER BY` SHOULD be normalized (for example `assumeNotNull()` when safe).
8. Signal value columns MUST be descriptive. Avoid redundant boolean/value columns named exactly like the signal when a richer metric exists.
9. Header comments MUST appear immediately after the `INSERT` column list:
   - `-- Signal: sig_<name>`
   - `-- Trigger: ...`
   - `-- Intent: ...`
10. Clause comment style MUST be consistent across all signal SQL files.
11. Query shape SHOULD remain simple and consistent across signals. Avoid unnecessary CTE layers and indirection.
12. Enrichment MUST be domain-relevant, not generic filler:
   - Passing: accuracy differential plus volume
   - Pressing: PPDA or press-success metrics
   - Shooting: xG, shot volume, on-target rate
   - Defending: defensive action counts
13. Tactical context metrics MUST be symmetric as `triggered_team_*` and `opponent_*` pairs. Unpaired fields are allowed only for explicit net/delta outputs.

## Analyst Query Contract (Ad-hoc SQL Before Production)

When generating analyst-facing exploratory SQL:

1. Output MUST be a single `SELECT` query (no `INSERT`, no DDL).
2. SQL SHOULD use the same comment style as production signal SQL.
3. Output MUST include minimum match context:
   - `match_id`, `match_date`
   - `home_team_id`, `home_team_name`
   - `away_team_id`, `away_team_name`
   - `home_score`, `away_score`
   - triggered entity identifier (team or player)
   - measured signal value
4. Enrichment SHOULD remain tactically relevant and symmetric.
5. A markdown schema table MUST follow SQL with exactly these headers:
   - `Column Name`
   - `Description`
   - `Reason`
6. The schema table MUST cover every selected column without omission.

## Runner Contract

1. Each runner MUST:
   - initialize `ClickHouseClient`
   - load SQL from its file
   - execute the insert query
   - run `OPTIMIZE TABLE <target> FINAL DEDUPLICATE`
   - exit non-zero on failure
2. Runner logic MUST NOT embed business SQL inline.
3. A runner MUST execute only its own signal SQL file.

## Bulk Execution Contract

`scripts/gold/load_clickhouse_scenarios.py` is the canonical orchestrator.

1. MUST execute base Gold SQL from `clickhouse/gold/*.sql`.
2. MUST discover and run `scripts/gold/scenario/scenario*.py` in sorted order.
3. MUST discover and run `scripts/gold/signal/runners/sig*.py` in sorted order.
4. MUST support `--dry-run` plan mode.
5. MUST run `assert_gold_layer_contracts` after scenario and signal execution.

## Catalog Contract

File: `scripts/gold/signal/catalogs/sig_<name>.md`

Each catalog MUST include:

1. Purpose
2. Tactical and statistical logic
3. Technical asset references:
   - SQL path
   - Runner path
   - Target table
4. Example execution command
5. Output schema table with:
   - `Column Name`
   - `Description`
   - `Reason`

Additional rules:

1. Catalogs MUST reference SQL by path and MUST NOT embed full SQL bodies.
2. `Reason` entries MUST explain analytical value (diagnostics, tactical interpretation, feature engineering, QA, or downstream modeling impact).
3. `catalogs/README.md` MUST link every active `sig_<name>.md`.

## Validation and Release Gate

Before merge or release, run:

1. `python scripts/gold/load_clickhouse_scenarios.py --dry-run`
2. `python scripts/gold/load_clickhouse_scenarios.py`
3. Verify no Gold-layer contract failures, including:
   - invalid `match_id`
   - missing signal tables
   - runner execution failures

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
