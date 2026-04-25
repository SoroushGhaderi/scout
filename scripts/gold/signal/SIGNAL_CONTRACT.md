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
- `gold.match_signal_reference` as the match-level signal availability reference

Compatibility note:

- The bulk loader currently supports both `sig_*.py` and legacy `signal_*.py` runner prefixes.
- New work MUST use `sig_` naming. Legacy `signal_` support exists only for backward compatibility.

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
2. Prefix MUST be `sig_` only; `signal_` is not allowed.
3. SQL filename, runner filename, and table suffix MUST match exactly by `<name>`.
4. Runner constants MUST reference matching assets:
   - `TARGET_TABLE = "gold.sig_<name>"`
   - SQL resolution MUST deterministically map runner stem to SQL stem (`sig_<name>.py` -> `sig_<name>.sql`), whether direct-path or controlled recursive lookup is used.
5. Catalog filename MUST be `catalogs/sig_<name>.md`.

## Column Naming Contract

Gold signal output columns are consumed by downstream analytics, feature engineering, and QA. Names MUST be stable, explicit, and easy to classify without reading SQL.

1. Output column names MUST be `snake_case`.
2. Output columns MUST use full football/domain words:
   - Use `accuracy`, not `acc`.
   - Use `opposition` for pitch zones (`opposition_half`, `opposition_box`), not `opp`.
   - Use `opponent` only for the opposing team/entity, not as a pitch-zone abbreviation.
3. Percentage and rate columns MUST end with `_pct`.
   - Correct: `triggered_team_pass_accuracy_pct`
   - Incorrect: `triggered_team_pass_accuracy`, `triggered_team_pass_acc_pct`
4. Delta columns MUST include the measured unit when unit-sensitive:
   - Correct: `pass_accuracy_delta_pct`
   - Correct: `xg_delta`
   - Incorrect: `pass_accuracy_delta` when the value is a percentage-point delta
5. Period-specific columns MUST spell out the period before the unit suffix:
   - Correct: `triggered_team_pass_accuracy_first_half_pct`
   - Correct: `opponent_opposition_half_passes_second_half`
   - Incorrect: `triggered_team_pass_acc_fh`, `opponent_opp_half_passes_sh`
6. Triggered-side team metrics MUST use `triggered_team_*`.
7. Opponent team metrics MUST use `opponent_*`.
8. Player-triggered metrics MUST use `triggered_player_*` for player values and `triggered_team_*` for the player's team context.
9. Side-oriented non-triggered metrics MUST put the side first:
   - Correct: `home_pass_accuracy_pct`, `away_opposition_half_pass_pct`
   - Incorrect: `pass_accuracy_home_pct`, `home_opp_half_pass_pct`
10. Count columns SHOULD use explicit football action nouns:
    - Attempts: `*_attempts`
    - Accurate completions: `accurate_*`
    - Successful actions: `successful_*`
11. New output columns MUST NOT introduce abbreviations unless the abbreviation is already canonical in football analytics (`xg`, `ppda`).

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
   - `-- Intent: ...`
10. `-- Trigger: ...` SHOULD be present and explicit when a threshold/rule exists. If omitted, trigger logic MUST still be obvious in SQL predicates.
11. Clause comment style MUST be consistent within each file and SHOULD be consistent across the signal family.
12. Query shape SHOULD remain simple and consistent across signals. Avoid unnecessary CTE layers and indirection.
13. `FINAL` on source tables SHOULD be used only when correctness requires it; if used, add a short comment explaining why the performance trade-off is justified.
14. Enrichment MUST be domain-relevant, not generic filler:
   - Passing: accuracy differential plus volume
   - Pressing: PPDA or press-success metrics
   - Shooting: xG, shot volume, on-target rate
   - Defending: defensive action counts
15. Tactical context metrics MUST be symmetric as `triggered_team_*` and `opponent_*` pairs. Unpaired fields are allowed only for explicit net/delta outputs.
16. Canonical side-orientation field is `triggered_side`. Legacy `triggered_team_side` is tolerated for existing tables, but new signals MUST use `triggered_side`.
17. If both teams can satisfy a trigger in one match but only one row is emitted, SQL MUST define deterministic precedence (for example home-priority) and expose an explicit bilateral flag (for example `both_sides_triggered`).
18. Player-triggered signals MUST store both player identity and team context in final output rows:
   - `triggered_player_id` and `triggered_player_name`
   - `triggered_team_id` and `triggered_team_name`
   - `triggered_side` plus opponent team identifiers (`opponent_team_id`, `opponent_team_name`) unless the signal is explicitly non-opponent-oriented.

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
   - for player-triggered signals: include both player identifiers (`triggered_player_id`, `triggered_player_name`) and triggered team identifiers (`triggered_team_id`, `triggered_team_name`)
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
4. Runner SQL discovery MUST be deterministic and fail fast when the resolved SQL file is missing.
5. Any SQL used by shared signal orchestration helpers MUST live in `.sql` files. Python MAY render validated SQL-template placeholders and pass query parameters, but MUST NOT inline business or reference queries.

## Bulk Execution Contract

`scripts/gold/load_clickhouse_scenarios.py` is the canonical orchestrator.

1. MUST execute base Gold SQL from `clickhouse/gold/*.sql`.
2. MUST discover and run `scripts/gold/scenario/scenario*.py` in sorted order.
3. MUST discover and run `scripts/gold/signal/runners/sig*.py` in sorted order. It MAY also include legacy `signal*.py` during migration.
4. MUST support `--dry-run` plan mode.
5. MUST refresh `gold.match_signal_reference` after successful selected signal execution.
6. MUST run `assert_gold_layer_contracts` after scenario and signal execution.

## Match Signal Reference Contract

Table: `gold.match_signal_reference`

Purpose: a Gold-layer match reference that mirrors the match information in `bronze.match_reference` and records which Gold signals are available for each match.

1. The table MUST preserve the same match-information columns as `bronze.match_reference`.
2. The table MUST include:
   - `all_signal_ids`
   - `available_signal_ids`
   - `unavailable_signal_ids`
   - `signal_count`
   - `available_signal_count`
   - `has_any_signal`
3. Availability MUST be derived from Gold signal tables with `sig_` prefixes and valid `match_id` columns.
4. The reference MUST be refreshed only after selected signal jobs complete successfully.
5. Reference refresh SQL MUST live under `clickhouse/gold/reference/*.sql`.
6. Python orchestration MUST only load/render those SQL files and pass required variables or query parameters.
7. Report availability is out of scope for this contract for now.

## Catalog Contract

File: `scripts/gold/signal/catalogs/sig_<name>.md`

Each catalog MUST include:

1. Metadata block
2. Purpose
3. Tactical and statistical logic
4. Technical asset references:
   - SQL path
   - Runner path
   - Target table
5. Example execution command
6. Output schema table with:
   - `Column Name`
   - `Description`
   - `Reason`

Additional rules:

1. Catalogs MUST begin with a YAML metadata block before the heading:

   ```yaml
   ---
   signal_id: sig_<name>
   status: active
   entity: team
   family: possession
   subfamily: passing
   grain: match_team
   target_table: gold.sig_<name>
   sql_path: clickhouse/gold/signal/sig_<name>.sql
   runner_path: scripts/gold/signal/runners/sig_<name>.py
   primary_trigger: "human-readable trigger expression"
   row_identity:
     - match_id
     - triggered_side
   version: 1
   ---
   ```

2. Required metadata fields are:
   - `signal_id`
   - `status` (`active`, `experimental`, or `deprecated`)
   - `entity` (`team` or `player`)
   - `family`
   - `subfamily`
   - `grain`
   - `target_table`
   - `sql_path`
   - `runner_path`
   - `primary_trigger`
   - `row_identity`
   - `version`
3. `signal_id`, `target_table`, `sql_path`, and `runner_path` MUST match the signal package assets exactly.
4. `grain` MUST describe the row grain. Current accepted values are:
   - `match_team`
   - `match_player`
5. `row_identity` MUST list the stable deduplication identity for final rows:
   - Team-triggered signals SHOULD use `match_id` and `triggered_side`.
   - Player-triggered signals SHOULD use `match_id`, `triggered_player_id`, and `triggered_team_id`.
6. Catalogs MUST reference SQL by path and MUST NOT embed full SQL bodies.
7. `Reason` entries MUST explain analytical value (diagnostics, tactical interpretation, feature engineering, QA, or downstream modeling impact).
8. `catalogs/README.md` MUST include every active `sig_<name>.md` in a structured table with these headers:
   - `Signal ID`
   - `Entity`
   - `Family`
   - `Subfamily`
   - `Grain`
   - `Status`
   - `Catalog`
9. For player-triggered signals, catalog output schemas MUST document both `triggered_player_*` and `triggered_team_*` identity fields.

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
