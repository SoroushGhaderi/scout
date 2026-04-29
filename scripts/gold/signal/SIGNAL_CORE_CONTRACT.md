# Gold Signal Core Contract (Codex High)

This contract defines the high-value, creative part of Gold signal authoring:

1. Writing production signal SQL logic
2. Writing analyst-facing exploratory SQL
3. Producing the signal catalog markdown

This document is intended for deep tactical/analytical reasoning work where design quality matters most.

## Normative Language

- `MUST`: mandatory for production readiness.
- `SHOULD`: strong recommendation; exceptions require clear rationale.
- `MAY`: optional.

## Scope

This contract applies to:

- `clickhouse/gold/signal/sig_*.sql`
- `scripts/gold/signal/catalogs/sig_*.md`
- `scripts/gold/signal/catalogs/README.md`

## Core Principles

1. Reproducibility: deterministic logic, explicit filters, stable naming.
2. Interpretability: triggered behavior and context are easy to explain.
3. Analytical fairness: triggered-side and opponent metrics are symmetric unless intentionally net/delta.
4. Operational simplicity: readable SQL and predictable execution.

## Naming and Consistency Contract

1. Signal IDs MUST follow `sig_<name>` in `snake_case`.
2. Prefix MUST be `sig_` only; `signal_` is not allowed for new work.
3. SQL filename and target table suffix MUST match exactly by `<name>`.
4. Catalog filename MUST be `catalogs/sig_<name>.md`.

## Column Naming Contract

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
11. New output columns MUST NOT introduce abbreviations unless already canonical in football analytics (`xg`, `ppda`).

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
   - `triggered_side` plus opponent team identifiers (`opponent_team_id`, `opponent_team_name`) unless explicitly non-opponent-oriented.

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

## Catalog Contract

File: `scripts/gold/signal/catalogs/sig_<name>.md`

Each catalog MUST include:

1. Metadata block
2. Purpose
3. Tactical and statistical logic
4. Technical asset references (convention-based unless overridden)
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
   version: 2
   taxonomy:
     entity: team
     family: possession
     subfamily: passing
     grain: match_team
   pulse:
     headline: "Human-readable signal headline"
     default_surface: team_match_signal_card
     insight_type: tactical_diagnostic
     value_to_user:
       - diagnostics
       - tactical_interpretation
       - feature_engineering
     narrative_template: "{signal_id} triggered for {triggered_side_or_player} in match {match_id}"
   trigger:
     primary_expression: "human-readable trigger expression"
     trigger_scope: single_match
     polarity: higher_is_stronger
   identity:
     row_identity:
       - match_id
       - triggered_side
     required_output_keys:
       - triggered_side
     dedupe_policy: one_row_per_identity
   asset_binding:
     resolution: convention_based
     conventions:
       target_table: "gold.{signal_id}"
       sql_path: "clickhouse/gold/signal/{signal_id}.sql"
       runner_path: "scripts/gold/signal/runners/{signal_id}.py"
     overrides: {}
   quality:
     qa_expectations:
       - row_identity must be unique per run
       - trigger context fields must be internally consistent
     downstream_impact:
       - pulse_ui_explainability
       - tactical_clustering_features
   ---
   ```

2. Required metadata fields are:
   - `signal_id`
   - `status` (`active`, `experimental`, or `deprecated`)
   - `version`
   - `taxonomy.entity` (`team` or `player`)
   - `taxonomy.family`
   - `taxonomy.subfamily`
   - `taxonomy.grain`
   - `pulse.headline`
   - `pulse.default_surface`
   - `pulse.insight_type`
   - `pulse.value_to_user`
   - `pulse.narrative_template`
   - `trigger.primary_expression`
   - `trigger.trigger_scope`
   - `trigger.polarity`
   - `identity.row_identity`
   - `identity.required_output_keys`
   - `identity.dedupe_policy`
   - `asset_binding.resolution`
   - `asset_binding.conventions.target_table`
   - `asset_binding.conventions.sql_path`
   - `asset_binding.conventions.runner_path`
   - `asset_binding.overrides`
   - `quality.qa_expectations`
   - `quality.downstream_impact`
3. Asset references MUST resolve from convention by default:
   - target table: `gold.{signal_id}`
   - SQL path: `clickhouse/gold/signal/{signal_id}.sql`
   - runner path: `scripts/gold/signal/runners/{signal_id}.py`
4. If any signal deviates from convention, `asset_binding.overrides` MUST explicitly list non-default assets, and resolved assets MUST match package files exactly.
5. `taxonomy.grain` MUST describe row grain. Accepted values:
   - `match_team`
   - `match_player`
6. `identity.row_identity` MUST list stable deduplication identity for final rows:
   - Team-triggered signals SHOULD use `match_id` and `triggered_side`.
   - Player-triggered signals SHOULD use `match_id`, `triggered_player_id`, and `triggered_team_id`.
7. `identity.required_output_keys` MUST include fields needed for deterministic UI rendering and joins.
8. Catalogs MUST reference SQL by path and MUST NOT embed full SQL bodies.
9. `Reason` entries MUST explain analytical value (diagnostics, tactical interpretation, feature engineering, QA, or downstream modeling impact).
10. `catalogs/README.md` MUST include every active `sig_<name>.md` in a structured table with these headers:
    - `Signal ID`
    - `Entity`
    - `Family`
    - `Subfamily`
    - `Grain`
    - `Status`
    - `Catalog`
11. For player-triggered signals:
    - `identity.required_output_keys` MUST include both `triggered_player_*` and `triggered_team_*` identity fields.
    - catalog output schemas MUST document both `triggered_player_*` and `triggered_team_*` identity fields.
