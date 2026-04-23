# sig_team_possession_passing_final_third_efficiency

## Purpose

Detect teams that score at least 2 goals despite fewer than 10 final-third entries (proxied by `touches_opp_box`), highlighting extreme attacking efficiency.

## Tactical And Statistical Logic

- Signal name source: `-- sig_team_possession_passing_final_third_efficiency`
- Trigger condition source: `-- Trigger condition: team goals >= 2 with triggered_team_final_third_entries < 10 (entries proxied by touches_opp_box).`
- Signal isolates unusually clinical finishing profiles and enriches with bilateral shot quality, passing control, and territorial progression context.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_final_third_efficiency.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_final_third_efficiency.py`
- Target table: `gold.sig_team_possession_passing_final_third_efficiency`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_final_third_efficiency.py
```

## SQL

```sql
INSERT INTO gold.sig_team_possession_passing_final_third_efficiency (
    match_id,
    match_date,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score,
    away_score,
    triggered_side,
    triggered_team_id,
    triggered_team_name,
    opponent_team_id,
    opponent_team_name,
    sig_team_possession_passing_final_third_efficiency,
    triggered_team_goals,
    opponent_goals,
    goal_delta,
    triggered_team_final_third_entries,
    opponent_final_third_entries,
    final_third_entries_delta,
    triggered_team_goals_per_final_third_entry,
    opponent_goals_per_final_third_entry,
    goals_per_entry_delta,
    triggered_team_total_shots,
    opponent_total_shots,
    triggered_team_shots_on_target,
    opponent_shots_on_target,
    triggered_team_on_target_ratio_pct,
    opponent_on_target_ratio_pct,
    triggered_team_xg,
    opponent_xg,
    xg_delta,
    triggered_team_xg_per_shot,
    opponent_xg_per_shot,
    triggered_team_pass_attempts,
    opponent_pass_attempts,
    triggered_team_accurate_passes,
    opponent_accurate_passes,
    triggered_team_pass_acc_pct,
    opponent_pass_acc_pct,
    pass_accuracy_delta,
    triggered_team_opp_half_passes,
    opponent_opp_half_passes
)
-- sig_team_possession_passing_final_third_efficiency
-- Trigger condition: team goals >= 2 with triggered_team_final_third_entries < 10 (entries proxied by touches_opp_box).
-- Intent: detect unusually clinical output where a side scores multiple goals from very few final-third entries.

-- Home-side triggers.
SELECT
    -- Match identifiers.
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,

    -- Triggered team and opponent identifiers.
    'home' AS triggered_side,
    m.home_team_id AS triggered_team_id,
    m.home_team_name AS triggered_team_name,
    m.away_team_id AS opponent_team_id,
    m.away_team_name AS opponent_team_name,

    -- Measured signal value: goals per final-third entry.
    toFloat32(coalesce(round(coalesce(m.home_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_home, 0)), 1.0), 3), 0.0))
        AS sig_team_possession_passing_final_third_efficiency,

    -- Signal context: scoreline efficiency with final-third-entry proxy.
    coalesce(m.home_score, 0) AS triggered_team_goals,
    coalesce(m.away_score, 0) AS opponent_goals,
    coalesce(m.home_score, 0) - coalesce(m.away_score, 0) AS goal_delta,
    coalesce(ps.touches_opp_box_home, 0) AS triggered_team_final_third_entries,
    coalesce(ps.touches_opp_box_away, 0) AS opponent_final_third_entries,
    coalesce(ps.touches_opp_box_home, 0) - coalesce(ps.touches_opp_box_away, 0) AS final_third_entries_delta,
    toFloat32(coalesce(round(coalesce(m.home_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_home, 0)), 1.0), 3), 0.0))
        AS triggered_team_goals_per_final_third_entry,
    toFloat32(coalesce(round(coalesce(m.away_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_away, 0)), 1.0), 3), 0.0))
        AS opponent_goals_per_final_third_entry,
    round(
        coalesce(round(coalesce(m.home_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_home, 0)), 1.0), 3), 0.0)
      - coalesce(round(coalesce(m.away_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_away, 0)), 1.0), 3), 0.0),
        3
    ) AS goals_per_entry_delta,

    -- Shot profile context.
    coalesce(ps.total_shots_home, 0) AS triggered_team_total_shots,
    coalesce(ps.total_shots_away, 0) AS opponent_total_shots,
    coalesce(ps.shots_on_target_home, 0) AS triggered_team_shots_on_target,
    coalesce(ps.shots_on_target_away, 0) AS opponent_shots_on_target,
    toFloat32(coalesce(round(100.0 * coalesce(ps.shots_on_target_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 1), 0.0))
        AS triggered_team_on_target_ratio_pct,
    toFloat32(coalesce(round(100.0 * coalesce(ps.shots_on_target_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 1), 0.0))
        AS opponent_on_target_ratio_pct,

    -- Chance-quality context.
    toFloat32(coalesce(ps.expected_goals_home, 0)) AS triggered_team_xg,
    toFloat32(coalesce(ps.expected_goals_away, 0)) AS opponent_xg,
    toFloat32(round(coalesce(ps.expected_goals_home, 0) - coalesce(ps.expected_goals_away, 0), 3)) AS xg_delta,
    toFloat32(coalesce(round(coalesce(ps.expected_goals_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 3), 0.0))
        AS triggered_team_xg_per_shot,
    toFloat32(coalesce(round(coalesce(ps.expected_goals_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 3), 0.0))
        AS opponent_xg_per_shot,

    -- Passing-control context.
    coalesce(ps.pass_attempts_home, 0) AS triggered_team_pass_attempts,
    coalesce(ps.pass_attempts_away, 0) AS opponent_pass_attempts,
    coalesce(ps.accurate_passes_home, 0) AS triggered_team_accurate_passes,
    coalesce(ps.accurate_passes_away, 0) AS opponent_accurate_passes,
    toFloat32(coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0))
        AS triggered_team_pass_acc_pct,
    toFloat32(coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0))
        AS opponent_pass_acc_pct,
    toFloat32(round(
        coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0)
      - coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0),
        1
    )) AS pass_accuracy_delta,

    -- Territorial progression context.
    coalesce(ps.opposition_half_passes_home, 0) AS triggered_team_opp_half_passes,
    coalesce(ps.opposition_half_passes_away, 0) AS opponent_opp_half_passes

-- Join full-match period stats for finished matches.
FROM silver.match AS m
INNER JOIN silver.period_stat AS ps
    ON ps.match_id = m.match_id
   AND ps.match_date = m.match_date
   AND ps.period = 'All'

-- Apply home-side trigger.
WHERE m.match_finished = 1
  AND coalesce(m.home_score, 0) >= 2
  AND coalesce(ps.touches_opp_box_home, 0) < 10

UNION ALL

-- Away-side triggers.
SELECT
    -- Match identifiers.
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,

    -- Triggered team and opponent identifiers.
    'away' AS triggered_side,
    m.away_team_id AS triggered_team_id,
    m.away_team_name AS triggered_team_name,
    m.home_team_id AS opponent_team_id,
    m.home_team_name AS opponent_team_name,

    -- Measured signal value: goals per final-third entry.
    toFloat32(coalesce(round(coalesce(m.away_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_away, 0)), 1.0), 3), 0.0))
        AS sig_team_possession_passing_final_third_efficiency,

    -- Signal context: scoreline efficiency with final-third-entry proxy.
    coalesce(m.away_score, 0) AS triggered_team_goals,
    coalesce(m.home_score, 0) AS opponent_goals,
    coalesce(m.away_score, 0) - coalesce(m.home_score, 0) AS goal_delta,
    coalesce(ps.touches_opp_box_away, 0) AS triggered_team_final_third_entries,
    coalesce(ps.touches_opp_box_home, 0) AS opponent_final_third_entries,
    coalesce(ps.touches_opp_box_away, 0) - coalesce(ps.touches_opp_box_home, 0) AS final_third_entries_delta,
    toFloat32(coalesce(round(coalesce(m.away_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_away, 0)), 1.0), 3), 0.0))
        AS triggered_team_goals_per_final_third_entry,
    toFloat32(coalesce(round(coalesce(m.home_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_home, 0)), 1.0), 3), 0.0))
        AS opponent_goals_per_final_third_entry,
    round(
        coalesce(round(coalesce(m.away_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_away, 0)), 1.0), 3), 0.0)
      - coalesce(round(coalesce(m.home_score, 0) / greatest(toFloat32(coalesce(ps.touches_opp_box_home, 0)), 1.0), 3), 0.0),
        3
    ) AS goals_per_entry_delta,

    -- Shot profile context.
    coalesce(ps.total_shots_away, 0) AS triggered_team_total_shots,
    coalesce(ps.total_shots_home, 0) AS opponent_total_shots,
    coalesce(ps.shots_on_target_away, 0) AS triggered_team_shots_on_target,
    coalesce(ps.shots_on_target_home, 0) AS opponent_shots_on_target,
    toFloat32(coalesce(round(100.0 * coalesce(ps.shots_on_target_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 1), 0.0))
        AS triggered_team_on_target_ratio_pct,
    toFloat32(coalesce(round(100.0 * coalesce(ps.shots_on_target_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 1), 0.0))
        AS opponent_on_target_ratio_pct,

    -- Chance-quality context.
    toFloat32(coalesce(ps.expected_goals_away, 0)) AS triggered_team_xg,
    toFloat32(coalesce(ps.expected_goals_home, 0)) AS opponent_xg,
    toFloat32(round(coalesce(ps.expected_goals_away, 0) - coalesce(ps.expected_goals_home, 0), 3)) AS xg_delta,
    toFloat32(coalesce(round(coalesce(ps.expected_goals_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 3), 0.0))
        AS triggered_team_xg_per_shot,
    toFloat32(coalesce(round(coalesce(ps.expected_goals_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 3), 0.0))
        AS opponent_xg_per_shot,

    -- Passing-control context.
    coalesce(ps.pass_attempts_away, 0) AS triggered_team_pass_attempts,
    coalesce(ps.pass_attempts_home, 0) AS opponent_pass_attempts,
    coalesce(ps.accurate_passes_away, 0) AS triggered_team_accurate_passes,
    coalesce(ps.accurate_passes_home, 0) AS opponent_accurate_passes,
    toFloat32(coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0))
        AS triggered_team_pass_acc_pct,
    toFloat32(coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0))
        AS opponent_pass_acc_pct,
    toFloat32(round(
        coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0)
      - coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0),
        1
    )) AS pass_accuracy_delta,

    -- Territorial progression context.
    coalesce(ps.opposition_half_passes_away, 0) AS triggered_team_opp_half_passes,
    coalesce(ps.opposition_half_passes_home, 0) AS opponent_opp_half_passes

-- Join full-match period stats for finished matches.
FROM silver.match AS m
INNER JOIN silver.period_stat AS ps
    ON ps.match_id = m.match_id
   AND ps.match_date = m.match_date
   AND ps.period = 'All'

-- Apply away-side trigger.
WHERE m.match_finished = 1
  AND coalesce(m.away_score, 0) >= 2
  AND coalesce(ps.touches_opp_box_away, 0) < 10

-- Prioritize highest scoring efficiency.
ORDER BY
    assumeNotNull(sig_team_possession_passing_final_third_efficiency) DESC,
    match_date DESC,
    match_id DESC;
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | identifier |
| `match_date` | Match date | identifier |
| `home_team_id` | Home team identifier | identifier |
| `home_team_name` | Home team name | identifier |
| `away_team_id` | Away team identifier | identifier |
| `away_team_name` | Away team name | identifier |
| `home_score` | Home team goals | identifier |
| `away_score` | Away team goals | identifier |
| `triggered_side` | Triggered side (`home` or `away`) | context |
| `triggered_team_id` | Triggered team identifier | identifier |
| `triggered_team_name` | Triggered team name | identifier |
| `opponent_team_id` | Opponent team identifier | identifier |
| `opponent_team_name` | Opponent team name | identifier |
| `sig_team_possession_passing_final_third_efficiency` | Signal value: goals per final-third entry proxy | signal |
| `triggered_team_goals` | Goals scored by triggered team | signal |
| `opponent_goals` | Goals scored by opponent | context |
| `goal_delta` | Triggered goals minus opponent goals | enrichment |
| `triggered_team_final_third_entries` | Triggered team final-third-entry proxy (`touches_opp_box`) | signal |
| `opponent_final_third_entries` | Opponent final-third-entry proxy (`touches_opp_box`) | context |
| `final_third_entries_delta` | Triggered minus opponent final-third entries | enrichment |
| `triggered_team_goals_per_final_third_entry` | Triggered team goals per final-third entry | enrichment |
| `opponent_goals_per_final_third_entry` | Opponent goals per final-third entry | enrichment |
| `goals_per_entry_delta` | Triggered minus opponent goals-per-entry efficiency | enrichment |
| `triggered_team_total_shots` | Triggered team total shots | enrichment |
| `opponent_total_shots` | Opponent total shots | enrichment |
| `triggered_team_shots_on_target` | Triggered team shots on target | enrichment |
| `opponent_shots_on_target` | Opponent shots on target | enrichment |
| `triggered_team_on_target_ratio_pct` | Triggered team on-target shot ratio (%) | enrichment |
| `opponent_on_target_ratio_pct` | Opponent on-target shot ratio (%) | enrichment |
| `triggered_team_xg` | Triggered team expected goals | enrichment |
| `opponent_xg` | Opponent expected goals | enrichment |
| `xg_delta` | Triggered xG minus opponent xG | enrichment |
| `triggered_team_xg_per_shot` | Triggered team xG per shot | enrichment |
| `opponent_xg_per_shot` | Opponent xG per shot | enrichment |
| `triggered_team_pass_attempts` | Triggered team pass attempts | enrichment |
| `opponent_pass_attempts` | Opponent pass attempts | enrichment |
| `triggered_team_accurate_passes` | Triggered team accurate passes | enrichment |
| `opponent_accurate_passes` | Opponent accurate passes | enrichment |
| `triggered_team_pass_acc_pct` | Triggered team pass accuracy (%) | enrichment |
| `opponent_pass_acc_pct` | Opponent pass accuracy (%) | enrichment |
| `pass_accuracy_delta` | Triggered minus opponent pass accuracy (%) | enrichment |
| `triggered_team_opp_half_passes` | Triggered team passes in opposition half | enrichment |
| `opponent_opp_half_passes` | Opponent passes in opposition half | enrichment |
