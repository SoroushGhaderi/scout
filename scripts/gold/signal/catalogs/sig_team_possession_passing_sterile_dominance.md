# sig_team_possession_passing_sterile_dominance

## Purpose

Detect teams that dominate possession (`>70%`) but create zero big chances, signaling sterile circulation without high-quality attacking outcomes.

## Tactical And Statistical Logic

- Signal name source: `-- sig_team_possession_passing_sterile_dominance`
- Trigger condition source: `-- Trigger condition: possession > 70 and big_chances = 0 for the triggered team in full-match period stats.`
- Triggered rows are side-specific (`home`/`away`) and preserve symmetric tactical context for passing control, shot output, chance quality, and territorial progression.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_sterile_dominance.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_sterile_dominance.py`
- Target table: `gold.sig_team_possession_passing_sterile_dominance`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_sterile_dominance.py
```

## SQL

```sql
-- sig_team_possession_passing_sterile_dominance
-- Trigger condition: possession > 70 and big_chances = 0 for the triggered team in full-match period stats.
-- Intent: identify high-possession teams that fail to generate high-quality chances, with bilateral passing, shooting, and territory context.

-- Home side triggers the signal.
SELECT
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,
    'home' AS triggered_side,
    m.home_team_id AS triggered_team_id,
    m.home_team_name AS triggered_team_name,
    m.away_team_id AS opponent_team_id,
    m.away_team_name AS opponent_team_name,
    toFloat32(assumeNotNull(ps.ball_possession_home)) AS sig_team_possession_passing_sterile_dominance,
    toFloat32(assumeNotNull(ps.ball_possession_home)) AS triggered_team_possession_pct,
    toFloat32(assumeNotNull(ps.ball_possession_away)) AS opponent_possession_pct,
    toFloat32(assumeNotNull(ps.ball_possession_home) - assumeNotNull(ps.ball_possession_away)) AS possession_delta,
    coalesce(ps.big_chances_home, 0) AS triggered_team_big_chances,
    coalesce(ps.big_chances_away, 0) AS opponent_big_chances,
    coalesce(ps.big_chances_home, 0) - coalesce(ps.big_chances_away, 0) AS big_chance_delta,
    coalesce(ps.total_shots_home, 0) AS triggered_team_total_shots,
    coalesce(ps.total_shots_away, 0) AS opponent_total_shots,
    coalesce(ps.total_shots_home, 0) - coalesce(ps.total_shots_away, 0) AS shot_volume_delta,
    coalesce(ps.shots_on_target_home, 0) AS triggered_team_shots_on_target,
    coalesce(ps.shots_on_target_away, 0) AS opponent_shots_on_target,
    coalesce(round(100.0 * coalesce(ps.shots_on_target_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 1), 0.0) AS triggered_team_on_target_ratio_pct,
    coalesce(round(100.0 * coalesce(ps.shots_on_target_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 1), 0.0) AS opponent_on_target_ratio_pct,
    coalesce(ps.expected_goals_home, 0) AS triggered_team_xg,
    coalesce(ps.expected_goals_away, 0) AS opponent_xg,
    round(coalesce(ps.expected_goals_home, 0) - coalesce(ps.expected_goals_away, 0), 3) AS xg_delta,
    coalesce(round(coalesce(ps.expected_goals_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 3), 0.0) AS triggered_team_xg_per_shot,
    coalesce(round(coalesce(ps.expected_goals_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 3), 0.0) AS opponent_xg_per_shot,
    coalesce(ps.pass_attempts_home, 0) AS triggered_team_pass_attempts,
    coalesce(ps.pass_attempts_away, 0) AS opponent_pass_attempts,
    coalesce(ps.accurate_passes_home, 0) AS triggered_team_accurate_passes,
    coalesce(ps.accurate_passes_away, 0) AS opponent_accurate_passes,
    coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0) AS triggered_team_pass_acc_pct,
    coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0) AS opponent_pass_acc_pct,
    round(
        coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0)
      - coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0),
        1
    ) AS pass_accuracy_delta,
    coalesce(ps.touches_opp_box_home, 0) AS triggered_team_touches_opp_box,
    coalesce(ps.touches_opp_box_away, 0) AS opponent_touches_opp_box,
    coalesce(ps.opposition_half_passes_home, 0) AS triggered_team_opp_half_passes,
    coalesce(ps.opposition_half_passes_away, 0) AS opponent_opp_half_passes
FROM silver.match AS m
INNER JOIN silver.period_stat AS ps
    ON ps.match_id = m.match_id
   AND ps.match_date = m.match_date
   AND ps.period = 'All'
WHERE m.match_finished = 1
  AND assumeNotNull(ps.ball_possession_home) > 70
  AND coalesce(ps.big_chances_home, 0) = 0

UNION ALL

-- Away side triggers the signal.
SELECT
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,
    'away' AS triggered_side,
    m.away_team_id AS triggered_team_id,
    m.away_team_name AS triggered_team_name,
    m.home_team_id AS opponent_team_id,
    m.home_team_name AS opponent_team_name,
    toFloat32(assumeNotNull(ps.ball_possession_away)) AS sig_team_possession_passing_sterile_dominance,
    toFloat32(assumeNotNull(ps.ball_possession_away)) AS triggered_team_possession_pct,
    toFloat32(assumeNotNull(ps.ball_possession_home)) AS opponent_possession_pct,
    toFloat32(assumeNotNull(ps.ball_possession_away) - assumeNotNull(ps.ball_possession_home)) AS possession_delta,
    coalesce(ps.big_chances_away, 0) AS triggered_team_big_chances,
    coalesce(ps.big_chances_home, 0) AS opponent_big_chances,
    coalesce(ps.big_chances_away, 0) - coalesce(ps.big_chances_home, 0) AS big_chance_delta,
    coalesce(ps.total_shots_away, 0) AS triggered_team_total_shots,
    coalesce(ps.total_shots_home, 0) AS opponent_total_shots,
    coalesce(ps.total_shots_away, 0) - coalesce(ps.total_shots_home, 0) AS shot_volume_delta,
    coalesce(ps.shots_on_target_away, 0) AS triggered_team_shots_on_target,
    coalesce(ps.shots_on_target_home, 0) AS opponent_shots_on_target,
    coalesce(round(100.0 * coalesce(ps.shots_on_target_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 1), 0.0) AS triggered_team_on_target_ratio_pct,
    coalesce(round(100.0 * coalesce(ps.shots_on_target_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 1), 0.0) AS opponent_on_target_ratio_pct,
    coalesce(ps.expected_goals_away, 0) AS triggered_team_xg,
    coalesce(ps.expected_goals_home, 0) AS opponent_xg,
    round(coalesce(ps.expected_goals_away, 0) - coalesce(ps.expected_goals_home, 0), 3) AS xg_delta,
    coalesce(round(coalesce(ps.expected_goals_away, 0) / nullIf(coalesce(ps.total_shots_away, 0), 0), 3), 0.0) AS triggered_team_xg_per_shot,
    coalesce(round(coalesce(ps.expected_goals_home, 0) / nullIf(coalesce(ps.total_shots_home, 0), 0), 3), 0.0) AS opponent_xg_per_shot,
    coalesce(ps.pass_attempts_away, 0) AS triggered_team_pass_attempts,
    coalesce(ps.pass_attempts_home, 0) AS opponent_pass_attempts,
    coalesce(ps.accurate_passes_away, 0) AS triggered_team_accurate_passes,
    coalesce(ps.accurate_passes_home, 0) AS opponent_accurate_passes,
    coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0) AS triggered_team_pass_acc_pct,
    coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0) AS opponent_pass_acc_pct,
    round(
        coalesce(round(100.0 * coalesce(ps.accurate_passes_away, 0) / nullIf(coalesce(ps.pass_attempts_away, 0), 0), 1), 0.0)
      - coalesce(round(100.0 * coalesce(ps.accurate_passes_home, 0) / nullIf(coalesce(ps.pass_attempts_home, 0), 0), 1), 0.0),
        1
    ) AS pass_accuracy_delta,
    coalesce(ps.touches_opp_box_away, 0) AS triggered_team_touches_opp_box,
    coalesce(ps.touches_opp_box_home, 0) AS opponent_touches_opp_box,
    coalesce(ps.opposition_half_passes_away, 0) AS triggered_team_opp_half_passes,
    coalesce(ps.opposition_half_passes_home, 0) AS opponent_opp_half_passes
FROM silver.match AS m
INNER JOIN silver.period_stat AS ps
    ON ps.match_id = m.match_id
   AND ps.match_date = m.match_date
   AND ps.period = 'All'
WHERE m.match_finished = 1
  AND assumeNotNull(ps.ball_possession_away) > 70
  AND coalesce(ps.big_chances_away, 0) = 0
ORDER BY
    assumeNotNull(sig_team_possession_passing_sterile_dominance) DESC,
    match_date DESC,
    match_id DESC;
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | identifier |
| `match_date` | Match calendar date | identifier |
| `home_team_id` | Home team identifier | identifier |
| `home_team_name` | Home team name | identifier |
| `away_team_id` | Away team identifier | identifier |
| `away_team_name` | Away team name | identifier |
| `home_score` | Home team goals | identifier |
| `away_score` | Away team goals | identifier |
| `triggered_side` | Which side triggered (`home`/`away`) | context |
| `triggered_team_id` | Team id that triggered the signal | identifier |
| `triggered_team_name` | Team name that triggered the signal | identifier |
| `opponent_team_id` | Opponent team id relative to triggered team | identifier |
| `opponent_team_name` | Opponent team name relative to triggered team | identifier |
| `sig_team_possession_passing_sterile_dominance` | Measured signal value (triggered-team possession %) | signal |
| `triggered_team_possession_pct` | Triggered-team possession % | signal |
| `opponent_possession_pct` | Opponent possession % | context |
| `possession_delta` | Triggered minus opponent possession % | enrichment |
| `triggered_team_big_chances` | Triggered-team big chances created | signal |
| `opponent_big_chances` | Opponent big chances created | context |
| `big_chance_delta` | Triggered minus opponent big chances | enrichment |
| `triggered_team_total_shots` | Triggered-team total shots | enrichment |
| `opponent_total_shots` | Opponent total shots | enrichment |
| `shot_volume_delta` | Triggered minus opponent shot volume | enrichment |
| `triggered_team_shots_on_target` | Triggered-team shots on target | enrichment |
| `opponent_shots_on_target` | Opponent shots on target | enrichment |
| `triggered_team_on_target_ratio_pct` | Triggered-team shots-on-target ratio % | enrichment |
| `opponent_on_target_ratio_pct` | Opponent shots-on-target ratio % | enrichment |
| `triggered_team_xg` | Triggered-team expected goals | enrichment |
| `opponent_xg` | Opponent expected goals | enrichment |
| `xg_delta` | Triggered minus opponent xG | enrichment |
| `triggered_team_xg_per_shot` | Triggered-team xG per shot | enrichment |
| `opponent_xg_per_shot` | Opponent xG per shot | enrichment |
| `triggered_team_pass_attempts` | Triggered-team pass attempts | enrichment |
| `opponent_pass_attempts` | Opponent pass attempts | enrichment |
| `triggered_team_accurate_passes` | Triggered-team accurate passes | enrichment |
| `opponent_accurate_passes` | Opponent accurate passes | enrichment |
| `triggered_team_pass_acc_pct` | Triggered-team pass accuracy % | enrichment |
| `opponent_pass_acc_pct` | Opponent pass accuracy % | enrichment |
| `pass_accuracy_delta` | Triggered minus opponent pass accuracy % | enrichment |
| `triggered_team_touches_opp_box` | Triggered-team touches in opponent box | enrichment |
| `opponent_touches_opp_box` | Opponent touches in triggered-team box | enrichment |
| `triggered_team_opp_half_passes` | Triggered-team passes in opposition half | enrichment |
| `opponent_opp_half_passes` | Opponent passes in opposition half | enrichment |
