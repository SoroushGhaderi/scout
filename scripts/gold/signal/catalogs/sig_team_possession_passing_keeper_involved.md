# sig_team_possession_passing_keeper_involved

## Purpose

Detect matches where a team goalkeeper records very high involvement in circulation (`> 50` touches), indicating build-up routed heavily through the keeper.

## Tactical And Statistical Logic

- Signal name source: `-- sig_team_possession_passing_keeper_involved`
- Trigger condition source: `-- Trigger condition: max goalkeeper touches by team in a finished match > 50.`
- Triggered rows are team-specific and include bilateral passing/possession context to distinguish controlled deep build-up from press-induced emergency recycling.

## Technical Assets

- SQL: `clickhouse/gold/signal/sig_team_possession_passing_keeper_involved.sql`
- Runner: `scripts/gold/signal/runners/sig_team_possession_passing_keeper_involved.py`
- Target table: `gold.sig_team_possession_passing_keeper_involved`

## Example Execution

```bash
python scripts/gold/signal/runners/sig_team_possession_passing_keeper_involved.py
```

## SQL

```sql
-- sig_team_possession_passing_keeper_involved
-- Trigger condition: max goalkeeper touches by team in a finished match > 50.
-- Intent: detect keeper-heavy build-up usage and enrich with symmetric passing and possession context.

-- Select triggered team rows with required match context and bilateral tactical enrichment.
SELECT
    -- Match identifiers and scoreline context
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,

    -- Triggered team + triggering goalkeeper identifiers
    gk.gk_team_id AS triggered_team_id,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        assumeNotNull(m.home_team_name),
        assumeNotNull(m.away_team_name)
    ) AS triggered_team_name,
    gk.triggered_gk_player_id AS triggered_gk_player_id,
    gk.triggered_gk_player_name AS triggered_gk_player_name,
    toInt32(gk.gk_touches) AS sig_team_possession_passing_keeper_involved,

    -- Opponent team identifiers
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        assumeNotNull(m.away_team_id),
        assumeNotNull(m.home_team_id)
    ) AS opponent_team_id,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        assumeNotNull(m.away_team_name),
        assumeNotNull(m.home_team_name)
    ) AS opponent_team_name,

    -- Possession context (symmetric pair)
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.ball_possession_home, 0),
        coalesce(ps.ball_possession_away, 0)
    ) AS triggered_team_possession_pct,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.ball_possession_away, 0),
        coalesce(ps.ball_possession_home, 0)
    ) AS opponent_possession_pct,

    -- Pass volume context (symmetric pair)
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.pass_attempts_home, 0),
        coalesce(ps.pass_attempts_away, 0)
    ) AS triggered_team_pass_attempts,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.pass_attempts_away, 0),
        coalesce(ps.pass_attempts_home, 0)
    ) AS opponent_pass_attempts,

    -- Pass execution quality (symmetric pair)
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_passes_home, 0)
                / nullIf(coalesce(ps.pass_attempts_home, 0), 0),
                1
            ),
            0.0
        ),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_passes_away, 0)
                / nullIf(coalesce(ps.pass_attempts_away, 0), 0),
                1
            ),
            0.0
        )
    ) AS triggered_team_pass_accuracy_pct,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_passes_away, 0)
                / nullIf(coalesce(ps.pass_attempts_away, 0), 0),
                1
            ),
            0.0
        ),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_passes_home, 0)
                / nullIf(coalesce(ps.pass_attempts_home, 0), 0),
                1
            ),
            0.0
        )
    ) AS opponent_pass_accuracy_pct,

    -- Build-up depth context (symmetric pair)
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.own_half_passes_home, 0),
        coalesce(ps.own_half_passes_away, 0)
    ) AS triggered_team_own_half_passes,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.own_half_passes_away, 0),
        coalesce(ps.own_half_passes_home, 0)
    ) AS opponent_own_half_passes,

    -- Long-ball release context (symmetric pair)
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.long_ball_attempts_home, 0),
        coalesce(ps.long_ball_attempts_away, 0)
    ) AS triggered_team_long_ball_attempts,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.long_ball_attempts_away, 0),
        coalesce(ps.long_ball_attempts_home, 0)
    ) AS opponent_long_ball_attempts,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_long_balls_home, 0)
                / nullIf(coalesce(ps.long_ball_attempts_home, 0), 0),
                1
            ),
            0.0
        ),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_long_balls_away, 0)
                / nullIf(coalesce(ps.long_ball_attempts_away, 0), 0),
                1
            ),
            0.0
        )
    ) AS triggered_team_long_ball_accuracy_pct,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_long_balls_away, 0)
                / nullIf(coalesce(ps.long_ball_attempts_away, 0), 0),
                1
            ),
            0.0
        ),
        coalesce(
            round(
                100.0 * coalesce(ps.accurate_long_balls_home, 0)
                / nullIf(coalesce(ps.long_ball_attempts_home, 0), 0),
                1
            ),
            0.0
        )
    ) AS opponent_long_ball_accuracy_pct,

    -- Bilateral net columns
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.ball_possession_home, 0) - coalesce(ps.ball_possession_away, 0),
        coalesce(ps.ball_possession_away, 0) - coalesce(ps.ball_possession_home, 0)
    ) AS possession_delta,
    if(
        gk.gk_team_id = assumeNotNull(m.home_team_id),
        coalesce(ps.pass_attempts_home, 0) - coalesce(ps.pass_attempts_away, 0),
        coalesce(ps.pass_attempts_away, 0) - coalesce(ps.pass_attempts_home, 0)
    ) AS pass_attempt_delta

-- Base match context for triggered rows.
FROM silver.match AS m

-- Join one goalkeeper trigger per team-match (highest GK touches, then threshold > 50).
INNER JOIN (
    SELECT
        pms.match_id,
        assumeNotNull(pms.team_id) AS gk_team_id,
        argMax(pms.player_id, coalesce(pms.touches, 0)) AS triggered_gk_player_id,
        argMax(pms.player_name, coalesce(pms.touches, 0)) AS triggered_gk_player_name,
        max(coalesce(pms.touches, 0)) AS gk_touches
    FROM silver.player_match_stat AS pms
    WHERE pms.is_goalkeeper = 1
      AND pms.team_id IS NOT NULL
    GROUP BY
        pms.match_id,
        assumeNotNull(pms.team_id)
    HAVING max(coalesce(pms.touches, 0)) > 50
) AS gk
    ON gk.match_id = m.match_id

-- Join full-match period stats for tactical enrichment.
LEFT JOIN silver.period_stat AS ps
    ON ps.match_id = m.match_id
   AND ps.match_date = m.match_date
   AND ps.period = 'All'

-- Restrict to finished matches.
WHERE m.match_finished = 1

-- Strongest keeper-involvement cases first.
ORDER BY
    assumeNotNull(gk.gk_touches) DESC,
    m.match_date DESC,
    m.match_id DESC;
```

## Output Schema

| Column Name | Description | Reason |
|---|---|---|
| `match_id` | Unique match identifier | Identifier |
| `match_date` | Calendar date the match was played | Identifier |
| `home_team_id` | Numeric ID of the home team | Identifier |
| `home_team_name` | Display name of the home team | Identifier |
| `away_team_id` | Numeric ID of the away team | Identifier |
| `away_team_name` | Display name of the away team | Identifier |
| `home_score` | Full-time goals scored by home team | Identifier |
| `away_score` | Full-time goals scored by away team | Identifier |
| `triggered_team_id` | Team ID of the side whose goalkeeper triggered the signal | Signal |
| `triggered_team_name` | Display name of the triggered team | Signal |
| `triggered_gk_player_id` | Player ID of the goalkeeper who recorded >50 touches | Signal |
| `triggered_gk_player_name` | Name of the triggering goalkeeper | Signal |
| `sig_team_possession_passing_keeper_involved` | Goalkeeper touch count (signal value) — >50 indicates heavy back-pass usage | Signal |
| `opponent_team_id` | Team ID of the opposition in the same match | Context |
| `opponent_team_name` | Display name of the opposition | Context |
| `triggered_team_possession_pct` | Ball possession percentage for the triggered team (full match) | Enrichment |
| `opponent_possession_pct` | Ball possession percentage for the opponent | Enrichment |
| `triggered_team_pass_attempts` | Total pass attempts by the triggered team | Enrichment |
| `opponent_pass_attempts` | Total pass attempts by the opponent | Enrichment |
| `triggered_team_pass_accuracy_pct` | Pass completion rate (%) for the triggered team | Enrichment |
| `opponent_pass_accuracy_pct` | Pass completion rate (%) for the opponent | Enrichment |
| `triggered_team_own_half_passes` | Passes completed in own half by the triggered team — proxy for deep build-up under pressure | Enrichment |
| `opponent_own_half_passes` | Passes completed in own half by the opponent | Enrichment |
| `triggered_team_long_ball_attempts` | Number of long ball attempts by the triggered team — indicates direct keeper distribution | Enrichment |
| `opponent_long_ball_attempts` | Number of long ball attempts by the opponent | Enrichment |
| `triggered_team_long_ball_accuracy_pct` | Successful long ball rate (%) for the triggered team | Enrichment |
| `opponent_long_ball_accuracy_pct` | Successful long ball rate (%) for the opponent | Enrichment |
| `possession_delta` | Triggered team possession % minus opponent possession % — negative value signals a team under sustained pressure | Enrichment (net) |
| `pass_attempt_delta` | Triggered team pass attempts minus opponent pass attempts — large negative delta reveals press suppression of build-up volume | Enrichment (net) |
