INSERT INTO gold.sig_team_possession_passing_keeper_involved (
    match_id,
    match_date,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score,
    away_score,
    triggered_team_id,
    triggered_team_name,
    triggered_gk_player_id,
    triggered_gk_player_name,
    sig_team_possession_passing_keeper_involved,
    opponent_team_id,
    opponent_team_name,
    triggered_team_possession_pct,
    opponent_possession_pct,
    triggered_team_pass_attempts,
    opponent_pass_attempts,
    triggered_team_pass_accuracy_pct,
    opponent_pass_accuracy_pct,
    triggered_team_own_half_passes,
    opponent_own_half_passes,
    triggered_team_long_ball_attempts,
    opponent_long_ball_attempts,
    triggered_team_long_ball_accuracy_pct,
    opponent_long_ball_accuracy_pct,
    possession_delta,
    pass_attempt_delta
)
-- === sig_team_possession_passing_keeper_involved ===
-- Detects matches where a goalkeeper records > 50 touches in a finished match.
-- Heavy keeper touch volume signals back-pass dependency, deep build-up routing,
-- or a team unable to circulate ball beyond their own block under press.
-- Enriched bilaterally with possession share, pass accuracy, long ball usage,
-- and own-half pass volume to distinguish tactical choice from defensive necessity.

WITH gk_touches AS (
    -- Identify the goalkeeper with the highest touch count per match (one GK per team per match)
    SELECT
        match_id,
        team_id,
        team_name,
        player_id,
        player_name,
        coalesce(touches, 0) AS gk_touches
    FROM silver.player_match_stat FINAL
    WHERE is_goalkeeper = 1
      AND coalesce(touches, 0) > 50
)

SELECT
    -- ── Identifiers ────────────────────────────────────────────────────────────
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,

    -- ── Signal: triggered team & goalkeeper identity ────────────────────────
    gk.team_id                                               AS triggered_team_id,
    gk.team_name                                             AS triggered_team_name,
    gk.player_id                                             AS triggered_gk_player_id,
    gk.player_name                                           AS triggered_gk_player_name,
    gk.gk_touches                                            AS sig_team_possession_passing_keeper_involved,

    -- ── Opponent identity ───────────────────────────────────────────────────
    if(gk.team_id = m.home_team_id,
       assumeNotNull(m.away_team_id),
       assumeNotNull(m.home_team_id))                        AS opponent_team_id,
    if(gk.team_id = m.home_team_id,
       m.away_team_name,
       m.home_team_name)                                     AS opponent_team_name,

    -- ── Possession share (bilateral) ─────────────────────────────────────────
    if(gk.team_id = m.home_team_id,
       coalesce(ps.ball_possession_home, 0),
       coalesce(ps.ball_possession_away, 0))                 AS triggered_team_possession_pct,
    if(gk.team_id = m.home_team_id,
       coalesce(ps.ball_possession_away, 0),
       coalesce(ps.ball_possession_home, 0))                 AS opponent_possession_pct,

    -- ── Pass volume (bilateral) ───────────────────────────────────────────────
    if(gk.team_id = m.home_team_id,
       coalesce(ps.pass_attempts_home, 0),
       coalesce(ps.pass_attempts_away, 0))                   AS triggered_team_pass_attempts,
    if(gk.team_id = m.home_team_id,
       coalesce(ps.pass_attempts_away, 0),
       coalesce(ps.pass_attempts_home, 0))                   AS opponent_pass_attempts,

    -- ── Pass accuracy % (bilateral) ──────────────────────────────────────────
    if(gk.team_id = m.home_team_id,
       if(coalesce(ps.pass_attempts_home, 0) > 0,
          round(coalesce(ps.accurate_passes_home, 0) * 100.0
                / coalesce(ps.pass_attempts_home, 1), 1), 0),
       if(coalesce(ps.pass_attempts_away, 0) > 0,
          round(coalesce(ps.accurate_passes_away, 0) * 100.0
                / coalesce(ps.pass_attempts_away, 1), 1), 0)) AS triggered_team_pass_accuracy_pct,
    if(gk.team_id = m.home_team_id,
       if(coalesce(ps.pass_attempts_away, 0) > 0,
          round(coalesce(ps.accurate_passes_away, 0) * 100.0
                / coalesce(ps.pass_attempts_away, 1), 1), 0),
       if(coalesce(ps.pass_attempts_home, 0) > 0,
          round(coalesce(ps.accurate_passes_home, 0) * 100.0
                / coalesce(ps.pass_attempts_home, 1), 1), 0)) AS opponent_pass_accuracy_pct,

    -- ── Own-half passes — proxy for deep build-up or press avoidance ────────
    if(gk.team_id = m.home_team_id,
       coalesce(ps.own_half_passes_home, 0),
       coalesce(ps.own_half_passes_away, 0))                 AS triggered_team_own_half_passes,
    if(gk.team_id = m.home_team_id,
       coalesce(ps.own_half_passes_away, 0),
       coalesce(ps.own_half_passes_home, 0))                 AS opponent_own_half_passes,

    -- ── Long ball usage (bilateral) — direct ball out from keeper ────────────
    if(gk.team_id = m.home_team_id,
       coalesce(ps.long_ball_attempts_home, 0),
       coalesce(ps.long_ball_attempts_away, 0))              AS triggered_team_long_ball_attempts,
    if(gk.team_id = m.home_team_id,
       coalesce(ps.long_ball_attempts_away, 0),
       coalesce(ps.long_ball_attempts_home, 0))              AS opponent_long_ball_attempts,

    if(gk.team_id = m.home_team_id,
       if(coalesce(ps.long_ball_attempts_home, 0) > 0,
          round(coalesce(ps.accurate_long_balls_home, 0) * 100.0
                / coalesce(ps.long_ball_attempts_home, 1), 1), 0),
       if(coalesce(ps.long_ball_attempts_away, 0) > 0,
          round(coalesce(ps.accurate_long_balls_away, 0) * 100.0
                / coalesce(ps.long_ball_attempts_away, 1), 1), 0)) AS triggered_team_long_ball_accuracy_pct,
    if(gk.team_id = m.home_team_id,
       if(coalesce(ps.long_ball_attempts_away, 0) > 0,
          round(coalesce(ps.accurate_long_balls_away, 0) * 100.0
                / coalesce(ps.long_ball_attempts_away, 1), 1), 0),
       if(coalesce(ps.long_ball_attempts_home, 0) > 0,
          round(coalesce(ps.accurate_long_balls_home, 0) * 100.0
                / coalesce(ps.long_ball_attempts_home, 1), 1), 0)) AS opponent_long_ball_accuracy_pct,

    -- ── Net / delta columns (bilateral by construction) ──────────────────────
    -- Positive = triggered team dominates possession; negative = under pressure
    if(gk.team_id = m.home_team_id,
       coalesce(ps.ball_possession_home, 0) - coalesce(ps.ball_possession_away, 0),
       coalesce(ps.ball_possession_away, 0) - coalesce(ps.ball_possession_home, 0))
                                                             AS possession_delta,

    -- Pass attempt gap: large negative delta → triggered team is under severe press
    if(gk.team_id = m.home_team_id,
       coalesce(ps.pass_attempts_home, 0) - coalesce(ps.pass_attempts_away, 0),
       coalesce(ps.pass_attempts_away, 0) - coalesce(ps.pass_attempts_home, 0))
                                                             AS pass_attempt_delta

FROM silver.match FINAL AS m
INNER JOIN gk_touches AS gk
        ON gk.match_id = m.match_id
LEFT JOIN silver.period_stat FINAL AS ps
       ON ps.match_id = m.match_id
      AND ps.period   = 'All'
WHERE m.match_finished = 1
ORDER BY gk.gk_touches DESC;
