INSERT INTO gold.sig_team_possession_passing_second_half_possession_collapse (
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
    possession_drop_pp,
    triggered_team_poss_fh,
    triggered_team_poss_sh,
    opponent_poss_fh,
    opponent_poss_sh,
    possession_swing_delta,
    triggered_team_pass_att_fh,
    triggered_team_pass_att_sh,
    opponent_pass_att_fh,
    opponent_pass_att_sh,
    triggered_team_pass_acc_fh,
    triggered_team_pass_acc_sh,
    opponent_pass_acc_fh,
    opponent_pass_acc_sh,
    triggered_team_pass_acc_delta,
    opponent_pass_acc_delta,
    triggered_team_opp_half_passes_fh,
    triggered_team_opp_half_passes_sh,
    opponent_opp_half_passes_fh,
    opponent_opp_half_passes_sh,
    triggered_team_xg_fh,
    triggered_team_xg_sh,
    opponent_xg_fh,
    opponent_xg_sh,
    xg_swing_delta
)
-- ============================================================
-- Signal: sig_team_possession_passing_second_half_possession_collapse
-- Intent: Detect teams whose possession drops by >20 percentage
--         points from first half to second half, with bilateral
--         pass and xG-half context to classify tactical surrender
--         versus forced collapse.
-- ============================================================

WITH half_stats AS (
    -- Pivot FirstHalf / SecondHalf into a single row per match
    SELECT
        match_id,
        -- Possession per half
        maxIf(coalesce(ball_possession_home, 0), period = 'FirstHalf')    AS fh_poss_home,
        maxIf(coalesce(ball_possession_away, 0), period = 'FirstHalf')    AS fh_poss_away,
        maxIf(coalesce(ball_possession_home, 0), period = 'SecondHalf')   AS sh_poss_home,
        maxIf(coalesce(ball_possession_away, 0), period = 'SecondHalf')   AS sh_poss_away,
        -- Pass volume per half
        maxIf(coalesce(pass_attempts_home, 0), period = 'FirstHalf')      AS fh_pass_att_home,
        maxIf(coalesce(pass_attempts_away, 0), period = 'FirstHalf')      AS fh_pass_att_away,
        maxIf(coalesce(pass_attempts_home, 0), period = 'SecondHalf')     AS sh_pass_att_home,
        maxIf(coalesce(pass_attempts_away, 0), period = 'SecondHalf')     AS sh_pass_att_away,
        maxIf(coalesce(accurate_passes_home, 0), period = 'FirstHalf')    AS fh_acc_passes_home,
        maxIf(coalesce(accurate_passes_away, 0), period = 'FirstHalf')    AS fh_acc_passes_away,
        maxIf(coalesce(accurate_passes_home, 0), period = 'SecondHalf')   AS sh_acc_passes_home,
        maxIf(coalesce(accurate_passes_away, 0), period = 'SecondHalf')   AS sh_acc_passes_away,
        -- xG per half (outcome proxy for whether the drop was tactical or forced)
        maxIf(coalesce(expected_goals_home, 0), period = 'FirstHalf')     AS fh_xg_home,
        maxIf(coalesce(expected_goals_away, 0), period = 'FirstHalf')     AS fh_xg_away,
        maxIf(coalesce(expected_goals_home, 0), period = 'SecondHalf')    AS sh_xg_home,
        maxIf(coalesce(expected_goals_away, 0), period = 'SecondHalf')    AS sh_xg_away,
        -- Opposition half passes: ball progression proxy
        maxIf(coalesce(opposition_half_passes_home, 0), period = 'FirstHalf')  AS fh_opp_half_passes_home,
        maxIf(coalesce(opposition_half_passes_away, 0), period = 'FirstHalf')  AS fh_opp_half_passes_away,
        maxIf(coalesce(opposition_half_passes_home, 0), period = 'SecondHalf') AS sh_opp_half_passes_home,
        maxIf(coalesce(opposition_half_passes_away, 0), period = 'SecondHalf') AS sh_opp_half_passes_away
    FROM fotmob.period_stat FINAL
    WHERE period IN ('FirstHalf', 'SecondHalf')
    GROUP BY match_id
)

-- ── HOME team is the triggered side ─────────────────────────
SELECT
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,
    -- Signal identity
    'home'                                                              AS triggered_side,
    m.home_team_id                                                      AS triggered_team_id,
    m.home_team_name                                                    AS triggered_team_name,
    m.away_team_id                                                      AS opponent_team_id,
    m.away_team_name                                                    AS opponent_team_name,
    -- Core signal value
    (h.sh_poss_home - h.fh_poss_home)                                  AS possession_drop_pp,
    -- Triggered team: possession by half
    h.fh_poss_home                                                      AS triggered_team_poss_fh,
    h.sh_poss_home                                                      AS triggered_team_poss_sh,
    -- Opponent: possession by half
    h.fh_poss_away                                                      AS opponent_poss_fh,
    h.sh_poss_away                                                      AS opponent_poss_sh,
    -- Possession swing (bilateral net shift)
    (h.sh_poss_home - h.sh_poss_away) - (h.fh_poss_home - h.fh_poss_away) AS possession_swing_delta,
    -- Triggered team: pass volume by half
    h.fh_pass_att_home                                                  AS triggered_team_pass_att_fh,
    h.sh_pass_att_home                                                  AS triggered_team_pass_att_sh,
    -- Opponent: pass volume by half
    h.fh_pass_att_away                                                  AS opponent_pass_att_fh,
    h.sh_pass_att_away                                                  AS opponent_pass_att_sh,
    -- Triggered team: pass accuracy by half (%)
    if(h.fh_pass_att_home > 0,
        round(h.fh_acc_passes_home * 100.0 / h.fh_pass_att_home, 1),
        NULL)                                                           AS triggered_team_pass_acc_fh,
    if(h.sh_pass_att_home > 0,
        round(h.sh_acc_passes_home * 100.0 / h.sh_pass_att_home, 1),
        NULL)                                                           AS triggered_team_pass_acc_sh,
    -- Opponent: pass accuracy by half (%)
    if(h.fh_pass_att_away > 0,
        round(h.fh_acc_passes_away * 100.0 / h.fh_pass_att_away, 1),
        NULL)                                                           AS opponent_pass_acc_fh,
    if(h.sh_pass_att_away > 0,
        round(h.sh_acc_passes_away * 100.0 / h.sh_pass_att_away, 1),
        NULL)                                                           AS opponent_pass_acc_sh,
    -- Pass accuracy drop (triggered team only, bilateral via sign)
    if(h.sh_pass_att_home > 0 AND h.fh_pass_att_home > 0,
        round((h.sh_acc_passes_home * 100.0 / h.sh_pass_att_home)
            - (h.fh_acc_passes_home * 100.0 / h.fh_pass_att_home), 1),
        NULL)                                                           AS triggered_team_pass_acc_delta,
    if(h.sh_pass_att_away > 0 AND h.fh_pass_att_away > 0,
        round((h.sh_acc_passes_away * 100.0 / h.sh_pass_att_away)
            - (h.fh_acc_passes_away * 100.0 / h.fh_pass_att_away), 1),
        NULL)                                                           AS opponent_pass_acc_delta,
    -- Triggered team: opposition-half passes (territory / progression proxy)
    h.fh_opp_half_passes_home                                          AS triggered_team_opp_half_passes_fh,
    h.sh_opp_half_passes_home                                          AS triggered_team_opp_half_passes_sh,
    -- Opponent: opposition-half passes
    h.fh_opp_half_passes_away                                          AS opponent_opp_half_passes_fh,
    h.sh_opp_half_passes_away                                          AS opponent_opp_half_passes_sh,
    -- xG per half — was the possession drop punished?
    h.fh_xg_home                                                       AS triggered_team_xg_fh,
    h.sh_xg_home                                                       AS triggered_team_xg_sh,
    h.fh_xg_away                                                       AS opponent_xg_fh,
    h.sh_xg_away                                                       AS opponent_xg_sh,
    -- Net xG swing (bilateral)
    (h.sh_xg_home - h.sh_xg_away) - (h.fh_xg_home - h.fh_xg_away)    AS xg_swing_delta
FROM fotmob.match FINAL AS m
JOIN half_stats h USING (match_id)
WHERE m.match_finished = 1
  AND (h.sh_poss_home - h.fh_poss_home) < -20

UNION ALL

-- ── AWAY team is the triggered side ─────────────────────────
SELECT
    m.match_id,
    m.match_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,
    'away'                                                              AS triggered_side,
    m.away_team_id                                                      AS triggered_team_id,
    m.away_team_name                                                    AS triggered_team_name,
    m.home_team_id                                                      AS opponent_team_id,
    m.home_team_name                                                    AS opponent_team_name,
    (h.sh_poss_away - h.fh_poss_away)                                  AS possession_drop_pp,
    h.fh_poss_away                                                      AS triggered_team_poss_fh,
    h.sh_poss_away                                                      AS triggered_team_poss_sh,
    h.fh_poss_home                                                      AS opponent_poss_fh,
    h.sh_poss_home                                                      AS opponent_poss_sh,
    (h.sh_poss_away - h.sh_poss_home) - (h.fh_poss_away - h.fh_poss_home) AS possession_swing_delta,
    h.fh_pass_att_away                                                  AS triggered_team_pass_att_fh,
    h.sh_pass_att_away                                                  AS triggered_team_pass_att_sh,
    h.fh_pass_att_home                                                  AS opponent_pass_att_fh,
    h.sh_pass_att_home                                                  AS opponent_pass_att_sh,
    if(h.fh_pass_att_away > 0,
        round(h.fh_acc_passes_away * 100.0 / h.fh_pass_att_away, 1),
        NULL)                                                           AS triggered_team_pass_acc_fh,
    if(h.sh_pass_att_away > 0,
        round(h.sh_acc_passes_away * 100.0 / h.sh_pass_att_away, 1),
        NULL)                                                           AS triggered_team_pass_acc_sh,
    if(h.fh_pass_att_home > 0,
        round(h.fh_acc_passes_home * 100.0 / h.fh_pass_att_home, 1),
        NULL)                                                           AS opponent_pass_acc_fh,
    if(h.sh_pass_att_home > 0,
        round(h.sh_acc_passes_home * 100.0 / h.sh_pass_att_home, 1),
        NULL)                                                           AS opponent_pass_acc_sh,
    if(h.sh_pass_att_away > 0 AND h.fh_pass_att_away > 0,
        round((h.sh_acc_passes_away * 100.0 / h.sh_pass_att_away)
            - (h.fh_acc_passes_away * 100.0 / h.fh_pass_att_away), 1),
        NULL)                                                           AS triggered_team_pass_acc_delta,
    if(h.sh_pass_att_home > 0 AND h.fh_pass_att_home > 0,
        round((h.sh_acc_passes_home * 100.0 / h.sh_pass_att_home)
            - (h.fh_acc_passes_home * 100.0 / h.fh_pass_att_home), 1),
        NULL)                                                           AS opponent_pass_acc_delta,
    h.fh_opp_half_passes_away                                          AS triggered_team_opp_half_passes_fh,
    h.sh_opp_half_passes_away                                          AS triggered_team_opp_half_passes_sh,
    h.fh_opp_half_passes_home                                          AS opponent_opp_half_passes_fh,
    h.sh_opp_half_passes_home                                          AS opponent_opp_half_passes_sh,
    h.fh_xg_away                                                       AS triggered_team_xg_fh,
    h.sh_xg_away                                                       AS triggered_team_xg_sh,
    h.fh_xg_home                                                       AS opponent_xg_fh,
    h.sh_xg_home                                                       AS opponent_xg_sh,
    (h.sh_xg_away - h.sh_xg_home) - (h.fh_xg_away - h.fh_xg_home)    AS xg_swing_delta
FROM fotmob.match FINAL AS m
JOIN half_stats h USING (match_id)
WHERE m.match_finished = 1
  AND (h.sh_poss_away - h.fh_poss_away) < -20

ORDER BY assumeNotNull(possession_drop_pp) ASC;
