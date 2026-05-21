INSERT INTO gold.sig_match_shooting_goals_high_pressure_finish (
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
    trigger_threshold_late_shot_minute_exclusive,
    trigger_threshold_match_total_late_shots_min,
    match_late_shots_total,
    home_late_shots,
    away_late_shots,
    match_late_shots_on_target_total,
    match_late_shot_accuracy_pct,
    triggered_team_late_shots,
    opponent_late_shots,
    late_shot_volume_delta,
    triggered_team_late_shots_on_target,
    opponent_late_shots_on_target,
    late_shot_on_target_delta,
    triggered_team_late_shot_accuracy_pct,
    opponent_late_shot_accuracy_pct,
    late_shot_accuracy_delta_pct,
    triggered_team_total_shots,
    opponent_total_shots,
    shot_volume_delta,
    triggered_team_shots_on_target,
    opponent_shots_on_target,
    shot_on_target_delta,
    triggered_team_xg,
    opponent_xg,
    xg_delta,
    triggered_team_goals,
    opponent_goals,
    goal_gap,
    triggered_team_shot_conversion_pct,
    opponent_shot_conversion_pct,
    shot_conversion_delta_pct,
    triggered_team_big_chances,
    opponent_big_chances,
    big_chance_delta,
    triggered_team_big_chances_missed,
    opponent_big_chances_missed,
    big_chances_missed_delta,
    triggered_team_touches_opposition_box,
    opponent_touches_opposition_box,
    opposition_box_touch_delta,
    triggered_team_possession_pct,
    opponent_possession_pct,
    possession_delta_pct,
    triggered_team_pass_accuracy_pct,
    opponent_pass_accuracy_pct,
    pass_accuracy_delta_pct
)
-- Signal: sig_match_shooting_goals_high_pressure_finish
-- Intent: detect matches with extreme late attacking pressure by both teams (85'+ combined shots)
--         and emit bilateral side-oriented context for late chaos diagnostics.
-- Trigger: combined late shots with effective minute > 85 is at least 10.
WITH late_shot_stats AS (
    SELECT
        se.match_id,
        toInt32(count()) AS match_late_shots_total,
        toInt32(countIf(se.shot_side = 'home')) AS home_late_shots,
        toInt32(countIf(se.shot_side = 'away')) AS away_late_shots,
        toInt32(countIf(se.is_on_target_flag = 1)) AS match_late_shots_on_target_total,
        toInt32(countIf(se.shot_side = 'home' AND se.is_on_target_flag = 1))
            AS home_late_shots_on_target,
        toInt32(countIf(se.shot_side = 'away' AND se.is_on_target_flag = 1))
            AS away_late_shots_on_target
    FROM (
        SELECT
            s.match_id,
            if(
                s.team_id = m.home_team_id,
                'home',
                if(s.team_id = m.away_team_id, 'away', 'unknown')
            ) AS shot_side,
            toUInt8(coalesce(s.is_on_target, 0)) AS is_on_target_flag
        FROM silver.shot AS s
        INNER JOIN silver.match AS m
            ON m.match_id = s.match_id
        WHERE s.match_id > 0
          AND m.match_finished = 1
          AND isNotNull(s.team_id)
          AND toInt32(
                coalesce(s.goal_time, s.minute, 0)
              + coalesce(s.goal_overload_time, s.minute_added, 0)
          ) > 85
    ) AS se
    WHERE se.shot_side IN ('home', 'away')
    GROUP BY se.match_id
    HAVING count() >= 10
),
base_stats AS (
    SELECT
        m.match_id,
        m.match_date,
        m.home_team_id,
        m.home_team_name,
        m.away_team_id,
        m.away_team_name,
        m.home_score,
        m.away_score,
        coalesce(m.home_score, 0) AS home_goals,
        coalesce(m.away_score, 0) AS away_goals,
        lss.match_late_shots_total,
        lss.home_late_shots,
        lss.away_late_shots,
        lss.match_late_shots_on_target_total,
        lss.home_late_shots_on_target,
        lss.away_late_shots_on_target,
        coalesce(ps.total_shots_home, 0) AS total_shots_home,
        coalesce(ps.total_shots_away, 0) AS total_shots_away,
        coalesce(ps.shots_on_target_home, 0) AS shots_on_target_home,
        coalesce(ps.shots_on_target_away, 0) AS shots_on_target_away,
        toFloat32(coalesce(ps.expected_goals_home, 0.0)) AS expected_goals_home,
        toFloat32(coalesce(ps.expected_goals_away, 0.0)) AS expected_goals_away,
        coalesce(ps.big_chances_home, 0) AS big_chances_home,
        coalesce(ps.big_chances_away, 0) AS big_chances_away,
        coalesce(ps.big_chances_missed_home, 0) AS big_chances_missed_home,
        coalesce(ps.big_chances_missed_away, 0) AS big_chances_missed_away,
        coalesce(ps.touches_opp_box_home, 0) AS touches_opposition_box_home,
        coalesce(ps.touches_opp_box_away, 0) AS touches_opposition_box_away,
        toFloat32(coalesce(ps.ball_possession_home, 0.0)) AS possession_home_pct,
        toFloat32(coalesce(ps.ball_possession_away, 0.0)) AS possession_away_pct,
        coalesce(ps.accurate_passes_home, 0) AS accurate_passes_home,
        coalesce(ps.accurate_passes_away, 0) AS accurate_passes_away,
        coalesce(ps.pass_attempts_home, 0) AS pass_attempts_home,
        coalesce(ps.pass_attempts_away, 0) AS pass_attempts_away
    FROM silver.match AS m
    INNER JOIN late_shot_stats AS lss
        ON lss.match_id = m.match_id
    INNER JOIN silver.period_stat AS ps
        ON ps.match_id = m.match_id
       AND ps.match_date = m.match_date
       AND ps.period = 'All'
    WHERE m.match_finished = 1
      AND m.match_id > 0
)

SELECT
    b.match_id,
    b.match_date,
    b.home_team_id,
    b.home_team_name,
    b.away_team_id,
    b.away_team_name,
    b.home_score,
    b.away_score,
    'home' AS triggered_side,
    b.home_team_id AS triggered_team_id,
    b.home_team_name AS triggered_team_name,
    b.away_team_id AS opponent_team_id,
    b.away_team_name AS opponent_team_name,
    toInt32(85) AS trigger_threshold_late_shot_minute_exclusive,
    toInt32(10) AS trigger_threshold_match_total_late_shots_min,
    b.match_late_shots_total,
    b.home_late_shots,
    b.away_late_shots,
    b.match_late_shots_on_target_total,
    toFloat32(coalesce(round(
        100.0 * b.match_late_shots_on_target_total / nullIf(toFloat64(b.match_late_shots_total), 0),
        1
    ), 0.0)) AS match_late_shot_accuracy_pct,
    b.home_late_shots AS triggered_team_late_shots,
    b.away_late_shots AS opponent_late_shots,
    b.home_late_shots - b.away_late_shots AS late_shot_volume_delta,
    b.home_late_shots_on_target AS triggered_team_late_shots_on_target,
    b.away_late_shots_on_target AS opponent_late_shots_on_target,
    b.home_late_shots_on_target - b.away_late_shots_on_target AS late_shot_on_target_delta,
    toFloat32(coalesce(round(
        100.0 * b.home_late_shots_on_target / nullIf(toFloat64(b.home_late_shots), 0),
        1
    ), 0.0)) AS triggered_team_late_shot_accuracy_pct,
    toFloat32(coalesce(round(
        100.0 * b.away_late_shots_on_target / nullIf(toFloat64(b.away_late_shots), 0),
        1
    ), 0.0)) AS opponent_late_shot_accuracy_pct,
    toFloat32(round(
        coalesce(round(100.0 * b.home_late_shots_on_target / nullIf(toFloat64(b.home_late_shots), 0), 1), 0.0)
        - coalesce(round(100.0 * b.away_late_shots_on_target / nullIf(toFloat64(b.away_late_shots), 0), 1), 0.0),
        1
    )) AS late_shot_accuracy_delta_pct,
    b.total_shots_home AS triggered_team_total_shots,
    b.total_shots_away AS opponent_total_shots,
    b.total_shots_home - b.total_shots_away AS shot_volume_delta,
    b.shots_on_target_home AS triggered_team_shots_on_target,
    b.shots_on_target_away AS opponent_shots_on_target,
    b.shots_on_target_home - b.shots_on_target_away AS shot_on_target_delta,
    b.expected_goals_home AS triggered_team_xg,
    b.expected_goals_away AS opponent_xg,
    toFloat32(round(b.expected_goals_home - b.expected_goals_away, 3)) AS xg_delta,
    b.home_goals AS triggered_team_goals,
    b.away_goals AS opponent_goals,
    b.home_goals - b.away_goals AS goal_gap,
    toFloat32(coalesce(round(
        100.0 * b.home_goals / nullIf(toFloat64(b.total_shots_home), 0),
        1
    ), 0.0)) AS triggered_team_shot_conversion_pct,
    toFloat32(coalesce(round(
        100.0 * b.away_goals / nullIf(toFloat64(b.total_shots_away), 0),
        1
    ), 0.0)) AS opponent_shot_conversion_pct,
    toFloat32(round(
        coalesce(round(100.0 * b.home_goals / nullIf(toFloat64(b.total_shots_home), 0), 1), 0.0)
        - coalesce(round(100.0 * b.away_goals / nullIf(toFloat64(b.total_shots_away), 0), 1), 0.0),
        1
    )) AS shot_conversion_delta_pct,
    b.big_chances_home AS triggered_team_big_chances,
    b.big_chances_away AS opponent_big_chances,
    b.big_chances_home - b.big_chances_away AS big_chance_delta,
    b.big_chances_missed_home AS triggered_team_big_chances_missed,
    b.big_chances_missed_away AS opponent_big_chances_missed,
    b.big_chances_missed_home - b.big_chances_missed_away AS big_chances_missed_delta,
    b.touches_opposition_box_home AS triggered_team_touches_opposition_box,
    b.touches_opposition_box_away AS opponent_touches_opposition_box,
    b.touches_opposition_box_home - b.touches_opposition_box_away AS opposition_box_touch_delta,
    b.possession_home_pct AS triggered_team_possession_pct,
    b.possession_away_pct AS opponent_possession_pct,
    toFloat32(round(b.possession_home_pct - b.possession_away_pct, 1)) AS possession_delta_pct,
    toFloat32(coalesce(round(
        100.0 * b.accurate_passes_home / nullIf(toFloat64(b.pass_attempts_home), 0),
        1
    ), 0.0)) AS triggered_team_pass_accuracy_pct,
    toFloat32(coalesce(round(
        100.0 * b.accurate_passes_away / nullIf(toFloat64(b.pass_attempts_away), 0),
        1
    ), 0.0)) AS opponent_pass_accuracy_pct,
    toFloat32(round(
        coalesce(round(100.0 * b.accurate_passes_home / nullIf(toFloat64(b.pass_attempts_home), 0), 1), 0.0)
        - coalesce(round(100.0 * b.accurate_passes_away / nullIf(toFloat64(b.pass_attempts_away), 0), 1), 0.0),
        1
    )) AS pass_accuracy_delta_pct
FROM base_stats AS b

UNION ALL

SELECT
    b.match_id,
    b.match_date,
    b.home_team_id,
    b.home_team_name,
    b.away_team_id,
    b.away_team_name,
    b.home_score,
    b.away_score,
    'away' AS triggered_side,
    b.away_team_id AS triggered_team_id,
    b.away_team_name AS triggered_team_name,
    b.home_team_id AS opponent_team_id,
    b.home_team_name AS opponent_team_name,
    toInt32(85) AS trigger_threshold_late_shot_minute_exclusive,
    toInt32(10) AS trigger_threshold_match_total_late_shots_min,
    b.match_late_shots_total,
    b.home_late_shots,
    b.away_late_shots,
    b.match_late_shots_on_target_total,
    toFloat32(coalesce(round(
        100.0 * b.match_late_shots_on_target_total / nullIf(toFloat64(b.match_late_shots_total), 0),
        1
    ), 0.0)) AS match_late_shot_accuracy_pct,
    b.away_late_shots AS triggered_team_late_shots,
    b.home_late_shots AS opponent_late_shots,
    b.away_late_shots - b.home_late_shots AS late_shot_volume_delta,
    b.away_late_shots_on_target AS triggered_team_late_shots_on_target,
    b.home_late_shots_on_target AS opponent_late_shots_on_target,
    b.away_late_shots_on_target - b.home_late_shots_on_target AS late_shot_on_target_delta,
    toFloat32(coalesce(round(
        100.0 * b.away_late_shots_on_target / nullIf(toFloat64(b.away_late_shots), 0),
        1
    ), 0.0)) AS triggered_team_late_shot_accuracy_pct,
    toFloat32(coalesce(round(
        100.0 * b.home_late_shots_on_target / nullIf(toFloat64(b.home_late_shots), 0),
        1
    ), 0.0)) AS opponent_late_shot_accuracy_pct,
    toFloat32(round(
        coalesce(round(100.0 * b.away_late_shots_on_target / nullIf(toFloat64(b.away_late_shots), 0), 1), 0.0)
        - coalesce(round(100.0 * b.home_late_shots_on_target / nullIf(toFloat64(b.home_late_shots), 0), 1), 0.0),
        1
    )) AS late_shot_accuracy_delta_pct,
    b.total_shots_away AS triggered_team_total_shots,
    b.total_shots_home AS opponent_total_shots,
    b.total_shots_away - b.total_shots_home AS shot_volume_delta,
    b.shots_on_target_away AS triggered_team_shots_on_target,
    b.shots_on_target_home AS opponent_shots_on_target,
    b.shots_on_target_away - b.shots_on_target_home AS shot_on_target_delta,
    b.expected_goals_away AS triggered_team_xg,
    b.expected_goals_home AS opponent_xg,
    toFloat32(round(b.expected_goals_away - b.expected_goals_home, 3)) AS xg_delta,
    b.away_goals AS triggered_team_goals,
    b.home_goals AS opponent_goals,
    b.away_goals - b.home_goals AS goal_gap,
    toFloat32(coalesce(round(
        100.0 * b.away_goals / nullIf(toFloat64(b.total_shots_away), 0),
        1
    ), 0.0)) AS triggered_team_shot_conversion_pct,
    toFloat32(coalesce(round(
        100.0 * b.home_goals / nullIf(toFloat64(b.total_shots_home), 0),
        1
    ), 0.0)) AS opponent_shot_conversion_pct,
    toFloat32(round(
        coalesce(round(100.0 * b.away_goals / nullIf(toFloat64(b.total_shots_away), 0), 1), 0.0)
        - coalesce(round(100.0 * b.home_goals / nullIf(toFloat64(b.total_shots_home), 0), 1), 0.0),
        1
    )) AS shot_conversion_delta_pct,
    b.big_chances_away AS triggered_team_big_chances,
    b.big_chances_home AS opponent_big_chances,
    b.big_chances_away - b.big_chances_home AS big_chance_delta,
    b.big_chances_missed_away AS triggered_team_big_chances_missed,
    b.big_chances_missed_home AS opponent_big_chances_missed,
    b.big_chances_missed_away - b.big_chances_missed_home AS big_chances_missed_delta,
    b.touches_opposition_box_away AS triggered_team_touches_opposition_box,
    b.touches_opposition_box_home AS opponent_touches_opposition_box,
    b.touches_opposition_box_away - b.touches_opposition_box_home AS opposition_box_touch_delta,
    b.possession_away_pct AS triggered_team_possession_pct,
    b.possession_home_pct AS opponent_possession_pct,
    toFloat32(round(b.possession_away_pct - b.possession_home_pct, 1)) AS possession_delta_pct,
    toFloat32(coalesce(round(
        100.0 * b.accurate_passes_away / nullIf(toFloat64(b.pass_attempts_away), 0),
        1
    ), 0.0)) AS triggered_team_pass_accuracy_pct,
    toFloat32(coalesce(round(
        100.0 * b.accurate_passes_home / nullIf(toFloat64(b.pass_attempts_home), 0),
        1
    ), 0.0)) AS opponent_pass_accuracy_pct,
    toFloat32(round(
        coalesce(round(100.0 * b.accurate_passes_away / nullIf(toFloat64(b.pass_attempts_away), 0), 1), 0.0)
        - coalesce(round(100.0 * b.accurate_passes_home / nullIf(toFloat64(b.pass_attempts_home), 0), 1), 0.0),
        1
    )) AS pass_accuracy_delta_pct
FROM base_stats AS b;
