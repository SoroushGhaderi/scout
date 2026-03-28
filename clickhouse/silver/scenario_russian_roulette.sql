-- scenario_russian_roulette: matches with heavy penalty-event volatility
INSERT INTO fotmob.silver_scenario_russian_roulette
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    match_duration,
    total_penalties,
    home_penalties,
    away_penalties,
    penalties_scored,
    penalties_missed,
    home_penalties_scored,
    away_penalties_scored,
    home_penalties_missed,
    away_penalties_missed,
    home_penalty_xgot,
    away_penalty_xgot,
    combined_penalty_xgot,
    winning_team,
    match_result,
    match_time_utc_date
)
SELECT
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    CASE
        WHEN notEmpty(assumeNotNull(t.first_extra_half_started)) THEN 120
        ELSE 90
    END AS match_duration,
    countIf(s.situation = 'Penalty') AS total_penalties,
    countIf(s.situation = 'Penalty' AND s.team_id = g.home_team_id) AS home_penalties,
    countIf(s.situation = 'Penalty' AND s.team_id = g.away_team_id) AS away_penalties,
    countIf(s.situation = 'Penalty' AND s.event_type = 'Goal') AS penalties_scored,
    countIf(s.situation = 'Penalty' AND s.event_type != 'Goal') AS penalties_missed,
    countIf(s.situation = 'Penalty' AND s.event_type = 'Goal' AND s.team_id = g.home_team_id) AS home_penalties_scored,
    countIf(s.situation = 'Penalty' AND s.event_type = 'Goal' AND s.team_id = g.away_team_id) AS away_penalties_scored,
    countIf(s.situation = 'Penalty' AND s.event_type != 'Goal' AND s.team_id = g.home_team_id) AS home_penalties_missed,
    countIf(s.situation = 'Penalty' AND s.event_type != 'Goal' AND s.team_id = g.away_team_id) AS away_penalties_missed,
    round(sumIf(s.expected_goals_on_target, s.situation = 'Penalty' AND s.team_id = g.home_team_id), 3) AS home_penalty_xgot,
    round(sumIf(s.expected_goals_on_target, s.situation = 'Penalty' AND s.team_id = g.away_team_id), 3) AS away_penalty_xgot,
    round(sumIf(s.expected_goals_on_target, s.situation = 'Penalty'), 3) AS combined_penalty_xgot,
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
        ELSE 'draw'
    END AS winning_team,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
        ELSE 'draw'
    END AS match_result,
    g.match_time_utc_date
FROM fotmob.bronze_shotmap AS s
INNER JOIN fotmob.bronze_general AS g
    ON s.match_id = g.match_id
LEFT JOIN fotmob.bronze_timeline AS t
    ON g.match_id = t.match_id
WHERE
    g.match_finished = 1
    AND s.situation = 'Penalty'
    AND s.is_own_goal != 1
GROUP BY
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    t.first_extra_half_started,
    g.match_time_utc_date
HAVING
    total_penalties >= 2
ORDER BY total_penalties DESC, penalties_missed DESC;
