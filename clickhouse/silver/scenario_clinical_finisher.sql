-- scenario_clinical_finisher: multi-goal output from low shot/xG volume
INSERT INTO fotmob.silver_scenario_clinical_finisher
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    player_id,
    player_name,
    team_id,
    goals,
    total_shots,
    combined_xg,
    team_side,
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
    s.player_id,
    s.player_name,
    s.team_id,
    countIf(s.event_type = 'Goal') AS goals,
    count() AS total_shots,
    round(sum(s.expected_goals), 3) AS combined_xg,
    CASE
        WHEN s.team_id = g.home_team_id THEN 'home'
        WHEN s.team_id = g.away_team_id THEN 'away'
    END AS team_side,
    g.match_time_utc_date
FROM fotmob.bronze_shotmap AS s
INNER JOIN fotmob.bronze_general AS g
    ON s.match_id = g.match_id
WHERE
    g.match_finished = 1
    AND s.is_own_goal != 1
    AND s.expected_goals IS NOT NULL
GROUP BY
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    s.player_id,
    s.player_name,
    s.team_id,
    g.match_time_utc_date
HAVING
    goals >= 2
    AND total_shots <= 3
    AND combined_xg < 1.0
ORDER BY combined_xg ASC;
