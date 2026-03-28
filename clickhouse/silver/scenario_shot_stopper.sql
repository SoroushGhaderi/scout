-- scenario_shot_stopper: goalkeepers with high expected-goals prevented
INSERT INTO fotmob.silver_scenario_shot_stopper
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    keeper_id,
    goalkeeper_name,
    goalkeeper_team_id,
    goalkeeper_team,
    saves,
    xg_saved,
    keeper_side,
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
    s.keeper_id,
    p.player_name AS goalkeeper_name,
    p.team_id AS goalkeeper_team_id,
    p.team_name AS goalkeeper_team,
    count() AS saves,
    round(sum(s.expected_goals_on_target), 3) AS xg_saved,
    CASE
        WHEN p.team_id = g.home_team_id THEN 'home'
        WHEN p.team_id = g.away_team_id THEN 'away'
    END AS keeper_side,
    g.match_time_utc_date
FROM fotmob.bronze_shotmap AS s
INNER JOIN fotmob.bronze_general AS g
    ON s.match_id = g.match_id
INNER JOIN fotmob.bronze_player AS p
    ON s.match_id = p.match_id
    AND s.keeper_id = p.player_id
WHERE
    g.match_finished = 1
    AND s.is_on_target = 1
    AND s.event_type != 'Goal'
    AND s.is_own_goal != 1
    AND s.expected_goals_on_target IS NOT NULL
GROUP BY
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    s.keeper_id,
    p.player_name,
    p.team_id,
    p.team_name,
    g.match_time_utc_date
HAVING xg_saved >= 1.5
ORDER BY xg_saved DESC;
