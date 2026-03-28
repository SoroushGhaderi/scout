-- scenario_tactical_stalemate: finished matches with very low combined xG
INSERT INTO fotmob.silver_scenario_tactical_stalemate
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    xg_home,
    xg_away,
    combined_xg,
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
    p.expected_goals_home AS xg_home,
    p.expected_goals_away AS xg_away,
    p.expected_goals_home + p.expected_goals_away AS combined_xg,
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
        ELSE NULL
    END AS winning_team,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
        ELSE 'draw'
    END AS match_result,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_period AS p
    ON g.match_id = p.match_id
    AND p.period = 'All'
WHERE
    g.match_finished = 1
    AND (p.expected_goals_home + p.expected_goals_away) < 1.0
ORDER BY combined_xg ASC;
