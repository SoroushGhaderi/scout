-- scenario_demolition: finished matches with a 3+ goal winning margin
INSERT INTO fotmob.silver_scenario_demolition
(
    match_id,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score,
    away_score,
    goal_diff,
    winning_side,
    match_time_utc_date
)
SELECT
    match_id,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score,
    away_score,
    abs(home_score - away_score) AS goal_diff,
    CASE
        WHEN home_score - away_score >= 3 THEN 'home'
        WHEN away_score - home_score >= 3 THEN 'away'
    END AS winning_side,
    match_time_utc_date
FROM fotmob.bronze_general FINAL
WHERE
    match_finished = 1
    AND abs(home_score - away_score) >= 3;
