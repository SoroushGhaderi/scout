-- scenario_great_escape: winner was losing at minute 60 and still won
INSERT INTO fotmob.silver_scenario_great_escape
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    goal_diff,
    home_score_at_60,
    away_score_at_60,
    winning_team,
    winning_side,
    match_time_utc_date
)
WITH goals_at_60 AS (
    SELECT
        match_id,
        sumIf(is_home = 1, goal_time <= 60) AS home_score_at_60,
        sumIf(is_home = 0, goal_time <= 60) AS away_score_at_60
    FROM fotmob.bronze_goal
    GROUP BY match_id
)
SELECT
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    abs(g.home_score - g.away_score) AS goal_diff,
    s.home_score_at_60,
    s.away_score_at_60,
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
    END AS winning_team,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
    END AS winning_side,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN goals_at_60 AS s
    ON g.match_id = s.match_id
WHERE
    g.match_finished = 1
    AND g.home_score != g.away_score
    AND (
        (g.home_score > g.away_score AND s.home_score_at_60 < s.away_score_at_60)
        OR
        (g.away_score > g.home_score AND s.away_score_at_60 < s.home_score_at_60)
    )
ORDER BY goal_diff DESC;
