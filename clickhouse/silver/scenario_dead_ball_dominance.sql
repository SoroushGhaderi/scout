-- scenario_dead_ball_dominance: winner scores at least two set-piece goals
INSERT INTO fotmob.silver_scenario_dead_ball_dominance
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    home_set_piece_goals,
    away_set_piece_goals,
    winning_team,
    winning_side,
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
    countIf(gl.is_home = 1 AND gl.shot_situation IN ('SetPiece', 'FromCorner', 'FreeKick')) AS home_set_piece_goals,
    countIf(gl.is_home = 0 AND gl.shot_situation IN ('SetPiece', 'FromCorner', 'FreeKick')) AS away_set_piece_goals,
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
INNER JOIN fotmob.bronze_goal AS gl
    ON g.match_id = gl.match_id
WHERE
    g.match_finished = 1
    AND g.home_score != g.away_score
GROUP BY
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    g.match_time_utc_date
HAVING
    (g.home_score > g.away_score AND home_set_piece_goals >= 2)
    OR
    (g.away_score > g.home_score AND away_set_piece_goals >= 2)
ORDER BY
    greatest(home_set_piece_goals, away_set_piece_goals) DESC;
