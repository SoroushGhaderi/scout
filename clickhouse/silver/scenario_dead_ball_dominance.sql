-- scenario_dead_ball_dominance: winner scores at least two set-piece goals
INSERT INTO fotmob.silver_scenario_dead_ball_dominance
(
    -- 1. Match Identity
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    -- 2. Set-Piece Metrics
    home_set_piece_goals,
    away_set_piece_goals,
    -- 3. Match Result Logic
    winning_team,
    match_result,
    winning_side,
    match_time_utc_date
)
SELECT
    -- 1. Match Identity
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    -- 2. Set-Piece Metrics
    countIf(gl.is_home = 1 AND gl.shot_situation IN ('SetPiece', 'FromCorner', 'FreeKick')) AS home_set_piece_goals,
    countIf(gl.is_home = 0 AND gl.shot_situation IN ('SetPiece', 'FromCorner', 'FreeKick')) AS away_set_piece_goals,
    -- 3. Match Result Logic
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
        ELSE 'Draw'
    END AS winning_team,
    CAST(CASE
        WHEN g.home_score > g.away_score THEN 'Home Win'
        WHEN g.away_score > g.home_score THEN 'Away Win'
        ELSE 'Draw'
    END AS LowCardinality(String)) AS match_result,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
        ELSE 'draw'
    END AS winning_side,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_goal AS gl
    ON g.match_id = gl.match_id
WHERE
    -- Finished non-draw matches only.
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
    -- Winning side must score at least two set-piece goals.
    (g.home_score > g.away_score AND home_set_piece_goals >= 2)
    OR
    (g.away_score > g.home_score AND away_set_piece_goals >= 2)
ORDER BY
    greatest(home_set_piece_goals, away_set_piece_goals) DESC;
