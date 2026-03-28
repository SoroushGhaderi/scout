-- scenario_low_block_heist: low-possession winner takes the match
INSERT INTO fotmob.silver_scenario_low_block_heist
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    goal_diff,
    possession_home,
    possession_away,
    winning_team,
    winning_side,
    winner_possession,
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
    abs(g.home_score - g.away_score) AS goal_diff,
    p.ball_possession_home AS possession_home,
    p.ball_possession_away AS possession_away,
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
    END AS winning_team,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
    END AS winning_side,
    CASE
        WHEN g.home_score > g.away_score THEN p.ball_possession_home
        WHEN g.away_score > g.home_score THEN p.ball_possession_away
    END AS winner_possession,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_period AS p
    ON g.match_id = p.match_id
    AND p.period = 'All'
WHERE
    g.match_finished = 1
    AND g.home_score != g.away_score
    AND (
        (g.home_score > g.away_score AND p.ball_possession_home < 35)
        OR
        (g.away_score > g.home_score AND p.ball_possession_away < 35)
    )
ORDER BY winner_possession ASC;
