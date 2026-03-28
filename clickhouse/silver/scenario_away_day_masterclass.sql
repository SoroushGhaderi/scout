-- scenario_away_day_masterclass: away wins with strong possession and xG edge
INSERT INTO fotmob.silver_scenario_away_day_masterclass
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
    xg_home,
    xg_away,
    xg_diff,
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
    p.expected_goals_home AS xg_home,
    p.expected_goals_away AS xg_away,
    round(p.expected_goals_away - p.expected_goals_home, 3) AS xg_diff,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_period AS p
    ON g.match_id = p.match_id
    AND p.period = 'All'
WHERE
    g.match_finished = 1
    AND g.away_score > g.home_score
    AND p.ball_possession_away > 65
    AND p.expected_goals_away > p.expected_goals_home
ORDER BY p.ball_possession_away DESC, xg_diff DESC;
