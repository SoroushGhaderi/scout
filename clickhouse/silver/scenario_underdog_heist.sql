-- scenario_underdog_heist: low-xG winner pulls off a win
INSERT INTO fotmob.silver_scenario_underdog_heist
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    goal_diff,
    xg_home,
    xg_away,
    xg_diff,
    winning_team,
    winning_side,
    winner_xg,
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
    p.expected_goals_home AS xg_home,
    p.expected_goals_away AS xg_away,
    abs(p.expected_goals_home - p.expected_goals_away) AS xg_diff,
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
    END AS winning_team,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
    END AS winning_side,
    CASE
        WHEN g.home_score > g.away_score THEN p.expected_goals_home
        WHEN g.away_score > g.home_score THEN p.expected_goals_away
    END AS winner_xg,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_period AS p
    ON g.match_id = p.match_id
    AND p.period = 'All'
WHERE
    g.match_finished = 1
    AND g.home_score != g.away_score
    AND (
        (g.home_score > g.away_score AND p.expected_goals_home < p.expected_goals_away AND p.expected_goals_home < 1.0)
        OR
        (g.away_score > g.home_score AND p.expected_goals_away < p.expected_goals_home AND p.expected_goals_away < 1.0)
    )
ORDER BY winner_xg ASC;
