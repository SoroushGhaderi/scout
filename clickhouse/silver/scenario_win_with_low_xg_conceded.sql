-- scenario_clean_sheet_dominance: winning side concedes very low xG (<0.3)
INSERT INTO fotmob.silver_scenario_win_with_low_xg_conceded
(
    match_id,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score,
    away_score,
    expected_goals_home,
    expected_goals_away,
    winning_team,
    winning_side,
    xg_conceded,
    match_time_utc_date
)
SELECT
    g.match_id,
    g.home_team_id,
    g.home_team_name,
    g.away_team_id,
    g.away_team_name,
    g.home_score,
    g.away_score,
    p.expected_goals_home,
    p.expected_goals_away,
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
    END AS winning_team,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
    END AS winning_side,
    CASE
        WHEN g.home_score > g.away_score THEN p.expected_goals_away
        WHEN g.away_score > g.home_score THEN p.expected_goals_home
    END AS xg_conceded,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_period AS p
    ON g.match_id = p.match_id
    AND p.period = 'All'
WHERE
    g.match_finished = 1
    AND g.home_score != g.away_score
    AND (
        (g.home_score > g.away_score AND p.expected_goals_away < 0.3)
        OR
        (g.away_score > g.home_score AND p.expected_goals_home < 0.3)
    );
