-- scenario_against_the_grain: elite pass security under low-possession match context
INSERT INTO fotmob.silver_scenario_against_the_grain
(
    -- 1. Match Identity
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    match_time_utc_date,

    -- 2. Press-Resistance Metrics
    player_name,
    player_team,
    accurate_passes,
    pass_accuracy,
    team_possession,
    fotmob_rating,

    -- 3. Match Result Logic
    winning_team,
    match_result,
    winning_side
)
SELECT
    -- 1. Match Identity
    p.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    g.match_time_utc_date,

    -- 2. Press-Resistance Metrics
    p.player_name,
    p.team_name AS player_team,
    p.accurate_passes,
    p.pass_accuracy,
    multiIf(p.team_id = g.home_team_id, per.ball_possession_home, per.ball_possession_away) AS team_possession,
    p.fotmob_rating,

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
    END AS winning_side
FROM fotmob.bronze_player AS p
INNER JOIN fotmob.bronze_general AS g
    ON p.match_id = g.match_id
INNER JOIN fotmob.bronze_period AS per
    ON p.match_id = per.match_id
WHERE
    -- Filter for full-match context and outfield players only.
    per.period = 'All'
    AND p.is_goalkeeper = 0

    -- Player performance: strong volume plus elite passing precision.
    AND p.accurate_passes >= 50
    AND p.pass_accuracy >= 95.0

    -- Pressure context: player team must operate below 45% possession.
    AND (
        (p.team_id = g.home_team_id AND per.ball_possession_home < 45)
        OR
        (p.team_id = g.away_team_id AND per.ball_possession_away < 45)
    )
ORDER BY p.pass_accuracy DESC, p.accurate_passes DESC;
