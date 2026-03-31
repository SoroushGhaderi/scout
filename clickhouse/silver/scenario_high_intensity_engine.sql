-- scenario_high_intensity_engine: relentless outfield ball-winners with strong anticipation
INSERT INTO fotmob.silver_scenario_high_intensity_engine
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    match_time_utc_date,
    player_name,
    player_team,
    recoveries,
    interceptions,
    defensive_actions,
    minutes_played,
    fotmob_rating,
    winning_team,
    match_result,
    winning_side
)
SELECT
    p.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    g.match_time_utc_date,
    p.player_name,
    p.team_name AS player_team,
    p.recoveries,
    p.interceptions,
    p.defensive_actions,
    p.minutes_played,
    p.fotmob_rating,
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
WHERE
    g.match_finished = 1
    AND p.is_goalkeeper = 0
    AND p.recoveries >= 12
    AND p.interceptions >= 4
    AND p.minutes_played >= 45
ORDER BY p.recoveries DESC, p.interceptions DESC;
