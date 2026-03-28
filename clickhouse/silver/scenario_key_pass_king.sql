-- scenario_key_pass_king: elite chance creators in finished matches
INSERT INTO fotmob.silver_scenario_key_pass_king
(
    match_id,
    player_id,
    player_name,
    team_id,
    team_name,
    chances_created,
    xa,
    assists,
    goals,
    minutes_played,
    fotmob_rating,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    team_side,
    match_result,
    match_time_utc_date
)
SELECT
    p.match_id,
    p.player_id,
    p.player_name,
    p.team_id,
    p.team_name,
    p.chances_created,
    round(p.expected_assists, 3) AS xa,
    p.assists,
    p.goals,
    p.minutes_played,
    p.fotmob_rating,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    CASE
        WHEN p.team_id = g.home_team_id THEN 'home'
        WHEN p.team_id = g.away_team_id THEN 'away'
    END AS team_side,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
        ELSE 'draw'
    END AS match_result,
    g.match_time_utc_date
FROM fotmob.bronze_player AS p
INNER JOIN fotmob.bronze_general AS g
    ON p.match_id = g.match_id
WHERE
    g.match_finished = 1
    AND p.chances_created >= 3
    AND p.expected_assists > 0.8
ORDER BY p.expected_assists DESC, p.chances_created DESC;
