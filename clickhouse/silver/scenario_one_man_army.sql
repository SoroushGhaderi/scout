-- scenario_one_man_army: high individual output in finished matches
INSERT INTO fotmob.silver_scenario_one_man_army
(
    match_id,
    player_id,
    player_name,
    team_id,
    team_name,
    goals,
    assists,
    goal_contributions,
    xg,
    xa,
    xg_xa,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    team_side,
    match_time_utc_date
)
SELECT
    p.match_id,
    p.player_id,
    p.player_name,
    p.team_id,
    p.team_name,
    p.goals,
    p.assists,
    p.goals + p.assists AS goal_contributions,
    p.expected_goals AS xg,
    p.expected_assists AS xa,
    p.expected_goals + p.expected_assists AS xg_xa,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    CASE
        WHEN p.team_id = g.home_team_id THEN 'home'
        WHEN p.team_id = g.away_team_id THEN 'away'
    END AS team_side,
    g.match_time_utc_date
FROM fotmob.bronze_player AS p
INNER JOIN fotmob.bronze_general AS g
    ON p.match_id = g.match_id
WHERE
    g.match_finished = 1
    AND (
        p.goals >= 2
        OR p.assists >= 2
    )
ORDER BY goal_contributions DESC, p.goals DESC;
