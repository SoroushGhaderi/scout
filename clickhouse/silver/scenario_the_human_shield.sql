-- scenario_the_human_shield: outfield defenders absorbing heavy fire via blocks, clearances, and defensive volume
INSERT INTO fotmob.silver_scenario_the_human_shield
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    player_id,
    player_name,
    team_id,
    team_name,
    shots_blocked,
    clearances,
    interceptions,
    tackles_won,
    duels_won,
    duels_lost,
    aerial_duels_won,
    shield_score,
    shots_faced,
    block_share_pct,
    fotmob_rating,
    minutes_played,
    fouls_committed,
    team_side,
    xg_faced,
    match_result,
    match_time_utc_date
)
WITH team_shots_faced AS (
    SELECT
        match_id,
        team_id,
        count()                                             AS shots_faced
    FROM fotmob.bronze_shotmap
    WHERE
        is_own_goal != 1
    GROUP BY match_id, team_id
)

SELECT
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    p.player_id,
    p.player_name,
    p.team_id,
    p.team_name,
    p.blocked_shots                                         AS shots_blocked,
    p.clearances,
    p.interceptions,
    p.tackles_won,
    p.duels_won,
    p.duels_lost,
    p.aerial_duels_won,
    round(
          (coalesce(p.blocked_shots, 0) * 3.0)
        + (coalesce(p.clearances, 0)   * 1.5)
        + (coalesce(p.interceptions, 0) * 1.2)
        + (coalesce(p.tackles_won, 0)  * 1.0)
    , 2)                                                    AS shield_score,
    tsf.shots_faced,
    round(p.blocked_shots
        / nullIf(tsf.shots_faced, 0) * 100, 1)             AS block_share_pct,
    p.fotmob_rating,
    p.minutes_played,
    p.fouls_committed,

    CASE
        WHEN p.team_id = g.home_team_id THEN 'home'
        WHEN p.team_id = g.away_team_id THEN 'away'
    END AS team_side,

    CASE
        WHEN p.team_id = g.home_team_id THEN p_period.expected_goals_away
        WHEN p.team_id = g.away_team_id THEN p_period.expected_goals_home
    END AS xg_faced,

    CASE
        WHEN g.home_score > g.away_score THEN 'home_win'
        WHEN g.away_score > g.home_score THEN 'away_win'
        ELSE 'draw'
    END AS match_result,
    g.match_time_utc_date

FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_player AS p
    ON g.match_id = p.match_id
INNER JOIN team_shots_faced AS tsf
    ON g.match_id = tsf.match_id
    AND tsf.team_id != p.team_id
INNER JOIN fotmob.bronze_period AS p_period
    ON g.match_id = p_period.match_id
    AND p_period.period = 'All'
WHERE
    g.match_finished = 1
    AND p.is_goalkeeper = 0
    AND coalesce(p.blocked_shots, 0) >= 4
    AND coalesce(p.clearances, 0) >= 5
    AND tsf.shots_faced >= 15
    AND p.minutes_played >= 60
ORDER BY shield_score DESC, p.blocked_shots DESC;
