-- scenario_the_flying_wingback: wide-progressor profile with elite dribbling and box activity
INSERT INTO fotmob.silver_scenario_the_flying_wingback
(
    -- 1. Match Identity & Result Context
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    match_time_utc_date,

    -- 2. Specialist Metrics
    player_name,
    player_team,
    successful_dribbles,
    dribble_attempts,
    dribble_success_rate,
    touches_opp_box,
    fotmob_rating,

    -- 3. Outcome Labeling
    winning_team,
    match_result,
    winning_side
)
SELECT
    -- 1. Match Identity & Result Context
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    g.match_time_utc_date,

    -- 2. Specialist Metrics
    p.player_name,
    p.team_name AS player_team,
    p.successful_dribbles,
    p.dribble_attempts,
    p.dribble_success_rate,
    p.touches_opp_box,
    p.fotmob_rating,

    -- 3. Outcome Labeling
    multiIf(
        g.home_score > g.away_score, g.home_team_name,
        g.away_score > g.home_score, g.away_team_name,
        'Draw'
    ) AS winning_team,
    CAST(
        multiIf(
            g.home_score > g.away_score, 'Home Win',
            g.away_score > g.home_score, 'Away Win',
            'Draw'
        ),
        'LowCardinality(String)'
    ) AS match_result,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
        ELSE 'draw'
    END AS winning_side
FROM fotmob.bronze_player AS p
INNER JOIN fotmob.bronze_general AS g
    ON p.match_id = g.match_id
WHERE
    -- Outfield players with enough minutes to influence both phases.
    p.is_goalkeeper = 0
    AND p.minutes_played >= 45
    -- Flying wingback profile: strong dribble volume, efficiency, and final-third penetration.
    AND p.successful_dribbles >= 5
    AND p.dribble_success_rate >= 60.0
    AND p.touches_opp_box >= 4
ORDER BY p.successful_dribbles DESC, p.touches_opp_box DESC;
