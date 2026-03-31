-- scenario_chaos_engine: high-intensity disruptors combining defensive chaos with chance generation
INSERT INTO fotmob.silver_scenario_chaos_engine
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

    -- 2. Disruptor Metrics
    player_name,
    player_team,
    defensive_actions,
    fouls_committed,
    chances_created,
    touches_opp_box,
    fotmob_rating,

    -- 3. Outcome Labeling
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

    -- 2. Disruptor Metrics
    p.player_name,
    p.team_name AS player_team,
    (p.tackles_won + p.interceptions) AS defensive_actions,
    p.fouls_committed,
    p.chances_created,
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
    -- Outfield players with substantial minutes.
    p.is_goalkeeper = 0
    AND p.minutes_played >= 60

    -- Disruptor profile: high defensive actions + tactical fouling.
    AND (p.tackles_won + p.interceptions) >= 5
    AND p.fouls_committed >= 3

    -- Productive disruption and attacking-zone engagement.
    AND p.chances_created >= 1
    AND p.touches_opp_box >= 2
ORDER BY (p.tackles_won + p.interceptions) DESC, p.fouls_committed DESC;
