-- scenario_the_human_shield: outfield defenders absorbing heavy pressure with elite shot blocking
INSERT INTO fotmob.silver_scenario_the_human_shield
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

    -- 2. Human Shield Metrics
    player_name,
    player_team,
    blocked_shots,
    clearances,
    interceptions,
    fotmob_rating,

    -- 3. Match Context
    shots_faced,

    -- 4. Outcome Labeling
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

    -- 2. Human Shield Metrics
    p.player_name,
    p.team_name AS player_team,
    p.blocked_shots,
    p.clearances,
    p.interceptions,
    p.fotmob_rating,

    -- 3. Match Context
    multiIf(p.team_id = g.home_team_id, per.total_shots_away, per.total_shots_home) AS shots_faced,

    -- 4. Outcome Labeling
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
INNER JOIN fotmob.bronze_period AS per
    ON p.match_id = per.match_id
WHERE
    -- Outfield players only with full-match team context.
    p.is_goalkeeper = 0
    AND per.period = 'All'

    -- Human-shield profile: elite block volume and sustained defensive repulsion.
    AND p.blocked_shots >= 4
    AND p.clearances >= 5

    -- Contextual trigger: player's team was under heavy shot pressure.
    AND multiIf(p.team_id = g.home_team_id, per.total_shots_away, per.total_shots_home) >= 15
ORDER BY p.blocked_shots DESC, p.clearances DESC;
