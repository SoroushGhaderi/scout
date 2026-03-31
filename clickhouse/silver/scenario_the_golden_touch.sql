-- scenario_the_golden_touch: low-touch substitute cameos with direct scoreline impact
INSERT INTO fotmob.silver_scenario_the_golden_touch
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

    -- 2. Golden Touch Metrics
    player_name,
    player_team,
    goals,
    assists,
    touches,
    minutes_played,
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

    -- 2. Golden Touch Metrics
    p.player_name,
    p.team_name AS player_team,
    p.goals,
    p.assists,
    p.touches,
    p.minutes_played,
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
    -- Outfield cameo profile only.
    p.is_goalkeeper = 0
    AND p.minutes_played <= 25
    AND p.minutes_played > 0

    -- Clinical cameo logic: direct output with extremely low touch volume.
    AND (p.goals >= 1 OR p.assists >= 1)
    AND p.touches <= 12
ORDER BY p.touches ASC, (p.goals + p.assists) DESC;
