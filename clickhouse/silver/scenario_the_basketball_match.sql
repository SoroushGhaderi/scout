-- scenario_the_basketball_match: end-to-end high-chaos matches with extreme chance and shot volume
INSERT INTO fotmob.silver_scenario_the_basketball_match
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

    -- 2. Chaotic Volume Metrics
    expected_goals_home,
    expected_goals_away,
    combined_xg,
    total_shots_home,
    total_shots_away,
    combined_shots,
    home_xg,
    away_xg,

    -- 3. Outcome Labeling
    winning_team,
    match_result,
    winning_side
)
SELECT
    -- 1. Match Identity
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    g.match_time_utc_date,

    -- 2. Chaotic Volume Metrics
    p.expected_goals_home,
    p.expected_goals_away,
    (p.expected_goals_home + p.expected_goals_away) AS combined_xg,
    p.total_shots_home,
    p.total_shots_away,
    (p.total_shots_home + p.total_shots_away) AS combined_shots,
    p.expected_goals_home AS home_xg,
    p.expected_goals_away AS away_xg,

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
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_period AS p
    ON g.match_id = p.match_id
WHERE
    -- Full-match aggregates only.
    p.period = 'All'

    -- Basketball-match logic: both sides create huge volume and quality.
    AND (p.expected_goals_home + p.expected_goals_away) > 4.5
    AND (p.total_shots_home + p.total_shots_away) > 35
    AND p.expected_goals_home > 1.5
    AND p.expected_goals_away > 1.5
ORDER BY combined_xg DESC, combined_shots DESC;
