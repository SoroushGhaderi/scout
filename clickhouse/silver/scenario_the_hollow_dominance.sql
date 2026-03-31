-- scenario_the_hollow_dominance: high-volume attacking siege that fails to convert into a win
INSERT INTO fotmob.silver_scenario_the_hollow_dominance
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

    -- 2. Siege Metrics
    expected_goals_home,
    expected_goals_away,
    total_shots_home,
    total_shots_away,
    big_chances_home,
    big_chances_away,
    ball_possession_home,
    ball_possession_away,

    -- 3. Match Result Logic
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

    -- 2. Siege Metrics
    p.expected_goals_home,
    p.expected_goals_away,
    p.total_shots_home,
    p.total_shots_away,
    p.big_chances_home,
    p.big_chances_away,
    p.ball_possession_home,
    p.ball_possession_away,

    -- 3. Match Result Logic
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
    AND (
        -- Case A: Home team dominates chance volume but fails to win.
        (
            p.total_shots_home >= 20
            AND p.expected_goals_home >= p.expected_goals_away
            AND g.home_score <= 1
            AND g.home_score <= g.away_score
        )
        OR
        -- Case B: Away team dominates chance volume but fails to win.
        (
            p.total_shots_away >= 20
            AND p.expected_goals_away >= p.expected_goals_home
            AND g.away_score <= 1
            AND g.away_score <= g.home_score
        )
    )
ORDER BY (p.expected_goals_home + p.expected_goals_away) DESC;
