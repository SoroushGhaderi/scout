-- signal_team_possession_passing_total_dominance
-- Triggers when either team completes more than 700 accurate passes in a finished match.
INSERT INTO gold.signal_team_possession_passing_total_dominance
(
    match_id,
    match_time_utc_date,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_score,
    away_score,
    triggered_side,
    triggered_team_id,
    triggered_team_name,
    accurate_passes_home,
    accurate_passes_away
)
SELECT
    m.match_id,
    toString(m.match_date) AS match_time_utc_date,
    m.home_team_id,
    m.home_team_name,
    m.away_team_id,
    m.away_team_name,
    m.home_score,
    m.away_score,
    multiIf(
        coalesce(ps.accurate_passes_home, 0) > 700 AND coalesce(ps.accurate_passes_away, 0) > 700, 'both',
        coalesce(ps.accurate_passes_home, 0) > 700, 'home',
        'away'
    ) AS triggered_side,
    multiIf(
        coalesce(ps.accurate_passes_home, 0) > 700 AND coalesce(ps.accurate_passes_away, 0) > 700,
        CAST(NULL, 'Nullable(Int32)'),
        coalesce(ps.accurate_passes_home, 0) > 700,
        m.home_team_id,
        m.away_team_id
    ) AS triggered_team_id,
    multiIf(
        coalesce(ps.accurate_passes_home, 0) > 700 AND coalesce(ps.accurate_passes_away, 0) > 700,
        CAST(NULL, 'Nullable(String)'),
        coalesce(ps.accurate_passes_home, 0) > 700,
        m.home_team_name,
        m.away_team_name
    ) AS triggered_team_name,
    coalesce(ps.accurate_passes_home, 0) AS accurate_passes_home,
    coalesce(ps.accurate_passes_away, 0) AS accurate_passes_away
FROM silver.match AS m
INNER JOIN silver.period_stat AS ps
    ON m.match_id = ps.match_id
WHERE
    m.match_finished = 1
    AND ps.period = 'All'
    AND (
        coalesce(ps.accurate_passes_home, 0) > 700
        OR coalesce(ps.accurate_passes_away, 0) > 700
    )
ORDER BY greatest(
    coalesce(ps.accurate_passes_home, 0),
    coalesce(ps.accurate_passes_away, 0)
) DESC;
