-- signal_team_possession_passing_possession_without_purpose
-- Triggers when a team dominates possession (>65%) but fails to convert it into meaningful attacks (<2 shots on target) by full time.
INSERT INTO gold.signal_team_possession_passing_possession_without_purpose
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
    possession_pct,
    shots_on_target
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
        assumeNotNull(ps.ball_possession_home) > 65 AND coalesce(ps.shots_on_target_home, 0) < 2, 'home',
        'away'
    ) AS triggered_side,
    multiIf(
        assumeNotNull(ps.ball_possession_home) > 65 AND coalesce(ps.shots_on_target_home, 0) < 2, m.home_team_id,
        m.away_team_id
    ) AS triggered_team_id,
    multiIf(
        assumeNotNull(ps.ball_possession_home) > 65 AND coalesce(ps.shots_on_target_home, 0) < 2, m.home_team_name,
        m.away_team_name
    ) AS triggered_team_name,
    multiIf(
        assumeNotNull(ps.ball_possession_home) > 65 AND coalesce(ps.shots_on_target_home, 0) < 2, assumeNotNull(ps.ball_possession_home),
        assumeNotNull(ps.ball_possession_away)
    ) AS possession_pct,
    multiIf(
        assumeNotNull(ps.ball_possession_home) > 65 AND coalesce(ps.shots_on_target_home, 0) < 2, coalesce(ps.shots_on_target_home, 0),
        coalesce(ps.shots_on_target_away, 0)
    ) AS shots_on_target
FROM silver.match AS m
INNER JOIN silver.period_stat AS ps
    ON m.match_id = ps.match_id
WHERE
    m.match_finished = 1
    AND ps.period = 'All'
    AND (
        (assumeNotNull(ps.ball_possession_home) > 65 AND coalesce(ps.shots_on_target_home, 0) < 2)
        OR (assumeNotNull(ps.ball_possession_away) > 65 AND coalesce(ps.shots_on_target_away, 0) < 2)
    )
ORDER BY possession_pct DESC;
