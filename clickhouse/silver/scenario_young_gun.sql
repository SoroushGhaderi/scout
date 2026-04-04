-- scenario_young_gun: high-impact young starters with above-average ratings
INSERT INTO silver.scenario_young_gun
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
    age,
    team_side,
    is_captain,
    goals,
    assists,
    goal_contributions,
    xg,
    xa,
    rating,
    avg_rating,
    rating_above_avg,
    minutes_played,
    winning_team,
    match_result,
    match_time_utc_date
)
WITH avg_rating AS (
    SELECT avg(fotmob_rating) AS overall_avg_rating
    FROM bronze.player
    WHERE fotmob_rating IS NOT NULL
)
SELECT
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    st.player_id,
    st.name AS player_name,
    st.age,
    st.team_side,
    st.is_captain,
    p.goals,
    p.assists,
    p.goals + p.assists AS goal_contributions,
    round(p.expected_goals, 3) AS xg,
    round(p.expected_assists, 3) AS xa,
    round(p.fotmob_rating, 2) AS rating,
    round(ar.overall_avg_rating, 2) AS avg_rating,
    round(p.fotmob_rating - ar.overall_avg_rating, 2) AS rating_above_avg,
    p.minutes_played,
    CASE
        WHEN g.home_score > g.away_score THEN g.home_team_name
        WHEN g.away_score > g.home_score THEN g.away_team_name
        ELSE 'Draw'
    END AS winning_team,
    CASE
        WHEN g.home_score > g.away_score THEN 'Home Win'
        WHEN g.away_score > g.home_score THEN 'Away Win'
        ELSE 'Draw'
    END AS match_result,
    g.match_time_utc_date
FROM bronze.starters AS st
INNER JOIN bronze.general AS g
    ON st.match_id = g.match_id
INNER JOIN bronze.player AS p
    ON st.match_id = p.match_id
    AND st.player_id = p.player_id
CROSS JOIN avg_rating AS ar
WHERE
    g.match_finished = 1
    AND st.age <= 21
    AND p.fotmob_rating > ar.overall_avg_rating
    AND (p.goals >= 1 OR p.assists >= 1)
    AND p.minutes_played > 0
ORDER BY rating_above_avg DESC, st.age ASC;
