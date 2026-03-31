-- scenario_ten_men_stand: teams that avoid defeat after going down to ten men
INSERT INTO fotmob.silver_scenario_ten_men_stand
(
    match_id,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    home_score,
    away_score,
    goal_diff,
    home_red_cards,
    away_red_cards,
    home_first_red_minute,
    away_first_red_minute,
    home_score_at_red,
    away_score_at_red_home_event,
    home_score_at_red_away_event,
    away_score_at_red,
    red_card_side,
    match_result,
    resilient_team,
    heroic_result,
    winning_side,
    match_time_utc_date
)
WITH first_red AS (
    SELECT
        match_id,
        is_home,
        home_score AS score_home_at_red,
        away_score AS score_away_at_red,
        red_card_time,
        player_id,
        player_name,
        row_number() OVER (
            PARTITION BY match_id, is_home
            ORDER BY red_card_time ASC
        ) AS rn
    FROM fotmob.bronze_red_card
)
SELECT
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    abs(g.home_score - g.away_score) AS goal_diff,
    countIf(rc.is_home = 1) AS home_red_cards,
    countIf(rc.is_home = 0) AS away_red_cards,
    min(if(rc.is_home = 1, rc.red_card_time, NULL)) AS home_first_red_minute,
    min(if(rc.is_home = 0, rc.red_card_time, NULL)) AS away_first_red_minute,
    anyIf(fr.score_home_at_red, rc.is_home = 1) AS home_score_at_red,
    anyIf(fr.score_away_at_red, rc.is_home = 1) AS away_score_at_red_home_event,
    anyIf(fr.score_home_at_red, rc.is_home = 0) AS home_score_at_red_away_event,
    anyIf(fr.score_away_at_red, rc.is_home = 0) AS away_score_at_red,
    CASE
        WHEN countIf(rc.is_home = 1) > 0
         AND countIf(rc.is_home = 0) = 0 THEN 'home'
        WHEN countIf(rc.is_home = 0) > 0
         AND countIf(rc.is_home = 1) = 0 THEN 'away'
        ELSE 'both'
    END AS red_card_side,
    CASE
        WHEN g.home_score > g.away_score THEN 'Home Win'
        WHEN g.away_score > g.home_score THEN 'Away Win'
        ELSE 'Draw'
    END AS match_result,
    CASE
        WHEN countIf(rc.is_home = 1) > 0
         AND countIf(rc.is_home = 0) = 0
         AND g.home_score >= g.away_score THEN g.home_team_name
        WHEN countIf(rc.is_home = 0) > 0
         AND countIf(rc.is_home = 1) = 0
         AND g.away_score >= g.home_score THEN g.away_team_name
    END AS resilient_team,
    CASE
        WHEN g.home_score > g.away_score
         AND countIf(rc.is_home = 1) > 0
         AND countIf(rc.is_home = 0) = 0 THEN 'win'
        WHEN g.away_score > g.home_score
         AND countIf(rc.is_home = 0) > 0
         AND countIf(rc.is_home = 1) = 0 THEN 'win'
        WHEN g.home_score = g.away_score THEN 'draw'
    END AS heroic_result,
    CASE
        WHEN g.home_score > g.away_score THEN 'home'
        WHEN g.away_score > g.home_score THEN 'away'
        ELSE 'draw'
    END AS winning_side,
    g.match_time_utc_date
FROM fotmob.bronze_general AS g
INNER JOIN fotmob.bronze_red_card AS rc
    ON g.match_id = rc.match_id
INNER JOIN first_red AS fr
    ON rc.match_id = fr.match_id
    AND rc.is_home = fr.is_home
    AND fr.rn = 1
WHERE
    g.match_finished = 1
    AND (
        (rc.is_home = 1 AND g.home_score >= g.away_score)
        OR
        (rc.is_home = 0 AND g.away_score >= g.home_score)
    )
GROUP BY
    g.match_id,
    g.home_team_id,
    g.away_team_id,
    g.home_team_name,
    g.away_team_name,
    g.home_score,
    g.away_score,
    g.match_time_utc_date
HAVING
    resilient_team IS NOT NULL
ORDER BY home_red_cards + away_red_cards DESC, goal_diff DESC;
