INSERT INTO silver.card
SELECT
    c.match_id,
    ifNull(toDate(parseDateTimeBestEffortOrNull(g.match_time_utc_date)), ifNull(toDate(parseDateTimeBestEffortOrNull(g.match_time_utc)), toDate('1970-01-01'))) AS match_date,
    c.event_id,
    c.event_time AS card_minute,
    c.added_time,
    c.player_id, c.player_name,
    c.team AS team_side,
    c.card_type, c.description,
    c.score_at_event AS score_at_time,
    now() AS _loaded_at
FROM bronze.cards AS c FINAL
LEFT JOIN bronze.general AS g FINAL ON c.match_id = g.match_id;
