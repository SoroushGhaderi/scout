INSERT INTO silver.dim_player
SELECT
    p.player_id AS player_id,
    ifNull(p.player_name, '') AS player_name,
    p.opta_id,
    argMax(s.country_name, s.inserted_at) AS country_name,
    argMax(s.country_code, s.inserted_at) AS country_code,
    argMax(s.usual_playing_position_id, s.inserted_at) AS usual_playing_position_id,
    now() AS _loaded_at
FROM bronze.player AS p FINAL
LEFT JOIN bronze.starters AS s FINAL ON toInt32(s.player_id) = p.player_id
GROUP BY p.player_id, p.player_name, p.opta_id;
