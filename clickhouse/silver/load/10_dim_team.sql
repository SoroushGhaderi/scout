INSERT INTO silver.dim_team
SELECT DISTINCT
    home_team_id AS team_id,
    ifNull(home_team_name, '') AS team_name,
    now() AS _loaded_at
FROM bronze.general FINAL
WHERE home_team_id IS NOT NULL;

INSERT INTO silver.dim_team
SELECT DISTINCT
    away_team_id AS team_id,
    ifNull(away_team_name, '') AS team_name,
    now() AS _loaded_at
FROM bronze.general FINAL
WHERE away_team_id IS NOT NULL;
