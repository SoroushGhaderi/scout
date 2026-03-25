-- Silver views in single fotmob database (schema-style via silver_ prefix)

CREATE OR REPLACE VIEW fotmob.silver_general AS
SELECT
    match_id,
    league_id,
    league_name,
    home_team_id,
    away_team_id,
    home_team_name,
    away_team_name,
    match_round,
    match_time_utc,
    match_time_utc_date,
    match_started,
    match_finished,
    full_score,
    inserted_at
FROM fotmob.bronze_general
WHERE match_id IS NOT NULL;

CREATE OR REPLACE VIEW fotmob.silver_player AS
SELECT
    match_id,
    player_id,
    player_name,
    team_id,
    team_name,
    coalesce(goals, 0) AS goals,
    coalesce(assists, 0) AS assists,
    toFloat32(coalesce(fotmob_rating, 0.0)) AS rating,
    coalesce(minutes_played, 0) AS minutes_played,
    coalesce(expected_goals, 0.0) AS expected_goals,
    coalesce(expected_assists, 0.0) AS expected_assists,
    inserted_at
FROM fotmob.bronze_player
WHERE player_id IS NOT NULL
  AND match_id IS NOT NULL;

CREATE OR REPLACE VIEW fotmob.silver_shotmap AS
SELECT
    match_id,
    id,
    team_id,
    player_id,
    player_name,
    coalesce(event_type, 'unknown') AS event_type,
    coalesce(expected_goals, 0.0) AS expected_goals,
    coalesce(expected_goals_on_target, 0.0) AS expected_goals_on_target,
    coalesce(is_on_target, 0) AS is_on_target,
    min,
    inserted_at
FROM fotmob.bronze_shotmap
WHERE match_id IS NOT NULL;

CREATE OR REPLACE VIEW fotmob.silver_period AS
SELECT
    match_id,
    period,
    coalesce(ball_possession_home, 0) AS ball_possession_home,
    coalesce(ball_possession_away, 0) AS ball_possession_away,
    coalesce(expected_goals_home, 0.0) AS expected_goals_home,
    coalesce(expected_goals_away, 0.0) AS expected_goals_away,
    inserted_at
FROM fotmob.bronze_period
WHERE match_id IS NOT NULL;

CREATE OR REPLACE VIEW fotmob.silver_venue AS
SELECT
    match_id,
    stadium_name,
    stadium_city,
    stadium_country,
    attendance,
    referee_name,
    match_date_utc,
    tournament_id,
    tournament_name,
    inserted_at
FROM fotmob.bronze_venue
WHERE match_id IS NOT NULL;
