-- Create tables for FotMob data warehouse
-- This script creates all 14 tables for FotMob match data
-- Run this AFTER creating the databases (00_create_databases_fotmob_and_aiscore.sql)

USE fotmob;

-- 1. General Match Statistics
CREATE TABLE IF NOT EXISTS general (
    match_id Int32,
    match_round Nullable(String),
    team_color_dark_mode_home Nullable(String),
    team_color_dark_mode_away Nullable(String),
    team_color_light_mode_home Nullable(String),
    team_color_light_mode_away Nullable(String),
    league_id Nullable(Int32),
    league_name Nullable(String),
    league_round_name Nullable(String),
    parent_league_id Nullable(Int32),
    country_code Nullable(String),
    parent_league_name Nullable(String),
    parent_league_season Nullable(String),
    parent_league_tournament_id Nullable(Int32),
    home_team_name Nullable(String),
    home_team_id Nullable(Int32),
    away_team_name Nullable(String),
    away_team_id Nullable(Int32),
    coverage_level Nullable(String),
    match_time_utc Nullable(String),
    match_time_utc_date Nullable(String),
    match_started UInt8,
    match_finished UInt8,
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)));

-- 2. Match Timeline
CREATE TABLE IF NOT EXISTS timeline (
    match_id Int32,
    match_time_utc Nullable(DateTime),
    first_half_started Nullable(String),
    first_half_ended Nullable(String),
    second_half_started Nullable(String),
    second_half_ended Nullable(String),
    first_extra_half_started Nullable(String),
    second_extra_half_started Nullable(String),
    game_ended Nullable(String),
    game_finished UInt8,
    game_started UInt8,
    game_cancelled UInt8,
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id)
PARTITION BY toYYYYMM(toDate(assumeNotNull(match_time_utc)));

-- 3. Match Venue Information
CREATE TABLE IF NOT EXISTS venue (
    match_id Int32,
    stadium_name Nullable(String),
    stadium_city Nullable(String),
    stadium_country Nullable(String),
    stadium_latitude Nullable(Float64),
    stadium_longitude Nullable(Float64),
    stadium_capacity Nullable(Int32),
    stadium_surface Nullable(String),
    attendance Nullable(Int32),
    referee_name Nullable(String),
    referee_country Nullable(String),
    referee_image_url Nullable(String),
    match_date_utc Nullable(String),
    match_date_verified Nullable(UInt8),
    tournament_id Nullable(Int32),
    tournament_name Nullable(String),
    tournament_round Nullable(String),
    tournament_parent_league_id Nullable(Int32),
    tournament_link Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_date_utc)));

-- 4. Player Statistics
CREATE TABLE IF NOT EXISTS player (
    match_id Int32,
    player_id Nullable(Int32),
    player_name Nullable(String),
    opta_id Nullable(String),
    team_id Nullable(Int32),
    team_name Nullable(String),
    is_goalkeeper Nullable(UInt8),
    fotmob_rating Nullable(Float32),
    minutes_played Nullable(Int32),
    goals Nullable(Int32),
    assists Nullable(Int32),
    total_shots Nullable(Int32),
    shots_on_target Nullable(Int32),
    shots_off_target Nullable(Int32),
    blocked_shots Nullable(Int32),
    expected_goals Nullable(Float32),
    expected_assists Nullable(Float32),
    xg_plus_xa Nullable(Float32),
    xg_non_penalty Nullable(Float32),
    chances_created Nullable(Int32),
    touches Nullable(Int32),
    touches_opp_box Nullable(Int32),
    successful_dribbles Nullable(Int32),
    dribble_attempts Nullable(Int32),
    dribble_success_rate Nullable(Float32),
    accurate_passes Nullable(Int32),
    total_passes Nullable(Int32),
    pass_accuracy Nullable(Float32),
    passes_final_third Nullable(Int32),
    accurate_crosses Nullable(Int32),
    cross_attempts Nullable(Int32),
    cross_success_rate Nullable(Float32),
    accurate_long_balls Nullable(Int32),
    long_ball_attempts Nullable(Int32),
    long_ball_success_rate Nullable(Float32),
    tackles_won Nullable(Int32),
    tackle_attempts Nullable(Int32),
    tackle_success_rate Nullable(Float32),
    interceptions Nullable(Int32),
    clearances Nullable(Int32),
    defensive_actions Nullable(Int32),
    recoveries Nullable(Int32),
    dribbled_past Nullable(Int32),
    duels_won Nullable(Int32),
    duels_lost Nullable(Int32),
    ground_duels_won Nullable(Int32),
    ground_duel_attempts Nullable(Int32),
    ground_duel_success_rate Nullable(Float32),
    aerial_duels_won Nullable(Int32),
    aerial_duel_attempts Nullable(Int32),
    aerial_duel_success_rate Nullable(Float32),
    fouls_committed Nullable(Int32),
    was_fouled Nullable(Int32),
    shotmap_count Nullable(Int32),
    average_xg_per_shot Nullable(Float32),
    total_xg Nullable(Float32),
    fun_facts Array(Nullable(String)),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, assumeNotNull(player_id));

-- 5. Shot Map Events
CREATE TABLE IF NOT EXISTS shotmap (
    match_id Int32,
    id Nullable(Int64),
    event_type Nullable(String),
    team_id Nullable(Int32),
    player_id Nullable(Int32),
    player_name Nullable(String),
    x Nullable(Float32),
    y Nullable(Float32),
    min Nullable(Int32),
    min_added Nullable(Int32),
    is_blocked Nullable(UInt8),
    is_on_target Nullable(UInt8),
    blocked_x Nullable(Float32),
    blocked_y Nullable(Float32),
    goal_crossed_y Nullable(Float32),
    goal_crossed_z Nullable(Float32),
    expected_goals Nullable(Float32),
    expected_goals_on_target Nullable(Float32),
    shot_type Nullable(String),
    situation Nullable(String),
    period Nullable(String),
    is_own_goal Nullable(UInt8),
    on_goal_shot_x Nullable(Float32),
    on_goal_shot_y Nullable(Float32),
    on_goal_shot_zoom_ratio Nullable(Float32),
    is_saved_off_line Nullable(UInt8),
    is_from_inside_box Nullable(UInt8),
    keeper_id Nullable(Int32),
    first_name Nullable(String),
    last_name Nullable(String),
    full_name Nullable(String),
    team_color Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, assumeNotNull(id));

-- 6. Goal Events
CREATE TABLE IF NOT EXISTS goal (
    match_id Int32,
    event_id Int64,
    goal_time Int32,
    goal_overload_time Nullable(Int32),
    home_score Int32,
    away_score Int32,
    is_home UInt8,
    is_own_goal Nullable(UInt8),
    goal_description Nullable(String),
    assist_player_id Nullable(Int32),
    assist_player_name Nullable(String),
    player_id Nullable(Int32),
    player_name Nullable(String),
    shot_event_id Nullable(Int64),
    shot_x_loc Nullable(Float32),
    shot_y_loc Nullable(Float32),
    shot_minute Nullable(Int32),
    shot_minute_added Nullable(Int32),
    shot_expected_goal Nullable(Float32),
    shot_expected_goal_on_target Nullable(Float32),
    shot_type Nullable(String),
    shot_situation Nullable(String),
    shot_period Nullable(String),
    shot_from_inside_box Nullable(UInt8),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, event_id);

-- 7. Card Events
CREATE TABLE IF NOT EXISTS cards (
    match_id Int32,
    event_id Int64,
    time Int32,
    added_time Nullable(Int32),
    player_id Nullable(Int32),
    player_name Nullable(String),
    player_profile_url Nullable(String),
    team String,
    card_type String,
    description Nullable(String),
    score String,
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, event_id);

-- 8. Red Card Events
CREATE TABLE IF NOT EXISTS red_card (
    match_id Int32,
    event_id Int64,
    red_card_time Int32,
    red_card_overload_time Nullable(Int32),
    player_id Nullable(Int32),
    player_name Nullable(String),
    home_score Int32,
    away_score Int32,
    is_home UInt8,
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, event_id);

-- 9. Period Statistics
CREATE TABLE IF NOT EXISTS period (
    match_id Int32,
    period String,
    ball_possession_home Nullable(Int32),
    ball_possession_away Nullable(Int32),
    expected_goals_home Nullable(Float32),
    expected_goals_away Nullable(Float32),
    expected_goals_open_play_home Nullable(Float32),
    expected_goals_open_play_away Nullable(Float32),
    expected_goals_set_play_home Nullable(Float32),
    expected_goals_set_play_away Nullable(Float32),
    expected_goals_non_penalty_home Nullable(Float32),
    expected_goals_non_penalty_away Nullable(Float32),
    expected_goals_on_target_home Nullable(Float32),
    expected_goals_on_target_away Nullable(Float32),
    distance_covered_home Nullable(Float32),
    distance_covered_away Nullable(Float32),
    walking_distance_home Nullable(Float32),
    walking_distance_away Nullable(Float32),
    running_distance_home Nullable(Float32),
    running_distance_away Nullable(Float32),
    sprinting_distance_home Nullable(Float32),
    sprinting_distance_away Nullable(Float32),
    number_of_sprints_home Nullable(Int32),
    number_of_sprints_away Nullable(Int32),
    top_speed_home Nullable(Float32),
    top_speed_away Nullable(Float32),
    total_shots_home Nullable(Int32),
    total_shots_away Nullable(Int32),
    shots_on_target_home Nullable(Int32),
    shots_on_target_away Nullable(Int32),
    shots_off_target_home Nullable(Int32),
    shots_off_target_away Nullable(Int32),
    blocked_shots_home Nullable(Int32),
    blocked_shots_away Nullable(Int32),
    shots_woodwork_home Nullable(Int32),
    shots_woodwork_away Nullable(Int32),
    shots_inside_box_home Nullable(Int32),
    shots_inside_box_away Nullable(Int32),
    shots_outside_box_home Nullable(Int32),
    shots_outside_box_away Nullable(Int32),
    big_chances_home Nullable(Int32),
    big_chances_away Nullable(Int32),
    big_chances_missed_home Nullable(Int32),
    big_chances_missed_away Nullable(Int32),
    fouls_home Nullable(Int32),
    fouls_away Nullable(Int32),
    corners_home Nullable(Int32),
    corners_away Nullable(Int32),
    shots_home Nullable(Int32),
    shots_away Nullable(Int32),
    passes_home Nullable(Int32),
    passes_away Nullable(Int32),
    accurate_passes_home Nullable(String),
    accurate_passes_away Nullable(String),
    own_half_passes_home Nullable(Int32),
    own_half_passes_away Nullable(Int32),
    opposition_half_passes_home Nullable(Int32),
    opposition_half_passes_away Nullable(Int32),
    long_balls_accurate_home Nullable(String),
    long_balls_accurate_away Nullable(String),
    accurate_crosses_home Nullable(String),
    accurate_crosses_away Nullable(String),
    player_throws_home Nullable(Int32),
    player_throws_away Nullable(Int32),
    touches_opp_box_home Nullable(Int32),
    touches_opp_box_away Nullable(Int32),
    tackles_succeeded_home Nullable(String),
    tackles_succeeded_away Nullable(String),
    interceptions_home Nullable(Int32),
    interceptions_away Nullable(Int32),
    shot_blocks_home Nullable(Int32),
    shot_blocks_away Nullable(Int32),
    clearances_home Nullable(Int32),
    clearances_away Nullable(Int32),
    keeper_saves_home Nullable(Int32),
    keeper_saves_away Nullable(Int32),
    duels_won_home Nullable(Int32),
    duels_won_away Nullable(Int32),
    ground_duels_won_home Nullable(String),
    ground_duels_won_away Nullable(String),
    aerials_won_home Nullable(String),
    aerials_won_away Nullable(String),
    dribbles_succeeded_home Nullable(String),
    dribbles_succeeded_away Nullable(String),
    yellow_cards_home Nullable(Int32),
    yellow_cards_away Nullable(Int32),
    red_cards_home Nullable(Int32),
    red_cards_away Nullable(Int32),
    offsides_home Nullable(Int32),
    offsides_away Nullable(Int32),
    home_color Nullable(String),
    away_color Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, period);

-- 10. Momentum Data
CREATE TABLE IF NOT EXISTS momentum (
    match_id Int32,
    minute Nullable(Float32),
    value Nullable(Int32),
    momentum_team Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, assumeNotNull(minute));

-- 11. Starting Lineup
CREATE TABLE IF NOT EXISTS starters (
    match_id Int32,
    team_side String,
    player_id Int64,
    age Nullable(Int32),
    name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    position_id Nullable(Int32),
    usual_playing_position_id Nullable(Int32),
    shirt_number Nullable(String),
    is_captain Nullable(UInt8),
    country_name Nullable(String),
    country_code Nullable(String),
    horizontal_x Nullable(Float32),
    horizontal_y Nullable(Float32),
    horizontal_height Nullable(Float32),
    horizontal_width Nullable(Float32),
    vertical_x Nullable(Float32),
    vertical_y Nullable(Float32),
    vertical_height Nullable(Float32),
    vertical_width Nullable(Float32),
    performance_rating Nullable(Float32),
    substitution_time Nullable(Int32),
    substitution_type Nullable(String),
    substitution_reason Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, player_id);

-- 12. Substitute Players
CREATE TABLE IF NOT EXISTS substitutes (
    match_id Int32,
    team_side String,
    player_id Int64,
    age Nullable(Int32),
    name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    usual_playing_position_id Nullable(Int32),
    shirt_number Nullable(String),
    country_name Nullable(String),
    country_code Nullable(String),
    performance_rating Nullable(Float32),
    substitution_time Nullable(Int32),
    substitution_type Nullable(String),
    substitution_reason Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, player_id);

-- 13. Team Coaches
CREATE TABLE IF NOT EXISTS coaches (
    match_id Int32,
    team_side String,
    id Int64,
    age Nullable(Int32),
    name Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    country_name Nullable(String),
    country_code Nullable(String),
    primary_team_id Nullable(Int32),
    primary_team_name Nullable(String),
    is_coach Nullable(UInt8)
) ENGINE = MergeTree()
ORDER BY (match_id, id);

-- 14. Team Form
CREATE TABLE IF NOT EXISTS team_form (
    match_id Int32,
    team_side String,
    team_id Int64,
    team_name Nullable(String),
    form_position Int32,
    result Int32,
    result_string String,
    score Nullable(String),
    form_match_date Nullable(String),
    form_match_id Nullable(String),
    form_match_link Nullable(String),
    opponent_id Nullable(Int32),
    opponent_name Nullable(String),
    opponent_image_url Nullable(String),
    is_home_match Nullable(UInt8),
    home_team_id Nullable(Int32),
    home_team_name Nullable(String),
    home_score Nullable(String),
    away_team_id Nullable(Int32),
    away_team_name Nullable(String),
    away_score Nullable(String),
    inserted_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (match_id, team_id, form_position);

