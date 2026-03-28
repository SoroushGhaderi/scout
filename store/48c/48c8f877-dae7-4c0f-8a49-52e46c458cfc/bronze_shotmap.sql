ATTACH TABLE _ UUID 'e3b46d5a-45a1-4d97-af0a-555c3932f128'
(
    `match_id` Int32,
    `id` Nullable(Int64),
    `event_type` Nullable(String),
    `team_id` Nullable(Int32),
    `player_id` Nullable(Int32),
    `player_name` Nullable(String),
    `x` Nullable(Float32),
    `y` Nullable(Float32),
    `min` Nullable(Int32),
    `min_added` Nullable(Int32),
    `is_blocked` Nullable(UInt8),
    `is_on_target` Nullable(UInt8),
    `blocked_x` Nullable(Float32),
    `blocked_y` Nullable(Float32),
    `goal_crossed_y` Nullable(Float32),
    `goal_crossed_z` Nullable(Float32),
    `expected_goals` Nullable(Float32),
    `expected_goals_on_target` Nullable(Float32),
    `shot_type` Nullable(String),
    `situation` Nullable(String),
    `period` Nullable(String),
    `is_own_goal` Nullable(UInt8),
    `on_goal_shot_x` Nullable(Float32),
    `on_goal_shot_y` Nullable(Float32),
    `on_goal_shot_zoom_ratio` Nullable(Float32),
    `is_saved_off_line` Nullable(UInt8),
    `is_from_inside_box` Nullable(UInt8),
    `keeper_id` Nullable(Int32),
    `first_name` Nullable(String),
    `last_name` Nullable(String),
    `full_name` Nullable(String),
    `team_color` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, assumeNotNull(id))
SETTINGS index_granularity = 8192
