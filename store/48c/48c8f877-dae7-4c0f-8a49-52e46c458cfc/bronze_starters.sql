ATTACH TABLE _ UUID '6192a9ad-9bdd-4bd6-9a5f-e0614f2fbd35'
(
    `match_id` Int32,
    `team_side` String,
    `player_id` Int64,
    `age` Nullable(Int32),
    `name` Nullable(String),
    `first_name` Nullable(String),
    `last_name` Nullable(String),
    `position_id` Nullable(Int32),
    `usual_playing_position_id` Nullable(Int32),
    `shirt_number` Nullable(String),
    `is_captain` Nullable(UInt8),
    `country_name` Nullable(String),
    `country_code` Nullable(String),
    `horizontal_x` Nullable(Float32),
    `horizontal_y` Nullable(Float32),
    `horizontal_height` Nullable(Float32),
    `horizontal_width` Nullable(Float32),
    `vertical_x` Nullable(Float32),
    `vertical_y` Nullable(Float32),
    `vertical_height` Nullable(Float32),
    `vertical_width` Nullable(Float32),
    `performance_rating` Nullable(Float32),
    `substitution_time` Nullable(Int32),
    `substitution_type` Nullable(String),
    `substitution_reason` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, player_id)
SETTINGS index_granularity = 8192
