ATTACH TABLE _ UUID '56a9405f-1f1b-4dcb-ac5a-914ace1eeb46'
(
    `match_id` Int32,
    `team_side` String,
    `player_id` Int64,
    `age` Nullable(Int32),
    `name` Nullable(String),
    `first_name` Nullable(String),
    `last_name` Nullable(String),
    `usual_playing_position_id` Nullable(Int32),
    `shirt_number` Nullable(String),
    `country_name` Nullable(String),
    `country_code` Nullable(String),
    `performance_rating` Nullable(Float32),
    `substitution_time` Nullable(Int32),
    `substitution_type` Nullable(String),
    `substitution_reason` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, player_id)
SETTINGS index_granularity = 8192
