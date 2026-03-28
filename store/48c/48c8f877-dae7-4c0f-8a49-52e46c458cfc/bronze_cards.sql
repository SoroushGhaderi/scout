ATTACH TABLE _ UUID '84de75f7-2dd9-4eec-be6e-0072873f3d46'
(
    `match_id` Int32,
    `event_id` Int64,
    `time` Int32,
    `added_time` Nullable(Int32),
    `player_id` Nullable(Int32),
    `player_name` Nullable(String),
    `player_profile_url` Nullable(String),
    `team` String,
    `card_type` String,
    `description` Nullable(String),
    `score` String,
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, event_id)
SETTINGS index_granularity = 8192
