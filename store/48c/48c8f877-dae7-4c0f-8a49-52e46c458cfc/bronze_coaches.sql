ATTACH TABLE _ UUID 'e6ced2c8-ccf7-4380-ab3b-c941d3f796fb'
(
    `match_id` Int32,
    `team_side` String,
    `id` Int64,
    `age` Nullable(Int32),
    `name` Nullable(String),
    `first_name` Nullable(String),
    `last_name` Nullable(String),
    `country_name` Nullable(String),
    `country_code` Nullable(String),
    `primary_team_id` Nullable(Int32),
    `primary_team_name` Nullable(String),
    `is_coach` Nullable(UInt8),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (match_id, id)
SETTINGS index_granularity = 8192
