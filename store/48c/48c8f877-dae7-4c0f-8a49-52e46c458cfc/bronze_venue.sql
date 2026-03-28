ATTACH TABLE _ UUID '0558b4ad-9516-4b7e-9bf3-ceab843c8e32'
(
    `match_id` Int32,
    `stadium_name` Nullable(String),
    `stadium_city` Nullable(String),
    `stadium_country` Nullable(String),
    `stadium_latitude` Nullable(Float64),
    `stadium_longitude` Nullable(Float64),
    `stadium_capacity` Nullable(Int32),
    `stadium_surface` Nullable(String),
    `attendance` Nullable(Int32),
    `referee_name` Nullable(String),
    `referee_country` Nullable(String),
    `referee_image_url` Nullable(String),
    `match_date_utc` Nullable(String),
    `match_date_verified` Nullable(UInt8),
    `tournament_id` Nullable(Int32),
    `tournament_name` Nullable(String),
    `tournament_round` Nullable(String),
    `tournament_parent_league_id` Nullable(Int32),
    `tournament_link` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_date_utc)))
ORDER BY match_id
SETTINGS index_granularity = 8192
