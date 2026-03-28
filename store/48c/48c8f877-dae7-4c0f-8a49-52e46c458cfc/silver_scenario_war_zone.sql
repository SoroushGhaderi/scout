ATTACH TABLE _ UUID '9c311cff-3859-44cb-bd62-edce66e7c938'
(
    `match_id` Int32,
    `home_team_id` Nullable(Int32),
    `away_team_id` Nullable(Int32),
    `home_team_name` Nullable(String),
    `away_team_name` Nullable(String),
    `home_score` Nullable(Int32),
    `away_score` Nullable(Int32),
    `fouls_home` Nullable(Int32),
    `fouls_away` Nullable(Int32),
    `combined_fouls` Nullable(Int32),
    `yellow_cards_home` Nullable(Int32),
    `yellow_cards_away` Nullable(Int32),
    `combined_yellow_cards` Nullable(Int32),
    `red_cards_home` Nullable(Int32),
    `red_cards_away` Nullable(Int32),
    `combined_red_cards` Nullable(Int32),
    `match_result` LowCardinality(String),
    `match_time_utc_date` Nullable(String),
    `inserted_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(assumeNotNull(toDateOrZero(match_time_utc_date)))
ORDER BY (match_id, match_result)
SETTINGS index_granularity = 8192
